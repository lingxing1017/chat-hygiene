mod common;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chathygiene::processing::{FatalRuntimeEvent, FatalRuntimeNotifier, ProcessingEngine};
use chathygiene::storage::{
    OwnerIdentity, UnitOfWork, claim_owner, connect, initialize_or_load_owner_identity,
    load_owner_identity, migrate,
};
use chathygiene::telegram::{
    AuthoritativeBusinessConnection, BoxFuture, BusinessConnectionApi, BusinessRights,
    RawBusinessEvent, RawEventKind, TelegramError,
};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

#[derive(Clone)]
struct QueueBusinessApi {
    outcomes: Arc<Mutex<VecDeque<Result<AuthoritativeBusinessConnection, TelegramError>>>>,
    pool: SqlitePool,
    calls: Arc<AtomicUsize>,
}

impl QueueBusinessApi {
    fn new(
        pool: SqlitePool,
        outcomes: Vec<Result<AuthoritativeBusinessConnection, TelegramError>>,
    ) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            pool,
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl BusinessConnectionApi for QueueBusinessApi {
    fn get_business_connection<'a>(
        &'a self,
        _connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let connection = tokio::time::timeout(Duration::from_millis(100), self.pool.acquire())
                .await
                .map_err(|_| {
                    TelegramError::Protocol(
                        "authoritative lookup was called with a transaction open".to_owned(),
                    )
                })?
                .map_err(|_| TelegramError::Transport)?;
            drop(connection);
            self.outcomes.lock().unwrap().pop_front().unwrap()
        })
    }
}

#[derive(Clone)]
struct RevisionBumpingNotFoundApi {
    pool: SqlitePool,
}

impl BusinessConnectionApi for RevisionBumpingNotFoundApi {
    fn get_business_connection<'a>(
        &'a self,
        connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async move {
            sqlx::query(
                "UPDATE business_connection
                 SET state_revision = state_revision + 1
                 WHERE connection_id = ?",
            )
            .bind(connection_id)
            .execute(&self.pool)
            .await
            .map_err(|_| TelegramError::Transport)?;
            Err(TelegramError::Api {
                error_code: 400,
                description: "Bad Request: business connection not found".to_owned(),
                retry_after: None,
            })
        })
    }
}

#[derive(Clone, Default)]
struct CountingFatalNotifier {
    calls: Arc<AtomicUsize>,
}

impl CountingFatalNotifier {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl FatalRuntimeNotifier for CountingFatalNotifier {
    fn notify(&self, event: FatalRuntimeEvent) {
        assert_eq!(event, FatalRuntimeEvent::TelegramAuthentication);
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

fn authoritative(
    connection_id: &str,
    user_id: i64,
    established_at: i64,
    enabled: bool,
    full_rights: bool,
) -> AuthoritativeBusinessConnection {
    AuthoritativeBusinessConnection {
        connection_id: connection_id.to_owned(),
        business_user_id: user_id,
        user_chat_id: Some(user_id * 100),
        connection_established_at: established_at,
        rights: BusinessRights {
            can_reply: full_rights,
            can_read_messages: full_rights,
            can_delete_sent_messages: full_rights,
            can_delete_all_messages: full_rights,
        },
        enabled,
    }
}

fn trigger(connection_id: &str, now: DateTime<Utc>) -> RawBusinessEvent {
    RawBusinessEvent {
        kind: RawEventKind::BusinessConnectionChanged,
        connection_id: Some(connection_id.to_owned()),
        chat_id: None,
        message_id: None,
        media_group_id: None,
        content: None,
        deleted_message_ids: Vec::new(),
        connection: None,
        owner_claim: None,
        owner_command: None,
        contact_display_name: None,
        contact_username: None,
        occurred_at: now,
    }
}

async fn database() -> (tempfile::TempDir, SqlitePool, DateTime<Utc>) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    let now = common::at("2026-07-14T00:00:00Z");
    initialize_or_load_owner_identity(&pool, now).await.unwrap();
    (directory, pool, now)
}

fn engine(
    pool: SqlitePool,
    clock: common::TestClock,
    api: QueueBusinessApi,
) -> ProcessingEngine<common::MutableDetector, common::FixedVerifier, common::TestClock> {
    ProcessingEngine::new(
        pool,
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        clock,
        true,
    )
    .with_business_connection_api(api)
}

#[tokio::test]
async fn unclaimed_connection_stores_only_candidate_metadata_and_messages_remain_inert() {
    let (_directory, pool, now) = database().await;
    let api = QueueBusinessApi::new(
        pool.clone(),
        vec![Ok(authoritative("candidate-1", 42, 100, true, true))],
    );
    let mut engine = engine(pool.clone(), common::TestClock::new(now), api.clone());
    engine
        .process(1, trigger("candidate-1", now))
        .await
        .unwrap();

    let candidate: (String, i64, i64, bool, String) = sqlx::query_as(
        "SELECT connection_id, business_user_id, connection_established_at, enabled, rights_json
         FROM business_connection_candidate",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(candidate.0, "candidate-1");
    assert_eq!(candidate.1, 42);
    assert_eq!(candidate.2, 100);
    assert!(candidate.3);
    assert_eq!(
        candidate.4,
        r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#
    );
    let trusted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trusted_count, 0);
    assert_eq!(api.calls(), 1);

    let mut message = common::inbound(500, 50, Some("private sentinel body"), now);
    message.connection_id = Some("candidate-1".to_owned());
    engine.process(2, message).await.unwrap();
    for table in [
        "business_connection",
        "conversation",
        "message_ledger",
        "challenge",
        "outbox_action",
        "audit_event",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "unexpected pre-claim row in {table}");
    }
    let persisted: Vec<String> =
        sqlx::query_scalar("SELECT event_json FROM processed_update ORDER BY update_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(
        persisted
            .iter()
            .all(|event| !event.contains("private sentinel body"))
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM processed_update WHERE update_id = 1",)
            .fetch_one(&pool)
            .await
            .unwrap(),
        "APPLIED"
    );
}

#[tokio::test]
async fn claimed_routing_rejects_cross_user_and_old_generations_but_accepts_later_state() {
    let (_directory, pool, now) = database().await;
    let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
    claim_owner(&mut claim, 42, 4200, 1, now).await.unwrap();
    claim.commit().await.unwrap();
    let api = QueueBusinessApi::new(
        pool.clone(),
        vec![
            Ok(authoritative("generation-a", 42, 100, true, true)),
            Ok(authoritative("foreign", 99, 200, true, true)),
            Ok(authoritative("old", 42, 99, true, true)),
            Ok(authoritative("generation-b", 42, 100, true, true)),
            Ok(authoritative("generation-c", 42, 101, true, true)),
            Ok(authoritative("generation-c", 42, 101, false, false)),
        ],
    );
    let mut engine = engine(pool.clone(), common::TestClock::new(now), api);
    for (update_id, connection_id) in [
        (10, "generation-a"),
        (11, "foreign"),
        (12, "old"),
        (13, "generation-b"),
        (14, "generation-c"),
        (15, "generation-c"),
    ] {
        engine
            .process(update_id, trigger(connection_id, now))
            .await
            .unwrap();
    }

    let trusted: (String, i64, bool, String) = sqlx::query_as(
        "SELECT connection_id, connection_established_at, enabled, reconciliation_state
         FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        trusted,
        (
            "generation-c".to_owned(),
            101,
            false,
            "CONFIRMED".to_owned()
        )
    );
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            connection_floor_established_at: Some(100),
            ..
        }
    ));
    let statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM processed_update ORDER BY update_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(statuses, vec!["APPLIED"; 6]);
    let audits: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_event
         WHERE event_kind IN ('connection_owner_mismatch', 'connection_generation_rejected')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audits, 2);
}

#[tokio::test]
async fn not_found_is_connection_specific_and_auth_failure_is_global_and_notified_once() {
    let (_directory, pool, now) = database().await;
    let candidate_api = QueueBusinessApi::new(
        pool.clone(),
        vec![
            Ok(authoritative("candidate", 42, 100, true, true)),
            Err(TelegramError::Api {
                error_code: 400,
                description: "Bad Request: business connection not found".to_owned(),
                retry_after: None,
            }),
        ],
    );
    let mut candidate_engine = engine(pool.clone(), common::TestClock::new(now), candidate_api);
    candidate_engine
        .process(20, trigger("candidate", now))
        .await
        .unwrap();
    candidate_engine
        .process(21, trigger("candidate", now))
        .await
        .unwrap();
    let candidate_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(candidate_count, 0);

    let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
    claim_owner(&mut claim, 42, 4200, 1, now).await.unwrap();
    claim.commit().await.unwrap();
    let auth_error = || TelegramError::Api {
        error_code: 401,
        description: "sentinel authentication description".to_owned(),
        retry_after: None,
    };
    let auth_api = QueueBusinessApi::new(
        pool.clone(),
        vec![
            Ok(authoritative("trusted", 42, 200, true, true)),
            Err(auth_error()),
            Err(auth_error()),
            Err(auth_error()),
        ],
    );
    let notifier = CountingFatalNotifier::default();
    let mut claimed_engine = engine(pool.clone(), common::TestClock::new(now), auth_api)
        .with_fatal_runtime_notifier(notifier.clone());
    claimed_engine
        .process(22, trigger("trusted", now))
        .await
        .unwrap();
    claimed_engine
        .process(23, trigger("trusted", now))
        .await
        .unwrap();
    claimed_engine
        .process(24, trigger("trusted", now))
        .await
        .unwrap();
    claimed_engine
        .process(23, trigger("trusted", now))
        .await
        .unwrap();

    let global: String = sqlx::query_scalar("SELECT state FROM telegram_reconciliation_state")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(global, "AUTH_FAILED");
    let trusted: (bool, String) =
        sqlx::query_as("SELECT enabled, reconciliation_state FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(trusted, (false, "PENDING".to_owned()));
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 23")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "RECORDED");
    assert_eq!(notifier.calls(), 1);
}

#[tokio::test]
async fn not_found_revision_conflict_keeps_trigger_recoverable() {
    let (_directory, pool, now) = database().await;
    let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
    claim_owner(&mut claim, 42, 4200, 1, now).await.unwrap();
    claim.commit().await.unwrap();

    let initial_api = QueueBusinessApi::new(
        pool.clone(),
        vec![Ok(authoritative("trusted", 42, 200, true, true))],
    );
    let mut initial_engine = engine(pool.clone(), common::TestClock::new(now), initial_api);
    initial_engine
        .process(30, trigger("trusted", now))
        .await
        .unwrap();

    let dummy_api = QueueBusinessApi::new(pool.clone(), Vec::new());
    let mut conflict_engine = engine(pool.clone(), common::TestClock::new(now), dummy_api)
        .with_business_connection_api(RevisionBumpingNotFoundApi { pool: pool.clone() });
    conflict_engine
        .process(31, trigger("trusted", now))
        .await
        .unwrap();

    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 31")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(status, "RECORDED");
    let trusted: (i64, String) = sqlx::query_as(
        "SELECT state_revision, reconciliation_state
         FROM business_connection WHERE connection_id = 'trusted'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trusted, (2, "PENDING".to_owned()));
}
