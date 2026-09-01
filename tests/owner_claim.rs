mod common;

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chathygiene::owner::{
    OwnerClaimContext, OwnerIdentityHandle, ParsedOwnerClaim, parse_owner_claim,
};
use chathygiene::processing::{ProcessingEngine, spawn_processing_worker};
use chathygiene::storage::{
    OwnerIdentity, UnitOfWork, connect, initialize_or_load_owner_identity, load_owner_identity,
    migrate,
};
use chathygiene::telegram::{
    AuthoritativeBusinessConnection, BoxFuture, BusinessConnectionApi, BusinessRights,
    RawBusinessEvent, RawEventKind, TelegramError, WebhookInbox, parse_update_with_owner_identity,
};
use secrecy::{ExposeSecret, SecretSlice};
use sqlx::SqlitePool;

const EXPECTED_TOKEN: [u8; 32] = [0x11; 32];
const EXPECTED_HEX: &str = "1111111111111111111111111111111111111111111111111111111111111111";
const WRONG_HEX: &str = "2222222222222222222222222222222222222222222222222222222222222222";

#[derive(Clone)]
struct QueueBusinessApi {
    outcomes: Arc<Mutex<VecDeque<Result<AuthoritativeBusinessConnection, TelegramError>>>>,
    calls: Arc<AtomicUsize>,
}

impl QueueBusinessApi {
    fn new(outcomes: Vec<Result<AuthoritativeBusinessConnection, TelegramError>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
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
            self.outcomes.lock().unwrap().pop_front().unwrap()
        })
    }
}

fn authoritative(connection_id: &str, established_at: i64) -> AuthoritativeBusinessConnection {
    AuthoritativeBusinessConnection {
        connection_id: connection_id.to_owned(),
        business_user_id: 100,
        user_chat_id: Some(9_999),
        connection_established_at: established_at,
        rights: BusinessRights {
            can_reply: true,
            can_read_messages: true,
            can_delete_sent_messages: true,
            can_delete_all_messages: true,
        },
        enabled: true,
    }
}

async fn database() -> (tempfile::TempDir, SqlitePool, OwnerIdentityHandle) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    let owner = initialize_or_load_owner_identity(&pool, common::at("2026-07-14T00:00:00Z"))
        .await
        .unwrap();
    (directory, pool, OwnerIdentityHandle::new(owner))
}

fn engine(
    pool: SqlitePool,
    api: QueueBusinessApi,
    owner: OwnerIdentityHandle,
) -> ProcessingEngine<common::MutableDetector, common::FixedVerifier, common::TestClock> {
    ProcessingEngine::new(
        pool,
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(common::at("2026-07-14T00:00:10Z")),
        false,
    )
    .with_business_connection_api(api)
    .with_owner_claim(Some(SecretSlice::from(EXPECTED_TOKEN.to_vec())), owner)
}

async fn parsed_claim(
    owner: &OwnerIdentityHandle,
    update_id: i64,
    token: &str,
) -> RawBusinessEvent {
    parsed_claim_for(owner, update_id, 100, 500, token).await
}

async fn parsed_claim_for(
    owner: &OwnerIdentityHandle,
    update_id: i64,
    from_user_id: i64,
    chat_id: i64,
    token: &str,
) -> RawBusinessEvent {
    let body = serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": from_user_id},
            "chat": {"id": chat_id, "type": "private"},
            "date": 1_783_987_270_i64 + update_id,
            "text": format!("/claim {token}")
        }
    });
    parse_update_with_owner_identity(&serde_json::to_vec(&body).unwrap(), &owner.snapshot().await)
        .unwrap()
        .event
}

fn trigger(connection_id: &str) -> RawBusinessEvent {
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
        occurred_at: common::at("2026-07-14T00:00:05Z"),
    }
}

#[test]
fn parser_requires_exact_private_claim_shape_and_redacts_debug() {
    let candidate = parse_owner_claim(OwnerClaimContext {
        ordinary_message: true,
        private_chat: true,
        from_user_id: Some(100),
        chat_id: 500,
        message_id: 7,
        message_date: 8,
        text: Some(concat!(
            "/claim ",
            "1111111111111111111111111111111111111111111111111111111111111111"
        )),
    });
    let ParsedOwnerClaim::Candidate { token, .. } = candidate else {
        panic!("exact claim was not accepted");
    };
    assert_eq!(token.expose_secret(), EXPECTED_TOKEN);
    assert!(!format!("{token:?}").contains(EXPECTED_HEX));

    for text in [
        "/claim",
        "/claim@ChatHygieneBot 1111111111111111111111111111111111111111111111111111111111111111",
        "/claim 111111111111111111111111111111111111111111111111111111111111111A",
        " /claim 1111111111111111111111111111111111111111111111111111111111111111",
        "/claim 1111111111111111111111111111111111111111111111111111111111111111 extra",
    ] {
        assert!(matches!(
            parse_owner_claim(OwnerClaimContext {
                ordinary_message: true,
                private_chat: true,
                from_user_id: Some(100),
                chat_id: 500,
                message_id: 7,
                message_date: 8,
                text: Some(text),
            }),
            ParsedOwnerClaim::Reject { reply_chat_id: 500 }
        ));
    }
    assert!(matches!(
        parse_owner_claim(OwnerClaimContext {
            ordinary_message: false,
            private_chat: false,
            from_user_id: None,
            chat_id: -100,
            message_id: 7,
            message_date: 8,
            text: Some("/claim bad"),
        }),
        ParsedOwnerClaim::Ignore
    ));
    assert!(matches!(
        parse_owner_claim(OwnerClaimContext {
            ordinary_message: true,
            private_chat: true,
            from_user_id: Some(100),
            chat_id: 500,
            message_id: 7,
            message_date: 8,
            text: Some("/health"),
        }),
        ParsedOwnerClaim::NotClaim
    ));
}

#[tokio::test]
async fn wrong_then_correct_claim_binds_once_and_never_persists_tokens() {
    let (_directory, pool, owner) = database().await;
    let api = QueueBusinessApi::new(Vec::new());
    let mut engine = engine(pool.clone(), api.clone(), owner.clone());

    engine
        .process(10, parsed_claim(&owner, 10, WRONG_HEX).await)
        .await
        .unwrap();
    assert_eq!(owner.snapshot().await, OwnerIdentity::Unclaimed);
    engine
        .process(11, parsed_claim(&owner, 11, EXPECTED_HEX).await)
        .await
        .unwrap();
    let claimed = owner.snapshot().await;
    assert!(matches!(
        claimed,
        OwnerIdentity::Claimed {
            owner_user_id: 100,
            owner_chat_id: 500,
            ..
        }
    ));
    assert_eq!(api.calls(), 0);

    assert_eq!(
        engine
            .process(11, parsed_claim(&owner, 11, EXPECTED_HEX).await)
            .await
            .unwrap(),
        chathygiene::events::RecordReceipt::DuplicateApplied
    );
    engine
        .process(12, parsed_claim(&owner, 12, EXPECTED_HEX).await)
        .await
        .unwrap();
    assert_eq!(owner.snapshot().await, claimed);

    let durable: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM processed_update
         UNION ALL SELECT COALESCE(error_message, '') FROM audit_event
         UNION ALL SELECT payload_json FROM outbox_action",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for secret in [EXPECTED_HEX, WRONG_HEX] {
        assert!(durable.iter().all(|value| !value.contains(secret)));
    }
    let confirmations: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE source_update_id = 11 AND action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(confirmations, 1);
}

#[tokio::test]
async fn unique_refreshed_candidate_is_promoted_by_user_not_chat() {
    let (_directory, pool, owner) = database().await;
    let api = QueueBusinessApi::new(vec![
        Ok(authoritative("business-1", 100)),
        Ok(authoritative("business-1", 100)),
    ]);
    let mut engine = engine(pool.clone(), api.clone(), owner.clone());
    engine.process(20, trigger("business-1")).await.unwrap();
    engine
        .process(21, parsed_claim(&owner, 21, EXPECTED_HEX).await)
        .await
        .unwrap();

    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let identity = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        identity,
        OwnerIdentity::Claimed {
            owner_user_id: 100,
            owner_chat_id: 500,
            ..
        }
    ));
    let trusted: (i64, bool, String) = sqlx::query_as(
        "SELECT owner_user_id, enabled, reconciliation_state FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trusted, (100, true, "CONFIRMED".to_owned()));
    let candidates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(candidates, 0);
    assert_eq!(api.calls(), 2);
}

#[tokio::test]
async fn multiple_same_user_candidates_stay_ambiguous_without_selective_lookup() {
    let (_directory, pool, owner) = database().await;
    let api = QueueBusinessApi::new(vec![
        Ok(authoritative("business-1", 100)),
        Ok(authoritative("business-2", 101)),
    ]);
    let mut engine = engine(pool.clone(), api.clone(), owner.clone());
    engine.process(30, trigger("business-1")).await.unwrap();
    engine.process(31, trigger("business-2")).await.unwrap();
    engine
        .process(32, parsed_claim(&owner, 32, EXPECTED_HEX).await)
        .await
        .unwrap();

    let trusted: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    let candidates: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trusted, 0);
    assert_eq!(candidates, 2);
    assert_eq!(api.calls(), 2);
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id = 32 AND action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(payload.contains("connection=ambiguous"));
}

#[tokio::test]
async fn claimed_owner_commands_work_without_business_connection() {
    let (_directory, pool, owner) = database().await;
    let api = QueueBusinessApi::new(Vec::new());
    let mut engine = engine(pool.clone(), api, owner.clone());
    engine
        .process(40, parsed_claim(&owner, 40, EXPECTED_HEX).await)
        .await
        .unwrap();
    let body = serde_json::json!({
        "update_id": 41,
        "message": {
            "message_id": 41,
            "from": {"id": 100},
            "chat": {"id": 500, "type": "private"},
            "date": 1_783_987_341_i64,
            "text": "/health"
        }
    });
    let parsed = parse_update_with_owner_identity(
        &serde_json::to_vec(&body).unwrap(),
        &owner.snapshot().await,
    )
    .unwrap();
    assert_eq!(parsed.event.kind, RawEventKind::OwnerCommand);
    engine.process(41, parsed.event).await.unwrap();
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id = 41 AND idempotency_key = '41:OWNER_COMMAND_REPLY'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(payload.contains("owner=claimed connection=missing"));
    assert!(payload.contains("\"owner_chat_id\":500"));
}

#[tokio::test]
async fn concurrent_valid_claims_bind_exactly_one_owner() {
    let (_directory, pool, owner) = database().await;
    let engine = engine(
        pool.clone(),
        QueueBusinessApi::new(Vec::new()),
        owner.clone(),
    );
    let handle = spawn_processing_worker(engine, 8);
    let first = parsed_claim_for(&owner, 50, 100, 500, EXPECTED_HEX).await;
    let second = parsed_claim_for(&owner, 51, 200, 600, EXPECTED_HEX).await;
    let (first_result, second_result) =
        tokio::join!(handle.submit(50, first), handle.submit(51, second),);
    first_result.unwrap();
    second_result.unwrap();

    let identity = owner.snapshot().await;
    assert!(matches!(
        identity,
        OwnerIdentity::Claimed {
            owner_user_id: 100,
            owner_chat_id: 500,
            ..
        } | OwnerIdentity::Claimed {
            owner_user_id: 200,
            owner_chat_id: 600,
            ..
        }
    ));
    let successes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE payload_json LIKE '%claim succeeded%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let failures: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE payload_json LIKE '%claim failed%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((successes, failures), (1, 1));
}
