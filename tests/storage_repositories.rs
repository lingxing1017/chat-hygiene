mod common;

use chathygiene::domain::ConversationState;
use chathygiene::storage::{
    BusinessConnectionRecord, ChallengeRecord, ConversationKey, LedgerMessage, MessageDirection,
    SenderKind, StorageError, UnitOfWork, active_challenge, active_owner_reply_ids,
    close_challenge, connect, create_challenge, eligible_deletion_ids, get_or_create_conversation,
    mark_message_deleted, migrate, record_message, save_conversation, upsert_business_connection,
};
use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

async fn database() -> (tempfile::TempDir, SqlitePool) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("seed connection");
    (directory, pool)
}

#[tokio::test]
async fn get_create_save_and_rollback_are_atomic() {
    let (_directory, pool) = database().await;
    let key = ConversationKey::new("business-1", 100);
    let now = at("2026-07-14T00:00:00Z");

    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    let mut conversation = get_or_create_conversation(&mut uow, &key, 100, now)
        .await
        .expect("create conversation");
    assert_eq!(conversation.state, ConversationState::New);
    assert_eq!(conversation.state_version, 0);
    uow.commit().await.expect("commit conversation");

    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    let existing = get_or_create_conversation(&mut uow, &key, 999, now + Duration::seconds(1))
        .await
        .expect("load conversation");
    assert_eq!(existing.user_id, 100);
    uow.rollback().await.expect("rollback read");

    conversation.state = ConversationState::VerifyPending;
    conversation.updated_at = now + Duration::seconds(2);
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    save_conversation(&mut uow, &mut conversation, 0)
        .await
        .expect("save version zero");
    uow.commit().await.expect("commit update");
    assert_eq!(conversation.state_version, 1);

    let mut stale = conversation.clone();
    stale.state_version = 0;
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    let error = save_conversation(&mut uow, &mut stale, 0)
        .await
        .expect_err("stale save must fail");
    assert!(matches!(error, StorageError::ConcurrentModification));
    uow.rollback().await.expect("rollback stale save");

    let rollback_key = ConversationKey::new("business-1", 200);
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    get_or_create_conversation(&mut uow, &rollback_key, 200, now)
        .await
        .expect("create rollback conversation");
    uow.rollback().await.expect("rollback create");
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM conversation WHERE connection_id = ? AND chat_id = ?",
    )
    .bind("business-1")
    .bind(200_i64)
    .fetch_one(&pool)
    .await
    .expect("count rollback rows");
    assert_eq!(count, 0);
}

#[tokio::test]
async fn message_ledger_is_idempotent_and_tracks_owner_reply_deletion() {
    let (_directory, pool) = database().await;
    let key = ConversationKey::new("business-1", 100);
    let now = at("2026-07-14T00:00:00Z");
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    get_or_create_conversation(&mut uow, &key, 100, now)
        .await
        .expect("create conversation");

    let inbound = LedgerMessage::new(
        key.clone(),
        10,
        MessageDirection::Inbound,
        SenderKind::External,
        false,
        now,
    );
    assert!(
        record_message(&mut uow, &inbound)
            .await
            .expect("insert inbound")
    );
    assert!(
        !record_message(&mut uow, &inbound)
            .await
            .expect("dedupe inbound")
    );

    for message_id in [20_i64, 21_i64] {
        let owner = LedgerMessage::new(
            key.clone(),
            message_id,
            MessageDirection::Outbound,
            SenderKind::Owner,
            true,
            now,
        );
        assert!(
            record_message(&mut uow, &owner)
                .await
                .expect("insert owner reply")
        );
    }

    assert_eq!(
        active_owner_reply_ids(&mut uow, &key).await.unwrap(),
        vec![20, 21]
    );
    assert!(
        mark_message_deleted(&mut uow, &key, 20, now + Duration::seconds(1))
            .await
            .expect("delete owner reply")
    );
    assert_eq!(
        active_owner_reply_ids(&mut uow, &key).await.unwrap(),
        vec![21]
    );
    assert_eq!(
        eligible_deletion_ids(&mut uow, &key).await.unwrap(),
        vec![10, 21]
    );
    uow.commit().await.expect("commit ledger");
}

#[tokio::test]
async fn challenge_repository_closes_the_single_active_challenge() {
    let (_directory, pool) = database().await;
    let key = ConversationKey::new("business-1", 100);
    let now = at("2026-07-14T00:00:00Z");
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin transaction");
    get_or_create_conversation(&mut uow, &key, 100, now)
        .await
        .expect("create conversation");

    let challenge = ChallengeRecord::pending(
        key.clone(),
        "7 + 5 - 3",
        "answer-hmac",
        now,
        now + Duration::minutes(2),
    );
    let id = create_challenge(&mut uow, &challenge)
        .await
        .expect("create challenge");
    let active = active_challenge(&mut uow, &key)
        .await
        .expect("load challenge")
        .expect("active challenge");
    assert_eq!(active.id, id);
    assert_eq!(active.attempts_used, 0);

    assert!(
        close_challenge(&mut uow, id, now + Duration::seconds(30))
            .await
            .unwrap()
    );
    assert!(active_challenge(&mut uow, &key).await.unwrap().is_none());
    uow.commit().await.expect("commit challenge");
}

type OutboxSnapshot = (
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

async fn seed_replacement_fixture(pool: &SqlitePool, created_at: DateTime<Utc>) {
    let old_key = ConversationKey::new("business-1", 100);
    let mut uow = UnitOfWork::begin(pool).await.expect("begin setup");
    get_or_create_conversation(&mut uow, &old_key, 100, created_at)
        .await
        .expect("create old conversation");
    record_message(
        &mut uow,
        &LedgerMessage::new(
            old_key.clone(),
            10,
            MessageDirection::Inbound,
            SenderKind::External,
            false,
            created_at,
        ),
    )
    .await
    .expect("record old message");
    create_challenge(
        &mut uow,
        &ChallengeRecord::pending(
            old_key,
            "7 + 5 - 3",
            "answer-hmac",
            created_at,
            created_at + Duration::minutes(2),
        ),
    )
    .await
    .expect("create old challenge");
    uow.commit().await.expect("commit old state");

    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (8000, 'lifecycle', '{}', 'APPLIED', ?, ?)",
    )
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .expect("seed processed update");
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, claimed_at, next_attempt_at,
          created_at, updated_at)
         VALUES
          (8000, 'business-1', 100, 'READ_BUSINESS_MESSAGE', '{}',
           'old-pending', 'PENDING', 0, ?, NULL, ?, ?),
          (8000, 'business-1', 100, 'DELETE_BUSINESS_MESSAGES', '{}',
           'old-retry', 'RETRY', 1, ?, ?, ?, ?),
          (8000, 'business-1', 100, 'SEND_OWNER_MESSAGE', '{}',
           'old-succeeded', 'SUCCEEDED', 1, NULL, NULL, ?, ?)",
    )
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind((created_at + Duration::minutes(5)).to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .expect("seed outbox actions");
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', 'true', ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                                        updated_at = excluded.updated_at",
    )
    .bind(created_at.to_rfc3339())
    .execute(pool)
    .await
    .expect("enable destructive mode");
}

async fn replace_connection(pool: &SqlitePool, replaced_at: DateTime<Utc>) {
    let mut uow = UnitOfWork::begin(pool).await.expect("begin replacement");
    upsert_business_connection(
        &mut uow,
        &BusinessConnectionRecord {
            connection_id: "business-2".to_owned(),
            owner_user_id: 42,
            rights_json: r#"{"can_reply":true}"#.to_owned(),
            enabled: true,
            updated_at: replaced_at,
        },
    )
    .await
    .expect("replace connection for owner");
    uow.commit().await.expect("commit replacement");
}

async fn assert_replacement_state(pool: &SqlitePool, replaced_at: DateTime<Utc>) {
    let connections: Vec<(String, bool)> = sqlx::query_as(
        "SELECT connection_id, enabled FROM business_connection ORDER BY connection_id",
    )
    .fetch_all(pool)
    .await
    .expect("load connections");
    assert_eq!(connections, vec![("business-2".to_owned(), true)]);
    for table in ["conversation", "message_ledger", "challenge"] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE connection_id = 'business-1'"
        ))
        .fetch_one(pool)
        .await
        .expect("count stale rows");
        assert_eq!(count, 0, "stale {table} rows must be removed");
    }

    let outbox: Vec<OutboxSnapshot> = sqlx::query_as(
        "SELECT idempotency_key, status, last_error, claimed_at, next_attempt_at
             FROM outbox_action ORDER BY idempotency_key",
    )
    .fetch_all(pool)
    .await
    .expect("load outbox actions");
    assert_eq!(
        outbox,
        vec![
            (
                "old-pending".to_owned(),
                "PERMANENT_FAILURE".to_owned(),
                Some("business_connection_replaced".to_owned()),
                None,
                None,
            ),
            (
                "old-retry".to_owned(),
                "PERMANENT_FAILURE".to_owned(),
                Some("business_connection_replaced".to_owned()),
                None,
                None,
            ),
            (
                "old-succeeded".to_owned(),
                "SUCCEEDED".to_owned(),
                None,
                None,
                None,
            ),
        ]
    );
    let runtime: (String, String) = sqlx::query_as(
        "SELECT value, updated_at FROM runtime_setting WHERE key = 'destructive_mode'",
    )
    .fetch_one(pool)
    .await
    .expect("load runtime setting");
    assert_eq!(runtime, ("false".to_owned(), replaced_at.to_rfc3339()));
    let processed_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM processed_update WHERE update_id = 8000")
            .fetch_one(pool)
            .await
            .expect("count processed update");
    assert_eq!(processed_count, 1);
}

#[tokio::test]
async fn replacement_connection_retires_stale_state_and_forces_dry_run() {
    let (_directory, pool) = database().await;
    let created_at = at("2026-07-14T00:00:00Z");
    let replaced_at = at("2026-07-15T06:04:40Z");

    seed_replacement_fixture(&pool, created_at).await;
    replace_connection(&pool, replaced_at).await;
    assert_replacement_state(&pool, replaced_at).await;
}

#[tokio::test]
async fn refreshing_same_connection_preserves_state_and_runtime_mode() {
    let (_directory, pool) = database().await;
    let key = ConversationKey::new("business-1", 100);
    let now = at("2026-07-15T06:04:40Z");
    let mut uow = UnitOfWork::begin(&pool).await.expect("begin setup");
    get_or_create_conversation(&mut uow, &key, 100, now)
        .await
        .expect("create conversation");
    uow.commit().await.expect("commit conversation");
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', 'true', ?)",
    )
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .expect("enable destructive mode");

    let mut uow = UnitOfWork::begin(&pool).await.expect("begin refresh");
    upsert_business_connection(
        &mut uow,
        &BusinessConnectionRecord {
            connection_id: "business-1".to_owned(),
            owner_user_id: 42,
            rights_json: r#"{"can_reply":false}"#.to_owned(),
            enabled: false,
            updated_at: now + Duration::seconds(1),
        },
    )
    .await
    .expect("refresh connection");
    uow.commit().await.expect("commit refresh");

    let conversation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM conversation
         WHERE connection_id = 'business-1' AND chat_id = 100",
    )
    .fetch_one(&pool)
    .await
    .expect("count conversation");
    assert_eq!(conversation_count, 1);
    let runtime: String =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .expect("load runtime setting");
    assert_eq!(runtime, "true");
}
