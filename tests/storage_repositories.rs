mod common;

use chathygiene::domain::ConversationState;
use chathygiene::storage::{
    ChallengeRecord, ConversationKey, LedgerMessage, MessageDirection, SenderKind, StorageError,
    UnitOfWork, active_challenge, active_owner_reply_ids, close_challenge, connect,
    create_challenge, eligible_deletion_ids, get_or_create_conversation, mark_message_deleted,
    migrate, record_message, save_conversation,
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
