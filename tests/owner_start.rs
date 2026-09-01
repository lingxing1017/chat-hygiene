mod common;

use chathygiene::events::RecordReceipt;
use chathygiene::owner::OwnerIdentityHandle;
use chathygiene::processing::{ClaimSetupCapability, ProcessingEngine};
use chathygiene::storage::{
    OwnerIdentity, UnitOfWork, claim_owner, connect, initialize_or_load_owner_identity,
    load_owner_identity, migrate,
};
use chathygiene::telegram::{RawBusinessEvent, parse_update_with_owner_identity};
use secrecy::SecretSlice;
use sqlx::SqlitePool;

const SENTINEL_TOKEN: &str = "9999999999999999999999999999999999999999999999999999999999999999";

type TestEngine =
    ProcessingEngine<common::MutableDetector, common::FixedVerifier, common::TestClock>;

async fn database(claimed: bool) -> (tempfile::TempDir, SqlitePool, OwnerIdentityHandle) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    initialize_or_load_owner_identity(&pool, common::at("2026-07-14T00:00:00Z"))
        .await
        .unwrap();
    if claimed {
        let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
        claim_owner(&mut uow, 100, 500, 1, common::at("2026-07-14T00:00:01Z"))
            .await
            .unwrap();
        uow.commit().await.unwrap();
    }
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    (directory, pool, OwnerIdentityHandle::new(owner))
}

fn engine(
    pool: SqlitePool,
    owner: OwnerIdentityHandle,
    capability: ClaimSetupCapability,
) -> TestEngine {
    ProcessingEngine::new(
        pool,
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(common::at("2026-07-14T00:00:10Z")),
        false,
    )
    .with_owner_claim(Some(SecretSlice::from(vec![0x99; 32])), owner)
    .with_claim_setup_capability(capability)
}

async fn private_command(
    owner: &OwnerIdentityHandle,
    update_id: i64,
    from_user_id: i64,
    chat_id: i64,
    text: &str,
) -> RawBusinessEvent {
    let body = serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": from_user_id},
            "chat": {"id": chat_id, "type": "private"},
            "date": 1_783_987_270_i64 + update_id,
            "text": text,
        }
    });
    parse_update_with_owner_identity(&serde_json::to_vec(&body).unwrap(), &owner.snapshot().await)
        .unwrap()
        .event
}

#[tokio::test]
async fn only_available_unclaimed_runtime_queues_one_body_free_guide() {
    let (_directory, pool, owner) = database(false).await;
    let mut available = engine(pool.clone(), owner.clone(), ClaimSetupCapability::Available);
    let start = private_command(&owner, 1, 100, 500, "/start").await;
    assert_eq!(
        available.process(1, start.clone()).await.unwrap(),
        RecordReceipt::Recorded
    );
    assert_eq!(
        available.process(1, start).await.unwrap(),
        RecordReceipt::DuplicateApplied
    );

    let action: (String, String, Option<String>, Option<i64>) = sqlx::query_as(
        "SELECT action_type, payload_json, connection_id, chat_id
         FROM outbox_action WHERE source_update_id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(action.0, "SEND_PRIVATE_MESSAGE");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&action.1).unwrap(),
        serde_json::json!({"chat_id": 500, "message_kind": "OWNER_SETUP_GUIDE"})
    );
    assert_eq!(action.2, None);
    assert_eq!(action.3, None);
    assert!(!action.1.contains(SENTINEL_TOKEN));
    let persisted: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM processed_update
         UNION ALL SELECT payload_json FROM outbox_action
         UNION ALL SELECT COALESCE(error_message, '') FROM audit_event",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        persisted
            .iter()
            .all(|value| !value.contains(SENTINEL_TOKEN))
    );

    let mut unavailable = engine(
        pool.clone(),
        owner.clone(),
        ClaimSetupCapability::Unavailable,
    );
    unavailable
        .process(2, private_command(&owner, 2, 100, 500, "/start").await)
        .await
        .unwrap();
    let unavailable_actions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE source_update_id = 2")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unavailable_actions, 0);
    assert_eq!(owner.snapshot().await, OwnerIdentity::Unclaimed);
}

#[tokio::test]
async fn claimed_owner_start_matches_help_and_non_owner_stays_silent() {
    let (_directory, pool, owner) = database(true).await;
    let mut engine = engine(pool.clone(), owner.clone(), ClaimSetupCapability::Available);
    engine
        .process(10, private_command(&owner, 10, 100, 500, "/start").await)
        .await
        .unwrap();
    engine
        .process(11, private_command(&owner, 11, 100, 500, "/help").await)
        .await
        .unwrap();

    let replies: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id IN (10, 11) AND action_type = 'SEND_OWNER_MESSAGE'
           AND idempotency_key LIKE '%:OWNER_COMMAND_REPLY'
         ORDER BY source_update_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0], replies[1]);

    engine
        .process(12, private_command(&owner, 12, 200, 600, "/start").await)
        .await
        .unwrap();
    let non_owner_actions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE source_update_id = 12")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(non_owner_actions, 0);
    assert!(matches!(
        owner.snapshot().await,
        OwnerIdentity::Claimed {
            owner_user_id: 100,
            owner_chat_id: 500,
            ..
        }
    ));
    let candidate_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
            .fetch_one(&pool)
            .await
            .unwrap();
    let trusted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!((candidate_count, trusted_count), (0, 0));
}
