mod common;

use chathygiene::processing::ProcessingEngine;
use chathygiene::telegram::RawEventKind;
use chathygiene::verification::ArithmeticVerifier;
use chrono::Duration;
use rand::SeedableRng;
use rand::rngs::StdRng;
use secrecy::SecretSlice;

#[tokio::test]
async fn verification_handles_success_exhaustion_and_expiry() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let clock = common::TestClock::new(now);
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector,
        common::FixedVerifier,
        clock.clone(),
        true,
    );

    engine
        .process(1, common::inbound(1001, 1, Some("hello"), now))
        .await
        .unwrap();
    engine
        .process(2, common::inbound(1001, 2, Some("9"), now))
        .await
        .unwrap();
    let success: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(success, "VERIFIED_WAITING_OWNER");

    engine
        .process(3, common::inbound(1002, 10, Some("hello"), now))
        .await
        .unwrap();
    for (update_id, message_id) in [(4, 11), (5, 12), (6, 13)] {
        engine
            .process(update_id, common::inbound(1002, message_id, Some("8"), now))
            .await
            .unwrap();
    }
    let blocked: (String, Option<String>) =
        sqlx::query_as("SELECT state, block_expires_at FROM conversation WHERE chat_id = 1002")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(blocked.0, "TEMP_SOFT_BLOCKED");
    assert_eq!(
        blocked
            .1
            .unwrap()
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap(),
        now + Duration::hours(24)
    );
    let retained: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM message_ledger WHERE chat_id = 1002 AND deleted_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained, 4);

    engine
        .process(7, common::inbound(1003, 20, Some("hello"), now))
        .await
        .unwrap();
    clock.set(now + Duration::minutes(2));
    engine
        .process(
            8,
            common::inbound(1003, 21, Some("late answer"), now + Duration::minutes(2)),
        )
        .await
        .unwrap();
    let expired: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1003")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(expired, "NEW");
}

#[tokio::test]
async fn new_non_spam_replies_consume_attempts_but_edits_do_not() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );

    engine
        .process(20, common::inbound(2001, 20, Some("hello"), now))
        .await
        .unwrap();
    engine
        .process(21, common::inbound(2001, 21, Some("不是数字"), now))
        .await
        .unwrap();
    assert_eq!(challenge_attempts(&pool, 2001).await, 1);

    engine
        .process(22, common::inbound_photo(2001, 22, now))
        .await
        .unwrap();
    assert_eq!(challenge_attempts(&pool, 2001).await, 2);

    let mut edited = common::inbound(2001, 21, Some("8"), now);
    edited.kind = RawEventKind::EditedInboundMessage;
    engine.process(23, edited).await.unwrap();
    assert_eq!(challenge_attempts(&pool, 2001).await, 2);
}

#[tokio::test]
async fn verifier_version_mismatch_leaves_verification_state_unchanged() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let mut legacy = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    legacy
        .process(100, common::inbound(3001, 1, Some("hello"), now))
        .await
        .unwrap();
    let before_conversation: (String, i64) =
        sqlx::query_as("SELECT state, state_version FROM conversation WHERE chat_id = 3001")
            .fetch_one(&pool)
            .await
            .unwrap();
    let before_challenge: (i64, i64, String, Option<String>) = sqlx::query_as(
        "SELECT hmac_key_version, attempts_used, delivery_status, closed_at
         FROM challenge WHERE chat_id = 3001",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let before_outbox: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE chat_id = 3001")
            .fetch_one(&pool)
            .await
            .unwrap();

    let verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(1),
        SecretSlice::from(b"version-one".to_vec()),
        1,
    );
    let mut current = ProcessingEngine::new(
        pool.clone(),
        detector,
        verifier,
        common::TestClock::new(now),
        true,
    );
    let error = current
        .process(101, common::inbound(3001, 2, Some("9"), now))
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        chathygiene::processing::ProcessingError::InvalidEvent(_)
    ));
    assert_eq!(
        sqlx::query_as::<_, (String, i64)>(
            "SELECT state, state_version FROM conversation WHERE chat_id = 3001",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        before_conversation
    );
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, String, Option<String>)>(
            "SELECT hmac_key_version, attempts_used, delivery_status, closed_at
             FROM challenge WHERE chat_id = 3001",
        )
        .fetch_one(&pool)
        .await
        .unwrap(),
        before_challenge
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM outbox_action WHERE chat_id = 3001")
            .fetch_one(&pool)
            .await
            .unwrap(),
        before_outbox
    );

    legacy
        .process(101, common::inbound(3001, 2, Some("9"), now))
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 3001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "VERIFIED_WAITING_OWNER");
}

#[tokio::test]
async fn current_start_challenge_event_serializes_key_version() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(2),
        SecretSlice::from(b"version-one".to_vec()),
        1,
    );
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        verifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(110, common::inbound(3101, 1, Some("hello"), now))
        .await
        .unwrap();
    let event_json: String =
        sqlx::query_scalar("SELECT event_json FROM processed_update WHERE update_id = 110")
            .fetch_one(&pool)
            .await
            .unwrap();
    let event: serde_json::Value = serde_json::from_str(&event_json).unwrap();
    assert_eq!(event["facts"]["action"]["outcome"]["hmac_key_version"], 1);
}

async fn challenge_attempts(pool: &sqlx::SqlitePool, chat_id: i64) -> i64 {
    sqlx::query_scalar(
        "SELECT attempts_used FROM challenge
         WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
    )
    .bind(chat_id)
    .fetch_one(pool)
    .await
    .unwrap()
}
