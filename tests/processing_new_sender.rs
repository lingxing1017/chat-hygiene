mod common;

use chathygiene::processing::{ProcessingEngine, spawn_processing_worker};
use chathygiene::telegram::WebhookInbox;

#[tokio::test]
async fn new_sender_starts_one_body_free_challenge() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );

    engine
        .process(1, common::inbound(1001, 10, Some("private hello"), now))
        .await
        .expect("process first message");
    engine
        .process(2, common::inbound(1001, 11, Some("still here"), now))
        .await
        .expect("process second message");

    let state: String = sqlx::query_scalar(
        "SELECT state FROM conversation WHERE connection_id = 'business-1' AND chat_id = 1001",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "VERIFY_PENDING");
    let challenges: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(challenges, 1);
    let prompts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE action_type = 'SEND_CHALLENGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(prompts, 1);
    assert_eq!(detector.calls(), 2);

    let persisted: Vec<String> =
        sqlx::query_scalar("SELECT event_json FROM processed_update ORDER BY update_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert!(persisted.iter().all(|row| !row.contains("private hello")));
    assert!(persisted.iter().all(|row| !row.contains("still here")));
}

#[tokio::test]
async fn bounded_worker_serializes_fast_messages() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let engine = ProcessingEngine::new(
        pool.clone(),
        detector,
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let handle = spawn_processing_worker(engine, 8);

    let first = handle.submit(10, common::inbound(2001, 100, Some("first"), now));
    let second = handle.submit(11, common::inbound(2001, 101, Some("second"), now));
    let (first, second) = tokio::join!(first, second);
    first.expect("first receipt");
    second.expect("second receipt");

    let challenge_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 2001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(challenge_count, 1);
}

#[tokio::test]
async fn unknown_business_connections_are_acknowledged_without_state() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut event = common::inbound(3001, 200, Some("private foreign message"), now);
    event.connection_id = Some("foreign-business".to_owned());
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Spam),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );

    engine.process(20, event).await.unwrap();

    let state_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state_rows, 0);
    let event_json: String =
        sqlx::query_scalar("SELECT event_json FROM processed_update WHERE update_id = 20")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!event_json.contains("private foreign message"));
}
