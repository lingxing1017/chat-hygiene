mod common;

use chathygiene::processing::ProcessingEngine;

#[tokio::test]
async fn active_conversation_tracks_only_owner_replies_until_all_are_deleted() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );

    engine
        .process(1, common::inbound(1001, 10, Some("hello"), now))
        .await
        .unwrap();
    engine
        .process(2, common::owner_message(1001, 20, now))
        .await
        .unwrap();
    engine
        .process(3, common::owner_message(1001, 21, now))
        .await
        .unwrap();
    detector.set(common::DetectorMode::Spam);
    engine
        .process(4, common::inbound(1001, 30, Some("definite spam"), now))
        .await
        .unwrap();
    assert_eq!(detector.calls(), 1);
    let inbound_recorded: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM message_ledger WHERE chat_id = 1001 AND message_id = 30",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(inbound_recorded, 0);

    engine
        .process(5, common::deleted(1001, vec![20], now))
        .await
        .unwrap();
    let partial: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(partial, "ACTIVE");
    engine
        .process(6, common::deleted(1001, vec![21], now))
        .await
        .unwrap();
    let reset: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(reset, "NEW");
}
