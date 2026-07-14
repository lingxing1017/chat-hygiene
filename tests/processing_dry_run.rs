mod common;

use chathygiene::processing::ProcessingEngine;

#[tokio::test]
async fn dry_run_records_proposals_but_never_blocks_or_deletes() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Spam);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("spam"), now))
        .await
        .unwrap();
    let initial: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(initial, "VERIFY_PENDING");
    let destructive: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE action_type IN ('READ_BUSINESS_MESSAGE', 'DELETE_BUSINESS_MESSAGES')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(destructive, 0);
    let proposals: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE action_type = 'PROPOSED_DESTRUCTIVE_ACTION'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(proposals, 1);

    detector.set(common::DetectorMode::Allow);
    for (update_id, message_id) in [(2, 11), (3, 12), (4, 13)] {
        engine
            .process(update_id, common::inbound(1001, message_id, Some("8"), now))
            .await
            .unwrap();
    }
    let after_exhaustion: String =
        sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after_exhaustion, "NEW");
    let blocks: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM conversation WHERE chat_id = 1001 AND block_expires_at IS NOT NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(blocks, 0);
}
