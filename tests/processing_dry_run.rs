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

#[tokio::test]
async fn dry_run_spam_keeps_pending_challenge_without_consuming_attempt() {
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
        .process(20, common::inbound(2001, 20, Some("hello"), now))
        .await
        .unwrap();
    detector.set(common::DetectorMode::Spam);
    engine
        .process(21, common::inbound(2001, 21, Some("不是答案"), now))
        .await
        .unwrap();

    let state: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 2001")
        .fetch_one(&pool)
        .await
        .unwrap();
    let challenge: (i64, Option<String>) =
        sqlx::query_as("SELECT attempts_used, closed_at FROM challenge WHERE chat_id = 2001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "VERIFY_PENDING");
    assert_eq!(challenge.0, 0);
    assert!(challenge.1.is_none());

    let proposals: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE source_update_id = 21 AND action_type = 'PROPOSED_DESTRUCTIVE_ACTION'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let destructive: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE source_update_id = 21
           AND action_type IN ('READ_BUSINESS_MESSAGE', 'DELETE_BUSINESS_MESSAGES')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(proposals, 1);
    assert_eq!(destructive, 0);
}
