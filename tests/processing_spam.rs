mod common;

use chathygiene::detection::{MediaKind, MessageContent};
use chathygiene::processing::ProcessingEngine;

#[tokio::test]
async fn spam_blocks_and_cleans_known_messages_while_failures_open() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Spam);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("spam"), now))
        .await
        .unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "SPAM_SOFT_BLOCKED");
    let actions: Vec<String> = sqlx::query_scalar(
        "SELECT action_type FROM outbox_action WHERE chat_id = 1001 ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(actions.contains(&"READ_BUSINESS_MESSAGE".to_owned()));
    assert!(actions.contains(&"DELETE_BUSINESS_MESSAGES".to_owned()));
    engine
        .process(2, common::inbound(1001, 11, Some("again"), now))
        .await
        .unwrap();
    assert_eq!(detector.calls(), 1);
    engine
        .process(20, common::owner_message(1001, 12, now))
        .await
        .unwrap();
    let owner_unblocked: String =
        sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(owner_unblocked, "ACTIVE");

    detector.set(common::DetectorMode::Allow);
    let mut first_album = common::inbound(1002, 20, None, now);
    first_album.media_group_id = Some("album-1".to_owned());
    first_album.content = Some(MessageContent {
        media_kind: Some(MediaKind::Photo),
        ..MessageContent::default()
    });
    engine.process(3, first_album).await.unwrap();
    detector.set(common::DetectorMode::Spam);
    let mut second_album = common::inbound(1002, 21, Some("spam caption"), now);
    second_album.media_group_id = Some("album-1".to_owned());
    engine.process(4, second_album).await.unwrap();
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE chat_id = 1002 AND action_type = 'DELETE_BUSINESS_MESSAGES'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(payload.contains("20"));
    assert!(payload.contains("21"));

    let failing = common::MutableDetector::new(common::DetectorMode::Fail);
    let mut fail_open = ProcessingEngine::new(
        pool.clone(),
        failing,
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    fail_open
        .process(5, common::inbound(1003, 30, Some("uncertain"), now))
        .await
        .unwrap();
    let fail_state: String =
        sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1003")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(fail_state, "VERIFY_PENDING");
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE chat_id = 1003 AND action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 1);
}

#[tokio::test]
async fn live_spam_closes_pending_challenge_without_consuming_attempt() {
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
        .process(30, common::inbound(2001, 30, Some("hello"), now))
        .await
        .unwrap();
    detector.set(common::DetectorMode::Spam);
    engine
        .process(31, common::inbound(2001, 31, Some("8"), now))
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
    assert_eq!(state, "SPAM_SOFT_BLOCKED");
    assert_eq!(challenge.0, 0);
    assert!(challenge.1.is_some());
}
