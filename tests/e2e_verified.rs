mod common;

use chrono::Duration;
use common::{E2eHarness, business_message, challenge_answer};

#[tokio::test]
async fn verification_retains_messages_and_enforces_attempt_limits() {
    let harness = E2eHarness::new(true).await;
    harness.connect(1).await;
    let started_at = harness.clock.now();

    let safe_update =
        serde_json::from_str(include_str!("fixtures/telegram/e2e_safe_inbound.json")).unwrap();
    harness.post(safe_update).await;

    let challenge: (String, String, i64, String) = sqlx::query_as(
        "SELECT expression, expires_at, attempts_used, delivery_status
         FROM challenge WHERE chat_id = 2001",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(
        challenge.1,
        (started_at + Duration::minutes(2)).to_rfc3339()
    );
    assert_eq!(challenge.2, 0);
    assert_eq!(challenge.3, "PENDING");
    assert_eq!(harness.state(2001).await, "VERIFY_PENDING");
    assert_eq!(harness.ledger_ids(2001).await, vec![10]);

    harness.drain_outbox().await;
    let prompt_count = harness.telegram_request_count("sendMessage");
    assert_eq!(prompt_count, 1);

    harness
        .post(business_message(3, 2001, 11, 2001, Some("不是数字")))
        .await;
    assert_eq!(harness.challenge_attempts(2001).await, 0);

    for (update_id, message_id) in [(4, 12), (5, 13), (6, 14)] {
        harness
            .post(business_message(
                update_id,
                2001,
                message_id,
                2001,
                Some("999"),
            ))
            .await;
    }
    assert_eq!(harness.challenge_attempts(2001).await, 3);
    assert_eq!(harness.state(2001).await, "TEMP_SOFT_BLOCKED");
    assert_eq!(harness.ledger_ids(2001).await, vec![10, 11, 12, 13, 14]);
    let block_expires_at: String =
        sqlx::query_scalar("SELECT block_expires_at FROM conversation WHERE chat_id = 2001")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(
        block_expires_at,
        (started_at + Duration::hours(24)).to_rfc3339()
    );
}

#[tokio::test]
async fn correct_answer_waits_for_owner_without_deleting_messages() {
    let harness = E2eHarness::new(true).await;
    harness.connect(10).await;
    harness
        .post(business_message(
            11,
            2002,
            20,
            2002,
            Some("Please share the project details."),
        ))
        .await;
    let expression: String =
        sqlx::query_scalar("SELECT expression FROM challenge WHERE chat_id = 2002")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let answer = challenge_answer(&expression);

    harness
        .post(business_message(12, 2002, 21, 2002, Some(&answer)))
        .await;

    assert_eq!(harness.state(2002).await, "VERIFIED_WAITING_OWNER");
    assert_eq!(harness.ledger_ids(2002).await, vec![20, 21]);
    let delete_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE chat_id = 2002 AND action_type = 'DELETE_BUSINESS_MESSAGES'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(delete_count, 0);
}
