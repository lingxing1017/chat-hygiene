mod common;

use common::{
    E2eHarness, business_message, challenge_answer, deleted_business_messages,
    edited_business_message,
};

const OBVIOUS_SPAM: &str = "Contact me for promotion and guaranteed investment returns. Pay 0x1234567890abcdef1234567890abcdef12345678";

#[tokio::test]
async fn active_skips_detection_until_every_owner_reply_is_deleted() {
    let harness = E2eHarness::new(true).await;
    harness.connect(100).await;
    harness
        .post(business_message(
            101,
            4001,
            10,
            4001,
            Some("Can we discuss your project?"),
        ))
        .await;
    let expression: String =
        sqlx::query_scalar("SELECT expression FROM challenge WHERE chat_id = 4001")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    harness
        .post(business_message(
            102,
            4001,
            11,
            4001,
            Some(&challenge_answer(&expression)),
        ))
        .await;
    harness
        .post(business_message(
            103,
            4001,
            20,
            42,
            Some("First owner reply"),
        ))
        .await;
    harness
        .post(business_message(
            104,
            4001,
            21,
            42,
            Some("Second owner reply"),
        ))
        .await;
    assert_eq!(harness.state(4001).await, "ACTIVE");

    let audit_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_event WHERE chat_id = 4001")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    harness
        .post(business_message(105, 4001, 30, 4001, Some(OBVIOUS_SPAM)))
        .await;
    harness
        .post(edited_business_message(106, 4001, 30, 4001, OBVIOUS_SPAM))
        .await;
    let audit_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_event WHERE chat_id = 4001")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(audit_after, audit_before);
    assert_eq!(harness.state(4001).await, "ACTIVE");
    assert_eq!(harness.ledger_ids(4001).await, vec![10, 11, 20, 21]);

    harness
        .post(deleted_business_messages(107, 4001, &[20]))
        .await;
    assert_eq!(harness.state(4001).await, "ACTIVE");
    harness
        .post(deleted_business_messages(108, 4001, &[21]))
        .await;
    assert_eq!(harness.state(4001).await, "NEW");
    let open_before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM challenge WHERE chat_id = 4001 AND closed_at IS NULL",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(open_before, 0);

    harness
        .post(business_message(
            109,
            4001,
            31,
            4001,
            Some("Starting a new conversation"),
        ))
        .await;
    assert_eq!(harness.state(4001).await, "VERIFY_PENDING");
    let open_after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM challenge WHERE chat_id = 4001 AND closed_at IS NULL",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(open_after, 1);
}
