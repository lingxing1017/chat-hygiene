mod common;

use common::{E2eHarness, business_message, challenge_answer, photo_message};
use serde_json::Value;

const OBVIOUS_SPAM: &str = "Contact me for promotion and guaranteed investment returns. Pay 0x1234567890abcdef1234567890abcdef12345678";

#[tokio::test]
async fn spam_blocks_every_pre_active_state_with_rule_evidence() {
    let harness = E2eHarness::new(true).await;
    harness.connect(200).await;

    harness
        .post(business_message(201, 5001, 10, 5001, Some(OBVIOUS_SPAM)))
        .await;

    harness
        .post(business_message(202, 5002, 20, 5002, Some("Hello there")))
        .await;
    harness
        .post(business_message(203, 5002, 21, 5002, Some(OBVIOUS_SPAM)))
        .await;

    harness
        .post(business_message(
            204,
            5003,
            30,
            5003,
            Some("Project question"),
        ))
        .await;
    let expression: String =
        sqlx::query_scalar("SELECT expression FROM challenge WHERE chat_id = 5003")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    harness
        .post(business_message(
            205,
            5003,
            31,
            5003,
            Some(&challenge_answer(&expression)),
        ))
        .await;
    assert_eq!(harness.state(5003).await, "VERIFIED_WAITING_OWNER");
    harness
        .post(business_message(206, 5003, 32, 5003, Some(OBVIOUS_SPAM)))
        .await;

    for chat_id in [5001, 5002, 5003] {
        assert_eq!(harness.state(chat_id).await, "SPAM_SOFT_BLOCKED");
        let evidence: (i64, String, String, String) = sqlx::query_as(
            "SELECT score, reasons_json, rule_ids_json, rule_version
             FROM audit_event WHERE chat_id = ? AND score = 100
             ORDER BY id DESC LIMIT 1",
        )
        .bind(chat_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert_eq!(evidence.0, 100);
        assert_ne!(evidence.1, "[]");
        assert_ne!(evidence.2, "[]");
        assert!(evidence.3.starts_with("2026-07-14.1:"));
        let deletes: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_action
             WHERE chat_id = ? AND action_type = 'DELETE_BUSINESS_MESSAGES'",
        )
        .bind(chat_id)
        .fetch_one(&harness.pool)
        .await
        .unwrap();
        assert!(deletes >= 1);
    }
}

#[tokio::test]
async fn spam_cleanup_batches_every_known_message_at_telegram_limit() {
    let harness = E2eHarness::new(true).await;
    harness.connect(300).await;
    for index in 0..101_i64 {
        harness
            .post(business_message(
                301 + index,
                5010,
                1_000 + index,
                5010,
                Some("still waiting"),
            ))
            .await;
    }
    harness
        .post(business_message(500, 5010, 1_101, 5010, Some(OBVIOUS_SPAM)))
        .await;

    let payloads: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE chat_id = 5010 AND action_type = 'DELETE_BUSINESS_MESSAGES'
         ORDER BY id",
    )
    .fetch_all(&harness.pool)
    .await
    .unwrap();
    let lengths = payloads
        .iter()
        .map(|payload| {
            serde_json::from_str::<Value>(payload).unwrap()["message_ids"]
                .as_array()
                .unwrap()
                .len()
        })
        .collect::<Vec<_>>();
    assert_eq!(lengths, vec![100, 2]);
}

#[tokio::test]
async fn spam_album_caption_cleans_observed_items_but_plain_media_is_neutral() {
    let harness = E2eHarness::new(true).await;
    harness.connect(600).await;
    harness
        .post(photo_message(601, 5020, 60, "album-1", None))
        .await;
    assert_eq!(harness.state(5020).await, "VERIFY_PENDING");
    harness
        .post(photo_message(602, 5020, 61, "album-1", Some(OBVIOUS_SPAM)))
        .await;

    assert_eq!(harness.state(5020).await, "SPAM_SOFT_BLOCKED");
    let payload: String = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE chat_id = 5020 AND action_type = 'DELETE_BUSINESS_MESSAGES'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    let ids = serde_json::from_str::<Value>(&payload).unwrap()["message_ids"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(ids, vec![Value::from(60), Value::from(61)]);
}
