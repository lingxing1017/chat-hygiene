mod common;

use common::{E2eHarness, business_message, connection_update};

const OBVIOUS_SPAM: &str = "Contact me for promotion and guaranteed investment returns. Pay 0x1234567890abcdef1234567890abcdef12345678";

#[tokio::test]
async fn suspicious_and_missing_rights_keep_private_messages_visible() {
    let harness = E2eHarness::new(true).await;
    harness.connect(700).await;
    let suspicious_update = serde_json::from_str(include_str!(
        "fixtures/telegram/e2e_suspicious_inbound.json"
    ))
    .unwrap();
    harness.post(suspicious_update).await;
    assert_eq!(harness.state(6001).await, "VERIFY_PENDING");
    let suspicious: (i64, String) = sqlx::query_as(
        "SELECT score, rule_ids_json FROM audit_event
         WHERE chat_id = 6001 ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert!((50..=99).contains(&suspicious.0));
    assert_ne!(suspicious.1, "[]");

    harness.post(connection_update(702, true, false)).await;
    harness
        .post(business_message(703, 6002, 20, 6002, Some(OBVIOUS_SPAM)))
        .await;
    assert_eq!(harness.state(6002).await, "NEW");
    assert_eq!(harness.ledger_ids(6002).await, vec![20]);
    let destructive: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE chat_id = 6002
           AND action_type IN ('READ_BUSINESS_MESSAGE', 'DELETE_BUSINESS_MESSAGES')",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(destructive, 0);
}

#[tokio::test]
async fn dry_run_records_spam_evidence_without_bodies_or_cleanup() {
    let harness = E2eHarness::new(false).await;
    harness.connect(710).await;
    let private_body = "privacy-marker Contact me for promotion and guaranteed investment returns. Pay 0x1234567890abcdef1234567890abcdef12345678";
    let spam_update =
        serde_json::from_str(include_str!("fixtures/telegram/e2e_dry_run_spam.json")).unwrap();
    harness.post(spam_update).await;

    assert_eq!(harness.state(6010).await, "VERIFY_PENDING");
    assert_eq!(harness.ledger_ids(6010).await, vec![30]);
    let actions: Vec<String> = sqlx::query_scalar(
        "SELECT action_type FROM outbox_action WHERE chat_id = 6010 ORDER BY id",
    )
    .fetch_all(&harness.pool)
    .await
    .unwrap();
    assert_eq!(
        actions,
        vec!["PROPOSED_DESTRUCTIVE_ACTION", "SEND_CHALLENGE"]
    );
    let ordinary_rows: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM processed_update
         UNION ALL SELECT COALESCE(reasons_json, '') FROM audit_event
         UNION ALL SELECT payload_json FROM outbox_action",
    )
    .fetch_all(&harness.pool)
    .await
    .unwrap();
    assert!(ordinary_rows.iter().all(|row| !row.contains(private_body)));

    harness.drain_outbox().await;
    assert!(
        harness
            .telegram
            .requests()
            .iter()
            .all(|(_, body)| !body.to_string().contains(private_body))
    );
}
