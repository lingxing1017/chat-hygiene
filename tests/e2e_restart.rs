mod common;

use chathygiene::events::recover_recorded_events;
use chathygiene::processing::LifecycleHandler;
use chathygiene::telegram::DispatchOutcome;
use common::{E2eHarness, TelegramPlan, business_message};
use serde_json::Value;
use std::time::Duration;

#[tokio::test]
async fn duplicate_rapid_webhooks_create_one_lifecycle_effect() {
    let harness = E2eHarness::new(true).await;
    harness.connect(800).await;
    let update = business_message(801, 7001, 10, 7001, Some("Hello there"));

    let (first, second) = tokio::join!(harness.post(update.clone()), harness.post(update));
    assert_eq!(first, axum::http::StatusCode::OK);
    assert_eq!(second, axum::http::StatusCode::OK);

    let updates: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM processed_update WHERE update_id = 801")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(updates, 1);
    assert_eq!(harness.ledger_ids(7001).await, vec![10]);
    let challenges: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 7001")
        .fetch_one(&harness.pool)
        .await
        .unwrap();
    assert_eq!(challenges, 1);
    let prompts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE chat_id = 7001 AND action_type = 'SEND_CHALLENGE'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(prompts, 1);
}

#[tokio::test]
async fn restart_recovers_recorded_lifecycle_event_once() {
    let harness = E2eHarness::new(true).await;
    harness.connect(810).await;
    harness
        .post(business_message(
            811,
            7010,
            20,
            7010,
            Some("Original event"),
        ))
        .await;
    let event_json: String =
        sqlx::query_scalar("SELECT event_json FROM processed_update WHERE update_id = 811")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    let mut event = serde_json::from_str::<Value>(&event_json).unwrap();
    event["update_id"] = Value::from(812);
    event["facts"]["chat_id"] = Value::from(7011);
    event["facts"]["user_id"] = Value::from(7011);
    event["facts"]["message_id"] = Value::from(21);
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at)
         VALUES (812, 'lifecycle', ?, 'RECORDED', ?)",
    )
    .bind(event.to_string())
    .bind(harness.clock.now().to_rfc3339())
    .execute(&harness.pool)
    .await
    .unwrap();

    assert_eq!(
        recover_recorded_events(&harness.pool, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        recover_recorded_events(&harness.pool, &LifecycleHandler)
            .await
            .unwrap(),
        0
    );
    assert_eq!(harness.state(7011).await, "VERIFY_PENDING");
    assert_eq!(harness.ledger_ids(7011).await, vec![21]);
}

#[tokio::test]
async fn failed_application_rolls_back_then_recovers_after_restart() {
    let harness = E2eHarness::new(true).await;
    harness.connect(815).await;
    sqlx::query(
        "CREATE TRIGGER fail_conversation_insert
         BEFORE INSERT ON conversation
         BEGIN SELECT RAISE(ABORT, 'simulated apply failure'); END",
    )
    .execute(&harness.pool)
    .await
    .unwrap();

    let status = harness
        .post_status(business_message(816, 7015, 25, 7015, Some("Recover me")))
        .await;
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    let derived_rows: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM conversation WHERE chat_id = 7015)
              + (SELECT COUNT(*) FROM message_ledger WHERE chat_id = 7015)
              + (SELECT COUNT(*) FROM challenge WHERE chat_id = 7015)",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(derived_rows, 0);
    let recorded: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 816")
            .fetch_one(&harness.pool)
            .await
            .unwrap();
    assert_eq!(recorded, "RECORDED");

    sqlx::query("DROP TRIGGER fail_conversation_insert")
        .execute(&harness.pool)
        .await
        .unwrap();
    assert_eq!(
        recover_recorded_events(&harness.pool, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    assert_eq!(harness.state(7015).await, "VERIFY_PENDING");
    assert_eq!(harness.ledger_ids(7015).await, vec![25]);
}

#[tokio::test]
async fn ambiguous_challenge_send_is_not_repeated_after_restart() {
    let harness = E2eHarness::new(true).await;
    harness.connect(820).await;
    harness
        .post(business_message(821, 7020, 30, 7020, Some("Hello")))
        .await;
    harness
        .telegram
        .plan(TelegramPlan::Delay(Duration::from_millis(200)));

    let first = harness
        .fresh_dispatcher()
        .dispatch_next(harness.clock.now(), &harness.pool)
        .await
        .unwrap();
    assert!(matches!(first, DispatchOutcome::Uncertain { .. }));
    assert_eq!(harness.telegram_request_count("sendMessage"), 1);

    let restarted = harness.fresh_dispatcher();
    assert!(matches!(
        restarted
            .dispatch_next(harness.clock.now(), &harness.pool)
            .await
            .unwrap(),
        DispatchOutcome::Succeeded { .. }
    ));
    assert_eq!(harness.telegram_request_count("sendMessage"), 2);
    assert_eq!(
        restarted
            .dispatch_next(harness.clock.now(), &harness.pool)
            .await
            .unwrap(),
        DispatchOutcome::Idle
    );
    let requests = harness.telegram.requests();
    assert_eq!(
        requests
            .iter()
            .filter(|(_, body)| body.get("business_connection_id").is_some())
            .count(),
        1
    );
    let statuses: (String, String) = sqlx::query_as(
        "SELECT o.status, c.delivery_status
         FROM outbox_action AS o JOIN challenge AS c
           ON json_extract(o.payload_json, '$.challenge_id') = c.id
         WHERE o.source_update_id = 821",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(statuses, ("UNCERTAIN".to_owned(), "UNCERTAIN".to_owned()));
}
