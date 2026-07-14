mod common;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chathygiene::processing::ProcessingEngine;
use chathygiene::telegram::{
    BusinessApi, DeleteAction, DispatchOutcome, EditAction, OutboxDispatcher, ReadAction,
    SendAction, SentMessage, TelegramError,
};
use chrono::Duration;

#[derive(Clone, Default)]
struct CountingApi(Arc<AtomicUsize>);

impl BusinessApi for CountingApi {
    fn send_business_message<'a>(
        &'a self,
        _action: &'a SendAction,
    ) -> Pin<Box<dyn Future<Output = Result<SentMessage, TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(SentMessage { message_id: 700 })
        })
    }

    fn edit_business_message<'a>(
        &'a self,
        _action: &'a EditAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn read_business_message<'a>(
        &'a self,
        _action: &'a ReadAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }

    fn delete_business_messages<'a>(
        &'a self,
        _action: &'a DeleteAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn restart_recovers_only_pending_and_due_retry_actions() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (1, 'test', '{}', 'APPLIED', ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    for (index, (status, next_attempt_at)) in [
        ("PENDING", None),
        ("RETRY", Some(now - Duration::seconds(1))),
        ("RETRY", Some(now + Duration::seconds(1))),
        ("UNCERTAIN", None),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(
            "INSERT INTO outbox_action
             (source_update_id, connection_id, chat_id, action_type, payload_json,
              idempotency_key, status, attempts, next_attempt_at, claimed_at,
              created_at, updated_at)
             VALUES (1, 'business-1', 42, 'READ_BUSINESS_MESSAGE',
              '{\"message_id\":42}', ?, ?, 0, ?, ?, ?, ?)",
        )
        .bind(format!("recovery-{index}"))
        .bind(status)
        .bind(next_attempt_at.map(|value| value.to_rfc3339()))
        .bind((index == 1).then(|| now.to_rfc3339()))
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
    }

    let api = CountingApi::default();
    let first_process = OutboxDispatcher::new(api.clone());
    assert!(matches!(
        first_process.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { action_id: 1 }
    ));
    drop(first_process);

    let restarted = OutboxDispatcher::new(api.clone());
    assert!(matches!(
        restarted.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { action_id: 2 }
    ));
    assert_eq!(
        restarted.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Idle
    );
    assert_eq!(api.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restart_marks_an_interrupted_challenge_send_uncertain() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "UPDATE business_connection SET rights_json = ? WHERE connection_id = 'business-1'",
    )
    .bind(r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#)
    .execute(&pool)
    .await
    .unwrap();
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("hello"), now))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE outbox_action
         SET status = 'RETRY', attempts = 1, next_attempt_at = ?, claimed_at = ?
         WHERE id = 1",
    )
    .bind((now - Duration::seconds(1)).to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();

    let api = CountingApi::default();
    let restarted = OutboxDispatcher::new(api.clone());
    assert_eq!(
        restarted.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Uncertain { action_id: 1 }
    );
    assert_eq!(api.0.load(Ordering::SeqCst), 0);
    let delivery: String = sqlx::query_scalar("SELECT delivery_status FROM challenge WHERE id = 1")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(delivery, "UNCERTAIN");
}
