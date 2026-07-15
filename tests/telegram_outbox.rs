mod common;

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chathygiene::processing::ProcessingEngine;
use chathygiene::telegram::{
    BusinessApi, DeleteAction, DispatchOutcome, EditAction, OutboxDispatcher, ReadAction,
    SendAction, SentMessage, TelegramError,
};
use chrono::{DateTime, Duration, Utc};

#[derive(Debug, Clone)]
enum Planned {
    Send(Result<SentMessage, TelegramError>),
    Edit(Result<(), TelegramError>),
    Read(Result<(), TelegramError>),
    Delete(Result<(), TelegramError>),
}

#[derive(Clone, Default)]
struct FakeApi {
    planned: Arc<Mutex<VecDeque<Planned>>>,
    calls: Arc<Mutex<Vec<&'static str>>>,
    sends: Arc<Mutex<Vec<SendAction>>>,
    edits: Arc<Mutex<Vec<EditAction>>>,
}

impl FakeApi {
    fn with(planned: Vec<Planned>) -> Self {
        Self {
            planned: Arc::new(Mutex::new(planned.into())),
            calls: Arc::new(Mutex::new(Vec::new())),
            sends: Arc::new(Mutex::new(Vec::new())),
            edits: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn pop(&self, name: &'static str) -> Planned {
        self.calls.lock().unwrap().push(name);
        self.planned.lock().unwrap().pop_front().unwrap()
    }
}

impl BusinessApi for FakeApi {
    fn send_business_message<'a>(
        &'a self,
        action: &'a SendAction,
    ) -> Pin<Box<dyn Future<Output = Result<SentMessage, TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.sends.lock().unwrap().push(action.clone());
            let Planned::Send(result) = self.pop("send") else {
                panic!("unexpected fake call")
            };
            result
        })
    }

    fn edit_business_message<'a>(
        &'a self,
        action: &'a EditAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.edits.lock().unwrap().push(action.clone());
            let Planned::Edit(result) = self.pop("edit") else {
                panic!("unexpected fake call")
            };
            result
        })
    }

    fn read_business_message<'a>(
        &'a self,
        _action: &'a ReadAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            let Planned::Read(result) = self.pop("read") else {
                panic!("unexpected fake call")
            };
            result
        })
    }

    fn delete_business_messages<'a>(
        &'a self,
        _action: &'a DeleteAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            let Planned::Delete(result) = self.pop("delete") else {
                panic!("unexpected fake call")
            };
            result
        })
    }
}

async fn start_challenge(pool: &sqlx::SqlitePool, now: DateTime<Utc>) {
    sqlx::query(
        "UPDATE business_connection SET rights_json = ? WHERE connection_id = 'business-1'",
    )
    .bind(r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#)
    .execute(pool)
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
}

#[tokio::test]
async fn explicit_owner_message_dispatches_without_business_key() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (700, 'ignored', '{}', 'APPLIED', ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (700, NULL, NULL, 'SEND_OWNER_MESSAGE',
          '{\"message\":\"keyless trace\",\"owner_user_id\":4242}',
          '700:DRY_RUN_TRACE', 'PENDING', 0, ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let api = FakeApi::with(vec![Planned::Send(Ok(SentMessage { message_id: 901 }))]);
    let dispatcher = OutboxDispatcher::new(api.clone());

    assert_eq!(
        dispatcher.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { action_id: 1 }
    );
    assert_eq!(
        api.sends.lock().unwrap().as_slice(),
        &[SendAction {
            business_connection_id: None,
            chat_id: 4242,
            text: "keyless trace".to_owned(),
        }]
    );
}

#[tokio::test]
async fn challenge_edits_keep_expression_and_use_mode_specific_copy() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    let api = FakeApi::with(vec![
        Planned::Send(Ok(SentMessage { message_id: 901 })),
        Planned::Edit(Ok(())),
        Planned::Edit(Ok(())),
        Planned::Edit(Ok(())),
    ]);
    let dispatcher = OutboxDispatcher::new(api.clone());
    dispatcher.dispatch_next(now, &pool).await.unwrap();

    for (idempotency_suffix, status, attempts_remaining) in [
        ("incorrect", "incorrect", 2),
        ("exhausted-dry-run", "exhausted_dry_run", 0),
        ("exhausted", "exhausted", 0),
    ] {
        sqlx::query(
            "INSERT INTO outbox_action
             (source_update_id, connection_id, chat_id, action_type, payload_json,
              idempotency_key, status, attempts, created_at, updated_at)
             VALUES (?, 'business-1', 1001, 'EDIT_CHALLENGE', ?, ?,
              'PENDING', 0, ?, ?)",
        )
        .bind(1_i64)
        .bind(
            serde_json::json!({
                "challenge_id": 1,
                "status": status,
                "attempts_remaining": attempts_remaining,
            })
            .to_string(),
        )
        .bind(format!("edit-copy-{idempotency_suffix}"))
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
    }

    for _ in 0..3 {
        assert!(matches!(
            dispatcher.dispatch_next(now, &pool).await.unwrap(),
            DispatchOutcome::Succeeded { .. }
        ));
    }

    let edits = api.edits.lock().unwrap();
    assert_eq!(
        edits[0].text,
        "答案不正确，还可尝试 2 次。\n\n请重新回答：\n7 + 5 - 3 = ?"
    );
    assert_eq!(
        edits[1].text,
        "验证失败。\nDry-run：正式模式下将软屏蔽 24 小时，本次未执行屏蔽。"
    );
    assert_eq!(edits[2].text, "验证失败，请 24 小时后再试。");
}

#[tokio::test]
async fn successful_challenge_send_records_prompt_message_id() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    let dispatcher = OutboxDispatcher::new(FakeApi::with(vec![Planned::Send(Ok(SentMessage {
        message_id: 901,
    }))]));

    assert_eq!(
        dispatcher.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { action_id: 1 }
    );
    let challenge: (Option<i64>, String) =
        sqlx::query_as("SELECT prompt_message_id, delivery_status FROM challenge")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(challenge, (Some(901), "SENT".to_owned()));
}

#[tokio::test]
async fn send_timeout_is_uncertain_and_is_never_retried() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    let api = FakeApi::with(vec![Planned::Send(Err(TelegramError::Timeout))]);
    let dispatcher = OutboxDispatcher::new(api.clone());

    assert_eq!(
        dispatcher.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Uncertain { action_id: 1 }
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT status FROM outbox_action WHERE id = 1")
            .fetch_one(&pool)
            .await
            .unwrap(),
        "UNCERTAIN"
    );
    let status: String = sqlx::query_scalar("SELECT delivery_status FROM challenge")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "UNCERTAIN");
    assert_eq!(api.calls.lock().unwrap().as_slice(), &["send"]);
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 1);
}

#[tokio::test]
async fn rate_limits_and_server_errors_use_bounded_retry_policy() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    let api = FakeApi::with(vec![
        Planned::Send(Err(TelegramError::Api {
            error_code: 429,
            description: "rate limited".to_owned(),
            retry_after: Some(93),
        })),
        Planned::Send(Err(TelegramError::Api {
            error_code: 500,
            description: "server error".to_owned(),
            retry_after: None,
        })),
        Planned::Send(Ok(SentMessage { message_id: 902 })),
    ]);
    let dispatcher = OutboxDispatcher::new(api);

    let DispatchOutcome::RetryScheduled {
        next_attempt_at, ..
    } = dispatcher.dispatch_next(now, &pool).await.unwrap()
    else {
        panic!("expected retry")
    };
    assert_eq!(next_attempt_at, now + Duration::seconds(60));
    let DispatchOutcome::RetryScheduled {
        next_attempt_at, ..
    } = dispatcher
        .dispatch_next(now + Duration::seconds(60), &pool)
        .await
        .unwrap()
    else {
        panic!("expected retry")
    };
    assert_eq!(next_attempt_at, now + Duration::seconds(62));
    assert!(matches!(
        dispatcher
            .dispatch_next(now + Duration::seconds(62), &pool)
            .await
            .unwrap(),
        DispatchOutcome::Succeeded { .. }
    ));
}

#[tokio::test]
async fn already_applied_delete_succeeds_but_missing_right_disables_connection() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', 'true', ?)",
    )
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE outbox_action SET action_type = 'DELETE_BUSINESS_MESSAGES',
         payload_json = '{\"message_ids\":[10]}' WHERE id = 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    let dispatcher = OutboxDispatcher::new(FakeApi::with(vec![Planned::Delete(Err(
        TelegramError::Api {
            error_code: 400,
            description: "message to delete not found".to_owned(),
            retry_after: None,
        },
    ))]));
    assert!(matches!(
        dispatcher.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { .. }
    ));

    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (1, 'business-1', 1001, 'READ_BUSINESS_MESSAGE',
          '{\"message_id\":10}', 'rights-test', 'PENDING', 0, ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let dispatcher = OutboxDispatcher::new(FakeApi::with(vec![Planned::Read(Err(
        TelegramError::Api {
            error_code: 400,
            description: "not enough rights to manage business messages".to_owned(),
            retry_after: None,
        },
    ))]));
    assert!(matches!(
        dispatcher.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::PermanentFailure { .. }
    ));
    let enabled: bool = sqlx::query_scalar(
        "SELECT enabled FROM business_connection WHERE connection_id = 'business-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!enabled);
    let destructive_mode: String =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(destructive_mode, "false");
    let alerts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(alerts, 1);
}

#[tokio::test]
async fn already_applied_edit_is_success_and_retries_are_bounded() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    start_challenge(&pool, now).await;
    let sender = OutboxDispatcher::new(FakeApi::with(vec![Planned::Send(Ok(SentMessage {
        message_id: 901,
    }))]));
    sender.dispatch_next(now, &pool).await.unwrap();
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (1, 'business-1', 1001, 'EDIT_CHALLENGE',
          '{\"challenge_id\":1,\"status\":\"success\",\"attempts_remaining\":null}',
          'edit-test', 'PENDING', 0, ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let editor = OutboxDispatcher::new(FakeApi::with(vec![Planned::Edit(Err(
        TelegramError::Api {
            error_code: 400,
            description: "message is not modified".to_owned(),
            retry_after: None,
        },
    ))]));
    assert!(matches!(
        editor.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::Succeeded { action_id: 2 }
    ));

    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (1, 'business-1', 1001, 'READ_BUSINESS_MESSAGE',
          '{\"message_id\":10}', 'bounded-retry', 'RETRY', 6, ?, ?)",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let exhausted = OutboxDispatcher::new(FakeApi::with(vec![Planned::Read(Err(
        TelegramError::Api {
            error_code: 503,
            description: "unavailable".to_owned(),
            retry_after: None,
        },
    ))]));
    assert!(matches!(
        exhausted.dispatch_next(now, &pool).await.unwrap(),
        DispatchOutcome::PermanentFailure { action_id: 3 }
    ));
}

#[tokio::test]
async fn disabled_business_connection_forces_processing_to_fail_open() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query("UPDATE business_connection SET enabled = 0 WHERE connection_id = 'business-1'")
        .execute(&pool)
        .await
        .unwrap();
    let detector = common::MutableDetector::new(common::DetectorMode::Spam);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector,
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(10, common::inbound(9001, 91, Some("spam"), now))
        .await
        .unwrap();

    let state: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 9001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "NEW");
    let destructive: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE source_update_id = 10
           AND action_type IN ('READ_BUSINESS_MESSAGE', 'DELETE_BUSINESS_MESSAGES')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(destructive, 0);
    let retained: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM message_ledger
         WHERE chat_id = 9001 AND message_id = 91 AND deleted_at IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(retained, 1);
}
