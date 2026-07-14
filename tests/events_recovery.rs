mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{future::Future, pin::Pin};

use chathygiene::events::{
    EventApplier, EventError, PreparedEvent, apply_recorded_event, record_prepared_event,
    recover_recorded_events,
};
use chathygiene::storage::{
    ConversationKey, UnitOfWork, connect, get_or_create_conversation, migrate,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::SqlitePool;

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

fn event(update_id: i64) -> PreparedEvent {
    PreparedEvent::new(
        update_id,
        "business_message",
        at("2026-07-14T00:00:00Z"),
        json!({"connection_id": "business-1", "chat_id": 100}),
    )
}

async fn database() -> (tempfile::TempDir, SqlitePool) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("seed connection");
    (directory, pool)
}

struct CountingApplier {
    applications: Arc<AtomicUsize>,
}

impl EventApplier for CountingApplier {
    fn apply<'a>(
        &'a self,
        _event: &'a PreparedEvent,
        _uow: &'a mut UnitOfWork<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EventError>> + Send + 'a>> {
        Box::pin(async move {
            self.applications.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    }
}

struct FailingApplier;

impl EventApplier for FailingApplier {
    fn apply<'a>(
        &'a self,
        _event: &'a PreparedEvent,
        uow: &'a mut UnitOfWork<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EventError>> + Send + 'a>> {
        Box::pin(async move {
            get_or_create_conversation(
                uow,
                &ConversationKey::new("business-1", 100),
                100,
                at("2026-07-14T00:00:00Z"),
            )
            .await?;
            Err(EventError::Application("simulated crash".to_owned()))
        })
    }
}

#[tokio::test]
async fn recorded_event_is_recovered_after_restart() {
    let (_directory, pool) = database().await;
    record_prepared_event(&pool, &event(20))
        .await
        .expect("record event before crash");
    let applications = Arc::new(AtomicUsize::new(0));
    let applier = CountingApplier {
        applications: Arc::clone(&applications),
    };

    assert_eq!(
        recover_recorded_events(&pool, &applier)
            .await
            .expect("recover event"),
        1
    );
    assert_eq!(applications.load(Ordering::SeqCst), 1);
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 20")
            .fetch_one(&pool)
            .await
            .expect("read status");
    assert_eq!(status, "APPLIED");
}

#[tokio::test]
async fn failed_application_rolls_back_derived_state() {
    let (_directory, pool) = database().await;
    record_prepared_event(&pool, &event(30))
        .await
        .expect("record event");

    let error = apply_recorded_event(&pool, 30, &FailingApplier)
        .await
        .expect_err("application must fail");
    assert!(matches!(error, EventError::Application(_)));

    let conversation_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM conversation")
        .fetch_one(&pool)
        .await
        .expect("count conversations");
    assert_eq!(conversation_count, 0);
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 30")
            .fetch_one(&pool)
            .await
            .expect("read status");
    assert_eq!(status, "RECORDED");
}
