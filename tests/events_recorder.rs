mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use chathygiene::events::{
    ApplyReceipt, EventApplier, EventError, PreparedEvent, RecordReceipt, apply_recorded_event,
    record_prepared_event,
};
use chathygiene::storage::{UnitOfWork, connect, migrate};
use chrono::{DateTime, Utc};
use serde_json::json;

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

fn event(update_id: i64) -> PreparedEvent {
    PreparedEvent::new(
        update_id,
        "business_message",
        at("2026-07-14T00:00:00Z"),
        json!({
            "connection_id": "business-1",
            "chat_id": 100,
            "message_id": 10,
            "normalized_hash": "sha256:abc"
        }),
    )
}

struct CountingApplier {
    applications: Arc<AtomicUsize>,
}

impl EventApplier for CountingApplier {
    async fn apply(
        &self,
        _event: &PreparedEvent,
        _uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        self.applications.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn duplicate_update_is_applied_exactly_once() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    let prepared = event(1000);

    assert_eq!(
        record_prepared_event(&pool, &prepared)
            .await
            .expect("record event"),
        RecordReceipt::Recorded
    );
    let applications = Arc::new(AtomicUsize::new(0));
    let applier = CountingApplier {
        applications: Arc::clone(&applications),
    };
    assert_eq!(
        apply_recorded_event(&pool, prepared.update_id, &applier)
            .await
            .expect("apply event"),
        ApplyReceipt::Applied
    );

    assert_eq!(
        record_prepared_event(&pool, &prepared)
            .await
            .expect("deduplicate event"),
        RecordReceipt::DuplicateApplied
    );
    assert_eq!(
        apply_recorded_event(&pool, prepared.update_id, &applier)
            .await
            .expect("deduplicate application"),
        ApplyReceipt::AlreadyApplied
    );
    assert_eq!(applications.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn event_facts_reject_message_bodies_but_allow_hashes() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");

    for (update_id, facts) in [
        (1, json!({"text": "private message"})),
        (2, json!({"nested": {"caption": "private caption"}})),
        (3, json!({"safe_key": "x".repeat(513)})),
    ] {
        let unsafe_event = PreparedEvent::new(
            update_id,
            "business_message",
            at("2026-07-14T00:00:00Z"),
            facts,
        );
        let error = record_prepared_event(&pool, &unsafe_event)
            .await
            .expect_err("sensitive facts must be rejected");
        assert!(matches!(error, EventError::UnsafeFacts(_)));
    }

    assert_eq!(
        record_prepared_event(&pool, &event(4))
            .await
            .expect("hash-only facts are safe"),
        RecordReceipt::Recorded
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM processed_update")
        .fetch_one(&pool)
        .await
        .expect("count events");
    assert_eq!(count, 1);
}
