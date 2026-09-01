mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{future::Future, pin::Pin};

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

#[tokio::test]
async fn connection_trigger_recording_atomically_gates_only_matching_trust() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled,
          connection_established_at, state_revision, reconciliation_state, updated_at)
         VALUES ('business-1', 42, '{}', 1, 100, 7, 'CONFIRMED', ?)",
    )
    .bind(at("2026-07-14T00:00:00Z").to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let trigger = |update_id, connection_id: &str| {
        PreparedEvent::new(
            update_id,
            "business_connection_changed",
            at("2026-07-14T00:00:01Z"),
            json!({
                "connection_id": connection_id,
                "chat_id": null,
                "user_id": null,
                "message_id": null,
                "media_group_id": null,
                "occurred_at": "2026-07-14T00:00:01Z",
                "action": {"kind": "IGNORE"}
            }),
        )
    };

    assert_eq!(
        record_prepared_event(&pool, &trigger(10, "other"))
            .await
            .unwrap(),
        RecordReceipt::Recorded
    );
    let unchanged: (String, i64) =
        sqlx::query_as("SELECT reconciliation_state, state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(unchanged, ("CONFIRMED".to_owned(), 7));

    let matching = trigger(11, "business-1");
    assert_eq!(
        record_prepared_event(&pool, &matching).await.unwrap(),
        RecordReceipt::Recorded
    );
    let gated: (String, i64) =
        sqlx::query_as("SELECT reconciliation_state, state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(gated, ("PENDING".to_owned(), 8));
    assert_eq!(
        record_prepared_event(&pool, &matching).await.unwrap(),
        RecordReceipt::DuplicateRecorded
    );
    let revision: i64 = sqlx::query_scalar("SELECT state_revision FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 8);
}
