mod common;

use chathygiene::events::{PreparedEvent, record_prepared_event, recover_recorded_events};
use chathygiene::processing::LifecycleHandler;
use chathygiene::storage::{connect, migrate};
use chrono::{DateTime, Utc};
use serde_json::json;

fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

#[tokio::test]
async fn recorded_replacement_connection_is_recovered_after_restart() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    let old_at = at("2026-07-15T05:59:19Z");
    let replacement_at = at("2026-07-15T06:04:40Z");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-old', 42, '{}', 0, ?)",
    )
    .bind(old_at.to_rfc3339())
    .execute(&pool)
    .await
    .expect("seed stale connection");
    sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES ('business-old', 100, 100, 'ACTIVE', ?, ?)",
    )
    .bind(old_at.to_rfc3339())
    .bind(old_at.to_rfc3339())
    .execute(&pool)
    .await
    .expect("seed stale conversation");
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', 'true', ?)",
    )
    .bind(old_at.to_rfc3339())
    .execute(&pool)
    .await
    .expect("enable destructive mode");

    let event = PreparedEvent::new(
        645_499_116,
        "lifecycle",
        replacement_at,
        json!({
            "connection_id": "business-new",
            "chat_id": null,
            "user_id": null,
            "message_id": null,
            "media_group_id": null,
            "occurred_at": replacement_at,
            "action": {
                "kind": "CONNECTION_CHANGED",
                "owner_user_id": 42,
                "enabled": true,
                "rights_json": "{\"can_reply\":true,\"can_read_messages\":true,\"can_delete_sent_messages\":true,\"can_delete_all_messages\":true}"
            }
        }),
    );
    record_prepared_event(&pool, &event)
        .await
        .expect("record replacement event");

    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .expect("recover replacement event"),
        1
    );
    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .expect("second recovery is idempotent"),
        0
    );

    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 645499116")
            .fetch_one(&pool)
            .await
            .expect("load update status");
    assert_eq!(status, "APPLIED");
    let connections: Vec<(String, bool)> = sqlx::query_as(
        "SELECT connection_id, enabled FROM business_connection ORDER BY connection_id",
    )
    .fetch_all(&pool)
    .await
    .expect("load connections");
    assert_eq!(connections, vec![("business-new".to_owned(), true)]);
    let stale_conversation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM conversation WHERE connection_id = 'business-old'",
    )
    .fetch_one(&pool)
    .await
    .expect("count stale conversations");
    assert_eq!(stale_conversation_count, 0);
    let runtime: String =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .expect("load runtime setting");
    assert_eq!(runtime, "false");
}
