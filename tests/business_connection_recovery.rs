mod common;

use chathygiene::events::{PreparedEvent, record_prepared_event, recover_recorded_events};
use chathygiene::processing::LifecycleHandler;
use chathygiene::storage::{
    OwnerChatSource, OwnerIdentity, UnitOfWork, claim_owner, connect,
    initialize_or_load_owner_identity, load_owner_identity, migrate,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::SqlitePool;

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
    initialize_or_load_owner_identity(&pool, replacement_at)
        .await
        .expect("import legacy owner");

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
                "owner_chat_id": 4200,
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

    assert_recovered_replacement(&pool).await;
}

async fn assert_recovered_replacement(pool: &SqlitePool) {
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 645499116")
            .fetch_one(pool)
            .await
            .expect("load update status");
    assert_eq!(status, "APPLIED");
    let connections: Vec<(String, bool)> = sqlx::query_as(
        "SELECT connection_id, enabled FROM business_connection ORDER BY connection_id",
    )
    .fetch_all(pool)
    .await
    .expect("load connections");
    assert_eq!(connections, vec![("business-new".to_owned(), true)]);
    let stale_conversation_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM conversation WHERE connection_id = 'business-old'",
    )
    .fetch_one(pool)
    .await
    .expect("count stale conversations");
    assert_eq!(stale_conversation_count, 0);
    let runtime: String =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(pool)
            .await
            .expect("load runtime setting");
    assert_eq!(runtime, "false");
    let owner = read_owner(pool).await;
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            owner_user_id: 42,
            owner_chat_id: 4200,
            owner_chat_source: OwnerChatSource::BusinessConnection,
            ..
        }
    ));
}

#[tokio::test]
async fn mismatching_recorded_connection_is_audited_once_without_mutation() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    initialize_or_load_owner_identity(&pool, at("2026-07-15T00:00:00Z"))
        .await
        .unwrap();
    let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
    claim_owner(&mut claim, 42, 4200, 1, at("2026-07-15T00:00:01Z"))
        .await
        .unwrap();
    claim.commit().await.unwrap();
    let event = PreparedEvent::new(
        700,
        "lifecycle",
        at("2026-07-15T00:00:02Z"),
        json!({
            "connection_id": "untrusted",
            "occurred_at": "2026-07-15T00:00:02Z",
            "action": {
                "kind": "CONNECTION_CHANGED",
                "owner_user_id": 99,
                "owner_chat_id": 9900,
                "enabled": true,
                "rights_json": "{}"
            }
        }),
    );
    record_prepared_event(&pool, &event).await.unwrap();
    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .unwrap(),
        0
    );
    let connection_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_event
         WHERE source_update_id = 700 AND event_kind = 'owner_identity_security'
           AND error_code = 'connection_owner_mismatch' AND error_message IS NULL",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(connection_count, 0);
    assert_eq!(audit_count, 1);
}

#[tokio::test]
async fn authoritative_claim_chat_survives_same_user_connection_chat_mismatch() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    initialize_or_load_owner_identity(&pool, at("2026-07-15T00:00:00Z"))
        .await
        .unwrap();
    let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
    claim_owner(&mut claim, 42, 4200, 1, at("2026-07-15T00:00:01Z"))
        .await
        .unwrap();
    claim.commit().await.unwrap();
    let event = PreparedEvent::new(
        701,
        "lifecycle",
        at("2026-07-15T00:00:02Z"),
        json!({
            "connection_id": "same-owner",
            "occurred_at": "2026-07-15T00:00:02Z",
            "action": {
                "kind": "CONNECTION_CHANGED",
                "owner_user_id": 42,
                "owner_chat_id": 9999,
                "enabled": true,
                "rights_json": "{}"
            }
        }),
    );
    record_prepared_event(&pool, &event).await.unwrap();
    recover_recorded_events(&pool, &LifecycleHandler)
        .await
        .unwrap();
    let owner = read_owner(&pool).await;
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            owner_chat_id: 4200,
            owner_chat_source: OwnerChatSource::Claim,
            ..
        }
    ));
    let connection_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM business_connection WHERE connection_id = 'same-owner'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let audit_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_event
         WHERE source_update_id = 701 AND error_code = 'connection_owner_chat_mismatch'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(connection_count, 1);
    assert_eq!(audit_count, 1);
}

async fn read_owner(pool: &SqlitePool) -> OwnerIdentity {
    let mut read = UnitOfWork::begin(pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    owner
}
