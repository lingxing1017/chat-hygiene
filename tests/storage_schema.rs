mod common;

use std::collections::BTreeSet;

use chathygiene::storage::{connect, migrate};
use sqlx::Row;

#[tokio::test]
async fn migration_is_idempotent_and_creates_expected_tables() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");

    migrate(&pool).await.expect("first migration");
    migrate(&pool).await.expect("second migration");

    let table_names = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
    )
    .fetch_all(&pool)
    .await
    .expect("list tables")
    .into_iter()
    .map(|row| row.get::<String, _>("name"))
    .collect::<BTreeSet<_>>();

    for expected in [
        "_sqlx_migrations",
        "audit_event",
        "business_connection",
        "business_connection_candidate",
        "business_connection_candidate_guard",
        "challenge",
        "conversation",
        "ham_sample",
        "key_material",
        "message_ledger",
        "outbox_action",
        "owner_identity",
        "processed_update",
        "rule_set",
        "runtime_setting",
        "spam_sample",
        "telegram_reconciliation_state",
    ] {
        assert!(table_names.contains(expected), "missing table {expected}");
    }

    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations");
    assert_eq!(applied, 8);
    let key_material = sqlx::query(
        "SELECT singleton, key_version, state, master_seed, seed_checksum,
                initialized_at, telegram_bot_id
         FROM key_material",
    )
    .fetch_one(&pool)
    .await
    .expect("read key material sentinel");
    assert_eq!(key_material.get::<i64, _>("singleton"), 1);
    assert_eq!(key_material.get::<i64, _>("key_version"), 1);
    assert_eq!(key_material.get::<String, _>("state"), "PENDING");
    assert!(
        key_material
            .get::<Option<Vec<u8>>, _>("master_seed")
            .is_none()
    );
    assert!(
        key_material
            .get::<Option<Vec<u8>>, _>("seed_checksum")
            .is_none()
    );
    assert!(
        key_material
            .get::<Option<String>, _>("initialized_at")
            .is_none()
    );
    assert!(
        key_material
            .get::<Option<i64>, _>("telegram_bot_id")
            .is_none()
    );
    assert_evolved_columns(&pool).await;
    assert_owner_sentinel(&pool).await;
    assert_connection_state_sentinels(&pool).await;
    let runtime_override: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .expect("inspect runtime override");
    assert_eq!(runtime_override, 0);
}

async fn assert_evolved_columns(pool: &sqlx::SqlitePool) {
    assert!(
        table_columns(pool, "outbox_action")
            .await
            .contains("claimed_at")
    );
    assert!(
        table_columns(pool, "challenge")
            .await
            .contains("hmac_key_version")
    );
    let business_columns = table_columns(pool, "business_connection").await;
    for expected in [
        "connection_established_at",
        "state_revision",
        "reconciliation_state",
    ] {
        assert!(business_columns.contains(expected));
    }
}

async fn table_columns(pool: &sqlx::SqlitePool, table: &str) -> BTreeSet<String> {
    sqlx::query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await
        .expect("list table columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect()
}

async fn assert_connection_state_sentinels(pool: &sqlx::SqlitePool) {
    let guard: (i64, Option<i64>, i64, String) = sqlx::query_as(
        "SELECT singleton, overflow_established_at, state_revision, updated_at
         FROM business_connection_candidate_guard",
    )
    .fetch_one(pool)
    .await
    .expect("read candidate guard");
    assert_eq!(guard.0, 1);
    assert_eq!(guard.1, None);
    assert_eq!(guard.2, 0);
    assert!(!guard.3.is_empty());
    let global: (i64, String, i64, String) = sqlx::query_as(
        "SELECT singleton, state, state_revision, updated_at
         FROM telegram_reconciliation_state",
    )
    .fetch_one(pool)
    .await
    .expect("read global reconciliation state");
    assert_eq!(global.0, 1);
    assert_eq!(global.1, "READY");
    assert_eq!(global.2, 0);
    assert!(!global.3.is_empty());
}

#[tokio::test]
async fn candidate_schema_enforces_strict_identifiers_and_state() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    let rights = r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#;

    sqlx::query(
        "INSERT INTO business_connection_candidate
         (connection_id, business_user_id, user_chat_id, rights_json, enabled,
          connection_established_at, state_revision, observed_at)
         VALUES ('candidate-1', 42, 4200, ?, 1, 100, 0, '2026-07-14T00:00:00Z')",
    )
    .bind(rights)
    .execute(&pool)
    .await
    .expect("insert valid candidate");

    for statement in [
        "INSERT INTO business_connection_candidate VALUES ('', 42, 4200, '{}', 1, 100, 0, '2026-07-14T00:00:00Z')",
        "INSERT INTO business_connection_candidate VALUES ('bad-user', 0, 4200, '{}', 1, 100, 0, '2026-07-14T00:00:00Z')",
        "INSERT INTO business_connection_candidate VALUES ('bad-chat', 42, 0, '{}', 1, 100, 0, '2026-07-14T00:00:00Z')",
        "INSERT INTO business_connection_candidate VALUES ('bad-enabled', 42, 4200, '{}', 2, 100, 0, '2026-07-14T00:00:00Z')",
        "INSERT INTO business_connection_candidate VALUES ('bad-date', 42, 4200, '{}', 1, 0, 0, '2026-07-14T00:00:00Z')",
        "INSERT INTO business_connection_candidate VALUES ('bad-revision', 42, 4200, '{}', 1, 100, -1, '2026-07-14T00:00:00Z')",
    ] {
        assert!(sqlx::query(statement).execute(&pool).await.is_err());
    }
    assert!(
        sqlx::query(
            "INSERT INTO business_connection_candidate_guard
             (singleton, overflow_established_at, state_revision, updated_at)
             VALUES (2, NULL, 0, '2026-07-14T00:00:00Z')",
        )
        .execute(&pool)
        .await
        .is_err()
    );
    assert!(
        sqlx::query(
            "UPDATE telegram_reconciliation_state SET state = 'UNKNOWN' WHERE singleton = 1",
        )
        .execute(&pool)
        .await
        .is_err()
    );
}

async fn assert_owner_sentinel(pool: &sqlx::SqlitePool) {
    let owner = sqlx::query(
        "SELECT singleton, state, owner_user_id, owner_chat_id, owner_chat_source,
                connection_floor_established_at, bound_at
         FROM owner_identity",
    )
    .fetch_one(pool)
    .await
    .expect("read owner identity sentinel");
    assert_eq!(owner.get::<i64, _>("singleton"), 1);
    assert_eq!(owner.get::<String, _>("state"), "PENDING");
    assert!(owner.get::<Option<i64>, _>("owner_user_id").is_none());
    assert!(owner.get::<Option<i64>, _>("owner_chat_id").is_none());
    assert!(
        owner
            .get::<Option<String>, _>("owner_chat_source")
            .is_none()
    );
    assert!(
        owner
            .get::<Option<i64>, _>("connection_floor_established_at")
            .is_none()
    );
    assert!(owner.get::<Option<String>, _>("bound_at").is_none());
}

#[tokio::test]
async fn schema_enforces_identity_and_active_challenge_constraints() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");

    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("insert connection");

    let invalid_state = sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES ('business-1', 100, 100, 'UNKNOWN', '2026-07-14T00:00:00Z',
                 '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await;
    assert!(invalid_state.is_err());

    for chat_id in [100_i64, 101_i64] {
        sqlx::query(
            "INSERT INTO conversation
             (connection_id, chat_id, user_id, state, created_at, updated_at)
             VALUES (?, ?, ?, 'VERIFY_PENDING', '2026-07-14T00:00:00Z',
                     '2026-07-14T00:00:00Z')",
        )
        .bind("business-1")
        .bind(chat_id)
        .bind(chat_id)
        .execute(&pool)
        .await
        .expect("insert conversation");
    }

    for chat_id in [100_i64, 101_i64] {
        sqlx::query(
            "INSERT INTO message_ledger
             (connection_id, chat_id, message_id, direction, sender_kind,
              manual_owner_reply, sent_at, eligible_for_deletion)
             VALUES ('business-1', ?, 7, 'INBOUND', 'EXTERNAL', 0,
                     '2026-07-14T00:00:00Z', 1)",
        )
        .bind(chat_id)
        .execute(&pool)
        .await
        .expect("same message ID is valid in a different chat");
    }

    sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, created_at, expires_at,
          attempts_used, max_attempts, delivery_status)
         VALUES ('business-1', 100, '7 + 5 - 3', 'hash',
                 '2026-07-14T00:00:00Z', '2026-07-14T00:02:00Z', 0, 3, 'PENDING')",
    )
    .execute(&pool)
    .await
    .expect("insert first active challenge");
    let default_version: i64 = sqlx::query_scalar(
        "SELECT hmac_key_version FROM challenge
         WHERE connection_id = 'business-1' AND chat_id = 100",
    )
    .fetch_one(&pool)
    .await
    .expect("read legacy challenge key version");
    assert_eq!(default_version, 0);

    let second_active_challenge = sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, created_at, expires_at,
          attempts_used, max_attempts, delivery_status)
         VALUES ('business-1', 100, '1 + 2 + 3', 'other-hash',
                 '2026-07-14T00:00:01Z', '2026-07-14T00:02:01Z', 0, 3, 'PENDING')",
    )
    .execute(&pool)
    .await;
    assert!(second_active_challenge.is_err());
}
