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
        "challenge",
        "conversation",
        "ham_sample",
        "key_material",
        "message_ledger",
        "outbox_action",
        "processed_update",
        "rule_set",
        "runtime_setting",
        "spam_sample",
    ] {
        assert!(table_names.contains(expected), "missing table {expected}");
    }

    let applied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .expect("count migrations");
    assert_eq!(applied, 5);
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
    let outbox_columns = sqlx::query("PRAGMA table_info(outbox_action)")
        .fetch_all(&pool)
        .await
        .expect("list outbox columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<BTreeSet<_>>();
    assert!(outbox_columns.contains("claimed_at"));
    let challenge_columns = sqlx::query("PRAGMA table_info(challenge)")
        .fetch_all(&pool)
        .await
        .expect("list challenge columns")
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect::<BTreeSet<_>>();
    assert!(challenge_columns.contains("hmac_key_version"));
    let runtime_override: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .expect("inspect runtime override");
    assert_eq!(runtime_override, 0);
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
