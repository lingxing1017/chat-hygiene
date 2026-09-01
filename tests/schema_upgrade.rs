mod common;

use std::borrow::Cow;
use std::fs;

use chathygiene::events::recover_recorded_events;
use chathygiene::installation::{CURRENT_KEY_VERSION, derive_bot_independent_keys};
use chathygiene::processing::LifecycleHandler;
use chathygiene::storage::{
    OwnerChatSource, OwnerIdentity, connect, initialize_or_load_owner_identity,
    load_or_initialize_master_seed, migrate,
};
use chathygiene::verification::{ArithmeticVerifier, upgrade_active_challenge_hmacs};
use rand::SeedableRng;
use rand::rngs::StdRng;
use sqlx::migrate::{MigrateError, Migrator};

type BusinessSnapshot = (i64, String, i64);
type ChallengeSnapshot = (String, String, i64, Option<i64>, String);
type OutboxSnapshot = (String, String, i64);

#[derive(Debug, PartialEq, Eq, sqlx::FromRow)]
struct OutboxMigrationSnapshot {
    id: i64,
    source_update_id: i64,
    connection_id: Option<String>,
    chat_id: Option<i64>,
    action_type: String,
    payload_json: String,
    idempotency_key: String,
    status: String,
    attempts: i64,
    next_attempt_at: Option<String>,
    last_error: Option<String>,
    created_at: String,
    updated_at: String,
    claimed_at: Option<String>,
}

fn three_migration_migrator() -> Migrator {
    let full = sqlx::migrate!();
    Migrator {
        migrations: Cow::Owned(full.iter().take(3).cloned().collect()),
        ..Migrator::DEFAULT
    }
}

fn seven_migration_migrator() -> Migrator {
    let full = sqlx::migrate!();
    Migrator {
        migrations: Cow::Owned(full.iter().take(7).cloned().collect()),
        ..Migrator::DEFAULT
    }
}

async fn seed_private_message_migration_fixture(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES
         (801, 'test', '{}', 'APPLIED', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z'),
         (802, 'test', '{}', 'APPLIED', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z'),
         (803, 'test', '{}', 'APPLIED', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z'),
         (899, 'test', '{}', 'APPLIED', '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox_action
         (id, source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, next_attempt_at, last_error, created_at,
          updated_at, claimed_at)
         VALUES
         (11, 801, NULL, NULL, 'SEND_OWNER_MESSAGE', '{\"message\":\"pending\"}',
          'pending', 'PENDING', 0, NULL, NULL, '2026-08-30T00:00:00Z',
          '2026-08-30T00:00:00Z', NULL),
         (12, 802, 'business-1', 42, 'READ_BUSINESS_MESSAGE', '{\"message_id\":9}',
          'succeeded', 'SUCCEEDED', 2, NULL, NULL, '2026-08-30T00:00:01Z',
          '2026-08-30T00:00:02Z', '2026-08-30T00:00:01Z'),
         (13, 803, NULL, NULL, 'SEND_OWNER_MESSAGE', '{\"message\":\"uncertain\"}',
          'uncertain', 'UNCERTAIN', 1, '2026-08-30T00:00:05Z', 'timeout',
          '2026-08-30T00:00:03Z', '2026-08-30T00:00:04Z',
          '2026-08-30T00:00:03Z'),
         (100, 899, NULL, NULL, 'SEND_OWNER_MESSAGE', '{}', 'sequence-only',
          'PENDING', 0, NULL, NULL, '2026-08-30T00:00:00Z',
          '2026-08-30T00:00:00Z', NULL)",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query("DELETE FROM outbox_action WHERE id = 100")
        .execute(pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn private_message_migration_preserves_rows_and_autoincrement_sequence() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    seven_migration_migrator().run(&pool).await.unwrap();
    seed_private_message_migration_fixture(&pool).await;
    let before: Vec<OutboxMigrationSnapshot> = sqlx::query_as(
        "SELECT id, source_update_id, connection_id, chat_id, action_type,
                payload_json, idempotency_key, status, attempts, next_attempt_at,
                last_error, created_at, updated_at, claimed_at
         FROM outbox_action ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let sequence_before: i64 =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'outbox_action'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sequence_before, 100);

    migrate(&pool).await.unwrap();

    let after: Vec<OutboxMigrationSnapshot> = sqlx::query_as(
        "SELECT id, source_update_id, connection_id, chat_id, action_type,
                payload_json, idempotency_key, status, attempts, next_attempt_at,
                last_error, created_at, updated_at, claimed_at
         FROM outbox_action ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
    let sequence_after: i64 =
        sqlx::query_scalar("SELECT seq FROM sqlite_sequence WHERE name = 'outbox_action'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(sequence_after, sequence_before);
    let foreign_table: String = sqlx::query_scalar(
        "SELECT \"table\" FROM pragma_foreign_key_list('outbox_action')
         WHERE \"from\" = 'source_update_id'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(foreign_table, "processed_update");
    let due_index: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master
         WHERE type = 'index' AND name = 'outbox_due_idx' AND tbl_name = 'outbox_action'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(due_index, 1);
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, action_type, payload_json, idempotency_key, status,
          attempts, created_at, updated_at)
         VALUES (899, 'SEND_PRIVATE_MESSAGE',
                 '{\"chat_id\":42,\"message_kind\":\"OWNER_SETUP_GUIDE\"}',
                 'new-private', 'PENDING', 0,
                 '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let new_id: i64 =
        sqlx::query_scalar("SELECT id FROM outbox_action WHERE idempotency_key = 'new-private'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(new_id, 101);
}

#[tokio::test]
async fn version_three_backup_upgrades_and_remains_the_only_rollback_path() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("current.db");
    let backup_path = directory.path().join("pre-upgrade.db");
    let restored_path = directory.path().join("restored.db");
    let database_url = format!("sqlite://{}", database_path.display());
    let restored_url = format!("sqlite://{}", restored_path.display());
    let legacy = three_migration_migrator();

    let pool = connect(&database_url).await.unwrap();
    legacy.run(&pool).await.unwrap();
    seed_version_three_data(&pool).await;
    let business_before: BusinessSnapshot = sqlx::query_as(
        "SELECT owner_user_id, rights_json, enabled FROM business_connection
         WHERE connection_id = 'business-1'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let challenge_before: ChallengeSnapshot = sqlx::query_as(
        "SELECT expression, answer_hmac, attempts_used, prompt_message_id, delivery_status
         FROM challenge WHERE chat_id = 100",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let outbox_before: OutboxSnapshot = sqlx::query_as(
        "SELECT action_type, payload_json, attempts FROM outbox_action WHERE id = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    fs::copy(&database_path, &backup_path).unwrap();

    let upgraded = connect(&database_url).await.unwrap();
    migrate(&upgraded).await.unwrap();
    let seed = load_or_initialize_master_seed(&upgraded, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        recover_recorded_events(&upgraded, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    let owner = initialize_or_load_owner_identity(&upgraded, common::at("2026-08-30T00:00:01Z"))
        .await
        .unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            owner_user_id: 42,
            owner_chat_id: 42,
            owner_chat_source: OwnerChatSource::LegacyFallback,
            connection_floor_established_at: None,
            ..
        }
    ));
    let keys = derive_bot_independent_keys(seed.key_version, &seed.bytes).unwrap();
    let verifier = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(1),
        keys.challenge_hmac_key,
        keys.key_version,
    );
    assert_eq!(
        upgrade_active_challenge_hmacs(&upgraded, &verifier)
            .await
            .unwrap(),
        1
    );

    assert_upgraded_state(
        &upgraded,
        &business_before,
        &challenge_before,
        &outbox_before,
    )
    .await;

    let old_binary_error = three_migration_migrator().run(&upgraded).await.unwrap_err();
    assert!(matches!(old_binary_error, MigrateError::VersionMissing(4)));
    upgraded.close().await;

    fs::copy(&backup_path, &restored_path).unwrap();
    let restored = connect(&restored_url).await.unwrap();
    three_migration_migrator().run(&restored).await.unwrap();
    assert_restored_state(
        &restored,
        &business_before,
        &challenge_before,
        &outbox_before,
    )
    .await;
}

#[tokio::test]
async fn recorded_connection_is_recovered_before_owner_import() {
    let directory = tempfile::tempdir().unwrap();
    let database_path = directory.path().join("recorded.db");
    let database_url = format!("sqlite://{}", database_path.display());
    let pool = connect(&database_url).await.unwrap();
    three_migration_migrator().run(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at)
         VALUES (
           20, 'lifecycle',
           '{\"update_id\":20,\"event_type\":\"lifecycle\",\"occurred_at\":\"2026-08-30T00:00:00Z\",\"facts\":{\"connection_id\":\"business-recovered\",\"occurred_at\":\"2026-08-30T00:00:00Z\",\"action\":{\"kind\":\"CONNECTION_CHANGED\",\"owner_user_id\":84,\"enabled\":true,\"rights_json\":\"{}\"}}}',
           'RECORDED', '2026-08-30T00:00:00Z'
         )",
    )
    .execute(&pool)
    .await
    .unwrap();
    migrate(&pool).await.unwrap();

    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    let owner = initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:01Z"))
        .await
        .unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            owner_user_id: 84,
            owner_chat_id: 84,
            owner_chat_source: OwnerChatSource::LegacyFallback,
            connection_floor_established_at: None,
            ..
        }
    ));
}

async fn assert_upgraded_state(
    pool: &sqlx::SqlitePool,
    business_before: &BusinessSnapshot,
    challenge_before: &ChallengeSnapshot,
    outbox_before: &OutboxSnapshot,
) {
    let key_state: (String, i64) =
        sqlx::query_as("SELECT state, key_version FROM key_material WHERE singleton = 1")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(key_state, ("READY".to_owned(), CURRENT_KEY_VERSION));
    assert_eq!(
        sqlx::query_as::<_, BusinessSnapshot>(
            "SELECT owner_user_id, rights_json, enabled FROM business_connection
             WHERE connection_id = 'business-1'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        *business_before
    );
    let connection_state: (Option<i64>, i64, String) = sqlx::query_as(
        "SELECT connection_established_at, state_revision, reconciliation_state
         FROM business_connection WHERE connection_id = 'business-1'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(connection_state, (None, 0, "CONFIRMED".to_owned()));
    let challenge_after: (String, i64, Option<i64>, String, i64) = sqlx::query_as(
        "SELECT expression, attempts_used, prompt_message_id, delivery_status, hmac_key_version
         FROM challenge WHERE chat_id = 100",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(challenge_after.0, challenge_before.0);
    assert_eq!(challenge_after.1, challenge_before.2);
    assert_eq!(challenge_after.2, challenge_before.3);
    assert_eq!(challenge_after.3, challenge_before.4);
    assert_eq!(challenge_after.4, 1);
    assert_eq!(
        sqlx::query_as::<_, OutboxSnapshot>(
            "SELECT action_type, payload_json, attempts FROM outbox_action WHERE id = 1",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        *outbox_before
    );
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 10")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(status, "APPLIED");
}

async fn assert_restored_state(
    pool: &sqlx::SqlitePool,
    business_before: &BusinessSnapshot,
    challenge_before: &ChallengeSnapshot,
    outbox_before: &OutboxSnapshot,
) {
    assert_eq!(
        sqlx::query_as::<_, BusinessSnapshot>(
            "SELECT owner_user_id, rights_json, enabled FROM business_connection
             WHERE connection_id = 'business-1'",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        *business_before
    );
    assert_eq!(
        sqlx::query_as::<_, ChallengeSnapshot>(
            "SELECT expression, answer_hmac, attempts_used, prompt_message_id, delivery_status
             FROM challenge WHERE chat_id = 100",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        *challenge_before
    );
    assert_eq!(
        sqlx::query_as::<_, OutboxSnapshot>(
            "SELECT action_type, payload_json, attempts FROM outbox_action WHERE id = 1",
        )
        .fetch_one(pool)
        .await
        .unwrap(),
        *outbox_before
    );
    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 10")
            .fetch_one(pool)
            .await
            .unwrap();
    assert_eq!(status, "RECORDED");
}

async fn seed_version_three_data(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{\"can_reply\":true}', 1,
                 '2026-08-30T00:00:00Z')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES ('business-1', 100, 100, 'VERIFY_PENDING',
                 '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, created_at, expires_at,
          attempts_used, max_attempts, prompt_message_id, delivery_status)
         VALUES ('business-1', 100, '7 + 5 - 3', 'legacy-hmac',
                 '2026-08-30T00:00:00Z', '2026-08-30T00:02:00Z', 1, 3, 900, 'SENT')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES
         (10, 'lifecycle',
          '{\"update_id\":10,\"event_type\":\"lifecycle\",\"occurred_at\":\"2026-08-30T00:00:00Z\",\"facts\":{\"occurred_at\":\"2026-08-30T00:00:00Z\",\"action\":{\"kind\":\"IGNORE\"}}}',
          'RECORDED', '2026-08-30T00:00:00Z', NULL),
         (11, 'lifecycle', '{}', 'APPLIED', '2026-08-30T00:00:00Z',
          '2026-08-30T00:00:00Z')",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (11, 'business-1', 100, 'SEND_CHALLENGE',
                 '{\"challenge_id\":1}', 'legacy-outbox', 'PENDING', 0,
                 '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')",
    )
    .execute(pool)
    .await
    .unwrap();
}
