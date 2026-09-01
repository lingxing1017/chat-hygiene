mod common;

use chathygiene::installation::CURRENT_KEY_VERSION;
use chathygiene::storage::{
    StorageError, UnitOfWork, connect, load_or_initialize_master_seed, migrate,
    pin_or_verify_telegram_bot_id,
};
use secrecy::ExposeSecret;
use sqlx::Row;
use subtle::ConstantTimeEq;

#[derive(Clone, Copy, Debug)]
enum Corruption {
    MissingSingleton,
    ShortSeed,
    ShortChecksum,
    TextSeed,
    TextChecksum,
    ChangedSeed,
    UnknownVersion,
    MissingReadySeed,
    MissingReadyChecksum,
    MissingReadyTimestamp,
    PopulatedPending,
    WrongSingleton,
    MultipleSingletons,
    BlobTimestamp,
    InvalidTimestamp,
    TextBotId,
    ZeroBotId,
    NegativeBotId,
}

#[derive(Clone, Copy)]
enum ExpectedError {
    Missing,
    Unsupported(i64),
    Invalid(&'static str),
    AnyInvalid,
}

#[tokio::test]
async fn initialization_commits_one_stable_seed() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();

    let first = load_or_initialize_master_seed(&pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let second = load_or_initialize_master_seed(&pool, common::at("2026-08-30T00:00:01Z"))
        .await
        .unwrap();
    assert_eq!(first.key_version, CURRENT_KEY_VERSION);
    assert_eq!(first.bytes.expose_secret().len(), 32);
    assert!(
        bool::from(
            first
                .bytes
                .expose_secret()
                .ct_eq(second.bytes.expose_secret())
        ),
        "reloaded installation seed changed"
    );
    let initialized_at: String =
        sqlx::query_scalar("SELECT initialized_at FROM key_material WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(initialized_at, "2026-08-30T00:00:00+00:00");

    pool.close().await;
    let reopened = connect(&url).await.unwrap();
    let third = load_or_initialize_master_seed(&reopened, common::at("2026-08-30T00:00:02Z"))
        .await
        .unwrap();
    assert!(
        bool::from(
            first
                .bytes
                .expose_secret()
                .ct_eq(third.bytes.expose_secret())
        ),
        "reopened installation seed changed"
    );
    let row = sqlx::query("SELECT state, initialized_at FROM key_material WHERE singleton = 1")
        .fetch_one(&reopened)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("state"), "READY");
    assert_eq!(
        row.get::<String, _>("initialized_at"),
        "2026-08-30T00:00:00+00:00"
    );

    let rendered = format!("{third:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains(&hex::encode(third.bytes.expose_secret())));
}

#[tokio::test]
async fn missing_or_corrupt_ready_material_never_regenerates() {
    let cases = [
        (Corruption::MissingSingleton, ExpectedError::Missing),
        (Corruption::ShortSeed, ExpectedError::AnyInvalid),
        (Corruption::ShortChecksum, ExpectedError::AnyInvalid),
        (Corruption::TextSeed, ExpectedError::AnyInvalid),
        (Corruption::TextChecksum, ExpectedError::AnyInvalid),
        (Corruption::ChangedSeed, ExpectedError::AnyInvalid),
        (Corruption::UnknownVersion, ExpectedError::Unsupported(2)),
        (Corruption::MissingReadySeed, ExpectedError::AnyInvalid),
        (Corruption::MissingReadyChecksum, ExpectedError::AnyInvalid),
        (Corruption::MissingReadyTimestamp, ExpectedError::AnyInvalid),
        (Corruption::PopulatedPending, ExpectedError::AnyInvalid),
        (
            Corruption::WrongSingleton,
            ExpectedError::Invalid("singleton row must have id 1"),
        ),
        (
            Corruption::MultipleSingletons,
            ExpectedError::Invalid("expected exactly one singleton row"),
        ),
        (Corruption::BlobTimestamp, ExpectedError::AnyInvalid),
        (Corruption::InvalidTimestamp, ExpectedError::AnyInvalid),
        (Corruption::TextBotId, ExpectedError::AnyInvalid),
        (Corruption::ZeroBotId, ExpectedError::AnyInvalid),
        (Corruption::NegativeBotId, ExpectedError::AnyInvalid),
    ];

    for (case, expected) in cases {
        let (_directory, url) = common::temporary_database();
        let pool = connect(&url).await.unwrap();
        migrate(&pool).await.unwrap();
        load_or_initialize_master_seed(&pool, common::at("2026-08-30T00:00:00Z"))
            .await
            .unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&pool)
            .await
            .unwrap();
        apply_corruption(&pool, case).await;
        sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(&pool)
            .await
            .unwrap();
        let before = material_snapshot(&pool).await;

        let calls = if matches!(case, Corruption::MissingSingleton) {
            2
        } else {
            1
        };
        for _ in 0..calls {
            let result =
                load_or_initialize_master_seed(&pool, common::at("2026-08-30T00:00:01Z")).await;
            assert!(result.is_err(), "corruption was accepted: {case:?}");
            let error = result.unwrap_err();
            assert_expected_error(&error, expected);
            assert_eq!(
                material_snapshot(&pool).await,
                before,
                "invalid key material was modified"
            );
        }
    }
}

#[tokio::test]
async fn concurrent_initializers_after_migration_share_one_seed() {
    let (_directory, url) = common::temporary_database();
    let first_pool = connect(&url).await.unwrap();
    migrate(&first_pool).await.unwrap();
    let second_pool = connect(&url).await.unwrap();
    let initialized_at = common::at("2026-08-30T00:00:00Z");

    let (first, second) = tokio::join!(
        load_or_initialize_master_seed(&first_pool, initialized_at),
        load_or_initialize_master_seed(&second_pool, initialized_at)
    );
    let first = first.unwrap();
    let second = second.unwrap();
    assert!(
        bool::from(
            first
                .bytes
                .expose_secret()
                .ct_eq(second.bytes.expose_secret())
        ),
        "serialized initializers returned different seeds"
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM key_material WHERE state = 'READY'")
        .fetch_one(&first_pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn bot_identity_is_pinned_once_and_verified_across_restart() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    load_or_initialize_master_seed(&pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let before: (Vec<u8>, Vec<u8>) =
        sqlx::query_as("SELECT master_seed, seed_checksum FROM key_material")
            .fetch_one(&pool)
            .await
            .unwrap();

    let mut first = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        pin_or_verify_telegram_bot_id(&mut first, 123)
            .await
            .unwrap(),
        123
    );
    first.commit().await.unwrap();
    pool.close().await;

    let reopened = connect(&url).await.unwrap();
    let mut same = UnitOfWork::begin_immediate(&reopened).await.unwrap();
    assert_eq!(
        pin_or_verify_telegram_bot_id(&mut same, 123).await.unwrap(),
        123
    );
    same.commit().await.unwrap();
    let mut different = UnitOfWork::begin_immediate(&reopened).await.unwrap();
    assert!(matches!(
        pin_or_verify_telegram_bot_id(&mut different, 456).await,
        Err(StorageError::TelegramBotIdentityMismatch)
    ));
    different.rollback().await.unwrap();
    for invalid in [0, -1] {
        let mut uow = UnitOfWork::begin_immediate(&reopened).await.unwrap();
        assert!(matches!(
            pin_or_verify_telegram_bot_id(&mut uow, invalid).await,
            Err(StorageError::InvalidKeyMaterial(_))
        ));
        uow.rollback().await.unwrap();
    }

    let after: (Vec<u8>, Vec<u8>, i64) =
        sqlx::query_as("SELECT master_seed, seed_checksum, telegram_bot_id FROM key_material")
            .fetch_one(&reopened)
            .await
            .unwrap();
    assert_eq!(before.0, after.0);
    assert_eq!(before.1, after.1);
    assert_eq!(after.2, 123);
}

#[tokio::test]
async fn racing_bot_identity_transactions_converge_on_the_same_id() {
    let (_directory, url) = common::temporary_database();
    let first_pool = connect(&url).await.unwrap();
    migrate(&first_pool).await.unwrap();
    load_or_initialize_master_seed(&first_pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let second_pool = connect(&url).await.unwrap();

    let (first, second) = tokio::join!(pin_same_id(&first_pool), pin_same_id(&second_pool));
    assert_eq!(first.unwrap(), 777);
    assert_eq!(second.unwrap(), 777);
}

#[tokio::test]
async fn bot_identity_requires_valid_ready_material() {
    let (_pending_directory, pending_url) = common::temporary_database();
    let pending_pool = connect(&pending_url).await.unwrap();
    migrate(&pending_pool).await.unwrap();
    let mut pending = UnitOfWork::begin_immediate(&pending_pool).await.unwrap();
    assert!(matches!(
        pin_or_verify_telegram_bot_id(&mut pending, 123).await,
        Err(StorageError::InvalidKeyMaterial(_))
    ));
    pending.rollback().await.unwrap();

    let (_corrupt_directory, corrupt_url) = common::temporary_database();
    let corrupt_pool = connect(&corrupt_url).await.unwrap();
    migrate(&corrupt_pool).await.unwrap();
    load_or_initialize_master_seed(&corrupt_pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    sqlx::query("UPDATE key_material SET seed_checksum = zeroblob(32)")
        .execute(&corrupt_pool)
        .await
        .unwrap();
    let mut corrupt = UnitOfWork::begin_immediate(&corrupt_pool).await.unwrap();
    assert!(matches!(
        pin_or_verify_telegram_bot_id(&mut corrupt, 123).await,
        Err(StorageError::InvalidKeyMaterial(_))
    ));
    corrupt.rollback().await.unwrap();
}

async fn apply_corruption(pool: &sqlx::SqlitePool, case: Corruption) {
    let statement = match case {
        Corruption::MissingSingleton => "DELETE FROM key_material",
        Corruption::ShortSeed => "UPDATE key_material SET master_seed = zeroblob(31)",
        Corruption::ShortChecksum => "UPDATE key_material SET seed_checksum = zeroblob(31)",
        Corruption::TextSeed => "UPDATE key_material SET master_seed = printf('%032d', 0)",
        Corruption::TextChecksum => "UPDATE key_material SET seed_checksum = printf('%032d', 0)",
        Corruption::ChangedSeed => "UPDATE key_material SET master_seed = zeroblob(32)",
        Corruption::UnknownVersion => "UPDATE key_material SET key_version = 2",
        Corruption::MissingReadySeed => "UPDATE key_material SET master_seed = NULL",
        Corruption::MissingReadyChecksum => "UPDATE key_material SET seed_checksum = NULL",
        Corruption::MissingReadyTimestamp => "UPDATE key_material SET initialized_at = NULL",
        Corruption::PopulatedPending => "UPDATE key_material SET state = 'PENDING'",
        Corruption::WrongSingleton => {
            "DELETE FROM key_material;
             INSERT INTO key_material
             (singleton, key_version, state, master_seed, seed_checksum, initialized_at,
              telegram_bot_id)
             VALUES (2, 1, 'PENDING', NULL, NULL, NULL, NULL)"
        }
        Corruption::MultipleSingletons => {
            "INSERT INTO key_material
             (singleton, key_version, state, master_seed, seed_checksum, initialized_at,
              telegram_bot_id)
             VALUES (2, 1, 'PENDING', NULL, NULL, NULL, NULL)"
        }
        Corruption::BlobTimestamp => "UPDATE key_material SET initialized_at = x'00'",
        Corruption::InvalidTimestamp => "UPDATE key_material SET initialized_at = 'not-rfc3339'",
        Corruption::TextBotId => "UPDATE key_material SET telegram_bot_id = 'bot'",
        Corruption::ZeroBotId => "UPDATE key_material SET telegram_bot_id = 0",
        Corruption::NegativeBotId => "UPDATE key_material SET telegram_bot_id = -1",
    };
    for part in statement
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        sqlx::query(part).execute(pool).await.unwrap();
    }
}

async fn material_snapshot(pool: &sqlx::SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT quote(singleton) || '|' || quote(key_version) || '|' || quote(state) ||
                '|' || quote(master_seed) || '|' || quote(seed_checksum) ||
                '|' || quote(initialized_at) || '|' || quote(telegram_bot_id)
         FROM key_material ORDER BY singleton",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

fn assert_expected_error(error: &StorageError, expected: ExpectedError) {
    match expected {
        ExpectedError::Missing => assert!(matches!(error, StorageError::KeyMaterialMissing)),
        ExpectedError::Unsupported(version) => assert!(matches!(
            error,
            StorageError::UnsupportedKeyVersion(actual) if *actual == version
        )),
        ExpectedError::Invalid(message) => assert!(matches!(
            error,
            StorageError::InvalidKeyMaterial(actual) if *actual == message
        )),
        ExpectedError::AnyInvalid => {
            assert!(matches!(error, StorageError::InvalidKeyMaterial(_)));
        }
    }
}

async fn pin_same_id(pool: &sqlx::SqlitePool) -> Result<i64, StorageError> {
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    let id = pin_or_verify_telegram_bot_id(&mut uow, 777).await?;
    uow.commit().await?;
    Ok(id)
}
