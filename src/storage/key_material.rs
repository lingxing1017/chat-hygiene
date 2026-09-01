use std::fmt;

use chrono::{DateTime, Utc};
use rand::{TryRngCore, rngs::OsRng};
use secrecy::{ExposeSecretMut, SecretSlice, zeroize::Zeroize};
use sqlx::{Row, SqliteConnection, SqlitePool};
use subtle::ConstantTimeEq;

use crate::installation::{CURRENT_KEY_VERSION, master_seed_checksum};

use super::{StorageError, UnitOfWork};

const MASTER_SEED_LEN: usize = 32;

pub struct MasterSeed {
    pub key_version: i64,
    pub bytes: SecretSlice<u8>,
}

impl fmt::Debug for MasterSeed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("MasterSeed")
            .field("key_version", &self.key_version)
            .field("bytes", &"[REDACTED]")
            .finish()
    }
}

struct SeedBuffer(Vec<u8>);

impl SeedBuffer {
    fn zeroed() -> Self {
        Self(vec![0_u8; MASTER_SEED_LEN])
    }

    fn from_vec(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    fn as_slice(&self) -> &[u8] {
        &self.0
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.0
    }

    fn copy_into_secret(&self) -> SecretSlice<u8> {
        let mut secret = SecretSlice::from(vec![0_u8; self.0.len()]);
        secret.expose_secret_mut().copy_from_slice(&self.0);
        secret
    }
}

impl Drop for SeedBuffer {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

trait SeedSource {
    fn fill_seed(&mut self, destination: &mut [u8]) -> Result<(), StorageError>;
}

struct OsSeedSource;

impl SeedSource for OsSeedSource {
    fn fill_seed(&mut self, destination: &mut [u8]) -> Result<(), StorageError> {
        OsRng
            .try_fill_bytes(destination)
            .map_err(|_| StorageError::EntropyUnavailable)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum MaterialState {
    Pending,
    Ready {
        initialized_at: DateTime<Utc>,
        telegram_bot_id: Option<i64>,
    },
}

struct MaterialMetadata {
    singleton: i64,
    key_version: i64,
    state: String,
    master_seed_type: String,
    master_seed_len: Option<i64>,
    checksum_type: String,
    checksum_len: Option<i64>,
    initialized_at_type: String,
    initialized_at: Option<String>,
    bot_id_type: String,
    telegram_bot_id: Option<i64>,
}

/// Loads the durable installation seed or initializes it exactly once.
///
/// # Errors
///
/// Returns [`StorageError`] when entropy or `SQLite` is unavailable, or when the
/// persisted singleton is missing, corrupt, or uses an unsupported version.
pub async fn load_or_initialize_master_seed(
    pool: &SqlitePool,
    initialized_at: DateTime<Utc>,
) -> Result<MasterSeed, StorageError> {
    load_or_initialize_master_seed_with(pool, initialized_at, &mut OsSeedSource).await
}

async fn load_or_initialize_master_seed_with<S: SeedSource>(
    pool: &SqlitePool,
    initialized_at: DateTime<Utc>,
    source: &mut S,
) -> Result<MasterSeed, StorageError> {
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    let metadata = load_metadata(uow.connection()).await?;
    let state = validate_metadata(&metadata)?;

    match state {
        MaterialState::Pending => {
            let mut generated_seed = SeedBuffer::zeroed();
            source.fill_seed(generated_seed.as_mut_slice())?;
            let checksum = checksum(metadata.key_version, generated_seed.as_slice())?;
            let result = sqlx::query(
                "UPDATE key_material
                 SET state = 'READY', master_seed = ?, seed_checksum = ?, initialized_at = ?
                 WHERE singleton = 1
                   AND key_version = 1
                   AND state = 'PENDING'
                   AND master_seed IS NULL
                   AND seed_checksum IS NULL
                   AND initialized_at IS NULL",
            )
            .bind(generated_seed.as_slice())
            .bind(checksum.as_slice())
            .bind(initialized_at.to_rfc3339())
            .execute(uow.connection())
            .await?;
            if result.rows_affected() != 1 {
                return Err(StorageError::InvalidKeyMaterial(
                    "pending seed transition did not update one row",
                ));
            }

            let ready_metadata = load_metadata(uow.connection()).await?;
            let MaterialState::Ready { .. } = validate_metadata(&ready_metadata)? else {
                return Err(StorageError::InvalidKeyMaterial(
                    "seed transition did not produce ready material",
                ));
            };
            let fetched_seed =
                load_and_validate_ready_seed(uow.connection(), ready_metadata.key_version).await?;
            if !bool::from(generated_seed.as_slice().ct_eq(fetched_seed.as_slice())) {
                return Err(StorageError::InvalidKeyMaterial(
                    "persisted seed differs from generated seed",
                ));
            }

            uow.commit().await?;
            let bytes = fetched_seed.copy_into_secret();
            drop(generated_seed);
            drop(fetched_seed);
            Ok(MasterSeed {
                key_version: ready_metadata.key_version,
                bytes,
            })
        }
        MaterialState::Ready { .. } => {
            let fetched_seed =
                load_and_validate_ready_seed(uow.connection(), metadata.key_version).await?;
            uow.commit().await?;
            let bytes = fetched_seed.copy_into_secret();
            drop(fetched_seed);
            Ok(MasterSeed {
                key_version: metadata.key_version,
                bytes,
            })
        }
    }
}

/// Pins the positive ID returned by authenticated Telegram `getMe`, or verifies
/// that a previously pinned ID is unchanged.
///
/// # Errors
///
/// Returns [`StorageError`] for an invalid or mismatching ID, corrupt key
/// material, or a database failure.
pub async fn pin_or_verify_telegram_bot_id(
    uow: &mut UnitOfWork<'_>,
    authenticated_bot_id: i64,
) -> Result<i64, StorageError> {
    if authenticated_bot_id <= 0 {
        return Err(StorageError::InvalidKeyMaterial(
            "authenticated Telegram bot id must be positive",
        ));
    }

    let metadata = load_metadata(uow.connection()).await?;
    let MaterialState::Ready {
        telegram_bot_id, ..
    } = validate_metadata(&metadata)?
    else {
        return Err(StorageError::InvalidKeyMaterial(
            "Telegram bot id requires ready key material",
        ));
    };
    let seed = load_and_validate_ready_seed(uow.connection(), metadata.key_version).await?;
    drop(seed);

    match telegram_bot_id {
        Some(pinned) if pinned == authenticated_bot_id => Ok(pinned),
        Some(_) => Err(StorageError::TelegramBotIdentityMismatch),
        None => {
            let result = sqlx::query(
                "UPDATE key_material
                 SET telegram_bot_id = ?
                 WHERE singleton = 1 AND state = 'READY' AND telegram_bot_id IS NULL",
            )
            .bind(authenticated_bot_id)
            .execute(uow.connection())
            .await?;
            if result.rows_affected() != 1 {
                return Err(StorageError::TelegramBotIdentityMismatch);
            }
            Ok(authenticated_bot_id)
        }
    }
}

async fn load_metadata(
    connection: &mut SqliteConnection,
) -> Result<MaterialMetadata, StorageError> {
    let rows = sqlx::query(
        "SELECT
             typeof(singleton) AS singleton_type,
             CASE WHEN typeof(singleton) = 'integer' THEN singleton END AS singleton_value,
             typeof(key_version) AS key_version_type,
             CASE WHEN typeof(key_version) = 'integer' THEN key_version END AS key_version_value,
             typeof(state) AS state_type,
             CASE WHEN typeof(state) = 'text' THEN state END AS state_value,
             typeof(master_seed) AS master_seed_type,
             length(master_seed) AS master_seed_len,
             typeof(seed_checksum) AS checksum_type,
             length(seed_checksum) AS checksum_len,
             typeof(initialized_at) AS initialized_at_type,
             CASE WHEN typeof(initialized_at) = 'text' THEN initialized_at END
                 AS initialized_at_value,
             typeof(telegram_bot_id) AS bot_id_type,
             CASE WHEN typeof(telegram_bot_id) = 'integer' THEN telegram_bot_id END
                 AS bot_id_value
         FROM key_material
         ORDER BY singleton",
    )
    .fetch_all(&mut *connection)
    .await?;

    if rows.is_empty() {
        return Err(StorageError::KeyMaterialMissing);
    }
    if rows.len() != 1 {
        return Err(StorageError::InvalidKeyMaterial(
            "expected exactly one singleton row",
        ));
    }
    let row = &rows[0];
    if row.try_get::<String, _>("singleton_type")? != "integer" {
        return Err(StorageError::InvalidKeyMaterial(
            "singleton row id must be an integer",
        ));
    }
    let singleton = row.try_get::<Option<i64>, _>("singleton_value")?.ok_or(
        StorageError::InvalidKeyMaterial("singleton row id must be an integer"),
    )?;
    if singleton != 1 {
        return Err(StorageError::InvalidKeyMaterial(
            "singleton row must have id 1",
        ));
    }
    if row.try_get::<String, _>("key_version_type")? != "integer" {
        return Err(StorageError::InvalidKeyMaterial(
            "key version must be an integer",
        ));
    }
    let key_version = row.try_get::<Option<i64>, _>("key_version_value")?.ok_or(
        StorageError::InvalidKeyMaterial("key version must be an integer"),
    )?;
    if row.try_get::<String, _>("state_type")? != "text" {
        return Err(StorageError::InvalidKeyMaterial("state must be text"));
    }
    let state = row
        .try_get::<Option<String>, _>("state_value")?
        .ok_or(StorageError::InvalidKeyMaterial("state must be text"))?;

    Ok(MaterialMetadata {
        singleton,
        key_version,
        state,
        master_seed_type: row.try_get("master_seed_type")?,
        master_seed_len: row.try_get("master_seed_len")?,
        checksum_type: row.try_get("checksum_type")?,
        checksum_len: row.try_get("checksum_len")?,
        initialized_at_type: row.try_get("initialized_at_type")?,
        initialized_at: row.try_get("initialized_at_value")?,
        bot_id_type: row.try_get("bot_id_type")?,
        telegram_bot_id: row.try_get("bot_id_value")?,
    })
}

fn validate_metadata(metadata: &MaterialMetadata) -> Result<MaterialState, StorageError> {
    debug_assert_eq!(metadata.singleton, 1);
    if metadata.key_version != CURRENT_KEY_VERSION {
        return Err(StorageError::UnsupportedKeyVersion(metadata.key_version));
    }

    match metadata.state.as_str() {
        "PENDING" => {
            if metadata.master_seed_type != "null"
                || metadata.master_seed_len.is_some()
                || metadata.checksum_type != "null"
                || metadata.checksum_len.is_some()
                || metadata.initialized_at_type != "null"
                || metadata.initialized_at.is_some()
                || metadata.bot_id_type != "null"
                || metadata.telegram_bot_id.is_some()
            {
                return Err(StorageError::InvalidKeyMaterial(
                    "pending material must not contain initialized values",
                ));
            }
            Ok(MaterialState::Pending)
        }
        "READY" => {
            if metadata.master_seed_type != "blob" || metadata.master_seed_len != Some(32) {
                return Err(StorageError::InvalidKeyMaterial(
                    "master seed must be a 32-byte blob",
                ));
            }
            if metadata.checksum_type != "blob" || metadata.checksum_len != Some(32) {
                return Err(StorageError::InvalidKeyMaterial(
                    "seed checksum must be a 32-byte blob",
                ));
            }
            if metadata.initialized_at_type != "text" {
                return Err(StorageError::InvalidKeyMaterial(
                    "initialized timestamp must be text",
                ));
            }
            let initialized_at = metadata
                .initialized_at
                .as_deref()
                .ok_or(StorageError::InvalidKeyMaterial(
                    "initialized timestamp is missing",
                ))?
                .parse::<DateTime<Utc>>()
                .map_err(|_| {
                    StorageError::InvalidKeyMaterial("initialized timestamp must be RFC3339")
                })?;
            let telegram_bot_id = match metadata.bot_id_type.as_str() {
                "null" if metadata.telegram_bot_id.is_none() => None,
                "integer" => match metadata.telegram_bot_id {
                    Some(value) if value > 0 => Some(value),
                    _ => {
                        return Err(StorageError::InvalidKeyMaterial(
                            "Telegram bot id must be positive",
                        ));
                    }
                },
                _ => {
                    return Err(StorageError::InvalidKeyMaterial(
                        "Telegram bot id must be an integer",
                    ));
                }
            };
            Ok(MaterialState::Ready {
                initialized_at,
                telegram_bot_id,
            })
        }
        _ => Err(StorageError::InvalidKeyMaterial(
            "state must be PENDING or READY",
        )),
    }
}

async fn load_and_validate_ready_seed(
    connection: &mut SqliteConnection,
    key_version: i64,
) -> Result<SeedBuffer, StorageError> {
    let row =
        sqlx::query("SELECT master_seed, seed_checksum FROM key_material WHERE singleton = 1")
            .fetch_one(&mut *connection)
            .await?;
    let seed = SeedBuffer::from_vec(row.try_get::<Vec<u8>, _>("master_seed")?);
    let stored_checksum = row.try_get::<Vec<u8>, _>("seed_checksum")?;
    let expected_checksum = checksum(key_version, seed.as_slice())?;
    if !bool::from(expected_checksum.as_slice().ct_eq(&stored_checksum)) {
        return Err(StorageError::InvalidKeyMaterial(
            "master seed checksum does not match",
        ));
    }
    Ok(seed)
}

fn checksum(key_version: i64, seed: &[u8]) -> Result<[u8; 32], StorageError> {
    master_seed_checksum(key_version, seed).map_err(|error| match error {
        crate::installation::InstallationKeyError::UnsupportedVersion(version) => {
            StorageError::UnsupportedKeyVersion(version)
        }
        _ => StorageError::InvalidKeyMaterial("master seed is invalid"),
    })
}

#[cfg(test)]
mod tests {
    use secrecy::ExposeSecret;
    use sqlx::Row;

    use crate::storage::{connect, migrate};

    use super::*;

    struct FixedSource([u8; MASTER_SEED_LEN]);

    impl SeedSource for FixedSource {
        fn fill_seed(&mut self, destination: &mut [u8]) -> Result<(), StorageError> {
            destination.copy_from_slice(&self.0);
            Ok(())
        }
    }

    struct FailingSource;

    impl SeedSource for FailingSource {
        fn fill_seed(&mut self, _destination: &mut [u8]) -> Result<(), StorageError> {
            Err(StorageError::EntropyUnavailable)
        }
    }

    async fn test_pool() -> (tempfile::TempDir, SqlitePool) {
        let directory = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", directory.path().join("test.db").display());
        let pool = connect(&url).await.unwrap();
        migrate(&pool).await.unwrap();
        (directory, pool)
    }

    #[tokio::test]
    async fn entropy_failure_remains_pending() {
        let (_directory, pool) = test_pool().await;
        let now = "2026-08-30T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let error = load_or_initialize_master_seed_with(&pool, now, &mut FailingSource)
            .await
            .unwrap_err();
        assert!(matches!(error, StorageError::EntropyUnavailable));
        let row = sqlx::query(
            "SELECT state, master_seed, seed_checksum, initialized_at FROM key_material",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.get::<String, _>("state"), "PENDING");
        assert!(row.get::<Option<Vec<u8>>, _>("master_seed").is_none());
        assert!(row.get::<Option<Vec<u8>>, _>("seed_checksum").is_none());
        assert!(row.get::<Option<String>, _>("initialized_at").is_none());

        let loaded = load_or_initialize_master_seed_with(
            &pool,
            now,
            &mut FixedSource([7_u8; MASTER_SEED_LEN]),
        )
        .await
        .unwrap();
        assert_eq!(loaded.bytes.expose_secret(), &[7_u8; MASTER_SEED_LEN]);
    }

    #[tokio::test]
    async fn failed_first_transition_remains_pending() {
        let (_directory, pool) = test_pool().await;
        sqlx::query(
            "CREATE TRIGGER reject_seed_transition
             BEFORE UPDATE OF state ON key_material
             BEGIN SELECT RAISE(ABORT, 'rejected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();
        let now = "2026-08-30T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let result = load_or_initialize_master_seed_with(
            &pool,
            now,
            &mut FixedSource([9_u8; MASTER_SEED_LEN]),
        )
        .await;
        assert!(matches!(result, Err(StorageError::Database(_))));
        sqlx::query("DROP TRIGGER reject_seed_transition")
            .execute(&pool)
            .await
            .unwrap();
        let state: String = sqlx::query_scalar("SELECT state FROM key_material")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(state, "PENDING");
        load_or_initialize_master_seed_with(&pool, now, &mut FixedSource([9_u8; MASTER_SEED_LEN]))
            .await
            .unwrap();
    }
}
