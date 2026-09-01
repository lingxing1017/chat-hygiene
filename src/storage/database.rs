use std::str::FromStr;
use std::time::Duration;
use std::{
    fs,
    path::{Path, PathBuf},
};

use rustix::fs::{Access, AtFlags, CWD, accessat};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{SqlitePool, migrate::MigrateError};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] MigrateError),
    #[error("conversation was modified by another transaction")]
    ConcurrentModification,
    #[error("invalid persisted data: {0}")]
    InvalidData(String),
    #[error("operating-system entropy is unavailable")]
    EntropyUnavailable,
    #[error("installation key material is missing")]
    KeyMaterialMissing,
    #[error("invalid installation key material: {0}")]
    InvalidKeyMaterial(&'static str),
    #[error("unsupported installation key version {0}")]
    UnsupportedKeyVersion(i64),
    #[error("authenticated Telegram bot does not match this installation")]
    TelegramBotIdentityMismatch,
    #[error("owner identity is missing")]
    OwnerIdentityMissing,
    #[error("invalid owner identity: {0}")]
    InvalidOwnerIdentity(&'static str),
    #[error("owner identity is already claimed")]
    OwnerAlreadyClaimed,
    #[error("owner chat does not match the claimed owner")]
    OwnerChatMismatch,
    #[error("business connection candidate state is missing")]
    ConnectionCandidateStateMissing,
    #[error("invalid business connection candidate state: {0}")]
    InvalidConnectionCandidate(&'static str),
    #[error("invalid candidate guard state: {0}")]
    InvalidCandidateGuard(&'static str),
    #[error("Telegram reconciliation state is missing")]
    TelegramReconciliationStateMissing,
    #[error("invalid Telegram reconciliation state: {0}")]
    InvalidTelegramReconciliationState(&'static str),
    #[error("invalid service database configuration: {0}")]
    InvalidServiceDatabase(&'static str),
    #[error("orphan claim-code entry exists beside a missing database")]
    OrphanClaimArtifact,
}

#[allow(dead_code)]
pub(crate) struct ServiceDatabaseDescriptor {
    options: SqliteConnectOptions,
    database_path: PathBuf,
    database_existed: bool,
}

#[allow(dead_code)]
impl ServiceDatabaseDescriptor {
    pub(crate) fn parse(
        raw_url: &str,
        service_working_directory: &Path,
    ) -> Result<Self, StorageError> {
        reject_memory_url(raw_url)?;
        let working_directory = if service_working_directory.is_absolute() {
            service_working_directory.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|_| StorageError::InvalidServiceDatabase("working directory is invalid"))?
                .join(service_working_directory)
        };
        let working_metadata = fs::metadata(&working_directory)
            .map_err(|_| StorageError::InvalidServiceDatabase("working directory is unusable"))?;
        if !working_metadata.is_dir() {
            return Err(StorageError::InvalidServiceDatabase(
                "working directory is unusable",
            ));
        }
        let parsed = SqliteConnectOptions::from_str(raw_url)
            .map_err(|_| StorageError::InvalidServiceDatabase("database URL is invalid"))?;
        let filename = parsed.get_filename();
        if filename.as_os_str().is_empty() || filename.file_name().is_none() {
            return Err(StorageError::InvalidServiceDatabase(
                "database filename is missing",
            ));
        }
        let database_path = if filename.is_absolute() {
            filename.to_path_buf()
        } else {
            working_directory.join(filename)
        };
        let basename = database_path.file_name().and_then(|name| name.to_str());
        if matches!(basename, Some("claim-code" | ".claim-code.tmp")) {
            return Err(StorageError::InvalidServiceDatabase(
                "database filename collides with a claim artifact",
            ));
        }
        let parent = database_path
            .parent()
            .ok_or(StorageError::InvalidServiceDatabase(
                "database parent directory is missing",
            ))?;
        if !fs::metadata(parent).is_ok_and(|metadata| metadata.is_dir())
            || accessat(
                CWD,
                parent,
                Access::READ_OK | Access::WRITE_OK | Access::EXEC_OK,
                AtFlags::EACCESS,
            )
            .is_err()
        {
            return Err(StorageError::InvalidServiceDatabase(
                "database parent directory is unusable",
            ));
        }
        let database_existed = no_follow_exists(&database_path)?;
        if database_existed
            && fs::symlink_metadata(&database_path).is_ok_and(|metadata| metadata.is_symlink())
        {
            return Err(StorageError::InvalidServiceDatabase(
                "database path must not be a symlink",
            ));
        }
        if !database_existed && sibling_claim_artifact_exists(parent)? {
            return Err(StorageError::OrphanClaimArtifact);
        }
        let options = parsed
            .filename(&database_path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        Ok(Self {
            options,
            database_path,
            database_existed,
        })
    }

    pub(crate) fn database_path(&self) -> &Path {
        &self.database_path
    }

    pub(crate) async fn connect(&self) -> Result<SqlitePool, StorageError> {
        if !self.database_existed {
            if no_follow_exists(&self.database_path)? {
                return Err(StorageError::InvalidServiceDatabase(
                    "database path changed after preflight",
                ));
            }
            let parent =
                self.database_path
                    .parent()
                    .ok_or(StorageError::InvalidServiceDatabase(
                        "database parent directory is missing",
                    ))?;
            if sibling_claim_artifact_exists(parent)? {
                return Err(StorageError::OrphanClaimArtifact);
            }
        }
        Ok(SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(self.options.clone())
            .await?)
    }
}

#[allow(dead_code)]
fn reject_memory_url(raw_url: &str) -> Result<(), StorageError> {
    let normalized = raw_url.trim().to_ascii_lowercase();
    if matches!(
        normalized.as_str(),
        ":memory:" | "sqlite::memory:" | "sqlite://:memory:"
    ) {
        return Err(StorageError::InvalidServiceDatabase(
            "in-memory databases are unsupported",
        ));
    }
    if let Some(query) = raw_url.split_once('?').map(|(_, query)| query)
        && url::form_urlencoded::parse(query.as_bytes()).any(|(key, value)| {
            key.eq_ignore_ascii_case("mode") && value.eq_ignore_ascii_case("memory")
        })
    {
        return Err(StorageError::InvalidServiceDatabase(
            "in-memory databases are unsupported",
        ));
    }
    Ok(())
}

#[allow(dead_code)]
fn no_follow_exists(path: &Path) -> Result<bool, StorageError> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(StorageError::InvalidServiceDatabase(
            "database path cannot be inspected",
        )),
    }
}

#[allow(dead_code)]
fn sibling_claim_artifact_exists(parent: &Path) -> Result<bool, StorageError> {
    Ok(no_follow_exists(&parent.join("claim-code"))?
        || no_follow_exists(&parent.join(".claim-code.tmp"))?)
}

/// Opens the single-connection `SQLite` pool used by the MVP worker.
///
/// # Errors
///
/// Returns [`StorageError`] when the URL is invalid or `SQLite` cannot open the
/// database.
pub async fn connect(database_url: &str) -> Result<SqlitePool, StorageError> {
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));

    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await?)
}

/// Applies every embedded migration exactly once.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLx` cannot inspect or update the schema.
pub async fn migrate(pool: &SqlitePool) -> Result<(), StorageError> {
    sqlx::migrate!("./migrations").run(pool).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::symlink;

    use super::*;

    fn assert_database_artifacts_absent(directory: &Path) {
        for name in ["service.db", "service.db-wal", "service.db-shm"] {
            assert!(!directory.join(name).exists());
        }
    }

    #[test]
    fn service_descriptor_resolves_relative_and_absolute_file_urls() {
        let directory = tempfile::tempdir().unwrap();
        let relative =
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()).unwrap();
        assert_eq!(
            relative.database_path(),
            directory.path().join("service.db")
        );

        let absolute_path = directory.path().join("absolute.db");
        let absolute_url = format!("sqlite://{}", absolute_path.display());
        let absolute = ServiceDatabaseDescriptor::parse(&absolute_url, directory.path()).unwrap();
        assert_eq!(absolute.database_path(), absolute_path);
    }

    #[test]
    fn service_descriptor_rejects_memory_collisions_and_unusable_parents() {
        let directory = tempfile::tempdir().unwrap();
        for url in [
            ":memory:",
            "sqlite::memory:",
            "sqlite://:memory:",
            "sqlite://service.db?mode=memory",
            "sqlite://service.db?MODE=MEMORY",
            "sqlite://service.db?m%6fde=mem%6fry",
            "sqlite://service.db?m%6Fde=%6De%6Dory",
            "sqlite://service.db?x=1&mode=memory&mode=rwc",
            "sqlite://service.db?mode=rwc&mode=memory",
            "sqlite://claim-code",
            "sqlite://.claim-code.tmp",
            "sqlite://",
            "sqlite://missing/service.db",
        ] {
            let result = ServiceDatabaseDescriptor::parse(url, directory.path());
            assert!(result.is_err(), "unexpectedly accepted {url}");
            assert_database_artifacts_absent(directory.path());
        }

        let non_directory = directory.path().join("not-a-directory");
        fs::write(&non_directory, b"sentinel").unwrap();
        assert!(ServiceDatabaseDescriptor::parse("sqlite://service.db", &non_directory).is_err());
        assert_eq!(fs::read(&non_directory).unwrap(), b"sentinel");
        assert_database_artifacts_absent(directory.path());
    }

    #[test]
    fn orphan_claim_entries_prevent_database_creation_without_mutation() {
        let directory = tempfile::tempdir().unwrap();
        for name in ["claim-code", ".claim-code.tmp"] {
            let path = directory.path().join(name);
            fs::write(&path, b"sentinel").unwrap();
            let result = ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path());
            assert!(matches!(result, Err(StorageError::OrphanClaimArtifact)));
            assert_eq!(fs::read(&path).unwrap(), b"sentinel");
            assert_database_artifacts_absent(directory.path());
            fs::remove_file(path).unwrap();
        }

        let target = directory.path().join("claim-code");
        let temp = directory.path().join(".claim-code.tmp");
        fs::write(&target, b"target-sentinel").unwrap();
        fs::write(&temp, b"temp-sentinel").unwrap();
        assert!(matches!(
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()),
            Err(StorageError::OrphanClaimArtifact)
        ));
        assert_eq!(fs::read(&target).unwrap(), b"target-sentinel");
        assert_eq!(fs::read(&temp).unwrap(), b"temp-sentinel");
        assert_database_artifacts_absent(directory.path());

        fs::remove_file(&target).unwrap();
        fs::remove_file(&temp).unwrap();
        let outside = directory.path().join("outside");
        fs::write(&outside, b"outside-sentinel").unwrap();
        symlink(&outside, &target).unwrap();
        assert!(matches!(
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()),
            Err(StorageError::OrphanClaimArtifact)
        ));
        assert!(
            fs::symlink_metadata(&target)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(&outside).unwrap(), b"outside-sentinel");
        assert_database_artifacts_absent(directory.path());
    }

    #[tokio::test]
    async fn descriptor_rechecks_orphans_immediately_before_connect() {
        let directory = tempfile::tempdir().unwrap();
        let descriptor =
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()).unwrap();
        let orphan = directory.path().join("claim-code");
        fs::write(&orphan, b"race-sentinel").unwrap();
        assert!(matches!(
            descriptor.connect().await,
            Err(StorageError::OrphanClaimArtifact)
        ));
        assert_eq!(fs::read(orphan).unwrap(), b"race-sentinel");
        assert_database_artifacts_absent(directory.path());
    }

    #[tokio::test]
    async fn descriptor_rejects_database_creation_or_symlink_races() {
        let directory = tempfile::tempdir().unwrap();
        let descriptor =
            ServiceDatabaseDescriptor::parse("sqlite://service.db", directory.path()).unwrap();
        fs::write(directory.path().join("service.db"), b"race-database").unwrap();
        assert!(matches!(
            descriptor.connect().await,
            Err(StorageError::InvalidServiceDatabase(_))
        ));
        assert_eq!(
            fs::read(directory.path().join("service.db")).unwrap(),
            b"race-database"
        );
        assert!(!directory.path().join("service.db-wal").exists());
        assert!(!directory.path().join("service.db-shm").exists());

        fs::remove_file(directory.path().join("service.db")).unwrap();
        let outside = directory.path().join("outside-db");
        fs::write(&outside, b"outside-db-sentinel").unwrap();
        symlink(&outside, directory.path().join("service.db")).unwrap();
        assert!(matches!(
            descriptor.connect().await,
            Err(StorageError::InvalidServiceDatabase(_))
        ));
        assert_eq!(fs::read(outside).unwrap(), b"outside-db-sentinel");
    }
}
