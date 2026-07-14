use std::str::FromStr;
use std::time::Duration;

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
