use sqlx::{Sqlite, SqliteConnection, SqlitePool, Transaction};

use super::StorageError;

pub struct UnitOfWork<'a> {
    transaction: Transaction<'a, Sqlite>,
}

impl<'a> UnitOfWork<'a> {
    /// Starts one atomic storage operation.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when `SQLite` cannot begin the transaction.
    pub async fn begin(pool: &'a SqlitePool) -> Result<Self, StorageError> {
        Ok(Self {
            transaction: pool.begin().await?,
        })
    }

    /// Commits every write made through this unit of work.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when `SQLite` cannot commit.
    pub async fn commit(self) -> Result<(), StorageError> {
        self.transaction.commit().await?;
        Ok(())
    }

    /// Rolls back every write made through this unit of work.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError`] when `SQLite` cannot roll back.
    pub async fn rollback(self) -> Result<(), StorageError> {
        self.transaction.rollback().await?;
        Ok(())
    }

    pub(crate) fn connection(&mut self) -> &mut SqliteConnection {
        &mut self.transaction
    }
}
