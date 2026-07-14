use chrono::Utc;
use sqlx::SqlitePool;

use crate::storage::{StorageError, UnitOfWork};

use super::models::{ApplyReceipt, PreparedEvent};
use super::recorder::EventError;

#[allow(async_fn_in_trait)]
pub trait EventApplier: Send + Sync {
    /// Applies only derived database state through the supplied transaction.
    ///
    /// # Errors
    ///
    /// Returns [`EventError`] when the event cannot be applied. The caller will
    /// roll back the transaction and leave the event recoverable.
    async fn apply(
        &self,
        event: &PreparedEvent,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError>;
}

/// Applies one recorded event and marks it applied in the same transaction.
///
/// # Errors
///
/// Returns [`EventError`] when the event is missing, malformed, in an invalid
/// state, or cannot be applied atomically.
pub async fn apply_recorded_event<A: EventApplier>(
    pool: &SqlitePool,
    update_id: i64,
    applier: &A,
) -> Result<ApplyReceipt, EventError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    let row: Option<(String, String)> =
        sqlx::query_as("SELECT event_json, status FROM processed_update WHERE update_id = ?")
            .bind(update_id)
            .fetch_optional(uow.connection())
            .await
            .map_err(StorageError::from)?;
    let Some((event_json, status)) = row else {
        uow.rollback().await?;
        return Err(EventError::MissingUpdate(update_id));
    };

    if status == "APPLIED" {
        uow.rollback().await?;
        return Ok(ApplyReceipt::AlreadyApplied);
    }
    if status != "RECORDED" {
        uow.rollback().await?;
        return Err(EventError::InvalidStatus(status));
    }

    let event: PreparedEvent = serde_json::from_str(&event_json)?;
    if let Err(error) = applier.apply(&event, &mut uow).await {
        uow.rollback().await?;
        return Err(error);
    }

    let result = sqlx::query(
        "UPDATE processed_update
         SET status = 'APPLIED', applied_at = ?, error_code = NULL,
             error_message = NULL
         WHERE update_id = ? AND status = 'RECORDED'",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(update_id)
    .execute(uow.connection())
    .await
    .map_err(StorageError::from)?;
    if result.rows_affected() != 1 {
        uow.rollback().await?;
        return Err(EventError::InvalidStatus(
            "changed during application".to_owned(),
        ));
    }
    uow.commit().await?;
    Ok(ApplyReceipt::Applied)
}

/// Replays every event left recorded by an interrupted worker in update order.
///
/// # Errors
///
/// Returns [`EventError`] when recorded events cannot be listed or when any
/// event fails to apply. Successfully committed earlier events remain applied.
pub async fn recover_recorded_events<A: EventApplier>(
    pool: &SqlitePool,
    applier: &A,
) -> Result<usize, EventError> {
    let update_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT update_id FROM processed_update
         WHERE status = 'RECORDED' ORDER BY update_id",
    )
    .fetch_all(pool)
    .await
    .map_err(StorageError::from)?;

    let mut recovered_count = 0;
    for update_id in update_ids {
        if apply_recorded_event(pool, update_id, applier).await? == ApplyReceipt::Applied {
            recovered_count += 1;
        }
    }
    Ok(recovered_count)
}
