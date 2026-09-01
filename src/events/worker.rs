use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use chrono::Utc;
use sqlx::SqlitePool;

use crate::runtime_workers::WorkerTask;
use crate::storage::{StorageError, UnitOfWork};
use crate::telegram::{BusinessApi, DispatchOutcome, OutboxDispatcher};

use super::models::{ApplyReceipt, PreparedEvent};
use super::recorder::EventError;

pub trait EventApplier: Send + Sync {
    /// Applies only derived database state through the supplied transaction.
    ///
    /// # Errors
    ///
    /// Returns [`EventError`] when the event cannot be applied. The caller will
    /// roll back the transaction and leave the event recoverable.
    fn apply<'a>(
        &'a self,
        event: &'a PreparedEvent,
        uow: &'a mut UnitOfWork<'_>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EventError>> + Send + 'a>>;
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
    let rows: Vec<(i64, String, String)> = sqlx::query_as(
        "SELECT update_id, event_type, event_json FROM processed_update
         WHERE status = 'RECORDED' ORDER BY update_id",
    )
    .fetch_all(pool)
    .await
    .map_err(StorageError::from)?;

    let mut recovered_count = 0;
    for (update_id, event_type, event_json) in rows {
        if is_authoritative_connection_trigger(&event_type, &event_json)? {
            continue;
        }
        if apply_recorded_event(pool, update_id, applier).await? == ApplyReceipt::Applied {
            recovered_count += 1;
        }
    }
    Ok(recovered_count)
}

/// Replays only pre-authoritative Business connection facts from an older
/// schema generation so Owner import can observe their provisional state.
///
/// # Errors
///
/// Returns [`EventError`] when recorded events cannot be decoded or applied.
pub async fn recover_legacy_recorded_connection_events<A: EventApplier>(
    pool: &SqlitePool,
    applier: &A,
) -> Result<usize, EventError> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT update_id, event_json FROM processed_update
         WHERE status = 'RECORDED' ORDER BY update_id",
    )
    .fetch_all(pool)
    .await
    .map_err(StorageError::from)?;

    let mut recovered_count = 0;
    for (update_id, event_json) in rows {
        let event: serde_json::Value = serde_json::from_str(&event_json)?;
        if event
            .pointer("/facts/action/kind")
            .and_then(serde_json::Value::as_str)
            != Some("CONNECTION_CHANGED")
        {
            continue;
        }
        if apply_recorded_event(pool, update_id, applier).await? == ApplyReceipt::Applied {
            recovered_count += 1;
        }
    }
    Ok(recovered_count)
}

fn is_authoritative_connection_trigger(
    event_type: &str,
    event_json: &str,
) -> Result<bool, EventError> {
    if event_type != "business_connection_changed" {
        return Ok(false);
    }
    let event: serde_json::Value = serde_json::from_str(event_json)?;
    Ok(event
        .pointer("/facts/action/kind")
        .and_then(serde_json::Value::as_str)
        == Some("IGNORE"))
}

#[must_use = "the outbox worker must remain owned until it is shut down"]
pub struct OutboxWorker {
    task: Option<WorkerTask>,
}

impl OutboxWorker {
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take()
            && task.stop_and_wait().await.is_err()
        {
            tracing::warn!(
                error_kind = "worker_shutdown",
                "outbox worker shutdown failed"
            );
        }
    }

    pub(crate) fn into_task(mut self) -> WorkerTask {
        self.task.take().expect("outbox worker task is owned")
    }
}

/// Starts the single polling worker that drains due Telegram outbox actions.
pub fn spawn_outbox_worker<C: BusinessApi + 'static>(
    dispatcher: OutboxDispatcher<C>,
    pool: SqlitePool,
    poll_interval: Duration,
) -> OutboxWorker {
    let (stop, mut stopped) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(poll_interval);
        'worker: loop {
            tokio::select! {
                biased;
                result = stopped.changed() => {
                    if result.is_err() || *stopped.borrow() {
                        break;
                    }
                }
                _ = ticker.tick() => {}
            }
            loop {
                if *stopped.borrow() {
                    break 'worker;
                }
                match dispatcher.dispatch_next(Utc::now(), &pool).await {
                    Ok(DispatchOutcome::Idle) => break,
                    Ok(_) => {}
                    Err(error) => {
                        tracing::error!(
                            error_code = "outbox_dispatch_failed",
                            error = %error,
                            "outbox dispatch paused until the next poll"
                        );
                        break;
                    }
                }
            }
        }
    });
    OutboxWorker {
        task: Some(WorkerTask::new(stop, task)),
    }
}
