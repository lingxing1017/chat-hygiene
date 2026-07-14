use std::future::Future;
use std::pin::Pin;

use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::clock::Clock;
use crate::detection::SpamDetector;
use crate::events::{EventError, RecordReceipt, apply_recorded_event, record_prepared_event};
use crate::storage::{StorageError, UnitOfWork};
use crate::telegram::{IngressError, RawBusinessEvent, WebhookInbox};
use crate::verification::ChallengeVerifier;

use super::handler::LifecycleHandler;
use super::preparer::EventPreparer;

#[derive(Debug, Error)]
pub enum ProcessingError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("invalid raw Business event: {0}")]
    InvalidEvent(String),
}

pub struct ProcessingEngine<D, V, C> {
    pool: SqlitePool,
    preparer: EventPreparer<D, V, C>,
    handler: LifecycleHandler,
}

struct WorkItem {
    update_id: i64,
    raw: RawBusinessEvent,
    receipt: oneshot::Sender<Result<RecordReceipt, ProcessingError>>,
}

#[derive(Clone)]
pub struct ProcessingHandle {
    sender: mpsc::Sender<WorkItem>,
}

impl<D, V, C> ProcessingEngine<D, V, C>
where
    D: SpamDetector,
    V: ChallengeVerifier,
    C: Clock,
{
    #[must_use]
    pub fn new(
        pool: SqlitePool,
        detector: D,
        verifier: V,
        clock: C,
        destructive_mode: bool,
    ) -> Self {
        Self {
            pool,
            preparer: EventPreparer::new(detector, verifier, clock, destructive_mode),
            handler: LifecycleHandler,
        }
    }

    /// Serially prepares, records, and atomically applies one raw update.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessingError`] when preparation, durable recording, or
    /// transactional application fails.
    pub async fn process(
        &mut self,
        update_id: i64,
        raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        let mut read = UnitOfWork::begin(&self.pool).await?;
        let prepared = self.preparer.prepare(update_id, raw, &mut read).await?;
        read.rollback().await?;

        let receipt = record_prepared_event(&self.pool, &prepared).await?;
        if receipt == RecordReceipt::DuplicateApplied {
            return Ok(receipt);
        }
        apply_recorded_event(&self.pool, update_id, &self.handler).await?;
        Ok(receipt)
    }
}

/// Starts the single bounded lifecycle worker used by the MVP.
#[must_use]
pub fn spawn_processing_worker<D, V, C>(
    mut engine: ProcessingEngine<D, V, C>,
    capacity: usize,
) -> ProcessingHandle
where
    D: SpamDetector + 'static,
    V: ChallengeVerifier + 'static,
    C: Clock + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<WorkItem>(capacity.max(1));
    tokio::spawn(async move {
        while let Some(item) = receiver.recv().await {
            let result = engine.process(item.update_id, item.raw).await;
            let _ = item.receipt.send(result);
        }
    });
    ProcessingHandle { sender }
}

impl WebhookInbox for ProcessingHandle {
    fn submit(
        &self,
        update_id: i64,
        event: RawBusinessEvent,
    ) -> Pin<Box<dyn Future<Output = Result<RecordReceipt, IngressError>> + Send + '_>> {
        Box::pin(async move {
            let (receipt, receiver) = oneshot::channel();
            self.sender
                .send(WorkItem {
                    update_id,
                    raw: event,
                    receipt,
                })
                .await
                .map_err(|_| {
                    IngressError::RecordingFailed("processing worker stopped".to_owned())
                })?;
            receiver
                .await
                .map_err(|_| {
                    IngressError::RecordingFailed("processing receipt was dropped".to_owned())
                })?
                .map_err(|error| IngressError::RecordingFailed(error.to_string()))
        })
    }
}
