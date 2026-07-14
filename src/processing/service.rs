use std::future::Future;
use std::pin::Pin;

use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::clock::Clock;
use crate::detection::SpamDetector;
use crate::events::{EventError, RecordReceipt, apply_recorded_event, record_prepared_event};
use crate::owner::{
    LabeledMessageBody, OwnerCommandError, OwnerCommandService, OwnerCommandSource,
    parse_owner_command,
};
use crate::storage::{
    ConversationKey, NewOutboxAction, OutboxActionKind, StorageError, UnitOfWork,
    enqueue_outbox_action,
};
use crate::telegram::{IngressError, RawBusinessEvent, RawEventKind, WebhookInbox};
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
        if raw.kind == RawEventKind::OwnerCommand {
            return self.process_owner_command(update_id, raw).await;
        }
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

    async fn process_owner_command(
        &mut self,
        update_id: i64,
        raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        let mut uow = UnitOfWork::begin(&self.pool).await?;
        if let Some(status) = sqlx::query_scalar::<_, String>(
            "SELECT status FROM processed_update WHERE update_id = ?",
        )
        .bind(update_id)
        .fetch_optional(uow.connection())
        .await
        .map_err(StorageError::from)?
        {
            uow.rollback().await?;
            return match status.as_str() {
                "APPLIED" => Ok(RecordReceipt::DuplicateApplied),
                "RECORDED" => Ok(RecordReceipt::DuplicateRecorded),
                _ => Err(ProcessingError::Event(EventError::InvalidStatus(status))),
            };
        }

        sqlx::query(
            "INSERT INTO processed_update
             (update_id, event_type, event_json, status, received_at, applied_at)
             VALUES (?, 'owner_command', ?, 'APPLIED', ?, ?)",
        )
        .bind(update_id)
        .bind(format!(
            "{{\"update_id\":{update_id},\"event_type\":\"owner_command\",\"facts\":{{\"kind\":\"OWNER_COMMAND\"}}}}"
        ))
        .bind(raw.occurred_at.to_rfc3339())
        .bind(raw.occurred_at.to_rfc3339())
        .execute(uow.connection())
        .await
        .map_err(StorageError::from)?;

        let snapshot = raw.owner_command.ok_or_else(|| {
            ProcessingError::InvalidEvent("owner command context is missing".to_owned())
        })?;
        let source = OwnerCommandSource {
            from_user_id: snapshot.from_user_id.unwrap_or_default(),
            private_chat: snapshot.private_chat,
            replied_sample: snapshot.replied_sample.map(|sample| LabeledMessageBody {
                body: sample.body,
                content_type: sample.content_type,
                source_chat_id: sample.source_chat_id,
                source_message_id: sample.source_message_id,
            }),
        };
        let service = OwnerCommandService::at(raw.occurred_at);
        let connection = match service.authorize(&source, &mut uow).await {
            Ok(connection) => connection,
            Err(OwnerCommandError::Unauthorized) => {
                uow.commit().await?;
                return Ok(RecordReceipt::Recorded);
            }
            Err(error) => return Err(ProcessingError::InvalidEvent(error.to_string())),
        };
        let response = match parse_owner_command(&snapshot.text) {
            Ok(command) => match service
                .execute_authorized(command, source, &connection, &mut uow)
                .await
            {
                Ok(response) => response,
                Err(OwnerCommandError::Storage(error)) => {
                    return Err(ProcessingError::InvalidEvent(error));
                }
                Err(error) => format!("error={error}"),
            },
            Err(error) => format!("error={error}"),
        };
        let owner_chat_id = raw.chat_id.ok_or_else(|| {
            ProcessingError::InvalidEvent("owner command chat ID is missing".to_owned())
        })?;
        enqueue_outbox_action(
            &mut uow,
            &NewOutboxAction {
                source_update_id: update_id,
                key: Some(ConversationKey::new(
                    connection.connection_id,
                    owner_chat_id,
                )),
                kind: OutboxActionKind::SendOwnerMessage,
                payload_json: serde_json::json!({"message": response}).to_string(),
                idempotency_key: format!("{update_id}:OWNER_COMMAND_REPLY"),
                created_at: raw.occurred_at,
            },
        )
        .await?;
        uow.commit().await?;
        Ok(RecordReceipt::Recorded)
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
