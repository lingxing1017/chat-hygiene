use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use sqlx::SqlitePool;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval};

use crate::clock::Clock;
use crate::detection::SpamDetector;
use crate::events::{EventError, RecordReceipt, apply_recorded_event, record_prepared_event};
use crate::owner::{
    LabeledMessageBody, OwnerCommandError, OwnerCommandService, OwnerCommandSource,
    OwnerTelegramAction, parse_owner_command,
};
use crate::retention::RetentionService;
use crate::storage::{
    ConversationKey, NewOutboxAction, OutboxActionKind, OwnerIdentity, StorageError, UnitOfWork,
    enqueue_outbox_action, find_business_connection, find_conversation,
    list_outbox_actions_for_update,
};
use crate::telegram::{
    IngressError, RawBusinessEvent, RawEventKind, WebhookInbox, delete_message_batches,
};
use crate::verification::ChallengeVerifier;

use super::handler::LifecycleHandler;
use super::notifications::{NewContactNotice, NewContactNotifier, NoopNewContactNotifier};
use super::preparer::{EventPreparer, OwnerLifecycleState, load_owner_lifecycle_state};
use super::trace::ProcessingTrace;

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
    retention: RetentionService<C>,
    handler: LifecycleHandler,
    default_destructive_mode: bool,
    new_contact_notifier: Arc<dyn NewContactNotifier>,
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
    pub fn new(pool: SqlitePool, detector: D, verifier: V, clock: C, destructive_mode: bool) -> Self
    where
        C: Clone,
    {
        Self {
            pool,
            preparer: EventPreparer::new(detector, verifier, clock.clone(), destructive_mode),
            retention: RetentionService::new(clock),
            handler: LifecycleHandler,
            default_destructive_mode: destructive_mode,
            new_contact_notifier: Arc::new(NoopNewContactNotifier),
        }
    }

    #[must_use]
    pub fn with_new_contact_notifier<N>(mut self, notifier: N) -> Self
    where
        N: NewContactNotifier + 'static,
    {
        self.new_contact_notifier = Arc::new(notifier);
        self
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
        let first_contact_notice = first_contact_notice(&raw, &mut read).await?;
        let prepared = self.preparer.prepare(update_id, raw, &mut read).await?;
        read.rollback().await?;

        let receipt = record_prepared_event(&self.pool, &prepared).await?;
        if receipt == RecordReceipt::DuplicateApplied {
            return Ok(receipt);
        }
        apply_recorded_event(&self.pool, update_id, &self.handler).await?;
        if let Some(notice) = first_contact_notice {
            let owner_user_id = notice.owner_user_id;
            let contact_chat_id = notice.contact_chat_id;
            if self.new_contact_notifier.try_notify(notice).is_err() {
                tracing::warn!(
                    owner_user_id,
                    contact_chat_id,
                    "new-contact notification was not queued"
                );
            }
        }
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
        let trace_enabled = dry_run_enabled(&mut uow, self.default_destructive_mode).await?;

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
        let service = OwnerCommandService::at(raw.occurred_at)
            .with_default_destructive_mode(self.default_destructive_mode);
        let connection = match service.authorize(&source, &mut uow).await {
            Ok(connection) => connection,
            Err(OwnerCommandError::Unauthorized) => {
                uow.commit().await?;
                return Ok(RecordReceipt::Recorded);
            }
            Err(error) => return Err(ProcessingError::InvalidEvent(error.to_string())),
        };
        let (response, telegram_actions) = match parse_owner_command(&snapshot.text) {
            Ok(command) => match service
                .execute_authorized(command, source, &connection, &mut uow)
                .await
            {
                Ok(execution) => (execution.response, execution.telegram_actions),
                Err(OwnerCommandError::Storage(error)) => {
                    return Err(ProcessingError::InvalidEvent(error));
                }
                Err(error) => (format!("error={error}"), Vec::new()),
            },
            Err(error) => (format!("error={error}"), Vec::new()),
        };
        enqueue_owner_telegram_actions(&mut uow, update_id, telegram_actions, raw.occurred_at)
            .await?;
        let owner_chat_id = raw.chat_id.ok_or_else(|| {
            ProcessingError::InvalidEvent("owner command chat ID is missing".to_owned())
        })?;
        let owner_message_id = raw.message_id;
        let owner_key = ConversationKey::new(connection.connection_id, owner_chat_id);
        enqueue_owner_command_reply(&mut uow, update_id, &owner_key, &response, raw.occurred_at)
            .await?;
        if trace_enabled {
            enqueue_owner_command_trace(
                &mut uow,
                update_id,
                owner_key,
                owner_chat_id,
                owner_message_id,
                raw.occurred_at,
            )
            .await?;
        }
        uow.commit().await?;
        Ok(RecordReceipt::Recorded)
    }
}

async fn enqueue_owner_command_reply(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    owner_key: &ConversationKey,
    response: &str,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: Some(owner_key.clone()),
            kind: OutboxActionKind::SendOwnerMessage,
            payload_json: serde_json::json!({"message": response}).to_string(),
            idempotency_key: format!("{update_id}:OWNER_COMMAND_REPLY"),
            created_at: occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn enqueue_owner_telegram_actions(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    actions: Vec<OwnerTelegramAction>,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    for action in actions {
        match action {
            OwnerTelegramAction::DeleteBusinessMessages { key, message_ids } => {
                for (batch_index, batch) in
                    delete_message_batches(&message_ids).into_iter().enumerate()
                {
                    enqueue_outbox_action(
                        uow,
                        &NewOutboxAction {
                            source_update_id: update_id,
                            key: Some(key.clone()),
                            kind: OutboxActionKind::DeleteBusinessMessages,
                            payload_json: serde_json::json!({"message_ids": batch}).to_string(),
                            idempotency_key: format!(
                                "{update_id}:DELETE_BUSINESS_MESSAGES:{}:{}:{batch_index}",
                                key.connection_id, key.chat_id
                            ),
                            created_at: occurred_at,
                        },
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn enqueue_owner_command_trace(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    owner_key: ConversationKey,
    owner_chat_id: i64,
    owner_message_id: Option<i64>,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    let source_actions = list_outbox_actions_for_update(uow, update_id).await?;
    let trace =
        ProcessingTrace::owner_command(update_id, owner_chat_id, owner_message_id, &source_actions);
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: Some(owner_key),
            kind: OutboxActionKind::SendOwnerMessage,
            payload_json: serde_json::json!({
                "message": trace.render(),
                "owner_chat_id": owner_chat_id,
            })
            .to_string(),
            idempotency_key: format!("{update_id}:DRY_RUN_TRACE"),
            created_at: occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn dry_run_enabled(
    uow: &mut UnitOfWork<'_>,
    default_destructive_mode: bool,
) -> Result<bool, ProcessingError> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_optional(uow.connection())
            .await
            .map_err(StorageError::from)?;
    Ok(!value
        .as_deref()
        .map_or(default_destructive_mode, |value| value == "true"))
}

async fn first_contact_notice(
    raw: &RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<Option<NewContactNotice>, ProcessingError> {
    if raw.kind != RawEventKind::InboundMessage {
        return Ok(None);
    }
    let (Some(connection_id), Some(contact_chat_id)) = (raw.connection_id.as_deref(), raw.chat_id)
    else {
        return Ok(None);
    };
    let Some(connection) = find_business_connection(uow, connection_id).await? else {
        return Ok(None);
    };
    let owner_chat_id = match load_owner_lifecycle_state(uow).await? {
        OwnerLifecycleState::Pending => connection.owner_user_id,
        OwnerLifecycleState::Ready(OwnerIdentity::Claimed {
            owner_user_id,
            owner_chat_id,
            ..
        }) if owner_user_id == connection.owner_user_id => owner_chat_id,
        OwnerLifecycleState::Ready(_) => return Ok(None),
    };
    let key = ConversationKey::new(connection_id, contact_chat_id);
    if find_conversation(uow, &key).await?.is_some() {
        return Ok(None);
    }
    Ok(Some(NewContactNotice {
        owner_user_id: connection.owner_user_id,
        owner_chat_id,
        contact_chat_id,
        username: raw.contact_username.clone(),
    }))
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
        let mut expiry_tick = interval(Duration::from_secs(15));
        expiry_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut purge_tick = interval(Duration::from_hours(1));
        purge_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                item = receiver.recv() => {
                    let Some(item) = item else {
                        break;
                    };
                    let result = engine.process(item.update_id, item.raw).await;
                    let _ = item.receipt.send(result);
                }
                _ = expiry_tick.tick() => {
                    if let Err(error) = engine.retention.expire_due_state(&engine.pool).await {
                        tracing::error!(%error, "state expiry pass failed");
                    }
                }
                _ = purge_tick.tick() => {
                    if let Err(error) = engine.retention.purge_history(&engine.pool).await {
                        tracing::error!(%error, "history retention pass failed");
                    }
                }
            }
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
