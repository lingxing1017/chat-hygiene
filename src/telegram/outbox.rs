use chrono::{DateTime, Duration, Utc};
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use thiserror::Error;

use crate::storage::{
    BusinessConnectionRecord, ChallengeRecord, ConversationKey, NewOutboxAction, OutboxActionKind,
    OutboxActionRecord, StorageError, UnitOfWork, claim_due_outbox_action,
    disable_business_connection, enqueue_outbox_action, find_business_connection,
    find_challenge_by_id, mark_challenge_sent, mark_challenge_uncertain,
    mark_outbox_permanent_failure, mark_outbox_retry, mark_outbox_succeeded, mark_outbox_uncertain,
};

use super::client::{
    BusinessApi, DeleteAction, EditAction, ReadAction, SendAction, SentMessage, TelegramError,
};

const RETRY_SECONDS: [i64; 6] = [1, 2, 4, 8, 16, 30];

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchOutcome {
    Idle,
    Succeeded {
        action_id: i64,
    },
    RetryScheduled {
        action_id: i64,
        next_attempt_at: DateTime<Utc>,
    },
    Uncertain {
        action_id: i64,
    },
    PermanentFailure {
        action_id: i64,
    },
}

#[derive(Debug, Error)]
pub enum DispatchError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub struct OutboxDispatcher<C> {
    client: C,
}

impl<C: BusinessApi> OutboxDispatcher<C> {
    #[must_use]
    pub const fn new(client: C) -> Self {
        Self { client }
    }

    /// Dispatches the oldest due action, if any.
    ///
    /// # Errors
    ///
    /// Returns [`DispatchError`] only for local persistence failures. Telegram
    /// failures are converted into durable retry or terminal outcomes.
    pub async fn dispatch_next(
        &self,
        now: DateTime<Utc>,
        pool: &SqlitePool,
    ) -> Result<DispatchOutcome, DispatchError> {
        let Some(action) = claim(pool, now).await? else {
            return Ok(DispatchOutcome::Idle);
        };

        if action.interrupted && !is_idempotent(action.kind) {
            return if action.kind == OutboxActionKind::SendChallenge {
                self.finish_uncertain(pool, &action, "interrupted_send", now)
                    .await
            } else {
                finish_action_uncertain(pool, &action, "interrupted_send", now).await
            };
        }

        match self.execute(pool, &action).await {
            Ok(success) => finish_success(pool, &action, success, now).await,
            Err(ActionFailure::UncertainDependency { challenge_id }) => {
                finish_uncertain(
                    pool,
                    &action,
                    Some(challenge_id),
                    "uncertain_prompt_dependency",
                    now,
                )
                .await
            }
            Err(ActionFailure::RightsUnavailable) => {
                finish_permanent(pool, &action, "business_rights_unavailable", true, now).await
            }
            Err(ActionFailure::Invalid(error_code)) => {
                finish_permanent(pool, &action, error_code, false, now).await
            }
            Err(ActionFailure::Storage(error)) => Err(error.into()),
            Err(ActionFailure::Telegram(error)) => {
                self.handle_telegram_failure(pool, &action, &error, now)
                    .await
            }
        }
    }

    async fn execute(
        &self,
        pool: &SqlitePool,
        action: &OutboxActionRecord,
    ) -> Result<ActionSuccess, ActionFailure> {
        match action.kind {
            OutboxActionKind::SendChallenge => {
                let key = required_key(action)?;
                ensure_right(pool, &key.connection_id, RequiredRight::Reply).await?;
                let payload: ChallengePayload = parse_payload(action)?;
                let challenge = load_challenge(pool, payload.challenge_id).await?;
                let sent = self
                    .client
                    .send_business_message(&SendAction {
                        business_connection_id: Some(key.connection_id.clone()),
                        chat_id: key.chat_id,
                        text: challenge_prompt(&challenge),
                    })
                    .await?;
                Ok(ActionSuccess::ChallengeSent {
                    challenge_id: challenge.id,
                    sent,
                })
            }
            OutboxActionKind::EditChallenge => {
                let key = required_key(action)?;
                ensure_right(pool, &key.connection_id, RequiredRight::Reply).await?;
                let payload: EditPayload = parse_payload(action)?;
                let challenge = load_challenge(pool, payload.challenge_id).await?;
                let Some(message_id) = challenge.prompt_message_id else {
                    if challenge.delivery_status == "UNCERTAIN" {
                        return Err(ActionFailure::UncertainDependency {
                            challenge_id: challenge.id,
                        });
                    }
                    return Err(ActionFailure::Invalid("challenge_prompt_missing"));
                };
                self.client
                    .edit_business_message(&EditAction {
                        business_connection_id: key.connection_id.clone(),
                        chat_id: key.chat_id,
                        message_id,
                        text: challenge_status_text(&challenge, &payload),
                    })
                    .await?;
                Ok(ActionSuccess::Plain)
            }
            OutboxActionKind::ReadBusinessMessage => {
                let key = required_key(action)?;
                ensure_right(pool, &key.connection_id, RequiredRight::Read).await?;
                let payload: ReadPayload = parse_payload(action)?;
                self.client
                    .read_business_message(&ReadAction {
                        business_connection_id: key.connection_id.clone(),
                        chat_id: key.chat_id,
                        message_id: payload.message_id,
                    })
                    .await?;
                Ok(ActionSuccess::Plain)
            }
            OutboxActionKind::DeleteBusinessMessages => {
                let key = required_key(action)?;
                ensure_right(pool, &key.connection_id, RequiredRight::DeleteAll).await?;
                let payload: DeletePayload = parse_payload(action)?;
                self.client
                    .delete_business_messages(&DeleteAction {
                        business_connection_id: key.connection_id.clone(),
                        message_ids: payload.message_ids,
                    })
                    .await?;
                Ok(ActionSuccess::Plain)
            }
            OutboxActionKind::SendOwnerMessage => {
                let key = required_key(action)?;
                let connection = load_connection(pool, &key.connection_id)
                    .await?
                    .ok_or(ActionFailure::RightsUnavailable)?;
                let payload: OwnerAlertPayload = parse_payload(action)?;
                self.client
                    .send_business_message(&SendAction {
                        business_connection_id: None,
                        chat_id: connection.owner_user_id,
                        text: owner_alert_text(&payload),
                    })
                    .await?;
                Ok(ActionSuccess::Plain)
            }
            OutboxActionKind::ProposedDestructiveAction => Ok(ActionSuccess::Plain),
        }
    }

    async fn handle_telegram_failure(
        &self,
        pool: &SqlitePool,
        action: &OutboxActionRecord,
        error: &TelegramError,
        now: DateTime<Utc>,
    ) -> Result<DispatchOutcome, DispatchError> {
        if already_applied(action.kind, error) {
            return finish_success(pool, action, ActionSuccess::Plain, now).await;
        }
        if invalid_connection_or_right(error) {
            return finish_permanent(pool, action, error_code(error), true, now).await;
        }
        if matches!(error, TelegramError::Timeout) && !is_idempotent(action.kind) {
            return if action.kind == OutboxActionKind::SendChallenge {
                self.finish_uncertain(pool, action, error_code(error), now)
                    .await
            } else {
                finish_action_uncertain(pool, action, error_code(error), now).await
            };
        }
        if let Some(delay) = retry_delay(error, action.attempts) {
            let next_attempt_at = now + delay;
            let mut uow = UnitOfWork::begin(pool).await?;
            mark_outbox_retry(&mut uow, action.id, next_attempt_at, error_code(error), now).await?;
            uow.commit().await?;
            return Ok(DispatchOutcome::RetryScheduled {
                action_id: action.id,
                next_attempt_at,
            });
        }
        finish_permanent(pool, action, error_code(error), false, now).await
    }

    async fn finish_uncertain(
        &self,
        pool: &SqlitePool,
        action: &OutboxActionRecord,
        error_code: &str,
        now: DateTime<Utc>,
    ) -> Result<DispatchOutcome, DispatchError> {
        let challenge_id = challenge_id(action)?;
        finish_uncertain(pool, action, Some(challenge_id), error_code, now).await
    }
}

#[derive(Debug)]
enum ActionSuccess {
    Plain,
    ChallengeSent {
        challenge_id: i64,
        sent: SentMessage,
    },
}

#[derive(Debug)]
enum ActionFailure {
    Telegram(TelegramError),
    Storage(StorageError),
    RightsUnavailable,
    UncertainDependency { challenge_id: i64 },
    Invalid(&'static str),
}

impl From<TelegramError> for ActionFailure {
    fn from(error: TelegramError) -> Self {
        Self::Telegram(error)
    }
}

impl From<StorageError> for ActionFailure {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<serde_json::Error> for ActionFailure {
    fn from(_error: serde_json::Error) -> Self {
        Self::Invalid("invalid_action_payload")
    }
}

#[derive(Debug, Deserialize)]
struct ChallengePayload {
    challenge_id: i64,
}

#[derive(Debug, Deserialize)]
struct EditPayload {
    challenge_id: i64,
    status: String,
    attempts_remaining: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct ReadPayload {
    message_id: i64,
}

#[derive(Debug, Deserialize)]
struct DeletePayload {
    message_ids: Vec<i64>,
}

#[derive(Debug, Deserialize)]
struct OwnerAlertPayload {
    alert: Option<String>,
    message: Option<String>,
    challenge_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct StoredRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

#[derive(Debug, Clone, Copy)]
enum RequiredRight {
    Reply,
    Read,
    DeleteAll,
}

async fn claim(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<Option<OutboxActionRecord>, StorageError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    let action = claim_due_outbox_action(&mut uow, now).await?;
    uow.commit().await?;
    Ok(action)
}

async fn finish_success(
    pool: &SqlitePool,
    action: &OutboxActionRecord,
    success: ActionSuccess,
    now: DateTime<Utc>,
) -> Result<DispatchOutcome, DispatchError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    mark_outbox_succeeded(&mut uow, action.id, now).await?;
    if let ActionSuccess::ChallengeSent { challenge_id, sent } = success {
        mark_challenge_sent(&mut uow, challenge_id, sent.message_id).await?;
    }
    uow.commit().await?;
    Ok(DispatchOutcome::Succeeded {
        action_id: action.id,
    })
}

async fn finish_uncertain(
    pool: &SqlitePool,
    action: &OutboxActionRecord,
    challenge_id: Option<i64>,
    error_code: &str,
    now: DateTime<Utc>,
) -> Result<DispatchOutcome, DispatchError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    mark_outbox_uncertain(&mut uow, action.id, error_code, now).await?;
    if let Some(challenge_id) = challenge_id {
        mark_challenge_uncertain(&mut uow, challenge_id).await?;
    }
    enqueue_owner_alert(
        &mut uow,
        action,
        "challenge_send_uncertain",
        challenge_id,
        now,
    )
    .await?;
    uow.commit().await?;
    Ok(DispatchOutcome::Uncertain {
        action_id: action.id,
    })
}

async fn finish_permanent(
    pool: &SqlitePool,
    action: &OutboxActionRecord,
    error_code: &str,
    disable_connection: bool,
    now: DateTime<Utc>,
) -> Result<DispatchOutcome, DispatchError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    mark_outbox_permanent_failure(&mut uow, action.id, error_code, now).await?;
    if disable_connection {
        if let Some(key) = &action.key {
            disable_business_connection(&mut uow, &key.connection_id, now).await?;
        }
        force_dry_run(&mut uow, now).await?;
        enqueue_owner_alert(&mut uow, action, "business_rights_unavailable", None, now).await?;
    }
    uow.commit().await?;
    Ok(DispatchOutcome::PermanentFailure {
        action_id: action.id,
    })
}

async fn force_dry_run(uow: &mut UnitOfWork<'_>, now: DateTime<Utc>) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', 'false', ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value,
           updated_at = excluded.updated_at",
    )
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(())
}

async fn finish_action_uncertain(
    pool: &SqlitePool,
    action: &OutboxActionRecord,
    error_code: &str,
    now: DateTime<Utc>,
) -> Result<DispatchOutcome, DispatchError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    mark_outbox_uncertain(&mut uow, action.id, error_code, now).await?;
    uow.commit().await?;
    Ok(DispatchOutcome::Uncertain {
        action_id: action.id,
    })
}

async fn enqueue_owner_alert(
    uow: &mut UnitOfWork<'_>,
    action: &OutboxActionRecord,
    alert: &str,
    challenge_id: Option<i64>,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    let Some(key) = action.key.clone() else {
        return Ok(());
    };
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: action.source_update_id,
            key: Some(key),
            kind: OutboxActionKind::SendOwnerMessage,
            payload_json: json!({"alert": alert, "challenge_id": challenge_id}).to_string(),
            idempotency_key: format!("outbox:{}:{alert}", action.id),
            created_at: now,
        },
    )
    .await?;
    Ok(())
}

async fn ensure_right(
    pool: &SqlitePool,
    connection_id: &str,
    required: RequiredRight,
) -> Result<(), ActionFailure> {
    let connection = load_connection(pool, connection_id)
        .await?
        .ok_or(ActionFailure::RightsUnavailable)?;
    if !connection.enabled {
        return Err(ActionFailure::RightsUnavailable);
    }
    let rights: StoredRights = serde_json::from_str(&connection.rights_json)?;
    let permitted = match required {
        RequiredRight::Reply => rights.can_reply,
        RequiredRight::Read => rights.can_read_messages,
        RequiredRight::DeleteAll => rights.can_delete_all_messages,
    };
    let _ = rights.can_delete_sent_messages;
    if permitted {
        Ok(())
    } else {
        Err(ActionFailure::RightsUnavailable)
    }
}

async fn load_connection(
    pool: &SqlitePool,
    connection_id: &str,
) -> Result<Option<BusinessConnectionRecord>, StorageError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    let connection = find_business_connection(&mut uow, connection_id).await?;
    uow.rollback().await?;
    Ok(connection)
}

async fn load_challenge(
    pool: &SqlitePool,
    challenge_id: i64,
) -> Result<ChallengeRecord, ActionFailure> {
    let mut uow = UnitOfWork::begin(pool).await?;
    let challenge = find_challenge_by_id(&mut uow, challenge_id)
        .await?
        .ok_or(ActionFailure::Invalid("challenge_not_found"))?;
    uow.rollback().await?;
    Ok(challenge)
}

fn required_key(action: &OutboxActionRecord) -> Result<&ConversationKey, ActionFailure> {
    action
        .key
        .as_ref()
        .ok_or(ActionFailure::Invalid("business_identity_missing"))
}

fn parse_payload<T: for<'de> Deserialize<'de>>(
    action: &OutboxActionRecord,
) -> Result<T, ActionFailure> {
    Ok(serde_json::from_str(&action.payload_json)?)
}

fn challenge_id(action: &OutboxActionRecord) -> Result<i64, DispatchError> {
    Ok(serde_json::from_str::<ChallengePayload>(&action.payload_json)?.challenge_id)
}

const fn is_idempotent(kind: OutboxActionKind) -> bool {
    matches!(
        kind,
        OutboxActionKind::EditChallenge
            | OutboxActionKind::ReadBusinessMessage
            | OutboxActionKind::DeleteBusinessMessages
            | OutboxActionKind::ProposedDestructiveAction
    )
}

fn retry_delay(error: &TelegramError, attempts: i64) -> Option<Duration> {
    if attempts > i64::try_from(RETRY_SECONDS.len()).ok()? {
        return None;
    }
    if let TelegramError::Api {
        error_code: 429,
        retry_after: Some(seconds),
        ..
    } = error
    {
        return Some(Duration::seconds(
            i64::try_from((*seconds).min(60)).unwrap_or(60),
        ));
    }
    let retryable = matches!(
        error,
        TelegramError::Timeout
            | TelegramError::Transport
            | TelegramError::Protocol(_)
            | TelegramError::Api {
                error_code: 429 | 500..=599,
                ..
            }
    );
    let retry_index = usize::try_from(attempts - 1).ok()?;
    retryable.then(|| Duration::seconds(RETRY_SECONDS[retry_index]))
}

fn already_applied(kind: OutboxActionKind, error: &TelegramError) -> bool {
    let TelegramError::Api {
        error_code: 400,
        description,
        ..
    } = error
    else {
        return false;
    };
    let description = description.to_ascii_lowercase();
    match kind {
        OutboxActionKind::EditChallenge => description.contains("message is not modified"),
        OutboxActionKind::DeleteBusinessMessages => {
            description.contains("message to delete not found")
        }
        _ => false,
    }
}

fn invalid_connection_or_right(error: &TelegramError) -> bool {
    let TelegramError::Api { description, .. } = error else {
        return false;
    };
    let description = description.to_ascii_lowercase();
    [
        "business connection not found",
        "business connection is not enabled",
        "not enough rights",
        "has no rights",
    ]
    .iter()
    .any(|needle| description.contains(needle))
}

fn error_code(error: &TelegramError) -> &'static str {
    match error {
        TelegramError::Timeout => "timeout",
        TelegramError::Transport => "transport",
        TelegramError::Api {
            error_code: 429, ..
        } => "api_429",
        TelegramError::Api {
            error_code: 500..=599,
            ..
        } => "api_5xx",
        TelegramError::Api { .. } => "api_rejected",
        TelegramError::Protocol(_) => "protocol",
        TelegramError::InvalidRequest(_) => "invalid_request",
    }
}

fn challenge_prompt(challenge: &ChallengeRecord) -> String {
    format!(
        "请在 2 分钟内完成验证：\n{} = ?\n最多可尝试 3 次。",
        challenge.expression
    )
}

fn challenge_status_text(challenge: &ChallengeRecord, payload: &EditPayload) -> String {
    match payload.status.as_str() {
        "success" => "验证通过，消息已放行。".to_owned(),
        "incorrect" => format!(
            "答案不正确，还可尝试 {} 次。\n\n请重新回答：\n{} = ?",
            payload.attempts_remaining.unwrap_or_default(),
            challenge.expression
        ),
        "exhausted_dry_run" => {
            "验证失败。\nDry-run：正式模式下将软屏蔽 24 小时，本次未执行屏蔽。".to_owned()
        }
        "exhausted" => "验证失败，请 24 小时后再试。".to_owned(),
        "expired" => "验证已过期，请重新发送消息开始验证。".to_owned(),
        _ => "验证状态已更新。".to_owned(),
    }
}

fn owner_alert_text(payload: &OwnerAlertPayload) -> String {
    if let Some(message) = &payload.message {
        return message.clone();
    }
    match payload.alert.as_deref().unwrap_or_default() {
        "challenge_send_uncertain" => format!(
            "ChatHygiene 无法确认验证提示是否已发送（challenge {}）。",
            payload
                .challenge_id
                .map_or_else(|| "unknown".to_owned(), |id| id.to_string())
        ),
        "business_rights_unavailable" => {
            "ChatHygiene 已暂停破坏性动作：Business 连接或权限不可用。".to_owned()
        }
        "detector_failed" => "ChatHygiene 检测器失败，本次消息已保留。".to_owned(),
        _ => "ChatHygiene 需要人工检查运行状态。".to_owned(),
    }
}
