mod claim;
mod claim_file;
mod commands;
mod identity;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::Row;
use thiserror::Error;

use crate::detection::normalized_text_hash;
use crate::storage::{
    BusinessConnectionRecord, ConversationKey, OwnerChatSource, OwnerIdentity, StorageError,
    UnitOfWork, eligible_deletion_ids, find_conversation, find_single_business_connection,
    load_owner_identity, promote_owner_chat,
};

pub use claim::{OwnerClaimContext, ParsedOwnerClaim, parse_owner_claim};
pub use claim_file::{ClaimFileError, ClaimFileManager};
pub use commands::{OwnerCommand, OwnerCommandParseError, parse_owner_command};
pub use identity::OwnerIdentityHandle;

const HELP_MESSAGE: &str = "owner commands:\n\
/help - list owner commands\n\
/health - show connection and dry-run status\n\
/inspect <chat_id> - show conversation state\n\
/reset <chat_id> - delete known messages and reset the conversation\n\
/unblock <chat_id> - clear a local soft block\n\
/dry_run on|off - set dry-run mode\n\
/errors [1..20] - show recent errors\n\
/mark_spam - label the replied message as spam\n\
/mark_ham - label the replied message as ham";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabeledMessageBody {
    pub body: String,
    pub content_type: String,
    pub source_chat_id: Option<i64>,
    pub source_message_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerCommandSource {
    pub from_user_id: i64,
    pub chat_id: i64,
    pub private_chat: bool,
    pub replied_sample: Option<LabeledMessageBody>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthorizedOwner {
    pub identity: OwnerIdentity,
    pub owner_user_id: i64,
    pub owner_chat_id: i64,
    pub connection: Option<BusinessConnectionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerCommandExecution {
    pub response: String,
    pub telegram_actions: Vec<OwnerTelegramAction>,
}

impl OwnerCommandExecution {
    fn plain(response: String) -> Self {
        Self {
            response,
            telegram_actions: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerTelegramAction {
    DeleteBusinessMessages {
        key: ConversationKey,
        message_ids: Vec<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OwnerCommandError {
    #[error("unauthorized owner command")]
    Unauthorized,
    #[error("a replied text or caption sample is required")]
    MissingSample,
    #[error("sample body or content type is invalid")]
    InvalidSample,
    #[error("Business connection or required rights are unavailable")]
    BusinessRightsUnavailable,
    #[error("target conversation was not found")]
    TargetNotFound,
    #[error("target conversation is not soft blocked")]
    TargetNotBlocked,
    #[error("owner command storage failed: {0}")]
    Storage(String),
}

impl From<StorageError> for OwnerCommandError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error.to_string())
    }
}

impl From<sqlx::Error> for OwnerCommandError {
    fn from(error: sqlx::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OwnerCommandService {
    now: DateTime<Utc>,
    default_destructive_mode: bool,
}

impl OwnerCommandService {
    #[must_use]
    pub const fn at(now: DateTime<Utc>) -> Self {
        Self {
            now,
            default_destructive_mode: false,
        }
    }

    #[must_use]
    pub const fn with_default_destructive_mode(mut self, enabled: bool) -> Self {
        self.default_destructive_mode = enabled;
        self
    }

    /// Authorizes and executes one command in the current transaction.
    ///
    /// # Errors
    ///
    /// Returns [`OwnerCommandError`] for unauthorized sources, invalid command
    /// state, rights gates, missing samples, or persistence failures.
    pub async fn execute(
        &self,
        command: OwnerCommand,
        source: OwnerCommandSource,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<OwnerCommandExecution, OwnerCommandError> {
        let owner = self.authorize(&source, uow).await?;
        self.execute_authorized(command, source, &owner, uow).await
    }

    pub(crate) async fn authorize(
        &self,
        source: &OwnerCommandSource,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<AuthorizedOwner, OwnerCommandError> {
        let identity = load_owner_identity(uow).await?;
        let OwnerIdentity::Claimed {
            owner_user_id,
            owner_chat_id,
            owner_chat_source,
            ..
        } = identity
        else {
            insert_security_audit(uow, self.now).await?;
            return Err(OwnerCommandError::Unauthorized);
        };
        if !source.private_chat
            || source.from_user_id != owner_user_id
            || source.chat_id <= 0
            || (owner_chat_source != OwnerChatSource::LegacyFallback
                && source.chat_id != owner_chat_id)
        {
            insert_security_audit(uow, self.now).await?;
            return Err(OwnerCommandError::Unauthorized);
        }
        let identity = if owner_chat_source == OwnerChatSource::LegacyFallback {
            promote_owner_chat(
                uow,
                owner_user_id,
                source.chat_id,
                OwnerChatSource::PrivateMessage,
            )
            .await?
        } else {
            identity
        };
        let connection = find_single_business_connection(uow).await?;
        if connection
            .as_ref()
            .is_some_and(|connection| connection.owner_user_id != owner_user_id)
        {
            return Err(OwnerCommandError::Storage(
                "Business connection belongs to another Owner".to_owned(),
            ));
        }
        let OwnerIdentity::Claimed { owner_chat_id, .. } = identity else {
            unreachable!("authorized identity remains claimed");
        };
        Ok(AuthorizedOwner {
            identity,
            owner_user_id,
            owner_chat_id,
            connection,
        })
    }

    pub(crate) async fn execute_authorized(
        &self,
        command: OwnerCommand,
        source: OwnerCommandSource,
        owner: &AuthorizedOwner,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<OwnerCommandExecution, OwnerCommandError> {
        match command {
            OwnerCommand::Help => Ok(OwnerCommandExecution::plain(HELP_MESSAGE.to_owned())),
            OwnerCommand::Health => Ok(OwnerCommandExecution::plain(
                health(
                    owner.connection.as_ref(),
                    self.default_destructive_mode,
                    uow,
                )
                .await?,
            )),
            OwnerCommand::Inspect { chat_id } => Ok(OwnerCommandExecution::plain(
                inspect(required_connection(owner)?, chat_id, uow).await?,
            )),
            OwnerCommand::Reset { chat_id } => {
                reset_conversation(required_connection(owner)?, chat_id, self.now, uow).await
            }
            OwnerCommand::Unblock { chat_id } => Ok(OwnerCommandExecution::plain(
                unblock_conversation(required_connection(owner)?, chat_id, self.now, uow).await?,
            )),
            OwnerCommand::DryRun { enabled } => Ok(OwnerCommandExecution::plain(
                set_dry_run(owner.connection.as_ref(), enabled, self.now, uow).await?,
            )),
            OwnerCommand::Errors { limit } => Ok(OwnerCommandExecution::plain(
                recent_errors(limit, uow).await?,
            )),
            OwnerCommand::MarkSpam => Ok(OwnerCommandExecution::plain(
                label_sample("spam_sample", source.replied_sample, self.now, uow).await?,
            )),
            OwnerCommand::MarkHam => Ok(OwnerCommandExecution::plain(
                label_sample("ham_sample", source.replied_sample, self.now, uow).await?,
            )),
        }
    }
}

fn required_connection(
    owner: &AuthorizedOwner,
) -> Result<&BusinessConnectionRecord, OwnerCommandError> {
    owner
        .connection
        .as_ref()
        .ok_or(OwnerCommandError::BusinessRightsUnavailable)
}

async fn health(
    connection: Option<&BusinessConnectionRecord>,
    default_destructive_mode: bool,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    let destructive: Option<String> =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_optional(uow.connection())
            .await?;
    let destructive_mode = destructive
        .as_deref()
        .map_or(default_destructive_mode, |value| value == "true");
    Ok(format!(
        "status=ok owner=claimed connection={} dry_run={}",
        connection.map_or("missing", |connection| if connection.enabled {
            "enabled"
        } else {
            "disabled"
        }),
        if destructive_mode { "off" } else { "on" }
    ))
}

async fn inspect(
    connection: &BusinessConnectionRecord,
    chat_id: i64,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    let key = ConversationKey::new(&connection.connection_id, chat_id);
    let conversation = find_conversation(uow, &key)
        .await?
        .ok_or(OwnerCommandError::TargetNotFound)?;
    Ok(format!(
        "chat_id={} state={} block_reason={} block_count={}",
        chat_id,
        conversation.state.as_str(),
        conversation.block_reason.as_deref().unwrap_or("none"),
        conversation.block_count
    ))
}

async fn reset_conversation(
    connection: &BusinessConnectionRecord,
    chat_id: i64,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<OwnerCommandExecution, OwnerCommandError> {
    let key = ConversationKey::new(&connection.connection_id, chat_id);
    find_conversation(uow, &key)
        .await?
        .ok_or(OwnerCommandError::TargetNotFound)?;

    let mut message_ids = eligible_deletion_ids(uow, &key).await?;
    let prompt_message_ids: Vec<i64> = sqlx::query_scalar(
        "SELECT prompt_message_id FROM challenge
         WHERE connection_id = ? AND chat_id = ?
           AND prompt_message_id IS NOT NULL",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .fetch_all(uow.connection())
    .await?;
    message_ids.extend(prompt_message_ids);
    message_ids.sort_unstable();
    message_ids.dedup();

    sqlx::query(
        "DELETE FROM outbox_action
         WHERE connection_id = ? AND chat_id = ?
           AND status IN ('PENDING', 'RETRY')",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .execute(uow.connection())
    .await?;
    sqlx::query("DELETE FROM challenge WHERE connection_id = ? AND chat_id = ?")
        .bind(&key.connection_id)
        .bind(key.chat_id)
        .execute(uow.connection())
        .await?;
    sqlx::query("DELETE FROM message_ledger WHERE connection_id = ? AND chat_id = ?")
        .bind(&key.connection_id)
        .bind(key.chat_id)
        .execute(uow.connection())
        .await?;
    sqlx::query(
        "UPDATE conversation
         SET state = 'NEW', block_expires_at = NULL, block_reason = NULL,
             block_count = 0, updated_at = ?, state_version = state_version + 1
         WHERE connection_id = ? AND chat_id = ?",
    )
    .bind(now.to_rfc3339())
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .execute(uow.connection())
    .await?;
    insert_recovery_audit(uow, &key.connection_id, key.chat_id, "reset", now).await?;

    let message_count = message_ids.len();
    Ok(OwnerCommandExecution {
        response: format!(
            "reset chat_id={chat_id} telegram_delete={} message_count={message_count}",
            if message_ids.is_empty() {
                "none"
            } else {
                "queued"
            }
        ),
        telegram_actions: (!message_ids.is_empty())
            .then_some(OwnerTelegramAction::DeleteBusinessMessages { key, message_ids })
            .into_iter()
            .collect(),
    })
}

async fn unblock_conversation(
    connection: &BusinessConnectionRecord,
    chat_id: i64,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    let changed = sqlx::query(
        "UPDATE conversation
         SET state = 'NEW', block_expires_at = NULL, block_reason = NULL,
             updated_at = ?, state_version = state_version + 1
         WHERE connection_id = ? AND chat_id = ?
           AND state IN ('TEMP_SOFT_BLOCKED', 'SPAM_SOFT_BLOCKED')",
    )
    .bind(now.to_rfc3339())
    .bind(&connection.connection_id)
    .bind(chat_id)
    .execute(uow.connection())
    .await?
    .rows_affected();
    if changed == 0 {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM conversation WHERE connection_id = ? AND chat_id = ?",
        )
        .bind(&connection.connection_id)
        .bind(chat_id)
        .fetch_optional(uow.connection())
        .await?;
        return Err(match state.as_deref() {
            None => OwnerCommandError::TargetNotFound,
            Some(_) => OwnerCommandError::TargetNotBlocked,
        });
    }
    sqlx::query(
        "UPDATE challenge SET closed_at = ?, delivery_status = 'CLOSED'
         WHERE connection_id = ? AND chat_id = ? AND closed_at IS NULL",
    )
    .bind(now.to_rfc3339())
    .bind(&connection.connection_id)
    .bind(chat_id)
    .execute(uow.connection())
    .await?;
    insert_recovery_audit(uow, &connection.connection_id, chat_id, "unblock", now).await?;
    Ok(format!("unblocked chat_id={chat_id}"))
}

#[derive(Debug, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
struct StoredRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

async fn set_dry_run(
    connection: Option<&BusinessConnectionRecord>,
    enabled: bool,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    if !enabled {
        let connection = connection.ok_or(OwnerCommandError::BusinessRightsUnavailable)?;
        let rights = serde_json::from_str::<StoredRights>(&connection.rights_json)
            .map_err(|_| OwnerCommandError::BusinessRightsUnavailable)?;
        if !connection.enabled
            || !rights.can_reply
            || !rights.can_read_messages
            || !rights.can_delete_sent_messages
            || !rights.can_delete_all_messages
        {
            return Err(OwnerCommandError::BusinessRightsUnavailable);
        }
    }
    sqlx::query(
        "INSERT INTO runtime_setting(key, value, updated_at)
         VALUES ('destructive_mode', ?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value,
           updated_at = excluded.updated_at",
    )
    .bind(if enabled { "false" } else { "true" })
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(format!("dry_run={}", if enabled { "on" } else { "off" }))
}

async fn recent_errors(limit: u8, uow: &mut UnitOfWork<'_>) -> Result<String, OwnerCommandError> {
    let rows = sqlx::query(
        "SELECT occurred_at, error_code, error_message FROM audit_event
         WHERE error_code IS NOT NULL ORDER BY occurred_at DESC, id DESC LIMIT ?",
    )
    .bind(i64::from(limit))
    .fetch_all(uow.connection())
    .await?;
    if rows.is_empty() {
        return Ok("no errors".to_owned());
    }
    Ok(rows
        .into_iter()
        .map(|row| {
            format!(
                "{} {} {}",
                row.get::<String, _>("occurred_at"),
                row.get::<Option<String>, _>("error_code")
                    .as_deref()
                    .unwrap_or("unknown"),
                row.get::<Option<String>, _>("error_message")
                    .as_deref()
                    .unwrap_or("")
            )
        })
        .collect::<Vec<_>>()
        .join("\n"))
}

async fn label_sample(
    table: &str,
    sample: Option<LabeledMessageBody>,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    let sample = sample.ok_or(OwnerCommandError::MissingSample)?;
    if sample.body.trim().is_empty() || !matches!(sample.content_type.as_str(), "text" | "caption")
    {
        return Err(OwnerCommandError::InvalidSample);
    }
    let hash = normalized_text_hash(&sample.body);
    let query = match table {
        "spam_sample" => {
            "INSERT INTO spam_sample
             (body, content_type, normalized_hash, source_chat_id,
              source_message_id, labeled_at) VALUES (?, ?, ?, ?, ?, ?)"
        }
        "ham_sample" => {
            "INSERT INTO ham_sample
             (body, content_type, normalized_hash, source_chat_id,
              source_message_id, labeled_at) VALUES (?, ?, ?, ?, ?, ?)"
        }
        _ => return Err(OwnerCommandError::InvalidSample),
    };
    sqlx::query(query)
        .bind(&sample.body)
        .bind(&sample.content_type)
        .bind(hash)
        .bind(sample.source_chat_id)
        .bind(sample.source_message_id)
        .bind(now.to_rfc3339())
        .execute(uow.connection())
        .await?;
    Ok(if table == "spam_sample" {
        "sample=spam stored".to_owned()
    } else {
        "sample=ham stored".to_owned()
    })
}

async fn insert_security_audit(
    uow: &mut UnitOfWork<'_>,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_event
         (event_kind, error_code, error_message, occurred_at)
         VALUES ('SECURITY', 'OWNER_COMMAND_UNAUTHORIZED',
                 'owner command source rejected', ?)",
    )
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(())
}

async fn insert_recovery_audit(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
    chat_id: i64,
    action: &str,
    now: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO audit_event
         (connection_id, chat_id, event_kind, error_message, occurred_at)
         VALUES (?, ?, 'OWNER_RECOVERY', ?, ?)",
    )
    .bind(connection_id)
    .bind(chat_id)
    .bind(action)
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(())
}
