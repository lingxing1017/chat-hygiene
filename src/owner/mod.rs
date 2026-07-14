mod commands;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::Row;
use thiserror::Error;

use crate::detection::normalized_text_hash;
use crate::storage::{
    BusinessConnectionRecord, ConversationKey, StorageError, UnitOfWork, find_conversation,
    find_single_business_connection,
};

pub use commands::{OwnerCommand, OwnerCommandParseError, parse_owner_command};

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
    pub private_chat: bool,
    pub replied_sample: Option<LabeledMessageBody>,
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
    #[error("ACTIVE conversation cannot be reset while owner replies remain")]
    ActiveConversation,
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
    ) -> Result<String, OwnerCommandError> {
        let connection = self.authorize(&source, uow).await?;
        self.execute_authorized(command, source, &connection, uow)
            .await
    }

    pub(crate) async fn authorize(
        &self,
        source: &OwnerCommandSource,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<BusinessConnectionRecord, OwnerCommandError> {
        let connection = find_single_business_connection(uow).await?;
        let authorized = source.private_chat
            && connection
                .as_ref()
                .is_some_and(|connection| connection.owner_user_id == source.from_user_id);
        if !authorized {
            insert_security_audit(uow, self.now).await?;
            return Err(OwnerCommandError::Unauthorized);
        }
        connection.ok_or(OwnerCommandError::Unauthorized)
    }

    pub(crate) async fn execute_authorized(
        &self,
        command: OwnerCommand,
        source: OwnerCommandSource,
        connection: &BusinessConnectionRecord,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<String, OwnerCommandError> {
        match command {
            OwnerCommand::Health => health(connection, self.default_destructive_mode, uow).await,
            OwnerCommand::Inspect { chat_id } => inspect(connection, chat_id, uow).await,
            OwnerCommand::Reset { chat_id } => {
                reset(connection, chat_id, false, self.now, uow).await
            }
            OwnerCommand::Unblock { chat_id } => {
                reset(connection, chat_id, true, self.now, uow).await
            }
            OwnerCommand::DryRun { enabled } => {
                set_dry_run(connection, enabled, self.now, uow).await
            }
            OwnerCommand::Errors { limit } => recent_errors(limit, uow).await,
            OwnerCommand::MarkSpam => {
                label_sample("spam_sample", source.replied_sample, self.now, uow).await
            }
            OwnerCommand::MarkHam => {
                label_sample("ham_sample", source.replied_sample, self.now, uow).await
            }
        }
    }
}

async fn health(
    connection: &BusinessConnectionRecord,
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
        "status=ok connection={} dry_run={}",
        if connection.enabled {
            "enabled"
        } else {
            "disabled"
        },
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

async fn reset(
    connection: &BusinessConnectionRecord,
    chat_id: i64,
    blocked_only: bool,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    let state_filter = if blocked_only {
        " AND state IN ('TEMP_SOFT_BLOCKED', 'SPAM_SOFT_BLOCKED')"
    } else {
        " AND state != 'ACTIVE'"
    };
    let query = format!(
        "UPDATE conversation
         SET state = 'NEW', block_expires_at = NULL, block_reason = NULL,
             updated_at = ?, state_version = state_version + 1
         WHERE connection_id = ? AND chat_id = ?{state_filter}"
    );
    let changed = sqlx::query(&query)
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
            Some("ACTIVE") if !blocked_only => OwnerCommandError::ActiveConversation,
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
    insert_recovery_audit(
        uow,
        &connection.connection_id,
        chat_id,
        if blocked_only { "unblock" } else { "reset" },
        now,
    )
    .await?;
    Ok(format!(
        "{} chat_id={chat_id}",
        if blocked_only { "unblocked" } else { "reset" }
    ))
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
    connection: &BusinessConnectionRecord,
    enabled: bool,
    now: DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<String, OwnerCommandError> {
    if !enabled {
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
