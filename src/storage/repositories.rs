use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::FromRow;

use crate::domain::ConversationState;

use super::models::{
    BusinessConnectionRecord, ChallengeRecord, Conversation, ConversationKey, LedgerMessage,
    NewAuditEvent, NewOutboxAction,
};
use super::{StorageError, UnitOfWork};

#[derive(FromRow)]
struct ConversationRow {
    connection_id: String,
    chat_id: i64,
    user_id: i64,
    state: String,
    created_at: String,
    updated_at: String,
    block_expires_at: Option<String>,
    block_reason: Option<String>,
    block_count: i64,
    state_version: i64,
}

#[derive(FromRow)]
struct ChallengeRow {
    id: i64,
    connection_id: String,
    chat_id: i64,
    expression: String,
    answer_hmac: String,
    created_at: String,
    expires_at: String,
    attempts_used: i64,
    max_attempts: i64,
    prompt_message_id: Option<i64>,
    delivery_status: String,
}

/// Loads a conversation or inserts its initial `NEW` state.
///
/// # Errors
///
/// Returns [`StorageError`] for database failures or invalid persisted values.
pub async fn get_or_create_conversation(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
    user_id: i64,
    now: DateTime<Utc>,
) -> Result<Conversation, StorageError> {
    if let Some(row) = load_conversation(uow, key).await? {
        return row.try_into();
    }

    sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES (?, ?, ?, 'NEW', ?, ?)",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .bind(user_id)
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;

    Ok(Conversation {
        key: key.clone(),
        user_id,
        state: ConversationState::New,
        created_at: now,
        updated_at: now,
        block_expires_at: None,
        block_reason: None,
        block_count: 0,
        state_version: 0,
    })
}

/// Loads a conversation without creating missing state.
///
/// # Errors
///
/// Returns [`StorageError`] for database failures or invalid persisted values.
pub async fn find_conversation(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<Option<Conversation>, StorageError> {
    load_conversation(uow, key)
        .await?
        .map(TryInto::try_into)
        .transpose()
}

/// Inserts or refreshes the one configured Business connection.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` rejects the connection state.
pub async fn upsert_business_connection(
    uow: &mut UnitOfWork<'_>,
    connection: &BusinessConnectionRecord,
) -> Result<(), StorageError> {
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES (?, ?, ?, ?, ?)
         ON CONFLICT(connection_id) DO UPDATE SET
           owner_user_id = excluded.owner_user_id,
           rights_json = excluded.rights_json,
           enabled = excluded.enabled,
           updated_at = excluded.updated_at",
    )
    .bind(&connection.connection_id)
    .bind(connection.owner_user_id)
    .bind(&connection.rights_json)
    .bind(connection.enabled)
    .bind(connection.updated_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(())
}

/// Saves a conversation only when its stored version matches `expected_version`.
///
/// # Errors
///
/// Returns [`StorageError::ConcurrentModification`] for a stale version, or a
/// database error when the update cannot run.
pub async fn save_conversation(
    uow: &mut UnitOfWork<'_>,
    conversation: &mut Conversation,
    expected_version: i64,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "UPDATE conversation
         SET state = ?, updated_at = ?, block_expires_at = ?, block_reason = ?,
             block_count = ?, state_version = state_version + 1
         WHERE connection_id = ? AND chat_id = ? AND state_version = ?",
    )
    .bind(conversation.state.as_str())
    .bind(conversation.updated_at.to_rfc3339())
    .bind(
        conversation
            .block_expires_at
            .map(|value| value.to_rfc3339()),
    )
    .bind(&conversation.block_reason)
    .bind(conversation.block_count)
    .bind(&conversation.key.connection_id)
    .bind(conversation.key.chat_id)
    .bind(expected_version)
    .execute(uow.connection())
    .await?;

    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    conversation.state_version = expected_version + 1;
    Ok(())
}

/// Inserts one observed message if its composite identity is new.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` rejects the message.
pub async fn record_message(
    uow: &mut UnitOfWork<'_>,
    message: &LedgerMessage,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT OR IGNORE INTO message_ledger
         (connection_id, chat_id, message_id, direction, sender_kind,
          manual_owner_reply, sent_at, eligible_for_deletion, media_group_id)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&message.key.connection_id)
    .bind(message.key.chat_id)
    .bind(message.message_id)
    .bind(message.direction.as_str())
    .bind(message.sender_kind.as_str())
    .bind(message.manual_owner_reply)
    .bind(message.sent_at.to_rfc3339())
    .bind(message.eligible_for_deletion)
    .bind(&message.media_group_id)
    .execute(uow.connection())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Marks one known message deleted once.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot update the ledger.
pub async fn mark_message_deleted(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
    message_id: i64,
    deleted_at: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE message_ledger SET deleted_at = ?
         WHERE connection_id = ? AND chat_id = ? AND message_id = ?
           AND deleted_at IS NULL",
    )
    .bind(deleted_at.to_rfc3339())
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .bind(message_id)
    .execute(uow.connection())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Lists undeleted manual owner replies in message order.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot query the ledger.
pub async fn active_owner_reply_ids(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<Vec<i64>, StorageError> {
    Ok(sqlx::query_scalar(
        "SELECT message_id FROM message_ledger
         WHERE connection_id = ? AND chat_id = ? AND manual_owner_reply = 1
           AND deleted_at IS NULL ORDER BY message_id",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .fetch_all(uow.connection())
    .await?)
}

/// Lists every known undeleted message eligible for cleanup.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot query the ledger.
pub async fn eligible_deletion_ids(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<Vec<i64>, StorageError> {
    Ok(sqlx::query_scalar(
        "SELECT message_id FROM message_ledger
         WHERE connection_id = ? AND chat_id = ? AND eligible_for_deletion = 1
           AND deleted_at IS NULL ORDER BY message_id",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .fetch_all(uow.connection())
    .await?)
}

/// Creates the only active challenge for a conversation.
///
/// # Errors
///
/// Returns [`StorageError`] when the values are invalid or an active challenge
/// already exists.
pub async fn create_challenge(
    uow: &mut UnitOfWork<'_>,
    challenge: &ChallengeRecord,
) -> Result<i64, StorageError> {
    let result = sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, created_at, expires_at,
          attempts_used, max_attempts, prompt_message_id, delivery_status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&challenge.key.connection_id)
    .bind(challenge.key.chat_id)
    .bind(&challenge.expression)
    .bind(&challenge.answer_hmac)
    .bind(challenge.created_at.to_rfc3339())
    .bind(challenge.expires_at.to_rfc3339())
    .bind(challenge.attempts_used)
    .bind(challenge.max_attempts)
    .bind(challenge.prompt_message_id)
    .bind(&challenge.delivery_status)
    .execute(uow.connection())
    .await?;
    Ok(result.last_insert_rowid())
}

/// Loads the active challenge, if one exists.
///
/// # Errors
///
/// Returns [`StorageError`] for database failures or invalid timestamps.
pub async fn active_challenge(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<Option<ChallengeRecord>, StorageError> {
    let row = sqlx::query_as::<_, ChallengeRow>(
        "SELECT id, connection_id, chat_id, expression, answer_hmac, created_at,
                expires_at, attempts_used, max_attempts, prompt_message_id,
                delivery_status
         FROM challenge WHERE connection_id = ? AND chat_id = ? AND closed_at IS NULL",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .fetch_optional(uow.connection())
    .await?;
    row.map(TryInto::try_into).transpose()
}

/// Closes an active challenge exactly once.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot update the challenge.
pub async fn close_challenge(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
    closed_at: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE challenge SET closed_at = ?, delivery_status = 'CLOSED'
         WHERE id = ? AND closed_at IS NULL",
    )
    .bind(closed_at.to_rfc3339())
    .bind(challenge_id)
    .execute(uow.connection())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Increments a challenge's numeric-attempt counter and returns the new value.
///
/// # Errors
///
/// Returns [`StorageError`] when the challenge is closed, exhausted, missing,
/// or cannot be updated.
pub async fn increment_challenge_attempts(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
) -> Result<i64, StorageError> {
    let attempts: Option<i64> = sqlx::query_scalar(
        "UPDATE challenge SET attempts_used = attempts_used + 1
         WHERE id = ? AND closed_at IS NULL AND attempts_used < max_attempts
         RETURNING attempts_used",
    )
    .bind(challenge_id)
    .fetch_optional(uow.connection())
    .await?;
    attempts.ok_or_else(|| {
        StorageError::InvalidData(format!(
            "challenge {challenge_id} cannot consume another attempt"
        ))
    })
}

/// Closes the active challenge for a conversation, if present.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot update the challenge.
pub async fn close_active_challenge(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
    closed_at: DateTime<Utc>,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "UPDATE challenge SET closed_at = ?, delivery_status = 'CLOSED'
         WHERE connection_id = ? AND chat_id = ? AND closed_at IS NULL",
    )
    .bind(closed_at.to_rfc3339())
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .execute(uow.connection())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Enqueues one idempotent Telegram-side action.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` rejects the action.
pub async fn enqueue_outbox_action(
    uow: &mut UnitOfWork<'_>,
    action: &NewOutboxAction,
) -> Result<bool, StorageError> {
    let result = sqlx::query(
        "INSERT OR IGNORE INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, 'PENDING', 0, ?, ?)",
    )
    .bind(action.source_update_id)
    .bind(action.key.as_ref().map(|key| key.connection_id.as_str()))
    .bind(action.key.as_ref().map(|key| key.chat_id))
    .bind(action.kind.as_str())
    .bind(&action.payload_json)
    .bind(&action.idempotency_key)
    .bind(action.created_at.to_rfc3339())
    .bind(action.created_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(result.rows_affected() == 1)
}

/// Records one body-free lifecycle or decision audit row.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` rejects the audit record.
pub async fn insert_audit_event(
    uow: &mut UnitOfWork<'_>,
    audit: &NewAuditEvent,
) -> Result<i64, StorageError> {
    let result = sqlx::query(
        "INSERT INTO audit_event
         (source_update_id, connection_id, chat_id, event_kind, state_before,
          state_after, score, reasons_json, rule_ids_json, normalized_hash,
          rule_version, error_code, error_message, occurred_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(audit.source_update_id)
    .bind(audit.key.as_ref().map(|key| key.connection_id.as_str()))
    .bind(audit.key.as_ref().map(|key| key.chat_id))
    .bind(&audit.event_kind)
    .bind(&audit.state_before)
    .bind(&audit.state_after)
    .bind(audit.score.map(i64::from))
    .bind(&audit.reasons_json)
    .bind(&audit.rule_ids_json)
    .bind(&audit.normalized_hash)
    .bind(&audit.rule_version)
    .bind(&audit.error_code)
    .bind(&audit.error_message)
    .bind(audit.occurred_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    Ok(result.last_insert_rowid())
}

async fn load_conversation(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<Option<ConversationRow>, StorageError> {
    Ok(sqlx::query_as::<_, ConversationRow>(
        "SELECT connection_id, chat_id, user_id, state, created_at, updated_at,
                block_expires_at, block_reason, block_count, state_version
         FROM conversation WHERE connection_id = ? AND chat_id = ?",
    )
    .bind(&key.connection_id)
    .bind(key.chat_id)
    .fetch_optional(uow.connection())
    .await?)
}

impl TryFrom<ConversationRow> for Conversation {
    type Error = StorageError;

    fn try_from(row: ConversationRow) -> Result<Self, Self::Error> {
        Ok(Self {
            key: ConversationKey::new(row.connection_id, row.chat_id),
            user_id: row.user_id,
            state: ConversationState::from_str(&row.state)
                .map_err(|error| StorageError::InvalidData(error.to_string()))?,
            created_at: parse_timestamp(&row.created_at)?,
            updated_at: parse_timestamp(&row.updated_at)?,
            block_expires_at: row
                .block_expires_at
                .as_deref()
                .map(parse_timestamp)
                .transpose()?,
            block_reason: row.block_reason,
            block_count: row.block_count,
            state_version: row.state_version,
        })
    }
}

impl TryFrom<ChallengeRow> for ChallengeRecord {
    type Error = StorageError;

    fn try_from(row: ChallengeRow) -> Result<Self, Self::Error> {
        Ok(Self {
            id: row.id,
            key: ConversationKey::new(row.connection_id, row.chat_id),
            expression: row.expression,
            answer_hmac: row.answer_hmac,
            created_at: parse_timestamp(&row.created_at)?,
            expires_at: parse_timestamp(&row.expires_at)?,
            attempts_used: row.attempts_used,
            max_attempts: row.max_attempts,
            prompt_message_id: row.prompt_message_id,
            delivery_status: row.delivery_status,
        })
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, StorageError> {
    value
        .parse()
        .map_err(|error| StorageError::InvalidData(format!("invalid timestamp {value}: {error}")))
}
