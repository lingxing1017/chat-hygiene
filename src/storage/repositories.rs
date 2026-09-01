use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::{FromRow, Row};

use crate::domain::ConversationState;

use super::connection_candidate::{GlobalReconciliationState, load_telegram_reconciliation_state};
use super::models::{
    BusinessConnectionRecord, ChallengeHmacUpgradeRecord, ChallengeRecord, Conversation,
    ConversationKey, LedgerMessage, NewAuditEvent, NewOutboxAction, OutboxActionKind,
    OutboxActionRecord, ReconciliationState,
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
    hmac_key_version: i64,
    created_at: String,
    expires_at: String,
    attempts_used: i64,
    max_attempts: i64,
    prompt_message_id: Option<i64>,
    delivery_status: String,
}

#[derive(FromRow)]
struct OutboxActionRow {
    id: i64,
    source_update_id: i64,
    connection_id: Option<String>,
    chat_id: Option<i64>,
    action_type: String,
    payload_json: String,
    status: String,
    attempts: i64,
    claimed_at: Option<String>,
    next_attempt_at: Option<String>,
    created_at: String,
    updated_at: String,
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

/// Inserts, refreshes, or replaces the one configured Business connection.
///
/// Telegram can issue a new connection ID for an owner whose previous
/// connection is still stored. Replacement retires unfinished actions and
/// resets destructive mode before connection-scoped state is removed.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` rejects the connection state.
pub async fn upsert_business_connection(
    uow: &mut UnitOfWork<'_>,
    connection: &BusinessConnectionRecord,
) -> Result<(), StorageError> {
    let stale_connection_id: Option<String> = sqlx::query_scalar(
        "SELECT connection_id FROM business_connection
         WHERE owner_user_id = ? AND connection_id != ?",
    )
    .bind(connection.owner_user_id)
    .bind(&connection.connection_id)
    .fetch_optional(uow.connection())
    .await?;

    if let Some(stale_connection_id) = stale_connection_id {
        sqlx::query(
            "UPDATE outbox_action
             SET status = 'PERMANENT_FAILURE', claimed_at = NULL,
                 next_attempt_at = NULL,
                 last_error = 'business_connection_replaced', updated_at = ?
             WHERE connection_id = ? AND status IN ('PENDING', 'RETRY')",
        )
        .bind(connection.updated_at.to_rfc3339())
        .bind(&stale_connection_id)
        .execute(uow.connection())
        .await?;

        sqlx::query("DELETE FROM business_connection WHERE connection_id = ?")
            .bind(&stale_connection_id)
            .execute(uow.connection())
            .await?;

        sqlx::query(
            "INSERT INTO runtime_setting(key, value, updated_at)
             VALUES ('destructive_mode', 'false', ?)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value,
                                            updated_at = excluded.updated_at",
        )
        .bind(connection.updated_at.to_rfc3339())
        .execute(uow.connection())
        .await?;
    }

    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled,
          connection_established_at, state_revision, reconciliation_state, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(connection_id) DO UPDATE SET
           owner_user_id = excluded.owner_user_id,
           rights_json = excluded.rights_json,
           enabled = excluded.enabled,
           connection_established_at = excluded.connection_established_at,
           state_revision = excluded.state_revision,
           reconciliation_state = excluded.reconciliation_state,
           updated_at = excluded.updated_at",
    )
    .bind(&connection.connection_id)
    .bind(connection.owner_user_id)
    .bind(&connection.rights_json)
    .bind(connection.enabled)
    .bind(connection.connection_established_at)
    .bind(connection.state_revision)
    .bind(connection.reconciliation_state.as_str())
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
         (connection_id, chat_id, expression, answer_hmac, hmac_key_version, created_at, expires_at,
          attempts_used, max_attempts, prompt_message_id, delivery_status)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&challenge.key.connection_id)
    .bind(challenge.key.chat_id)
    .bind(&challenge.expression)
    .bind(&challenge.answer_hmac)
    .bind(challenge.hmac_key_version)
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
        "SELECT id, connection_id, chat_id, expression, answer_hmac, hmac_key_version, created_at,
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

/// Lists open challenges whose HMAC was produced by another protocol version.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot read the ordered challenge set.
pub async fn active_challenges_not_on_version(
    uow: &mut UnitOfWork<'_>,
    target_version: i64,
) -> Result<Vec<ChallengeHmacUpgradeRecord>, StorageError> {
    Ok(sqlx::query_as::<_, (i64, String, i64)>(
        "SELECT id, expression, hmac_key_version
         FROM challenge
         WHERE closed_at IS NULL AND hmac_key_version != ?
         ORDER BY id ASC",
    )
    .bind(target_version)
    .fetch_all(uow.connection())
    .await?
    .into_iter()
    .map(
        |(id, expression, hmac_key_version)| ChallengeHmacUpgradeRecord {
            id,
            expression,
            hmac_key_version,
        },
    )
    .collect())
}

/// Replaces one open challenge HMAC using a version-checked compare-and-swap.
///
/// # Errors
///
/// Returns [`StorageError::ConcurrentModification`] when the row is absent,
/// closed, or no longer on `from_version`; database failures are propagated.
pub async fn replace_challenge_hmac(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
    from_version: i64,
    to_version: i64,
    answer_hmac: &str,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "UPDATE challenge
         SET answer_hmac = ?, hmac_key_version = ?
         WHERE id = ? AND closed_at IS NULL AND hmac_key_version = ?",
    )
    .bind(answer_hmac)
    .bind(to_version)
    .bind(challenge_id)
    .bind(from_version)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(())
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

/// Lists every outbox action already created by one source update.
///
/// # Errors
///
/// Returns [`StorageError`] when `SQLite` cannot read or decode the actions.
pub async fn list_outbox_actions_for_update(
    uow: &mut UnitOfWork<'_>,
    source_update_id: i64,
) -> Result<Vec<OutboxActionRecord>, StorageError> {
    let rows = sqlx::query_as::<_, OutboxActionRow>(
        "SELECT id, source_update_id, connection_id, chat_id, action_type,
                payload_json, status, attempts, claimed_at, next_attempt_at,
                created_at, updated_at
         FROM outbox_action
         WHERE source_update_id = ?
         ORDER BY id",
    )
    .bind(source_update_id)
    .fetch_all(uow.connection())
    .await?;
    rows.into_iter().map(TryInto::try_into).collect()
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

/// Claims the oldest due action for the single outbox worker.
///
/// The attempt counter is committed before the network call so a restarted
/// worker can conservatively detect an interrupted non-idempotent send.
///
/// # Errors
///
/// Returns [`StorageError`] when the row is malformed or cannot be claimed.
pub async fn claim_due_outbox_action(
    uow: &mut UnitOfWork<'_>,
    now: DateTime<Utc>,
) -> Result<Option<OutboxActionRecord>, StorageError> {
    let row = sqlx::query_as::<_, OutboxActionRow>(
        "SELECT id, source_update_id, connection_id, chat_id, action_type,
                payload_json, status, attempts, claimed_at, next_attempt_at,
                created_at, updated_at
         FROM outbox_action
         WHERE status IN ('PENDING', 'RETRY')
           AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
         ORDER BY id LIMIT 1",
    )
    .bind(now.to_rfc3339())
    .fetch_optional(uow.connection())
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let interrupted = row.claimed_at.is_some();
    let attempt_increment = i64::from(!interrupted);
    let result = sqlx::query(
        "UPDATE outbox_action
         SET attempts = attempts + ?, claimed_at = ?, updated_at = ?
         WHERE id = ? AND status = ? AND attempts = ?",
    )
    .bind(attempt_increment)
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .bind(row.id)
    .bind(&row.status)
    .bind(row.attempts)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    let mut action: OutboxActionRecord = row.try_into()?;
    action.attempts += attempt_increment;
    action.interrupted = interrupted;
    action.updated_at = now;
    Ok(Some(action))
}

/// Marks an outbox action successfully applied.
///
/// # Errors
///
/// Returns [`StorageError`] when the action cannot be updated.
pub async fn mark_outbox_succeeded(
    uow: &mut UnitOfWork<'_>,
    action_id: i64,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    set_outbox_terminal(uow, action_id, "SUCCEEDED", None, now).await
}

/// Schedules a retry for a recoverable outbox failure.
///
/// # Errors
///
/// Returns [`StorageError`] when the action cannot be updated.
pub async fn mark_outbox_retry(
    uow: &mut UnitOfWork<'_>,
    action_id: i64,
    next_attempt_at: DateTime<Utc>,
    error_code: &str,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    update_one(
        &sqlx::query(
            "UPDATE outbox_action
             SET status = 'RETRY', next_attempt_at = ?, claimed_at = NULL,
                 last_error = ?, updated_at = ?
             WHERE id = ? AND status IN ('PENDING', 'RETRY')",
        )
        .bind(next_attempt_at.to_rfc3339())
        .bind(error_code)
        .bind(now.to_rfc3339())
        .bind(action_id)
        .execute(uow.connection())
        .await?,
    )
}

/// Marks an ambiguous non-idempotent action for manual recovery.
///
/// # Errors
///
/// Returns [`StorageError`] when the action cannot be updated.
pub async fn mark_outbox_uncertain(
    uow: &mut UnitOfWork<'_>,
    action_id: i64,
    error_code: &str,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    set_outbox_terminal(uow, action_id, "UNCERTAIN", Some(error_code), now).await
}

/// Marks an outbox action permanently failed.
///
/// # Errors
///
/// Returns [`StorageError`] when the action cannot be updated.
pub async fn mark_outbox_permanent_failure(
    uow: &mut UnitOfWork<'_>,
    action_id: i64,
    error_code: &str,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    set_outbox_terminal(uow, action_id, "PERMANENT_FAILURE", Some(error_code), now).await
}

/// Loads one challenge by its stable outbox payload identity.
///
/// # Errors
///
/// Returns [`StorageError`] for malformed persisted values.
pub async fn find_challenge_by_id(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
) -> Result<Option<ChallengeRecord>, StorageError> {
    let row = sqlx::query_as::<_, ChallengeRow>(
        "SELECT id, connection_id, chat_id, expression, answer_hmac, hmac_key_version, created_at,
                expires_at, attempts_used, max_attempts, prompt_message_id,
                delivery_status
         FROM challenge WHERE id = ?",
    )
    .bind(challenge_id)
    .fetch_optional(uow.connection())
    .await?;
    row.map(TryInto::try_into).transpose()
}

/// Records a delivered challenge prompt without reopening a closed challenge.
///
/// # Errors
///
/// Returns [`StorageError`] when the challenge is missing or cannot be updated.
pub async fn mark_challenge_sent(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
    prompt_message_id: i64,
) -> Result<(), StorageError> {
    update_one(
        &sqlx::query(
            "UPDATE challenge
             SET prompt_message_id = ?,
                 delivery_status = CASE WHEN closed_at IS NULL THEN 'SENT'
                                        ELSE delivery_status END
             WHERE id = ?",
        )
        .bind(prompt_message_id)
        .bind(challenge_id)
        .execute(uow.connection())
        .await?,
    )
}

/// Records that Telegram may have delivered a challenge prompt.
///
/// # Errors
///
/// Returns [`StorageError`] when the challenge is missing or cannot be updated.
pub async fn mark_challenge_uncertain(
    uow: &mut UnitOfWork<'_>,
    challenge_id: i64,
) -> Result<(), StorageError> {
    update_one(
        &sqlx::query(
            "UPDATE challenge
             SET delivery_status = CASE WHEN closed_at IS NULL THEN 'UNCERTAIN'
                                        ELSE delivery_status END
             WHERE id = ?",
        )
        .bind(challenge_id)
        .execute(uow.connection())
        .await?,
    )
}

/// Loads a configured Business connection.
///
/// # Errors
///
/// Returns [`StorageError`] for malformed persisted timestamps.
pub async fn find_business_connection(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
) -> Result<Option<BusinessConnectionRecord>, StorageError> {
    if load_telegram_reconciliation_state(uow).await?.state != GlobalReconciliationState::Ready {
        return Ok(None);
    }
    let row = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                owner_user_id, typeof(owner_user_id) AS owner_user_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS connection_established_at_type,
                state_revision, typeof(state_revision) AS state_revision_type,
                reconciliation_state,
                typeof(reconciliation_state) AS reconciliation_state_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM business_connection
         WHERE connection_id = ?
           AND reconciliation_state = 'CONFIRMED'",
    )
    .bind(connection_id)
    .fetch_optional(uow.connection())
    .await?;
    row.as_ref().map(decode_business_connection).transpose()
}

/// Loads the only configured Business connection for the single-account MVP.
///
/// # Errors
///
/// Returns [`StorageError`] when multiple connections exist or persisted data
/// is malformed.
pub async fn find_single_business_connection(
    uow: &mut UnitOfWork<'_>,
) -> Result<Option<BusinessConnectionRecord>, StorageError> {
    if load_telegram_reconciliation_state(uow).await?.state != GlobalReconciliationState::Ready {
        return Ok(None);
    }
    let rows = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                owner_user_id, typeof(owner_user_id) AS owner_user_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS connection_established_at_type,
                state_revision, typeof(state_revision) AS state_revision_type,
                reconciliation_state,
                typeof(reconciliation_state) AS reconciliation_state_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM business_connection
         WHERE reconciliation_state = 'CONFIRMED'
         ORDER BY connection_id LIMIT 2",
    )
    .fetch_all(uow.connection())
    .await?;
    if rows.len() > 1 {
        return Err(StorageError::InvalidData(
            "single-account mode found multiple Business connections".to_owned(),
        ));
    }
    rows.first().map(decode_business_connection).transpose()
}

/// Loads one trusted connection without applying the global/effect gate.
///
/// # Errors
///
/// Returns [`StorageError`] when the row is corrupt or the query fails.
pub async fn load_business_connection_for_reconciliation(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
) -> Result<Option<BusinessConnectionRecord>, StorageError> {
    let row = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                owner_user_id, typeof(owner_user_id) AS owner_user_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS connection_established_at_type,
                state_revision, typeof(state_revision) AS state_revision_type,
                reconciliation_state,
                typeof(reconciliation_state) AS reconciliation_state_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM business_connection WHERE connection_id = ?",
    )
    .bind(connection_id)
    .fetch_optional(uow.connection())
    .await?;
    row.as_ref().map(decode_business_connection).transpose()
}

/// Loads the at-most-one trusted connection without applying the effect gate.
///
/// # Errors
///
/// Returns [`StorageError`] for multiple/corrupt rows or a query failure.
pub async fn load_single_trusted_connection(
    uow: &mut UnitOfWork<'_>,
) -> Result<Option<BusinessConnectionRecord>, StorageError> {
    let rows = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                owner_user_id, typeof(owner_user_id) AS owner_user_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS connection_established_at_type,
                state_revision, typeof(state_revision) AS state_revision_type,
                reconciliation_state,
                typeof(reconciliation_state) AS reconciliation_state_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM business_connection ORDER BY connection_id LIMIT 2",
    )
    .fetch_all(uow.connection())
    .await?;
    if rows.len() > 1 {
        return Err(StorageError::InvalidData(
            "single-account mode found multiple trusted Business connections".to_owned(),
        ));
    }
    rows.first().map(decode_business_connection).transpose()
}

/// Disables Business-side effects until a fresh connection update restores it.
///
/// # Errors
///
/// Returns [`StorageError`] when the connection cannot be updated.
pub async fn disable_business_connection(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    update_one(
        &sqlx::query(
            "UPDATE business_connection
             SET enabled = 0, state_revision = state_revision + 1, updated_at = ?
             WHERE connection_id = ?",
        )
        .bind(now.to_rfc3339())
        .bind(connection_id)
        .execute(uow.connection())
        .await?,
    )
}

async fn set_outbox_terminal(
    uow: &mut UnitOfWork<'_>,
    action_id: i64,
    status: &str,
    error_code: Option<&str>,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    update_one(
        &sqlx::query(
            "UPDATE outbox_action
             SET status = ?, next_attempt_at = NULL, claimed_at = NULL,
                 last_error = ?, updated_at = ?
             WHERE id = ? AND status IN ('PENDING', 'RETRY')",
        )
        .bind(status)
        .bind(error_code)
        .bind(now.to_rfc3339())
        .bind(action_id)
        .execute(uow.connection())
        .await?,
    )
}

fn update_one(result: &sqlx::sqlite::SqliteQueryResult) -> Result<(), StorageError> {
    if result.rows_affected() == 1 {
        Ok(())
    } else {
        Err(StorageError::ConcurrentModification)
    }
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
            hmac_key_version: row.hmac_key_version,
            created_at: parse_timestamp(&row.created_at)?,
            expires_at: parse_timestamp(&row.expires_at)?,
            attempts_used: row.attempts_used,
            max_attempts: row.max_attempts,
            prompt_message_id: row.prompt_message_id,
            delivery_status: row.delivery_status,
        })
    }
}

fn decode_business_connection(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<BusinessConnectionRecord, StorageError> {
    let invalid_identity =
        || StorageError::InvalidData("trusted Business connection identity is invalid".to_owned());
    let invalid_state =
        || StorageError::InvalidData("trusted Business connection state is invalid".to_owned());
    if row.try_get::<String, _>("connection_id_type")? != "text"
        || row.try_get::<String, _>("owner_user_id_type")? != "integer"
    {
        return Err(invalid_identity());
    }
    let connection_id = row.try_get::<String, _>("connection_id")?;
    let owner_user_id = row.try_get::<i64, _>("owner_user_id")?;
    if connection_id.trim().is_empty() || owner_user_id <= 0 {
        return Err(invalid_identity());
    }
    if row.try_get::<String, _>("rights_json_type")? != "text"
        || row.try_get::<String, _>("enabled_type")? != "integer"
        || row.try_get::<String, _>("state_revision_type")? != "integer"
        || row.try_get::<String, _>("reconciliation_state_type")? != "text"
        || row.try_get::<String, _>("updated_at_type")? != "text"
    {
        return Err(invalid_state());
    }
    let rights_json = row.try_get::<String, _>("rights_json")?;
    let enabled = match row.try_get::<i64, _>("enabled")? {
        0 => false,
        1 => true,
        _ => return Err(invalid_state()),
    };
    let connection_established_at = match row
        .try_get::<String, _>("connection_established_at_type")?
        .as_str()
    {
        "null" => None,
        "integer" => Some(row.try_get::<i64, _>("connection_established_at")?),
        _ => return Err(invalid_state()),
    };
    let state_revision = row.try_get::<i64, _>("state_revision")?;
    if connection_established_at.is_some_and(|established_at| established_at <= 0)
        || state_revision < 0
        || serde_json::from_str::<serde_json::Value>(&rights_json).is_err()
    {
        return Err(invalid_state());
    }
    let reconciliation_state =
        ReconciliationState::parse(&row.try_get::<String, _>("reconciliation_state")?)?;
    let updated_at = parse_timestamp(&row.try_get::<String, _>("updated_at")?)?;
    Ok(BusinessConnectionRecord {
        connection_id,
        owner_user_id,
        rights_json,
        enabled,
        connection_established_at,
        state_revision,
        reconciliation_state,
        updated_at,
    })
}

impl TryFrom<OutboxActionRow> for OutboxActionRecord {
    type Error = StorageError;

    fn try_from(row: OutboxActionRow) -> Result<Self, Self::Error> {
        let key = match (row.connection_id, row.chat_id) {
            (Some(connection_id), Some(chat_id)) => {
                Some(ConversationKey::new(connection_id, chat_id))
            }
            (None, None) => None,
            _ => {
                return Err(StorageError::InvalidData(
                    "outbox connection and chat identity must both be present or absent".to_owned(),
                ));
            }
        };
        Ok(Self {
            id: row.id,
            source_update_id: row.source_update_id,
            key,
            kind: row
                .action_type
                .parse::<OutboxActionKind>()
                .map_err(StorageError::InvalidData)?,
            payload_json: row.payload_json,
            status: row.status,
            attempts: row.attempts,
            interrupted: false,
            next_attempt_at: row
                .next_attempt_at
                .as_deref()
                .map(parse_timestamp)
                .transpose()?,
            created_at: parse_timestamp(&row.created_at)?,
            updated_at: parse_timestamp(&row.updated_at)?,
        })
    }
}

fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, StorageError> {
    value
        .parse()
        .map_err(|error| StorageError::InvalidData(format!("invalid timestamp {value}: {error}")))
}
