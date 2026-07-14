use std::collections::HashSet;

use chrono::{Duration, Utc};
use serde::Deserialize;
use sqlx::{FromRow, Row, SqlitePool};
use thiserror::Error;

use crate::clock::Clock;
use crate::storage::{
    ConversationKey, NewOutboxAction, OutboxActionKind, StorageError, UnitOfWork,
    enqueue_outbox_action,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExpiryReport {
    pub expired_challenges: u64,
    pub expired_temporary_blocks: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PurgeReport {
    pub purged_ledger_messages: u64,
    pub purged_audit_events: u64,
    pub purged_outbox_actions: u64,
    pub purged_processed_updates: u64,
}

impl PurgeReport {
    #[must_use]
    pub const fn total(self) -> u64 {
        self.purged_ledger_messages
            + self.purged_audit_events
            + self.purged_outbox_actions
            + self.purged_processed_updates
    }
}

#[derive(Debug, Error)]
pub enum RetentionError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

pub struct RetentionService<C> {
    clock: C,
}

impl<C: Clock> RetentionService<C> {
    #[must_use]
    pub const fn new(clock: C) -> Self {
        Self { clock }
    }

    /// Expires due challenges and temporary soft blocks atomically.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError`] when `SQLite` or outbox serialization fails.
    pub async fn expire_due_state(
        &self,
        pool: &SqlitePool,
    ) -> Result<ExpiryReport, RetentionError> {
        let now = self.clock.now();
        let mut uow = UnitOfWork::begin(pool).await?;
        let due = sqlx::query_as::<_, DueChallenge>(
            "SELECT id, connection_id, chat_id, prompt_message_id
             FROM challenge WHERE closed_at IS NULL AND expires_at <= ?
             ORDER BY id",
        )
        .bind(now.to_rfc3339())
        .fetch_all(uow.connection())
        .await
        .map_err(StorageError::from)?;

        let mut expired_challenges = 0;
        for challenge in due {
            let closed = sqlx::query(
                "UPDATE challenge SET closed_at = ?, delivery_status = 'CLOSED'
                 WHERE id = ? AND closed_at IS NULL",
            )
            .bind(now.to_rfc3339())
            .bind(challenge.id)
            .execute(uow.connection())
            .await
            .map_err(StorageError::from)?
            .rows_affected();
            if closed == 0 {
                continue;
            }
            expired_challenges += 1;
            sqlx::query(
                "UPDATE conversation
                 SET state = 'NEW', updated_at = ?, state_version = state_version + 1
                 WHERE connection_id = ? AND chat_id = ? AND state = 'VERIFY_PENDING'",
            )
            .bind(now.to_rfc3339())
            .bind(&challenge.connection_id)
            .bind(challenge.chat_id)
            .execute(uow.connection())
            .await
            .map_err(StorageError::from)?;
            if challenge.prompt_message_id.is_some() {
                enqueue_expiry_edit(&mut uow, &challenge, now).await?;
            }
        }

        let expired_temporary_blocks = sqlx::query(
            "UPDATE conversation
             SET state = 'NEW', block_expires_at = NULL, block_reason = NULL,
                 updated_at = ?, state_version = state_version + 1
             WHERE state = 'TEMP_SOFT_BLOCKED' AND block_expires_at <= ?",
        )
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(uow.connection())
        .await
        .map_err(StorageError::from)?
        .rows_affected();
        uow.commit().await?;
        Ok(ExpiryReport {
            expired_challenges,
            expired_temporary_blocks,
        })
    }

    /// Purges bounded operational history while retaining active identities,
    /// manual owner replies, persistent blocks, samples, rules, and settings.
    ///
    /// # Errors
    ///
    /// Returns [`RetentionError`] when persisted outbox payloads are malformed
    /// or `SQLite` cannot complete the atomic purge.
    pub async fn purge_history(&self, pool: &SqlitePool) -> Result<PurgeReport, RetentionError> {
        let now = self.clock.now();
        let mut uow = UnitOfWork::begin(pool).await?;
        let protected = protected_deletion_ids(&mut uow).await?;
        let purged_ledger_messages =
            purge_ledger(&mut uow, now - Duration::hours(72), &protected).await?;
        let purged_audit_events = purge_audits(&mut uow, now - Duration::days(90)).await?;
        let purged_outbox_actions =
            purge_outbox(&mut uow, now - Duration::days(30), now - Duration::days(90)).await?;
        let purged_processed_updates =
            purge_processed_updates(&mut uow, now - Duration::days(7)).await?;
        uow.commit().await?;
        Ok(PurgeReport {
            purged_ledger_messages,
            purged_audit_events,
            purged_outbox_actions,
            purged_processed_updates,
        })
    }
}

#[derive(FromRow)]
struct DueChallenge {
    id: i64,
    connection_id: String,
    chat_id: i64,
    prompt_message_id: Option<i64>,
}

#[derive(Deserialize)]
struct DeletePayload {
    message_ids: Vec<i64>,
}

async fn enqueue_expiry_edit(
    uow: &mut UnitOfWork<'_>,
    challenge: &DueChallenge,
    now: chrono::DateTime<Utc>,
) -> Result<(), RetentionError> {
    let source_update_id: Option<i64> = sqlx::query_scalar(
        "SELECT source_update_id FROM outbox_action
         WHERE action_type = 'SEND_CHALLENGE'
           AND json_extract(payload_json, '$.challenge_id') = ?
         ORDER BY id LIMIT 1",
    )
    .bind(challenge.id)
    .fetch_optional(uow.connection())
    .await
    .map_err(StorageError::from)?;
    let Some(source_update_id) = source_update_id else {
        return Ok(());
    };
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id,
            key: Some(ConversationKey::new(
                &challenge.connection_id,
                challenge.chat_id,
            )),
            kind: OutboxActionKind::EditChallenge,
            payload_json: serde_json::json!({
                "challenge_id": challenge.id,
                "status": "expired",
                "attempts_remaining": null,
            })
            .to_string(),
            idempotency_key: format!("retention:challenge:{}:expired", challenge.id),
            created_at: now,
        },
    )
    .await?;
    Ok(())
}

async fn protected_deletion_ids(
    uow: &mut UnitOfWork<'_>,
) -> Result<HashSet<(String, i64, i64)>, RetentionError> {
    let rows = sqlx::query(
        "SELECT connection_id, chat_id, payload_json FROM outbox_action
         WHERE action_type = 'DELETE_BUSINESS_MESSAGES'
           AND status IN ('PENDING', 'RETRY')",
    )
    .fetch_all(uow.connection())
    .await
    .map_err(StorageError::from)?;
    let mut protected = HashSet::new();
    for row in rows {
        let Some(connection_id) = row.get::<Option<String>, _>("connection_id") else {
            continue;
        };
        let Some(chat_id) = row.get::<Option<i64>, _>("chat_id") else {
            continue;
        };
        let payload: DeletePayload = serde_json::from_str(&row.get::<String, _>("payload_json"))?;
        protected.extend(
            payload
                .message_ids
                .into_iter()
                .map(|message_id| (connection_id.clone(), chat_id, message_id)),
        );
    }
    Ok(protected)
}

async fn purge_ledger(
    uow: &mut UnitOfWork<'_>,
    cutoff: chrono::DateTime<Utc>,
    protected: &HashSet<(String, i64, i64)>,
) -> Result<u64, StorageError> {
    let rows = sqlx::query(
        "SELECT connection_id, chat_id, message_id FROM message_ledger
         WHERE direction = 'INBOUND' AND manual_owner_reply = 0 AND sent_at <= ?",
    )
    .bind(cutoff.to_rfc3339())
    .fetch_all(uow.connection())
    .await?;
    let mut purged = 0;
    for row in rows {
        let key = (
            row.get::<String, _>("connection_id"),
            row.get::<i64, _>("chat_id"),
            row.get::<i64, _>("message_id"),
        );
        if protected.contains(&key) {
            continue;
        }
        purged += sqlx::query(
            "DELETE FROM message_ledger
             WHERE connection_id = ? AND chat_id = ? AND message_id = ?",
        )
        .bind(&key.0)
        .bind(key.1)
        .bind(key.2)
        .execute(uow.connection())
        .await?
        .rows_affected();
    }
    Ok(purged)
}

async fn purge_audits(
    uow: &mut UnitOfWork<'_>,
    cutoff: chrono::DateTime<Utc>,
) -> Result<u64, StorageError> {
    Ok(sqlx::query(
        "DELETE FROM audit_event
         WHERE occurred_at <= ?
           AND id NOT IN (
             SELECT MAX(a.id) FROM audit_event AS a
             JOIN conversation AS c
               ON c.connection_id = a.connection_id AND c.chat_id = a.chat_id
             WHERE c.state = 'SPAM_SOFT_BLOCKED'
             GROUP BY a.connection_id, a.chat_id
           )",
    )
    .bind(cutoff.to_rfc3339())
    .execute(uow.connection())
    .await?
    .rows_affected())
}

async fn purge_outbox(
    uow: &mut UnitOfWork<'_>,
    succeeded_cutoff: chrono::DateTime<Utc>,
    failed_cutoff: chrono::DateTime<Utc>,
) -> Result<u64, StorageError> {
    Ok(sqlx::query(
        "DELETE FROM outbox_action
         WHERE (status = 'SUCCEEDED' AND updated_at <= ?)
            OR (status IN ('UNCERTAIN', 'PERMANENT_FAILURE') AND updated_at <= ?)",
    )
    .bind(succeeded_cutoff.to_rfc3339())
    .bind(failed_cutoff.to_rfc3339())
    .execute(uow.connection())
    .await?
    .rows_affected())
}

async fn purge_processed_updates(
    uow: &mut UnitOfWork<'_>,
    cutoff: chrono::DateTime<Utc>,
) -> Result<u64, StorageError> {
    Ok(sqlx::query(
        "DELETE FROM processed_update
         WHERE received_at <= ?
           AND NOT EXISTS (
             SELECT 1 FROM outbox_action
             WHERE outbox_action.source_update_id = processed_update.update_id
           )
           AND NOT EXISTS (
             SELECT 1 FROM audit_event
             WHERE audit_event.source_update_id = processed_update.update_id
           )",
    )
    .bind(cutoff.to_rfc3339())
    .execute(uow.connection())
    .await?
    .rows_affected())
}
