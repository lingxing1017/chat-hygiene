use serde_json::Value;
use sqlx::SqlitePool;
use thiserror::Error;

use crate::storage::StorageError;

use super::models::{PreparedEvent, RecordReceipt};

const MAX_FACT_STRING_LENGTH: usize = 512;
const FORBIDDEN_FACT_KEYS: [&str; 5] = [
    "text",
    "caption",
    "filename",
    "raw_update",
    "reply_to_message",
];

#[derive(Debug, Error)]
pub enum EventError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("event serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("event facts are unsafe to persist: {0}")]
    UnsafeFacts(String),
    #[error("event application failed: {0}")]
    Application(String),
    #[error("processed update {0} does not exist")]
    MissingUpdate(i64),
    #[error("processed update has unsupported status {0}")]
    InvalidStatus(String),
}

/// Persists a body-free derived event before any state changes are applied.
///
/// # Errors
///
/// Returns [`EventError`] when facts contain message bodies, serialization
/// fails, or `SQLite` cannot record the update.
pub async fn record_prepared_event(
    pool: &SqlitePool,
    event: &PreparedEvent,
) -> Result<RecordReceipt, EventError> {
    validate_facts(&event.facts, "facts")?;
    let event_json = serde_json::to_string(event)?;
    let result = sqlx::query(
        "INSERT OR IGNORE INTO processed_update
         (update_id, event_type, event_json, status, received_at)
         VALUES (?, ?, ?, 'RECORDED', ?)",
    )
    .bind(event.update_id)
    .bind(&event.event_type)
    .bind(event_json)
    .bind(event.occurred_at.to_rfc3339())
    .execute(pool)
    .await
    .map_err(StorageError::from)?;

    if result.rows_affected() == 1 {
        return Ok(RecordReceipt::Recorded);
    }

    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = ?")
            .bind(event.update_id)
            .fetch_optional(pool)
            .await
            .map_err(StorageError::from)?
            .ok_or(EventError::MissingUpdate(event.update_id))?;
    match status.as_str() {
        "RECORDED" => Ok(RecordReceipt::DuplicateRecorded),
        "APPLIED" => Ok(RecordReceipt::DuplicateApplied),
        _ => Err(EventError::InvalidStatus(status)),
    }
}

fn validate_facts(value: &Value, path: &str) -> Result<(), EventError> {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let normalized_key = key.to_ascii_lowercase();
                if FORBIDDEN_FACT_KEYS.contains(&normalized_key.as_str()) {
                    return Err(EventError::UnsafeFacts(format!(
                        "forbidden key {path}.{key}"
                    )));
                }
                validate_facts(child, &format!("{path}.{key}"))?;
            }
        }
        Value::Array(values) => {
            for (index, child) in values.iter().enumerate() {
                validate_facts(child, &format!("{path}[{index}]"))?;
            }
        }
        Value::String(value) if value.chars().count() > MAX_FACT_STRING_LENGTH => {
            return Err(EventError::UnsafeFacts(format!(
                "string at {path} exceeds {MAX_FACT_STRING_LENGTH} characters"
            )));
        }
        _ => {}
    }
    Ok(())
}
