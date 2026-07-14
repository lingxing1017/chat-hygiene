use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreparedEvent {
    pub update_id: i64,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub facts: Value,
}

impl PreparedEvent {
    #[must_use]
    pub fn new(
        update_id: i64,
        event_type: impl Into<String>,
        occurred_at: DateTime<Utc>,
        facts: Value,
    ) -> Self {
        Self {
            update_id,
            event_type: event_type.into(),
            occurred_at,
            facts,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordReceipt {
    Recorded,
    DuplicateRecorded,
    DuplicateApplied,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyReceipt {
    Applied,
    AlreadyApplied,
}
