use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LifecycleFacts {
    pub connection_id: Option<String>,
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub message_id: Option<i64>,
    pub media_group_id: Option<String>,
    #[serde(default)]
    pub owner_user_id: Option<i64>,
    #[serde(default = "legacy_event_kind")]
    pub event_kind: String,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub state_before: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub action: PreparedAction,
}

fn legacy_event_kind() -> String {
    "LEGACY_LIFECYCLE".to_owned()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PreparedAction {
    ConnectionChanged {
        owner_user_id: i64,
        enabled: bool,
        rights_json: String,
    },
    Inbound {
        detection: Box<DetectionFacts>,
        outcome: InboundOutcome,
        dry_run_spam: bool,
    },
    ActiveInbound,
    BlockedInbound,
    BlockedFailOpen,
    ManualOwner,
    BotMessage {
        sender: PreparedSender,
    },
    MessagesDeleted {
        message_ids: Vec<i64>,
    },
    Ignore,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum PreparedSender {
    BusinessBot,
    Implicit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum InboundOutcome {
    StartChallenge {
        expression: String,
        answer_hmac: String,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        max_attempts: u8,
    },
    Retain,
    Correct {
        challenge_id: i64,
    },
    Incorrect {
        challenge_id: i64,
        exhausted: bool,
        #[serde(default = "legacy_attempts_remaining")]
        attempts_remaining: u8,
        block_expires_at: Option<DateTime<Utc>>,
    },
    Expired {
        challenge_id: i64,
    },
    Spam,
}

const fn legacy_attempts_remaining() -> u8 {
    u8::MAX
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct DetectionFacts {
    pub decision: String,
    pub score: u8,
    pub reasons: Vec<String>,
    pub matched_rules: Vec<String>,
    pub detector_name: String,
    pub detector_version: String,
    pub normalized_hash: String,
    pub error: Option<String>,
}
