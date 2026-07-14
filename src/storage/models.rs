use chrono::{DateTime, Utc};

use crate::domain::ConversationState;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationKey {
    pub connection_id: String,
    pub chat_id: i64,
}

impl ConversationKey {
    #[must_use]
    pub fn new(connection_id: impl Into<String>, chat_id: i64) -> Self {
        Self {
            connection_id: connection_id.into(),
            chat_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conversation {
    pub key: ConversationKey,
    pub user_id: i64,
    pub state: ConversationState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub block_expires_at: Option<DateTime<Utc>>,
    pub block_reason: Option<String>,
    pub block_count: i64,
    pub state_version: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageDirection {
    Inbound,
    Outbound,
}

impl MessageDirection {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Inbound => "INBOUND",
            Self::Outbound => "OUTBOUND",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SenderKind {
    External,
    Owner,
    BusinessBot,
    Implicit,
}

impl SenderKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::External => "EXTERNAL",
            Self::Owner => "OWNER",
            Self::BusinessBot => "BUSINESS_BOT",
            Self::Implicit => "IMPLICIT",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerMessage {
    pub key: ConversationKey,
    pub message_id: i64,
    pub direction: MessageDirection,
    pub sender_kind: SenderKind,
    pub manual_owner_reply: bool,
    pub sent_at: DateTime<Utc>,
    pub eligible_for_deletion: bool,
    pub media_group_id: Option<String>,
}

impl LedgerMessage {
    #[must_use]
    pub fn new(
        key: ConversationKey,
        message_id: i64,
        direction: MessageDirection,
        sender_kind: SenderKind,
        manual_owner_reply: bool,
        sent_at: DateTime<Utc>,
    ) -> Self {
        Self {
            key,
            message_id,
            direction,
            sender_kind,
            manual_owner_reply,
            sent_at,
            eligible_for_deletion: true,
            media_group_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChallengeRecord {
    pub id: i64,
    pub key: ConversationKey,
    pub expression: String,
    pub answer_hmac: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub attempts_used: i64,
    pub max_attempts: i64,
    pub prompt_message_id: Option<i64>,
    pub delivery_status: String,
}

impl ChallengeRecord {
    #[must_use]
    pub fn pending(
        key: ConversationKey,
        expression: impl Into<String>,
        answer_hmac: impl Into<String>,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: 0,
            key,
            expression: expression.into(),
            answer_hmac: answer_hmac.into(),
            created_at,
            expires_at,
            attempts_used: 0,
            max_attempts: 3,
            prompt_message_id: None,
            delivery_status: "PENDING".to_owned(),
        }
    }
}
