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
    pub hmac_key_version: i64,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub attempts_used: i64,
    pub max_attempts: i64,
    pub prompt_message_id: Option<i64>,
    pub delivery_status: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChallengeHmacUpgradeRecord {
    pub id: i64,
    pub expression: String,
    pub hmac_key_version: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessConnectionRecord {
    pub connection_id: String,
    pub owner_user_id: i64,
    pub rights_json: String,
    pub enabled: bool,
    pub connection_established_at: Option<i64>,
    pub state_revision: i64,
    pub reconciliation_state: ReconciliationState,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconciliationState {
    Pending,
    Confirmed,
}

impl ReconciliationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Confirmed => "CONFIRMED",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self, super::StorageError> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "CONFIRMED" => Ok(Self::Confirmed),
            _ => Err(super::StorageError::InvalidData(
                "unknown business connection reconciliation state".to_owned(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnerChatSource {
    LegacyFallback,
    Claim,
    BusinessConnection,
    PrivateMessage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerIdentity {
    Unclaimed,
    Claimed {
        owner_user_id: i64,
        owner_chat_id: i64,
        owner_chat_source: OwnerChatSource,
        connection_floor_established_at: Option<i64>,
        bound_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboxActionKind {
    SendChallenge,
    EditChallenge,
    ReadBusinessMessage,
    DeleteBusinessMessages,
    SendOwnerMessage,
    SendPrivateMessage,
    ProposedDestructiveAction,
}

impl OutboxActionKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SendChallenge => "SEND_CHALLENGE",
            Self::EditChallenge => "EDIT_CHALLENGE",
            Self::ReadBusinessMessage => "READ_BUSINESS_MESSAGE",
            Self::DeleteBusinessMessages => "DELETE_BUSINESS_MESSAGES",
            Self::SendOwnerMessage => "SEND_OWNER_MESSAGE",
            Self::SendPrivateMessage => "SEND_PRIVATE_MESSAGE",
            Self::ProposedDestructiveAction => "PROPOSED_DESTRUCTIVE_ACTION",
        }
    }
}

impl std::str::FromStr for OutboxActionKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "SEND_CHALLENGE" => Ok(Self::SendChallenge),
            "EDIT_CHALLENGE" => Ok(Self::EditChallenge),
            "READ_BUSINESS_MESSAGE" => Ok(Self::ReadBusinessMessage),
            "DELETE_BUSINESS_MESSAGES" => Ok(Self::DeleteBusinessMessages),
            "SEND_OWNER_MESSAGE" => Ok(Self::SendOwnerMessage),
            "SEND_PRIVATE_MESSAGE" => Ok(Self::SendPrivateMessage),
            "PROPOSED_DESTRUCTIVE_ACTION" => Ok(Self::ProposedDestructiveAction),
            _ => Err(format!("unknown outbox action kind {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboxActionRecord {
    pub id: i64,
    pub source_update_id: i64,
    pub key: Option<ConversationKey>,
    pub kind: OutboxActionKind,
    pub payload_json: String,
    pub status: String,
    pub attempts: i64,
    pub interrupted: bool,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewOutboxAction {
    pub source_update_id: i64,
    pub key: Option<ConversationKey>,
    pub kind: OutboxActionKind,
    pub payload_json: String,
    pub idempotency_key: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewAuditEvent {
    pub source_update_id: i64,
    pub key: Option<ConversationKey>,
    pub event_kind: String,
    pub state_before: Option<String>,
    pub state_after: Option<String>,
    pub score: Option<u8>,
    pub reasons_json: Option<String>,
    pub rule_ids_json: Option<String>,
    pub normalized_hash: Option<String>,
    pub rule_version: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl ChallengeRecord {
    #[must_use]
    pub fn pending(
        key: ConversationKey,
        expression: impl Into<String>,
        answer_hmac: impl Into<String>,
        hmac_key_version: i64,
        created_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id: 0,
            key,
            expression: expression.into(),
            answer_hmac: answer_hmac.into(),
            hmac_key_version,
            created_at,
            expires_at,
            attempts_used: 0,
            max_attempts: 3,
            prompt_message_id: None,
            delivery_status: "PENDING".to_owned(),
        }
    }
}
