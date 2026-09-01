use chrono::{DateTime, Utc};

use crate::detection::MessageContent;
use crate::owner::ParsedOwnerClaim;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawEventKind {
    BusinessConnectionChanged,
    InboundMessage,
    EditedInboundMessage,
    ManualOwnerMessage,
    BotBusinessMessage,
    ImplicitOwnerMessage,
    MessagesDeleted,
    OwnerClaim,
    OwnerStart,
    OwnerCommand,
    Ignored,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct BusinessRights {
    pub can_reply: bool,
    pub can_read_messages: bool,
    pub can_delete_sent_messages: bool,
    pub can_delete_all_messages: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessConnectionSnapshot {
    pub connection_id: String,
    pub owner_user_id: i64,
    pub owner_chat_id: Option<i64>,
    pub enabled: bool,
    pub rights: BusinessRights,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerReplySnapshot {
    pub body: String,
    pub content_type: String,
    pub source_chat_id: Option<i64>,
    pub source_message_id: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerCommandSnapshot {
    pub from_user_id: Option<i64>,
    pub private_chat: bool,
    pub text: String,
    pub replied_sample: Option<OwnerReplySnapshot>,
}

#[derive(Debug)]
pub struct RawBusinessEvent {
    pub kind: RawEventKind,
    pub connection_id: Option<String>,
    pub chat_id: Option<i64>,
    pub message_id: Option<i64>,
    pub media_group_id: Option<String>,
    pub content: Option<MessageContent>,
    pub deleted_message_ids: Vec<i64>,
    pub connection: Option<BusinessConnectionSnapshot>,
    pub owner_claim: Option<ParsedOwnerClaim>,
    pub owner_command: Option<OwnerCommandSnapshot>,
    pub contact_display_name: Option<String>,
    pub contact_username: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl Clone for RawBusinessEvent {
    fn clone(&self) -> Self {
        assert!(
            self.owner_claim.is_none(),
            "Owner claim events cannot be cloned"
        );
        Self {
            kind: self.kind,
            connection_id: self.connection_id.clone(),
            chat_id: self.chat_id,
            message_id: self.message_id,
            media_group_id: self.media_group_id.clone(),
            content: self.content.clone(),
            deleted_message_ids: self.deleted_message_ids.clone(),
            connection: self.connection.clone(),
            owner_claim: None,
            owner_command: self.owner_command.clone(),
            contact_display_name: self.contact_display_name.clone(),
            contact_username: self.contact_username.clone(),
            occurred_at: self.occurred_at,
        }
    }
}

#[derive(Debug)]
pub struct ParsedUpdate {
    pub update_id: i64,
    pub event: RawBusinessEvent,
}
