use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use serde_json::Value;
use thiserror::Error;

use crate::detection::{MediaKind, MessageContent, MessageEntity, MessageEntityKind};

use super::models::{
    OwnerCommandSnapshot, OwnerReplySnapshot, ParsedUpdate, RawBusinessEvent, RawEventKind,
};

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("invalid Telegram update JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    #[error("Telegram update contains an invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("Telegram Business connection contains an invalid connection ID")]
    InvalidConnectionId,
}

#[derive(Deserialize)]
struct Envelope {
    update_id: i64,
    business_connection: Option<BusinessConnection>,
    business_message: Option<BusinessMessage>,
    edited_business_message: Option<BusinessMessage>,
    deleted_business_messages: Option<DeletedBusinessMessages>,
    message: Option<BotMessage>,
    channel_post: Option<BotMessage>,
}

#[derive(Deserialize)]
struct User {
    id: i64,
    first_name: Option<String>,
    last_name: Option<String>,
    username: Option<String>,
}

#[derive(Deserialize)]
struct Chat {
    id: i64,
    first_name: Option<String>,
    last_name: Option<String>,
    username: Option<String>,
}

#[derive(Deserialize)]
struct BotChat {
    id: i64,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Deserialize)]
struct BotMessage {
    message_id: i64,
    from: Option<User>,
    chat: BotChat,
    date: i64,
    text: Option<String>,
    caption: Option<String>,
    reply_to_message: Option<Box<BotMessage>>,
    forward_origin: Option<Value>,
}

#[derive(Deserialize)]
struct BusinessConnection {
    id: String,
}

#[derive(Deserialize)]
struct BusinessMessage {
    message_id: i64,
    business_connection_id: String,
    from: User,
    chat: Chat,
    date: i64,
    edit_date: Option<i64>,
    text: Option<String>,
    caption: Option<String>,
    #[serde(default)]
    entities: Vec<Entity>,
    #[serde(default)]
    caption_entities: Vec<Entity>,
    media_group_id: Option<String>,
    sender_business_bot: Option<Value>,
    #[serde(default)]
    is_from_offline: bool,
    forward_origin: Option<Value>,
    photo: Option<Value>,
    video: Option<Value>,
    document: Option<Document>,
    voice: Option<Value>,
    sticker: Option<Value>,
}

#[derive(Deserialize)]
struct Document {
    file_name: Option<String>,
}

#[derive(Deserialize)]
struct Entity {
    #[serde(rename = "type")]
    kind: String,
    offset: usize,
    length: usize,
    url: Option<String>,
}

#[derive(Deserialize)]
struct DeletedBusinessMessages {
    business_connection_id: String,
    chat: Chat,
    message_ids: Vec<i64>,
}

/// Parses a Telegram update into a transient Business event.
///
/// # Errors
///
/// Returns [`ParseError`] when JSON, required fields, or Telegram timestamps
/// are invalid. Unusable entity ranges are ignored with the entity itself.
pub fn parse_update(body: &[u8], owner_user_id: i64) -> Result<ParsedUpdate, ParseError> {
    let update: Envelope = serde_json::from_slice(body)?;
    let event = if let Some(connection) = update.business_connection {
        connection_event(connection)?
    } else if let Some(message) = update.business_message {
        message_event(message, owner_user_id, false)?
    } else if let Some(message) = update.edited_business_message {
        message_event(message, owner_user_id, true)?
    } else if let Some(deleted) = update.deleted_business_messages {
        deletion_event(deleted)
    } else if let Some(message) = update.message.or(update.channel_post) {
        bot_message_event(message)?
    } else {
        ignored_event(Utc::now())
    };
    Ok(ParsedUpdate {
        update_id: update.update_id,
        event,
    })
}

fn connection_event(connection: BusinessConnection) -> Result<RawBusinessEvent, ParseError> {
    if connection.id.trim().is_empty() {
        return Err(ParseError::InvalidConnectionId);
    }
    Ok(RawBusinessEvent {
        kind: RawEventKind::BusinessConnectionChanged,
        connection_id: Some(connection.id),
        chat_id: None,
        message_id: None,
        media_group_id: None,
        content: None,
        deleted_message_ids: Vec::new(),
        connection: None,
        owner_command: None,
        contact_display_name: None,
        contact_username: None,
        occurred_at: Utc::now(),
    })
}

fn ignored_event(occurred_at: DateTime<Utc>) -> RawBusinessEvent {
    RawBusinessEvent {
        kind: RawEventKind::Ignored,
        connection_id: None,
        chat_id: None,
        message_id: None,
        media_group_id: None,
        content: None,
        deleted_message_ids: Vec::new(),
        connection: None,
        owner_command: None,
        contact_display_name: None,
        contact_username: None,
        occurred_at,
    }
}

fn message_event(
    message: BusinessMessage,
    owner_user_id: i64,
    edited: bool,
) -> Result<RawBusinessEvent, ParseError> {
    let kind = classify_message(&message, owner_user_id, edited);
    let occurred_at = timestamp(message.edit_date.unwrap_or(message.date))?;
    let content = message_content(&message);
    let (contact_display_name, contact_username) = message_contact_identity(&message, kind);
    Ok(RawBusinessEvent {
        kind,
        connection_id: Some(message.business_connection_id),
        chat_id: Some(message.chat.id),
        message_id: Some(message.message_id),
        media_group_id: message.media_group_id,
        content: Some(content),
        deleted_message_ids: Vec::new(),
        connection: None,
        owner_command: None,
        contact_display_name,
        contact_username,
        occurred_at,
    })
}

fn classify_message(message: &BusinessMessage, owner_user_id: i64, edited: bool) -> RawEventKind {
    if message.sender_business_bot.is_some() {
        return RawEventKind::BotBusinessMessage;
    }
    if message.is_from_offline {
        return RawEventKind::ImplicitOwnerMessage;
    }
    if message.from.id == owner_user_id {
        return RawEventKind::ManualOwnerMessage;
    }
    if edited {
        RawEventKind::EditedInboundMessage
    } else {
        RawEventKind::InboundMessage
    }
}

fn bot_message_event(message: BotMessage) -> Result<RawBusinessEvent, ParseError> {
    let occurred_at = timestamp(message.date)?;
    let command_text = message.text.clone().unwrap_or_default();
    let is_command = command_text.trim_start().starts_with('/');
    let replied_sample = message
        .reply_to_message
        .as_deref()
        .and_then(owner_reply_snapshot);
    Ok(RawBusinessEvent {
        kind: if is_command {
            RawEventKind::OwnerCommand
        } else {
            RawEventKind::Ignored
        },
        connection_id: None,
        chat_id: Some(message.chat.id),
        message_id: Some(message.message_id),
        media_group_id: None,
        content: None,
        deleted_message_ids: Vec::new(),
        connection: None,
        owner_command: is_command.then(|| OwnerCommandSnapshot {
            from_user_id: message.from.map(|user| user.id),
            private_chat: message.chat.kind == "private",
            text: command_text,
            replied_sample,
        }),
        contact_display_name: None,
        contact_username: None,
        occurred_at,
    })
}

fn owner_reply_snapshot(message: &BotMessage) -> Option<OwnerReplySnapshot> {
    let (body, content_type) = if let Some(text) = message.text.as_ref() {
        (text.clone(), "text".to_owned())
    } else {
        (message.caption.clone()?, "caption".to_owned())
    };
    let source_chat_id = message
        .forward_origin
        .as_ref()
        .and_then(|origin| origin.get("chat"))
        .and_then(|chat| chat.get("id"))
        .and_then(Value::as_i64);
    let source_message_id = message
        .forward_origin
        .as_ref()
        .and_then(|origin| origin.get("message_id"))
        .and_then(Value::as_i64);
    Some(OwnerReplySnapshot {
        body,
        content_type,
        source_chat_id,
        source_message_id,
    })
}

fn message_content(message: &BusinessMessage) -> MessageContent {
    let mut entities = extract_entities(message.text.as_deref(), &message.entities);
    entities.extend(extract_entities(
        message.caption.as_deref(),
        &message.caption_entities,
    ));
    MessageContent {
        text: message.text.clone(),
        caption: message.caption.clone(),
        entities,
        media_kind: media_kind(message),
        document_filename: message
            .document
            .as_ref()
            .and_then(|document| document.file_name.clone()),
        forwarded: message.forward_origin.is_some(),
    }
}

fn extract_entities(source: Option<&str>, entities: &[Entity]) -> Vec<MessageEntity> {
    entities
        .iter()
        .filter_map(|entity| {
            let value = entity.url.clone().or_else(|| {
                source.and_then(|text| utf16_slice(text, entity.offset, entity.length))
            })?;
            Some(MessageEntity {
                kind: match entity.kind.as_str() {
                    "url" => MessageEntityKind::Url,
                    "text_link" => MessageEntityKind::TextLink,
                    "mention" => MessageEntityKind::Mention,
                    "phone_number" => MessageEntityKind::PhoneNumber,
                    _ => MessageEntityKind::Other,
                },
                value,
            })
        })
        .collect()
}

fn utf16_slice(text: &str, offset: usize, length: usize) -> Option<String> {
    let units = text.encode_utf16().collect::<Vec<_>>();
    let end = offset.checked_add(length)?;
    String::from_utf16(units.get(offset..end)?).ok()
}

fn media_kind(message: &BusinessMessage) -> Option<MediaKind> {
    if message.photo.is_some() {
        Some(MediaKind::Photo)
    } else if message.video.is_some() {
        Some(MediaKind::Video)
    } else if message.document.is_some() {
        Some(MediaKind::Document)
    } else if message.voice.is_some() {
        Some(MediaKind::Voice)
    } else if message.sticker.is_some() {
        Some(MediaKind::Sticker)
    } else {
        None
    }
}

fn message_contact_identity(
    message: &BusinessMessage,
    kind: RawEventKind,
) -> (Option<String>, Option<String>) {
    let (chat_display_name, chat_username) = chat_identity(&message.chat);
    if !matches!(
        kind,
        RawEventKind::InboundMessage | RawEventKind::EditedInboundMessage
    ) {
        return (chat_display_name, chat_username);
    }
    (
        chat_display_name.or_else(|| {
            display_name(
                message.from.first_name.as_deref(),
                message.from.last_name.as_deref(),
            )
        }),
        chat_username.or_else(|| normalized_username(message.from.username.as_deref())),
    )
}

fn chat_identity(chat: &Chat) -> (Option<String>, Option<String>) {
    (
        display_name(chat.first_name.as_deref(), chat.last_name.as_deref()),
        normalized_username(chat.username.as_deref()),
    )
}

fn display_name(first_name: Option<&str>, last_name: Option<&str>) -> Option<String> {
    let name = first_name
        .into_iter()
        .chain(last_name)
        .flat_map(str::split_whitespace)
        .collect::<Vec<_>>()
        .join(" ");
    (!name.is_empty()).then_some(name)
}

fn normalized_username(username: Option<&str>) -> Option<String> {
    let username = username?.trim().trim_start_matches('@');
    (!username.is_empty()).then(|| username.to_owned())
}

fn deletion_event(deleted: DeletedBusinessMessages) -> RawBusinessEvent {
    let (contact_display_name, contact_username) = chat_identity(&deleted.chat);
    RawBusinessEvent {
        kind: RawEventKind::MessagesDeleted,
        connection_id: Some(deleted.business_connection_id),
        chat_id: Some(deleted.chat.id),
        message_id: None,
        media_group_id: None,
        content: None,
        deleted_message_ids: deleted.message_ids,
        connection: None,
        owner_command: None,
        contact_display_name,
        contact_username,
        occurred_at: Utc::now(),
    }
}

fn timestamp(seconds: i64) -> Result<DateTime<Utc>, ParseError> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .ok_or(ParseError::InvalidTimestamp(seconds))
}
