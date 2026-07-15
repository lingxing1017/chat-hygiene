use chathygiene::detection::{MediaKind, MessageEntityKind};
use chathygiene::telegram::{RawEventKind, parse_update};

fn parse(fixture: &str) -> chathygiene::telegram::ParsedUpdate {
    parse_update(fixture.as_bytes(), 42).expect("parse fixture")
}

#[test]
fn parses_connection_rights_and_ignores_unknown_fields() {
    let parsed = parse(include_str!("fixtures/telegram/business_connection.json"));
    assert_eq!(parsed.update_id, 100);
    assert_eq!(parsed.event.kind, RawEventKind::BusinessConnectionChanged);
    let connection = parsed.event.connection.expect("connection snapshot");
    assert_eq!(connection.connection_id, "business-1");
    assert_eq!(connection.owner_user_id, 42);
    assert!(connection.enabled);
    assert!(connection.rights.can_reply);
    assert!(connection.rights.can_read_messages);
    assert!(connection.rights.can_delete_sent_messages);
    assert!(connection.rights.can_delete_all_messages);
}

#[test]
fn ignores_business_connections_owned_by_another_account() {
    let mut update: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/telegram/business_connection.json")).unwrap();
    update["business_connection"]["user"]["id"] = serde_json::Value::from(99);

    let parsed = parse_update(&serde_json::to_vec(&update).unwrap(), 42).unwrap();

    assert_eq!(parsed.event.kind, RawEventKind::Ignored);
    assert!(parsed.event.connection.is_none());
    assert!(parsed.event.connection_id.is_none());
}

#[test]
fn distinguishes_inbound_owner_bot_implicit_and_edited_messages() {
    let inbound = parse(include_str!("fixtures/telegram/inbound_message.json"));
    assert_eq!(inbound.event.kind, RawEventKind::InboundMessage);
    assert_eq!(inbound.event.connection_id.as_deref(), Some("business-1"));
    assert_eq!(inbound.event.chat_id, Some(1001));
    assert_eq!(inbound.event.message_id, Some(501));
    let inbound_content = inbound.event.content.expect("inbound content");
    assert_eq!(
        inbound_content.text.as_deref(),
        Some("See https://example.com and @owner")
    );
    assert_eq!(inbound_content.entities.len(), 2);
    assert_eq!(inbound_content.entities[0].kind, MessageEntityKind::Url);
    assert_eq!(inbound_content.entities[0].value, "https://example.com");
    assert_eq!(inbound_content.entities[1].kind, MessageEntityKind::Mention);
    assert_eq!(inbound_content.entities[1].value, "@owner");

    assert_eq!(
        parse(include_str!("fixtures/telegram/manual_owner_message.json"))
            .event
            .kind,
        RawEventKind::ManualOwnerMessage
    );
    assert_eq!(
        parse(include_str!("fixtures/telegram/bot_business_message.json"))
            .event
            .kind,
        RawEventKind::BotBusinessMessage
    );
    assert_eq!(
        parse(include_str!("fixtures/telegram/implicit_message.json"))
            .event
            .kind,
        RawEventKind::ImplicitOwnerMessage
    );

    let edited = parse(include_str!("fixtures/telegram/edited_message.json"));
    assert_eq!(edited.event.kind, RawEventKind::EditedInboundMessage);
    let edited_content = edited.event.content.expect("edited content");
    assert_eq!(edited_content.media_kind, Some(MediaKind::Photo));
    assert_eq!(
        edited_content.caption.as_deref(),
        Some("合作推广 https://t.me/+abcdef")
    );
    assert_eq!(edited_content.entities[0].value, "https://t.me/+abcdef");
}

#[test]
fn preserves_album_identity_and_deletion_batches() {
    let album: Vec<serde_json::Value> =
        serde_json::from_str(include_str!("fixtures/telegram/album_messages.json")).unwrap();
    let parsed = album
        .iter()
        .map(|update| parse_update(&serde_json::to_vec(update).unwrap(), 42).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(parsed[0].event.media_group_id.as_deref(), Some("album-1"));
    assert_eq!(parsed[1].event.media_group_id.as_deref(), Some("album-1"));
    assert_eq!(parsed[0].event.message_id, Some(510));
    assert_eq!(parsed[1].event.message_id, Some(511));

    let deleted = parse(include_str!("fixtures/telegram/deleted_messages.json"));
    assert_eq!(deleted.event.kind, RawEventKind::MessagesDeleted);
    assert_eq!(deleted.event.deleted_message_ids, vec![501, 502, 503]);
    assert_eq!(
        deleted.event.contact_display_name.as_deref(),
        Some("Deleted Contact")
    );
    assert_eq!(
        deleted.event.contact_username.as_deref(),
        Some("deleted_contact")
    );
}

#[test]
fn recognizes_owner_commands_and_rejects_invalid_required_fields() {
    let command = br#"{
      "update_id": 109,
      "message": {
        "message_id": 520,
        "from": {"id": 42},
        "chat": {"id": 42, "type": "private"},
        "date": 1783987270,
        "text": "/health"
      }
    }"#;
    assert_eq!(
        parse_update(command, 42).unwrap().event.kind,
        RawEventKind::OwnerCommand
    );
    assert!(parse_update(br#"{"update_id":110,"business_message":{}}"#, 42).is_err());
    assert!(parse_update(b"not json", 42).is_err());
}

#[test]
fn parses_chat_identity_before_inbound_sender_identity() {
    let parsed = parse(
        r#"{
          "update_id": 120,
          "business_message": {
            "message_id": 530,
            "business_connection_id": "business-1",
            "from": {
              "id": 1001,
              "is_bot": false,
              "first_name": "Sender",
              "last_name": "Person",
              "username": "sender_user"
            },
            "chat": {
              "id": 1001,
              "type": "private",
              "first_name": "  Contact\n",
              "last_name": " Name  ",
              "username": "contact_user"
            },
            "date": 1783987280,
            "text": "hello"
          }
        }"#,
    );

    assert_eq!(
        parsed.event.contact_display_name.as_deref(),
        Some("Contact Name")
    );
    assert_eq!(
        parsed.event.contact_username.as_deref(),
        Some("contact_user")
    );
}
