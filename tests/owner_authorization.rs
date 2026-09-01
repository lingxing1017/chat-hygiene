mod common;

use chathygiene::owner::{
    OwnerCommand, OwnerCommandError, OwnerCommandService, OwnerCommandSource,
};
use chathygiene::processing::ProcessingEngine;
use chathygiene::storage::{
    OwnerChatSource, UnitOfWork, initialize_or_load_owner_identity, promote_owner_chat,
};
use chathygiene::telegram::{RawEventKind, parse_update};

fn source(from_user_id: i64, private_chat: bool) -> OwnerCommandSource {
    OwnerCommandSource {
        from_user_id,
        chat_id: from_user_id,
        private_chat,
        replied_sample: None,
    }
}

#[tokio::test]
async fn authorizes_only_numeric_owner_identity_in_private_bot_chat() {
    let (_directory, pool) = common::processing_database().await;
    initialize_or_load_owner_identity(&pool, common::at("2026-07-14T00:00:00Z"))
        .await
        .unwrap();
    let service = OwnerCommandService::at(common::at("2026-07-14T00:00:00Z"));

    for untrusted in [source(99, true), source(42, false)] {
        let mut uow = UnitOfWork::begin(&pool).await.unwrap();
        let error = service
            .execute(OwnerCommand::Health, untrusted, &mut uow)
            .await
            .unwrap_err();
        assert_eq!(error, OwnerCommandError::Unauthorized);
        uow.commit().await.unwrap();
    }
    let audits: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM audit_event WHERE event_kind = 'SECURITY' AND error_code = 'OWNER_COMMAND_UNAUTHORIZED'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(audits, 2);

    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    assert!(
        service
            .execute(OwnerCommand::Health, source(42, true), &mut uow)
            .await
            .is_ok()
    );
    uow.rollback().await.unwrap();
}

#[test]
fn parser_accepts_normal_private_bot_commands_but_not_business_messages() {
    let normal = br#"{
      "update_id": 200,
      "message": {
        "message_id": 1,
        "from": {"id": 42},
        "chat": {"id": 42, "type": "private"},
        "date": 1783987270,
        "text": "/health@ChatHygieneBot"
      }
    }"#;
    let parsed = parse_update(normal, 42).unwrap();
    assert_eq!(parsed.event.kind, RawEventKind::OwnerCommand);
    let command = parsed.event.owner_command.unwrap();
    assert_eq!(command.from_user_id, Some(42));
    assert!(command.private_chat);
    assert_eq!(command.text, "/health@ChatHygieneBot");

    let business = br#"{
      "update_id": 201,
      "business_message": {
        "message_id": 2,
        "business_connection_id": "business-1",
        "from": {"id": 42},
        "chat": {"id": 1001, "type": "private"},
        "date": 1783987270,
        "text": "/health"
      }
    }"#;
    assert_eq!(
        parse_update(business, 42).unwrap().event.kind,
        RawEventKind::ManualOwnerMessage
    );

    let channel = br#"{
      "update_id": 203,
      "channel_post": {
        "message_id": 3,
        "chat": {"id": -1001, "type": "channel"},
        "date": 1783987270,
        "text": "/health"
      }
    }"#;
    let parsed = parse_update(channel, 42).unwrap();
    assert_eq!(parsed.event.kind, RawEventKind::Ignored);
    assert!(parsed.event.owner_command.is_none());
}

#[test]
fn parser_preserves_explicit_replied_sample_only_for_transient_command_handling() {
    let update = br#"{
      "update_id": 202,
      "message": {
        "message_id": 3,
        "from": {"id": 42},
        "chat": {"id": 42, "type": "private"},
        "date": 1783987270,
        "text": "/mark_spam",
        "reply_to_message": {
          "message_id": 2,
          "from": {"id": 77},
          "chat": {"id": 42, "type": "private"},
          "date": 1783987200,
          "caption": "promo caption",
          "forward_origin": {
            "type": "channel",
            "chat": {"id": -1009001},
            "message_id": 88
          }
        }
      }
    }"#;
    let parsed = parse_update(update, 42).unwrap();
    let sample = parsed.event.owner_command.unwrap().replied_sample.unwrap();
    assert_eq!(sample.body, "promo caption");
    assert_eq!(sample.content_type, "caption");
    assert_eq!(sample.source_chat_id, Some(-1_009_001));
    assert_eq!(sample.source_message_id, Some(88));
}

#[tokio::test]
async fn processing_persists_body_only_in_sample_table_and_replies_via_outbox() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    initialize_or_load_owner_identity(&pool, now)
        .await
        .expect("import legacy owner");
    let mut owner = UnitOfWork::begin_immediate(&pool)
        .await
        .expect("begin owner chat promotion");
    promote_owner_chat(&mut owner, 42, 4200, OwnerChatSource::BusinessConnection)
        .await
        .expect("promote owner chat");
    owner.commit().await.expect("commit owner chat promotion");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let update = br#"{
      "update_id": 300,
      "message": {
        "message_id": 3,
        "from": {"id": 42},
        "chat": {"id": 4200, "type": "private"},
        "date": 1783987270,
        "text": "/mark_spam",
        "reply_to_message": {
          "message_id": 2,
          "from": {"id": 77},
          "chat": {"id": 4200, "type": "private"},
          "date": 1783987200,
          "text": "secret promotional body"
        }
      }
    }"#;
    let parsed = parse_update(update, 42).unwrap();
    engine
        .process(parsed.update_id, parsed.event)
        .await
        .unwrap();

    let body: String = sqlx::query_scalar("SELECT body FROM spam_sample")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(body, "secret promotional body");
    let event_json: String =
        sqlx::query_scalar("SELECT event_json FROM processed_update WHERE update_id = 300")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!event_json.contains("secret promotional body"));
    let replies: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id = 300 AND action_type = 'SEND_OWNER_MESSAGE'
         ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert!(
        replies
            .iter()
            .any(|reply| reply.contains("sample=spam stored"))
    );
    let replies = replies
        .iter()
        .map(|reply| serde_json::from_str::<serde_json::Value>(reply).unwrap())
        .collect::<Vec<_>>();
    assert!(
        replies
            .iter()
            .all(|reply| reply.get("owner_user_id").is_none())
    );
    assert!(replies.iter().any(|reply| reply["owner_chat_id"] == 4200));

    let unauthorized = br#"{
      "update_id": 301,
      "message": {
        "message_id": 4,
        "from": {"id": 99},
        "chat": {"id": 99, "type": "private"},
        "date": 1783987271,
        "text": "/health"
      }
    }"#;
    let parsed = parse_update(unauthorized, 42).unwrap();
    engine
        .process(parsed.update_id, parsed.event)
        .await
        .unwrap();
    let leaked_replies: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE source_update_id = 301")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(leaked_replies, 0);
}
