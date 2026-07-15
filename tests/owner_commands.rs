mod common;

use chathygiene::owner::{
    LabeledMessageBody, OwnerCommand, OwnerCommandError, OwnerCommandService, OwnerCommandSource,
    parse_owner_command,
};
use chathygiene::processing::ProcessingEngine;
use chathygiene::storage::UnitOfWork;
use chathygiene::telegram::parse_update;

fn owner(sample: Option<LabeledMessageBody>) -> OwnerCommandSource {
    OwnerCommandSource {
        from_user_id: 42,
        private_chat: true,
        replied_sample: sample,
    }
}

#[test]
fn parses_exact_commands_suffixes_and_arguments() {
    assert_eq!(
        parse_owner_command("/health").unwrap(),
        OwnerCommand::Health
    );
    assert_eq!(
        parse_owner_command("/health@ChatHygieneBot").unwrap(),
        OwnerCommand::Health
    );
    assert_eq!(
        parse_owner_command(" /inspect 123 ").unwrap(),
        OwnerCommand::Inspect { chat_id: 123 }
    );
    assert_eq!(
        parse_owner_command("/reset 123").unwrap(),
        OwnerCommand::Reset { chat_id: 123 }
    );
    assert_eq!(
        parse_owner_command("/unblock 123").unwrap(),
        OwnerCommand::Unblock { chat_id: 123 }
    );
    assert_eq!(
        parse_owner_command("/dry_run on").unwrap(),
        OwnerCommand::DryRun { enabled: true }
    );
    assert_eq!(
        parse_owner_command("/dry_run off").unwrap(),
        OwnerCommand::DryRun { enabled: false }
    );
    for (input, expected) in [
        ("/errors", 10),
        ("/errors nope", 10),
        ("/errors -3", 10),
        ("/errors 0", 10),
        ("/errors 1", 1),
        ("/errors 20", 20),
        ("/errors 21", 20),
        ("/errors 255", 20),
    ] {
        assert_eq!(
            parse_owner_command(input).unwrap(),
            OwnerCommand::Errors { limit: expected },
            "unexpected normalization for {input:?}"
        );
    }
    assert_eq!(
        parse_owner_command("/mark_spam").unwrap(),
        OwnerCommand::MarkSpam
    );
    assert_eq!(
        parse_owner_command("/mark_ham").unwrap(),
        OwnerCommand::MarkHam
    );
}

#[test]
fn rejects_unknown_commands_invalid_ids_and_limits() {
    for invalid in [
        "",
        "health",
        "/unknown",
        "/inspect",
        "/inspect 0",
        "/inspect nope",
        "/reset 1 extra",
        "/dry_run maybe",
        "/errors 1 extra",
        "/mark_spam extra",
        "/health@",
    ] {
        assert!(
            parse_owner_command(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[tokio::test]
async fn executes_recovery_runtime_and_error_commands() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("hello"), now))
        .await
        .unwrap();
    let service = OwnerCommandService::at(now);

    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let health = service
        .execute(OwnerCommand::Health, owner(None), &mut uow)
        .await
        .unwrap();
    assert!(health.contains("connection=enabled"));
    let inspection = service
        .execute(
            OwnerCommand::Inspect { chat_id: 1001 },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    assert!(inspection.contains("state=VERIFY_PENDING"));
    service
        .execute(OwnerCommand::Reset { chat_id: 1001 }, owner(None), &mut uow)
        .await
        .unwrap();
    uow.commit().await.unwrap();
    sqlx::query(
        "UPDATE conversation
         SET state = 'SPAM_SOFT_BLOCKED', block_reason = 'spam'
         WHERE chat_id = 1001",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    service
        .execute(
            OwnerCommand::Unblock { chat_id: 1001 },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    service
        .execute(
            OwnerCommand::DryRun { enabled: true },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    service
        .execute(
            OwnerCommand::DryRun { enabled: false },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    uow.commit().await.unwrap();
    let state: String = sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = 1001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "NEW");
    let runtime: String =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(runtime, "true");
    let recovery_audits: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM audit_event WHERE event_kind = 'OWNER_RECOVERY'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(recovery_audits, 2);
}

#[tokio::test]
async fn reports_recent_errors() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "INSERT INTO audit_event
         (event_kind, error_code, error_message, occurred_at)
         VALUES ('TEST', 'SIMULATED', 'simulated error', ?)",
    )
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let errors = service
        .execute(OwnerCommand::Errors { limit: 1 }, owner(None), &mut uow)
        .await
        .unwrap();
    assert!(errors.contains("SIMULATED"));
    uow.rollback().await.unwrap();
}

#[tokio::test]
async fn stores_bodies_only_for_explicit_label_commands() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let service = OwnerCommandService::at(now);
    let sample = LabeledMessageBody {
        body: "ＦＲＥＥ   AIRDROP https://t.me/+abc".to_owned(),
        content_type: "text".to_owned(),
        source_chat_id: Some(8001),
        source_message_id: Some(77),
    };
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    service
        .execute(OwnerCommand::MarkSpam, owner(Some(sample)), &mut uow)
        .await
        .unwrap();
    service
        .execute(
            OwnerCommand::MarkHam,
            owner(Some(LabeledMessageBody {
                body: "normal hello".to_owned(),
                content_type: "text".to_owned(),
                source_chat_id: None,
                source_message_id: None,
            })),
            &mut uow,
        )
        .await
        .unwrap();
    uow.commit().await.unwrap();
    let stored: (String, String, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT body, normalized_hash, source_chat_id, source_message_id FROM spam_sample",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(stored.0, "ＦＲＥＥ   AIRDROP https://t.me/+abc");
    assert_eq!(stored.2, Some(8001));
    assert_eq!(stored.3, Some(77));
    assert_eq!(stored.1.len(), 64);
    let ham: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM ham_sample")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(ham, 1);

    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let error = service
        .execute(OwnerCommand::MarkHam, owner(None), &mut uow)
        .await
        .unwrap_err();
    assert_eq!(error, OwnerCommandError::MissingSample);
    uow.rollback().await.unwrap();
}

#[tokio::test]
async fn destructive_mode_requires_enabled_connection_and_all_rights() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "UPDATE business_connection
         SET rights_json = '{\"can_reply\":true,\"can_read_messages\":true,\"can_delete_sent_messages\":false,\"can_delete_all_messages\":true}'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let error = service
        .execute(
            OwnerCommand::DryRun { enabled: false },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap_err();
    assert_eq!(error, OwnerCommandError::BusinessRightsUnavailable);
    uow.rollback().await.unwrap();
}

#[tokio::test]
async fn reset_preserves_active_until_owner_replies_are_deleted() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("hello"), now))
        .await
        .unwrap();
    engine
        .process(2, common::owner_message(1001, 11, now))
        .await
        .unwrap();
    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let error = service
        .execute(OwnerCommand::Reset { chat_id: 1001 }, owner(None), &mut uow)
        .await
        .unwrap_err();
    assert_eq!(error, OwnerCommandError::ActiveConversation);
    uow.rollback().await.unwrap();
}

#[tokio::test]
async fn health_reports_the_engine_destructive_default() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    let parsed = parse_update(
        br#"{
          "update_id": 900,
          "message": {
            "message_id": 1,
            "from": {"id": 42},
            "chat": {"id": 42, "type": "private"},
            "date": 1783987200,
            "text": "/health"
          }
        }"#,
        42,
    )
    .unwrap();

    engine
        .process(parsed.update_id, parsed.event)
        .await
        .unwrap();

    let reply: String = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id = 900 AND action_type = 'SEND_OWNER_MESSAGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(reply.contains("dry_run=off"));
}
