mod common;

use chathygiene::owner::{
    LabeledMessageBody, OwnerCommand, OwnerCommandError, OwnerCommandService, OwnerCommandSource,
    OwnerTelegramAction, parse_owner_command,
};
use chathygiene::processing::ProcessingEngine;
use chathygiene::storage::{ConversationKey, UnitOfWork};
use chathygiene::telegram::parse_update;

fn owner(sample: Option<LabeledMessageBody>) -> OwnerCommandSource {
    OwnerCommandSource {
        from_user_id: 42,
        chat_id: 42,
        private_chat: true,
        replied_sample: sample,
    }
}

async fn seed_active_reset_target(
    pool: &sqlx::SqlitePool,
    now: chrono::DateTime<chrono::Utc>,
) -> i64 {
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
    sqlx::query(
        "UPDATE challenge
         SET prompt_message_id = 901, delivery_status = 'SENT'
         WHERE chat_id = 1001",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE conversation
         SET block_expires_at = '2026-07-15T00:00:00Z',
             block_reason = 'old block', block_count = 2
         WHERE chat_id = 1001",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE outbox_action SET status = 'SUCCEEDED'
         WHERE id = (
           SELECT MIN(id) FROM outbox_action
           WHERE connection_id = 'business-1' AND chat_id = 1001
         )",
    )
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox_action
         (source_update_id, connection_id, chat_id, action_type, payload_json,
          idempotency_key, status, attempts, created_at, updated_at)
         VALUES (
           1, 'business-1', 1001, 'READ_BUSINESS_MESSAGE',
           '{\"message_id\":10}', 'reset-test-retry', 'RETRY', 1, ?, ?
         )",
    )
    .bind(now.to_rfc3339())
    .bind(now.to_rfc3339())
    .execute(pool)
    .await
    .unwrap();
    sqlx::query_scalar("SELECT state_version FROM conversation WHERE chat_id = 1001")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn assert_reset_local_cleanup(pool: &sqlx::SqlitePool, before_version: i64) {
    let conversation: (String, Option<String>, Option<String>, i64, i64) = sqlx::query_as(
        "SELECT state, block_expires_at, block_reason, block_count, state_version
         FROM conversation WHERE chat_id = 1001",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(conversation.0, "NEW");
    assert_eq!(conversation.1, None);
    assert_eq!(conversation.2, None);
    assert_eq!(conversation.3, 0);
    assert_eq!(conversation.4, before_version + 1);

    for table in ["message_ledger", "challenge"] {
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE chat_id = 1001"
        ))
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(count, 0, "{table} was not purged");
    }
    let unfinished: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE connection_id = 'business-1' AND chat_id = 1001
           AND status IN ('PENDING', 'RETRY')",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(unfinished, 0);
    let terminal: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE connection_id = 'business-1' AND chat_id = 1001
           AND status = 'SUCCEEDED'",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    assert_eq!(terminal, 1);
}

#[test]
fn parses_exact_commands_suffixes_and_arguments() {
    assert_eq!(parse_owner_command("/help").unwrap(), OwnerCommand::Help);
    assert_eq!(
        parse_owner_command("/help@ChatHygieneBot").unwrap(),
        OwnerCommand::Help
    );
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
        "/help extra",
        "/health@",
    ] {
        assert!(
            parse_owner_command(invalid).is_err(),
            "accepted {invalid:?}"
        );
    }
}

#[tokio::test]
async fn help_lists_supported_owner_commands() {
    let (_directory, pool) = common::processing_database().await;
    let service = OwnerCommandService::at(common::at("2026-07-14T00:00:00Z"));
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();

    let help = service
        .execute(OwnerCommand::Help, owner(None), &mut uow)
        .await
        .unwrap();

    assert_eq!(
        help.response,
        "owner commands:\n\
/help - list owner commands\n\
/health - show connection and dry-run status\n\
/inspect <chat_id> - show conversation state\n\
/reset <chat_id> - delete known messages and reset the conversation\n\
/unblock <chat_id> - clear a local soft block\n\
/dry_run on|off - set dry-run mode\n\
/errors [1..20] - show recent errors\n\
/mark_spam - label the replied message as spam\n\
/mark_ham - label the replied message as ham"
    );
    uow.rollback().await.unwrap();
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
    assert!(health.response.contains("connection=enabled"));
    let inspection = service
        .execute(
            OwnerCommand::Inspect { chat_id: 1001 },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    assert!(inspection.response.contains("state=VERIFY_PENDING"));
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
    assert!(errors.response.contains("SIMULATED"));
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
async fn reset_clears_active_conversation_and_returns_every_known_message() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let before_version = seed_active_reset_target(&pool, now).await;
    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let result = service
        .execute(OwnerCommand::Reset { chat_id: 1001 }, owner(None), &mut uow)
        .await
        .unwrap();
    assert_eq!(
        result.response,
        "reset chat_id=1001 telegram_delete=queued message_count=3"
    );
    assert_eq!(
        result.telegram_actions,
        vec![OwnerTelegramAction::DeleteBusinessMessages {
            key: ConversationKey::new("business-1", 1001),
            message_ids: vec![10, 11, 901],
        }]
    );
    uow.commit().await.unwrap();

    assert_reset_local_cleanup(&pool, before_version).await;
}

#[tokio::test]
async fn reset_without_undeleted_messages_returns_no_telegram_action() {
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
    sqlx::query("UPDATE message_ledger SET deleted_at = ? WHERE chat_id = 1001")
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();

    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let result = service
        .execute(OwnerCommand::Reset { chat_id: 1001 }, owner(None), &mut uow)
        .await
        .unwrap();
    assert_eq!(
        result.response,
        "reset chat_id=1001 telegram_delete=none message_count=0"
    );
    assert!(result.telegram_actions.is_empty());
    uow.commit().await.unwrap();
}

#[tokio::test]
async fn unblock_preserves_message_and_challenge_history() {
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
    sqlx::query(
        "UPDATE conversation
         SET state = 'SPAM_SOFT_BLOCKED', block_reason = 'spam', block_count = 1
         WHERE chat_id = 1001",
    )
    .execute(&pool)
    .await
    .unwrap();
    let ledger_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM message_ledger WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();
    let challenges_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();

    let service = OwnerCommandService::at(now);
    let mut uow = UnitOfWork::begin(&pool).await.unwrap();
    let result = service
        .execute(
            OwnerCommand::Unblock { chat_id: 1001 },
            owner(None),
            &mut uow,
        )
        .await
        .unwrap();
    assert_eq!(result.response, "unblocked chat_id=1001");
    assert!(result.telegram_actions.is_empty());
    uow.commit().await.unwrap();

    let ledger_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM message_ledger WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();
    let challenges_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 1001")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(ledger_after, ledger_before);
    assert_eq!(challenges_after, challenges_before);
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
