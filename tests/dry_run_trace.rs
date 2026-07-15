mod common;

use chathygiene::events::{record_prepared_event, recover_recorded_events};
use chathygiene::processing::{EventPreparer, LifecycleHandler, ProcessingEngine};
use chathygiene::storage::UnitOfWork;
use chathygiene::telegram::{RawEventKind, parse_update};

const CHALLENGE_TRACE: &str = concat!(
    "[DRY-RUN 追踪]\n\n",
    "更新 ID：1\n",
    "联系人：Sample Contact @sample_contact\n",
    "用户 ID：1001\n",
    "消息 ID：10\n",
    "事件：INBOUND_MESSAGE\n",
    "状态：NEW -> VERIFY_PENDING\n\n",
    "检测：\n",
    "- 判定：ALLOW\n",
    "- 分数：0\n",
    "- 原因：无\n",
    "- 规则：无\n",
    "- 失败：false\n\n",
    "验证：\n",
    "- 结果：CHALLENGE_STARTED\n",
    "- 剩余次数：3\n\n",
    "操作：\n",
    "- RECORD_MESSAGE：APPLIED\n",
    "- CREATE_CHALLENGE：APPLIED\n",
    "- UPDATE_CONVERSATION_STATE：APPLIED\n",
    "- SEND_CHALLENGE：QUEUED\n",
);

async fn trace_messages(pool: &sqlx::SqlitePool, update_id: i64) -> Vec<String> {
    let payloads: Vec<String> = sqlx::query_scalar(
        "SELECT payload_json FROM outbox_action
         WHERE source_update_id = ? AND idempotency_key = ?
         ORDER BY id",
    )
    .bind(update_id)
    .bind(format!("{update_id}:DRY_RUN_TRACE"))
    .fetch_all(pool)
    .await
    .unwrap();
    payloads
        .into_iter()
        .map(|payload| {
            serde_json::from_str::<serde_json::Value>(&payload).unwrap()["message"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect()
}

#[tokio::test]
async fn dry_run_spam_emits_one_body_free_trace_with_skipped_actions() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let detector = common::MutableDetector::new(common::DetectorMode::Allow);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector.clone(),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let mut first = common::inbound(1001, 10, Some("hello"), now);
    first.contact_display_name = Some("Sample Contact".to_owned());
    first.contact_username = Some("sample_contact".to_owned());
    engine.process(1, first).await.unwrap();

    let challenge = trace_messages(&pool, 1).await;
    assert_eq!(challenge, vec![CHALLENGE_TRACE]);

    detector.set(common::DetectorMode::Spam);
    let mut spam = common::inbound(1001, 11, Some("private-body-marker"), now);
    spam.contact_display_name = Some("Sample Contact".to_owned());
    spam.contact_username = Some("sample_contact".to_owned());
    let content = spam.content.as_mut().unwrap();
    content.caption = Some("private-caption-marker".to_owned());
    content.document_filename = Some("private-filename-marker.pdf".to_owned());
    engine.process(2, spam.clone()).await.unwrap();
    engine.process(2, spam).await.unwrap();

    let traces = trace_messages(&pool, 2).await;
    assert_eq!(traces.len(), 1);
    let trace = &traces[0];
    for expected in [
        "[DRY-RUN 追踪]",
        "更新 ID：2",
        "联系人：Sample Contact @sample_contact",
        "用户 ID：1001",
        "消息 ID：11",
        "事件：INBOUND_MESSAGE",
        "状态：VERIFY_PENDING -> VERIFY_PENDING",
        "检测：",
        "- 判定：SPAM",
        "- 分数：100",
        "- 原因：simulated spam",
        "- 规则：test_spam",
        "- 失败：false",
        "验证：\n- 结果：NOT_EVALUATED_SPAM_FIRST",
        "操作：",
        "- RECORD_MESSAGE：APPLIED",
        "- DELETE_MESSAGE：SKIPPED_DRY_RUN",
        "- SPAM_BLOCK：SKIPPED_DRY_RUN",
        "- KEEP_CHALLENGE_OPEN：APPLIED",
    ] {
        assert!(
            trace.contains(expected),
            "trace omitted {expected:?}: {trace}"
        );
    }
    for private in [
        "private-body-marker",
        "private-caption-marker",
        "private-filename-marker.pdf",
        "business-1",
    ] {
        assert!(
            !trace.contains(private),
            "trace persisted private marker {private:?}: {trace}"
        );
    }
    for old_label in [
        "[DRY-RUN TRACE]",
        "update_id:",
        "contact:",
        "message_id:",
        "detection:",
        "verification:",
        "actions:",
    ] {
        assert!(!trace.contains(old_label), "old label remained: {trace}");
    }
    assert!(!trace.contains("剩余次数"));
    let event_json: String =
        sqlx::query_scalar("SELECT event_json FROM processed_update WHERE update_id = 2")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(event_json.contains("Sample Contact"));
    assert!(event_json.contains("sample_contact"));
}

#[tokio::test]
async fn dry_run_trace_formats_missing_contact_identity() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );

    let mut display_only = common::inbound(7001, 71, Some("hello"), now);
    display_only.contact_display_name = Some("  Display\n Only  ".to_owned());
    engine.process(701, display_only).await.unwrap();
    assert!(trace_messages(&pool, 701).await[0].contains("联系人：Display Only\n用户 ID：7001"));

    let mut username_only = common::inbound(7002, 72, Some("hello"), now);
    username_only.contact_username = Some("username_only".to_owned());
    engine.process(702, username_only).await.unwrap();
    assert!(trace_messages(&pool, 702).await[0].contains("联系人：@username_only\n用户 ID：7002"));

    engine
        .process(703, common::inbound(7003, 73, Some("hello"), now))
        .await
        .unwrap();
    assert!(trace_messages(&pool, 703).await[0].contains("联系人：无\n用户 ID：7003"));
}

#[tokio::test]
async fn live_mode_emits_no_debug_trace() {
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
        .process(10, common::inbound(2001, 20, Some("hello"), now))
        .await
        .unwrap();

    assert!(trace_messages(&pool, 10).await.is_empty());
}

#[tokio::test]
async fn dry_run_traces_every_supported_lifecycle_event() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );

    let connection = parse_update(
        include_bytes!("fixtures/telegram/business_connection.json"),
        42,
    )
    .unwrap()
    .event;
    engine.process(100, connection).await.unwrap();
    engine
        .process(101, common::inbound(2001, 20, Some("hello"), now))
        .await
        .unwrap();

    let mut edited = common::inbound(2001, 20, Some("edited"), now);
    edited.kind = RawEventKind::EditedInboundMessage;
    engine.process(102, edited).await.unwrap();
    engine
        .process(103, common::owner_message(2001, 21, now))
        .await
        .unwrap();

    let mut bot = common::inbound(2001, 22, None, now);
    bot.kind = RawEventKind::BotBusinessMessage;
    engine.process(104, bot).await.unwrap();
    let mut implicit = common::inbound(2001, 23, None, now);
    implicit.kind = RawEventKind::ImplicitOwnerMessage;
    engine.process(105, implicit).await.unwrap();
    engine
        .process(106, common::deleted(2001, vec![21], now))
        .await
        .unwrap();

    let mut ignored = common::inbound(2001, 24, Some("ignored-private"), now);
    ignored.kind = RawEventKind::Ignored;
    ignored.connection_id = None;
    ignored.chat_id = None;
    ignored.message_id = None;
    engine.process(107, ignored).await.unwrap();

    for update_id in 100..=107 {
        assert_eq!(
            trace_messages(&pool, update_id).await.len(),
            1,
            "missing trace for update {update_id}"
        );
    }
}

#[tokio::test]
async fn dry_run_owner_command_emits_one_trace_without_command_text() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let command = parse_update(
        br#"{
          "update_id": 200,
          "message": {
            "message_id": 30,
            "from": {"id": 42},
            "chat": {"id": 42, "type": "private"},
            "date": 1783987300,
            "text": "/health"
          }
        }"#,
        42,
    )
    .unwrap();

    engine
        .process(command.update_id, command.event)
        .await
        .unwrap();

    let traces = trace_messages(&pool, 200).await;
    assert_eq!(traces.len(), 1);
    assert!(traces[0].contains("事件：OWNER_COMMAND"));
    assert!(traces[0].contains("- EXECUTE_OWNER_COMMAND：APPLIED"));
    assert!(traces[0].contains("- SEND_OWNER_MESSAGE：QUEUED"));
    assert!(!traces[0].contains("/health"));
}

#[tokio::test]
async fn recorded_lifecycle_recovery_inserts_trace_once() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut preparer = EventPreparer::new(
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let prepared_event = preparer
        .prepare(
            300,
            common::inbound(3001, 30, Some("recovery-private"), now),
            &mut read,
        )
        .await
        .unwrap();
    read.rollback().await.unwrap();
    record_prepared_event(&pool, &prepared_event).await.unwrap();

    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .unwrap(),
        1
    );
    assert_eq!(trace_messages(&pool, 300).await.len(), 1);
    assert_eq!(
        recover_recorded_events(&pool, &LifecycleHandler)
            .await
            .unwrap(),
        0
    );
    assert_eq!(trace_messages(&pool, 300).await.len(), 1);
}

#[tokio::test]
async fn dry_run_exhaustion_traces_attempts_and_skipped_block() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    engine
        .process(400, common::inbound(4001, 40, Some("hello"), now))
        .await
        .unwrap();
    for (update_id, message_id) in [(401, 41), (402, 42), (403, 43)] {
        engine
            .process(update_id, common::inbound(4001, message_id, Some("8"), now))
            .await
            .unwrap();
    }

    let second = trace_messages(&pool, 401).await;
    assert!(second[0].contains("- 结果：INCORRECT\n- 剩余次数：2"));
    let exhausted = trace_messages(&pool, 403).await;
    for expected in [
        "状态：VERIFY_PENDING -> NEW",
        "- 结果：INCORRECT\n- 剩余次数：0",
        "- CLOSE_CHALLENGE：APPLIED",
        "- UPDATE_CONVERSATION_STATE：APPLIED",
        "- TEMP_SOFT_BLOCK：SKIPPED_DRY_RUN",
    ] {
        assert!(exhausted[0].contains(expected));
    }
}

#[tokio::test]
async fn owner_mode_switch_traces_the_mode_active_at_update_start() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut dry_run_engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        false,
    );
    let off = owner_command(500, "/dry_run off");
    dry_run_engine
        .process(off.update_id, off.event)
        .await
        .unwrap();
    assert_eq!(trace_messages(&pool, 500).await.len(), 1);

    let (_directory, pool) = common::processing_database().await;
    let mut live_engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    );
    let on = owner_command(501, "/dry_run on");
    live_engine.process(on.update_id, on.event).await.unwrap();
    assert!(trace_messages(&pool, 501).await.is_empty());
    let health = owner_command(502, "/health");
    live_engine
        .process(health.update_id, health.event)
        .await
        .unwrap();
    assert_eq!(trace_messages(&pool, 502).await.len(), 1);
}

#[tokio::test]
async fn dispatching_trace_notifications_does_not_create_more_traces() {
    let harness = common::E2eHarness::new(false).await;
    harness.connect(600).await;
    harness
        .post(common::business_message(601, 6001, 60, 6001, Some("hello")))
        .await;
    let before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE idempotency_key LIKE '%:DRY_RUN_TRACE'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();

    harness.drain_outbox().await;

    let after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action
         WHERE idempotency_key LIKE '%:DRY_RUN_TRACE'",
    )
    .fetch_one(&harness.pool)
    .await
    .unwrap();
    assert_eq!(before, 2);
    assert_eq!(after, before);
}

fn owner_command(update_id: i64, text: &str) -> chathygiene::telegram::ParsedUpdate {
    let body = serde_json::json!({
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "from": {"id": 42},
            "chat": {"id": 42, "type": "private"},
            "date": 1_783_987_200_i64 + update_id,
            "text": text,
        }
    });
    parse_update(&serde_json::to_vec(&body).unwrap(), 42).unwrap()
}
