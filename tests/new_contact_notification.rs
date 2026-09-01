mod common;

use std::sync::{Arc, Mutex};

use chathygiene::processing::{
    NewContactNotice, NewContactNotifier, NewContactNotifyError, ProcessingEngine,
};
use chathygiene::storage::{
    OwnerChatSource, UnitOfWork, initialize_or_load_owner_identity, promote_owner_chat,
};

#[derive(Clone, Default)]
struct CapturingNotifier {
    notices: Arc<Mutex<Vec<NewContactNotice>>>,
    fail: bool,
}

impl CapturingNotifier {
    fn failing() -> Self {
        Self {
            notices: Arc::new(Mutex::new(Vec::new())),
            fail: true,
        }
    }
}

impl NewContactNotifier for CapturingNotifier {
    fn try_notify(&self, notice: NewContactNotice) -> Result<(), NewContactNotifyError> {
        self.notices.lock().unwrap().push(notice);
        if self.fail {
            return Err(NewContactNotifyError::QueueFull);
        }
        Ok(())
    }
}

#[tokio::test]
async fn first_inbound_notifies_once_without_live_identity_persistence() {
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
    let notifier = CapturingNotifier::default();
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    )
    .with_new_contact_notifier(notifier.clone());

    let mut first = common::inbound(1001, 10, Some("hello"), now);
    first.contact_display_name = Some("Live Contact Marker".to_owned());
    first.contact_username = Some("live_contact_marker".to_owned());
    engine.process(1, first).await.unwrap();
    engine
        .process(2, common::inbound(1001, 11, Some("9"), now))
        .await
        .unwrap();

    assert_eq!(
        notifier.notices.lock().unwrap().as_slice(),
        &[NewContactNotice {
            owner_user_id: 42,
            owner_chat_id: 4200,
            contact_chat_id: 1001,
            username: Some("live_contact_marker".to_owned()),
        }]
    );

    let durable_rows: Vec<String> = sqlx::query_scalar(
        "SELECT event_json FROM processed_update
         UNION ALL SELECT COALESCE(reasons_json, '') FROM audit_event
         UNION ALL SELECT COALESCE(rule_ids_json, '') FROM audit_event
         UNION ALL SELECT COALESCE(error_message, '') FROM audit_event
         UNION ALL SELECT payload_json FROM outbox_action",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for identity in ["Live Contact Marker", "live_contact_marker"] {
        assert!(durable_rows.iter().all(|row| !row.contains(identity)));
    }
}

#[tokio::test]
async fn duplicate_update_does_not_repeat_new_contact_notification() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let notifier = CapturingNotifier::default();
    let mut engine = ProcessingEngine::new(
        pool,
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    )
    .with_new_contact_notifier(notifier.clone());
    let event = common::inbound(1001, 10, Some("hello"), now);

    engine.process(10, event.clone()).await.unwrap();
    engine.process(10, event).await.unwrap();

    assert_eq!(notifier.notices.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn notification_queue_failure_does_not_roll_back_first_contact() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let notifier = CapturingNotifier::failing();
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(now),
        true,
    )
    .with_new_contact_notifier(notifier.clone());

    engine
        .process(20, common::inbound(2001, 20, Some("hello"), now))
        .await
        .unwrap();

    let status: String =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = 20")
            .fetch_one(&pool)
            .await
            .unwrap();
    let conversations: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM conversation WHERE chat_id = 2001")
            .fetch_one(&pool)
            .await
            .unwrap();
    let challenges: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM challenge WHERE chat_id = 2001")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(status, "APPLIED");
    assert_eq!(conversations, 1);
    assert_eq!(challenges, 1);
    assert_eq!(notifier.notices.lock().unwrap().len(), 1);
}
