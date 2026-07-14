#![allow(dead_code)]

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use chathygiene::clock::Clock;
use chathygiene::detection::{
    Decision, DetectionContext, DetectionResult, DetectorError, MessageContent, SpamDetector,
};
use chathygiene::storage::{connect, migrate};
use chathygiene::telegram::{RawBusinessEvent, RawEventKind};
use chathygiene::verification::{AnswerKind, ChallengeVerifier, GeneratedChallenge};
use chrono::{DateTime, Duration, Utc};
use sqlx::SqlitePool;
use tempfile::TempDir;

pub fn temporary_database() -> (TempDir, String) {
    let directory = tempfile::tempdir().expect("create temporary directory");
    let database_path = directory.path().join("chathygiene.db");
    let url = sqlite_url(&database_path);
    (directory, url)
}

fn sqlite_url(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

pub fn at(value: &str) -> DateTime<Utc> {
    value.parse().expect("valid timestamp")
}

pub async fn processing_database() -> (TempDir, SqlitePool) {
    let (directory, url) = temporary_database();
    let pool = connect(&url).await.expect("connect database");
    migrate(&pool).await.expect("migrate database");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .expect("seed connection");
    (directory, pool)
}

#[derive(Debug, Clone, Copy)]
pub enum DetectorMode {
    Allow,
    Spam,
    Fail,
}

#[derive(Clone)]
pub struct MutableDetector {
    mode: Arc<Mutex<DetectorMode>>,
    calls: Arc<AtomicUsize>,
}

impl MutableDetector {
    pub fn new(mode: DetectorMode) -> Self {
        Self {
            mode: Arc::new(Mutex::new(mode)),
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn set(&self, mode: DetectorMode) {
        *self.mode.lock().unwrap() = mode;
    }

    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl SpamDetector for MutableDetector {
    fn detect<'a>(
        &'a self,
        _message: &'a MessageContent,
        _context: &'a DetectionContext,
    ) -> Pin<Box<dyn Future<Output = Result<DetectionResult, DetectorError>> + Send + 'a>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match *self.mode.lock().unwrap() {
                DetectorMode::Allow => Ok(result(Decision::Allow, 0)),
                DetectorMode::Spam => Ok(result(Decision::Spam, 100)),
                DetectorMode::Fail => {
                    Err(DetectorError::InvalidConfig("simulated failure".to_owned()))
                }
            }
        })
    }
}

fn result(decision: Decision, score: u8) -> DetectionResult {
    DetectionResult {
        decision,
        score,
        reasons: if decision == Decision::Spam {
            vec!["simulated spam".to_owned()]
        } else {
            Vec::new()
        },
        matched_rules: if decision == Decision::Spam {
            vec!["test_spam".to_owned()]
        } else {
            Vec::new()
        },
        detector_name: "test".to_owned(),
        detector_version: "1".to_owned(),
        normalized_hash: "abc123".to_owned(),
    }
}

pub struct FixedVerifier;

impl ChallengeVerifier for FixedVerifier {
    fn generate(&mut self, now: DateTime<Utc>) -> GeneratedChallenge {
        GeneratedChallenge {
            expression: "7 + 5 - 3".to_owned(),
            answer_hmac: "fixed-hmac".to_owned(),
            created_at: now,
            expires_at: now + Duration::minutes(2),
            max_attempts: 3,
        }
    }

    fn evaluate(&self, raw: &str, _expected_hmac: &str) -> AnswerKind {
        match raw.trim() {
            "9" => AnswerKind::Correct,
            value if value.parse::<i64>().is_ok() => AnswerKind::Incorrect,
            _ => AnswerKind::NonNumeric,
        }
    }
}

#[derive(Clone)]
pub struct TestClock {
    now: Arc<Mutex<DateTime<Utc>>>,
}

impl TestClock {
    pub fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(Mutex::new(now)),
        }
    }

    pub fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap() = now;
    }
}

impl Clock for TestClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

pub fn inbound(
    chat_id: i64,
    message_id: i64,
    text: Option<&str>,
    now: DateTime<Utc>,
) -> RawBusinessEvent {
    RawBusinessEvent {
        kind: RawEventKind::InboundMessage,
        connection_id: Some("business-1".to_owned()),
        chat_id: Some(chat_id),
        message_id: Some(message_id),
        media_group_id: None,
        content: Some(MessageContent {
            text: text.map(str::to_owned),
            ..MessageContent::default()
        }),
        deleted_message_ids: Vec::new(),
        connection: None,
        occurred_at: now,
    }
}

pub fn owner_message(chat_id: i64, message_id: i64, now: DateTime<Utc>) -> RawBusinessEvent {
    let mut event = inbound(chat_id, message_id, Some("owner reply"), now);
    event.kind = RawEventKind::ManualOwnerMessage;
    event
}

pub fn deleted(chat_id: i64, message_ids: Vec<i64>, now: DateTime<Utc>) -> RawBusinessEvent {
    RawBusinessEvent {
        kind: RawEventKind::MessagesDeleted,
        connection_id: Some("business-1".to_owned()),
        chat_id: Some(chat_id),
        message_id: None,
        media_group_id: None,
        content: None,
        deleted_message_ids: message_ids,
        connection: None,
        occurred_at: now,
    }
}
