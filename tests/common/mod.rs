#![allow(dead_code)]

use std::collections::VecDeque;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration as StdDuration;

use axum::body::Body;
use axum::extract::{OriginalUri, State};
use axum::http::{Request, StatusCode};
use axum::routing::any;
use axum::{Json, Router};
use chathygiene::app::build_router_with_inbox;
use chathygiene::clock::Clock;
use chathygiene::detection::{
    Decision, DetectionContext, DetectionResult, DetectorError, MessageContent, RuleDetector,
    SpamDetector,
};
use chathygiene::owner::OwnerIdentityHandle;
use chathygiene::processing::{ProcessingEngine, ProcessingWorker, spawn_processing_worker};
use chathygiene::storage::{
    UnitOfWork, claim_owner, connect, initialize_or_load_owner_identity, migrate,
};
use chathygiene::telegram::{
    AuthoritativeBusinessConnection, BoxFuture, BusinessConnectionApi, BusinessRights,
    DispatchOutcome, OutboxDispatcher, RawBusinessEvent, RawEventKind, TelegramClient,
    TelegramError,
};
use chathygiene::verification::{
    AnswerKind, ArithmeticVerifier, ChallengeVerifier, GeneratedChallenge,
};
use chrono::{DateTime, Duration, Utc};
use rand::SeedableRng;
use rand::rngs::StdRng;
use secrecy::SecretString;
use serde_json::{Value, json};
use sqlx::SqlitePool;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tower::ServiceExt;
use url::Url;

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
         VALUES (
           'business-1', 42,
           '{\"can_reply\":true,\"can_read_messages\":true,\"can_delete_sent_messages\":true,\"can_delete_all_messages\":true}',
           1, '2026-07-14T00:00:00Z'
         )",
    )
    .execute(&pool)
    .await
    .expect("seed connection");
    initialize_or_load_owner_identity(&pool, at("2026-07-14T00:00:00Z"))
        .await
        .expect("initialize processing Owner");
    (directory, pool)
}

#[derive(Clone)]
struct TelegramStubState {
    requests: Arc<Mutex<Vec<(String, Value)>>>,
    plans: Arc<Mutex<VecDeque<TelegramPlan>>>,
    next_message_id: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub enum TelegramPlan {
    Success,
    Delay(StdDuration),
    Api(StatusCode, Value),
}

#[derive(Clone)]
pub struct TelegramStub {
    state: TelegramStubState,
    base_url: Url,
}

impl TelegramStub {
    async fn start() -> Self {
        let state = TelegramStubState {
            requests: Arc::new(Mutex::new(Vec::new())),
            plans: Arc::new(Mutex::new(VecDeque::new())),
            next_message_id: Arc::new(AtomicUsize::new(900)),
        };
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Telegram stub");
        let address = listener.local_addr().expect("read Telegram stub address");
        let app = Router::new()
            .fallback(any(capture_telegram_request))
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(listener, app)
                .await
                .expect("serve Telegram stub");
        });
        Self {
            state,
            base_url: Url::parse(&format!("http://{address}/")).expect("valid Telegram stub URL"),
        }
    }

    pub fn plan(&self, plan: TelegramPlan) {
        self.state.plans.lock().unwrap().push_back(plan);
    }

    pub fn request_count(&self, method: &str) -> usize {
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _)| path.ends_with(&format!("/{method}")))
            .count()
    }

    pub fn requests(&self) -> Vec<(String, Value)> {
        self.state.requests.lock().unwrap().clone()
    }

    fn client(&self) -> TelegramClient {
        let http = reqwest::Client::builder()
            .timeout(StdDuration::from_millis(50))
            .build()
            .expect("build Telegram test client");
        TelegramClient::with_base_url(
            http,
            SecretString::from("123456:test-token".to_owned()),
            self.base_url.clone(),
        )
    }
}

#[derive(Clone, Default)]
struct TestBusinessConnectionApi {
    response: Arc<Mutex<Option<AuthoritativeBusinessConnection>>>,
}

impl TestBusinessConnectionApi {
    fn stage_from_update(&self, update: &Value) {
        let Some(connection) = update.get("business_connection") else {
            return;
        };
        let rights = connection.get("rights").unwrap_or(&Value::Null);
        let mut response = self.response.lock().unwrap();
        let connection_id = connection["id"].as_str().unwrap().to_owned();
        let connection_established_at = response
            .as_ref()
            .filter(|current| current.connection_id == connection_id)
            .map_or_else(
                || connection["date"].as_i64().unwrap(),
                |current| current.connection_established_at,
            );
        *response = Some(AuthoritativeBusinessConnection {
            connection_id,
            business_user_id: connection["user"]["id"].as_i64().unwrap(),
            user_chat_id: connection["user_chat_id"].as_i64(),
            connection_established_at,
            rights: BusinessRights {
                can_reply: rights["can_reply"].as_bool().unwrap_or(false),
                can_read_messages: rights["can_read_messages"].as_bool().unwrap_or(false),
                can_delete_sent_messages: rights["can_delete_sent_messages"]
                    .as_bool()
                    .unwrap_or(false),
                can_delete_all_messages: rights["can_delete_all_messages"]
                    .as_bool()
                    .unwrap_or(false),
            },
            enabled: connection["is_enabled"].as_bool().unwrap_or(false),
        });
    }
}

impl BusinessConnectionApi for TestBusinessConnectionApi {
    fn get_business_connection<'a>(
        &'a self,
        _connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async move {
            self.response.lock().unwrap().clone().ok_or_else(|| {
                TelegramError::InvalidRequest("test authoritative state is missing".to_owned())
            })
        })
    }
}

async fn capture_telegram_request(
    State(state): State<TelegramStubState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<Value>,
) -> (StatusCode, Json<Value>) {
    let path = uri.path().to_owned();
    state.requests.lock().unwrap().push((path.clone(), body));
    let plan = state
        .plans
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or(TelegramPlan::Success);
    match plan {
        TelegramPlan::Success => telegram_success(&state, &path),
        TelegramPlan::Delay(duration) => {
            tokio::time::sleep(duration).await;
            telegram_success(&state, &path)
        }
        TelegramPlan::Api(status, body) => (status, Json(body)),
    }
}

fn telegram_success(state: &TelegramStubState, path: &str) -> (StatusCode, Json<Value>) {
    let result = if path.ends_with("/sendMessage") {
        json!({
            "message_id": state.next_message_id.fetch_add(1, Ordering::SeqCst)
        })
    } else {
        Value::Bool(true)
    };
    (StatusCode::OK, Json(json!({"ok": true, "result": result})))
}

pub struct E2eHarness {
    _directory: TempDir,
    pub pool: SqlitePool,
    pub router: Router,
    pub clock: TestClock,
    pub telegram: TelegramStub,
    authoritative_api: TestBusinessConnectionApi,
    dispatcher: OutboxDispatcher<TelegramClient>,
    _processing_worker: ProcessingWorker,
}

impl E2eHarness {
    pub async fn new(destructive_mode: bool) -> Self {
        let (directory, database_url) = temporary_database();
        let pool = connect(&database_url).await.expect("connect E2E database");
        migrate(&pool).await.expect("migrate E2E database");
        let clock = TestClock::new(at("2026-07-14T12:00:00Z"));
        initialize_or_load_owner_identity(&pool, clock.now())
            .await
            .expect("initialize E2E owner");
        let mut owner = UnitOfWork::begin_immediate(&pool)
            .await
            .expect("begin E2E owner claim");
        let claimed_owner = claim_owner(&mut owner, 42, 4200, 1, clock.now())
            .await
            .expect("claim E2E owner");
        owner.commit().await.expect("commit E2E owner claim");
        let telegram = TelegramStub::start().await;
        let authoritative_api = TestBusinessConnectionApi::default();
        let detector = RuleDetector::from_defaults().expect("load embedded rules");
        let verifier = ArithmeticVerifier::new(
            StdRng::seed_from_u64(7),
            SecretString::from("e2e-challenge-key".to_owned()),
        );
        let engine = ProcessingEngine::new(
            pool.clone(),
            detector,
            verifier,
            clock.clone(),
            destructive_mode,
        )
        .with_business_connection_api(authoritative_api.clone());
        let processing_worker = spawn_processing_worker(engine, 128);
        let inbox = Arc::new(processing_worker.handle());
        let router = build_router_with_inbox(
            SecretString::from("e2e-webhook-secret".to_owned()),
            OwnerIdentityHandle::new(claimed_owner),
            inbox,
        );
        let dispatcher = OutboxDispatcher::new(telegram.client());
        Self {
            _directory: directory,
            pool,
            router,
            clock,
            telegram,
            authoritative_api,
            dispatcher,
            _processing_worker: processing_worker,
        }
    }

    pub async fn connect(&self, update_id: i64) {
        self.post(connection_update(update_id, true, true)).await;
        let state: Option<(String, bool, Option<i64>, String)> = sqlx::query_as(
            "SELECT connection_id, enabled, connection_established_at, reconciliation_state
             FROM business_connection",
        )
        .fetch_optional(&self.pool)
        .await
        .expect("read authoritative connection state");
        let candidate_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
                .fetch_one(&self.pool)
                .await
                .expect("count connection candidates");
        let owner_state: String =
            sqlx::query_scalar("SELECT state FROM owner_identity WHERE singleton = 1")
                .fetch_one(&self.pool)
                .await
                .expect("read owner state");
        assert_eq!(
            state,
            Some((
                "business-1".to_owned(),
                true,
                Some(1_783_987_200_i64 + update_id),
                "CONFIRMED".to_owned(),
            )),
            "candidate_count={candidate_count}, owner_state={owner_state}, requests={:?}",
            self.telegram.requests(),
        );
    }

    pub async fn post(&self, update: Value) -> StatusCode {
        let update_id = update["update_id"].as_i64().expect("update ID");
        let response_status = self.post_status(update).await;
        assert_eq!(response_status, StatusCode::OK);
        let status: String =
            sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = ?")
                .bind(update_id)
                .fetch_one(&self.pool)
                .await
                .expect("read processed update");
        assert_eq!(
            status,
            "APPLIED",
            "telegram_requests={:?}",
            self.telegram.requests()
        );
        response_status
    }

    pub async fn post_status(&self, update: Value) -> StatusCode {
        self.authoritative_api.stage_from_update(&update);
        let request = Request::builder()
            .method("POST")
            .uri("/telegram/webhook")
            .header("X-Telegram-Bot-Api-Secret-Token", "e2e-webhook-secret")
            .header("content-type", "application/json")
            .body(Body::from(update.to_string()))
            .expect("build webhook request");
        let response = self
            .router
            .clone()
            .oneshot(request)
            .await
            .expect("call webhook");
        response.status()
    }

    pub async fn drain_outbox(&self) {
        for _ in 0..1_000 {
            match self
                .dispatcher
                .dispatch_next(self.clock.now(), &self.pool)
                .await
                .expect("dispatch outbox")
            {
                DispatchOutcome::Idle | DispatchOutcome::RetryScheduled { .. } => return,
                DispatchOutcome::Succeeded { .. }
                | DispatchOutcome::Uncertain { .. }
                | DispatchOutcome::PermanentFailure { .. } => {}
            }
        }
        panic!("outbox did not drain");
    }

    pub fn fresh_dispatcher(&self) -> OutboxDispatcher<TelegramClient> {
        OutboxDispatcher::new(self.telegram.client())
    }

    pub async fn state(&self, chat_id: i64) -> String {
        sqlx::query_scalar("SELECT state FROM conversation WHERE chat_id = ?")
            .bind(chat_id)
            .fetch_one(&self.pool)
            .await
            .expect("read conversation state")
    }

    pub async fn ledger_ids(&self, chat_id: i64) -> Vec<i64> {
        sqlx::query_scalar(
            "SELECT message_id FROM message_ledger
             WHERE chat_id = ? ORDER BY message_id",
        )
        .bind(chat_id)
        .fetch_all(&self.pool)
        .await
        .expect("read ledger IDs")
    }

    pub async fn challenge_attempts(&self, chat_id: i64) -> i64 {
        sqlx::query_scalar(
            "SELECT attempts_used FROM challenge
             WHERE chat_id = ? ORDER BY id DESC LIMIT 1",
        )
        .bind(chat_id)
        .fetch_one(&self.pool)
        .await
        .expect("read challenge attempts")
    }

    pub fn telegram_request_count(&self, method: &str) -> usize {
        self.telegram.request_count(method)
    }
}

pub fn connection_update(update_id: i64, enabled: bool, all_rights: bool) -> Value {
    json!({
        "update_id": update_id,
        "business_connection": {
            "id": "business-1",
            "user": {"id": 42, "is_bot": false, "first_name": "Owner"},
            "user_chat_id": 4200,
            "date": 1_783_987_200_i64 + update_id,
            "can_reply": all_rights,
            "is_enabled": enabled,
            "rights": {
                "can_reply": all_rights,
                "can_read_messages": all_rights,
                "can_delete_sent_messages": all_rights,
                "can_delete_all_messages": all_rights
            }
        }
    })
}

pub fn business_message(
    update_id: i64,
    chat_id: i64,
    message_id: i64,
    from_user_id: i64,
    text: Option<&str>,
) -> Value {
    let mut message = json!({
        "message_id": message_id,
        "business_connection_id": "business-1",
        "from": {"id": from_user_id, "is_bot": false, "first_name": "User"},
        "chat": {"id": chat_id, "type": "private"},
        "date": 1_783_987_200_i64 + update_id
    });
    if let Some(text) = text {
        message["text"] = Value::String(text.to_owned());
    }
    json!({"update_id": update_id, "business_message": message})
}

pub fn edited_business_message(
    update_id: i64,
    chat_id: i64,
    message_id: i64,
    from_user_id: i64,
    text: &str,
) -> Value {
    let mut update = business_message(update_id, chat_id, message_id, from_user_id, Some(text));
    let message = update
        .as_object_mut()
        .and_then(|object| object.remove("business_message"))
        .expect("business message");
    update["edited_business_message"] = message;
    update
}

pub fn deleted_business_messages(update_id: i64, chat_id: i64, message_ids: &[i64]) -> Value {
    json!({
        "update_id": update_id,
        "deleted_business_messages": {
            "business_connection_id": "business-1",
            "chat": {"id": chat_id, "type": "private"},
            "message_ids": message_ids
        }
    })
}

pub fn photo_message(
    update_id: i64,
    chat_id: i64,
    message_id: i64,
    media_group_id: &str,
    caption: Option<&str>,
) -> Value {
    let mut update = business_message(update_id, chat_id, message_id, chat_id, None);
    let message = &mut update["business_message"];
    message["media_group_id"] = Value::String(media_group_id.to_owned());
    message["photo"] = json!([{
        "file_id": "sanitized",
        "file_unique_id": format!("sanitized-{message_id}"),
        "width": 1,
        "height": 1
    }]);
    if let Some(caption) = caption {
        message["caption"] = Value::String(caption.to_owned());
    }
    update
}

pub fn challenge_answer(expression: &str) -> String {
    let parts = expression.split_whitespace().collect::<Vec<_>>();
    assert_eq!(parts.len(), 5, "two-operation expression");
    let first = parts[0].parse::<i32>().expect("first operand");
    let second = parts[2].parse::<i32>().expect("second operand");
    let third = parts[4].parse::<i32>().expect("third operand");
    if parts[3] == "×" && parts[1] != "×" {
        let product = apply_operator(second, parts[3], third);
        apply_operator(first, parts[1], product).to_string()
    } else {
        let intermediate = apply_operator(first, parts[1], second);
        apply_operator(intermediate, parts[3], third).to_string()
    }
}

fn apply_operator(left: i32, operator: &str, right: i32) -> i32 {
    match operator {
        "+" => left + right,
        "-" => left - right,
        "×" => left * right,
        _ => panic!("unsupported operator {operator}"),
    }
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
    fn key_version(&self) -> i64 {
        0
    }

    fn generate(&mut self, now: DateTime<Utc>) -> GeneratedChallenge {
        GeneratedChallenge {
            expression: "7 + 5 - 3".to_owned(),
            answer_hmac: "fixed-hmac".to_owned(),
            hmac_key_version: 0,
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

    pub fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
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
        owner_claim: None,
        owner_command: None,
        contact_display_name: None,
        contact_username: None,
        occurred_at: now,
    }
}

pub fn inbound_photo(chat_id: i64, message_id: i64, now: DateTime<Utc>) -> RawBusinessEvent {
    let mut event = inbound(chat_id, message_id, None, now);
    event.content = Some(MessageContent {
        media_kind: Some(chathygiene::detection::MediaKind::Photo),
        ..MessageContent::default()
    });
    event
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
        owner_claim: None,
        owner_command: None,
        contact_display_name: None,
        contact_username: None,
        occurred_at: now,
    }
}
