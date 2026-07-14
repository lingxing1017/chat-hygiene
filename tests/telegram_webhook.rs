use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chathygiene::app::build_router_with_inbox;
use chathygiene::config::Settings;
use chathygiene::events::RecordReceipt;
use chathygiene::telegram::{IngressError, RawBusinessEvent, WebhookInbox};
use secrecy::SecretString;
use tower::ServiceExt;

#[derive(Clone)]
struct FakeInbox {
    receipt: Result<RecordReceipt, String>,
    submitted: Arc<Mutex<Vec<(i64, RawBusinessEvent)>>>,
}

impl FakeInbox {
    fn returning(receipt: RecordReceipt) -> Self {
        Self {
            receipt: Ok(receipt),
            submitted: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn failing() -> Self {
        Self {
            receipt: Err("database unavailable".to_owned()),
            submitted: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl WebhookInbox for FakeInbox {
    fn submit(
        &self,
        update_id: i64,
        event: RawBusinessEvent,
    ) -> Pin<Box<dyn Future<Output = Result<RecordReceipt, IngressError>> + Send + '_>> {
        Box::pin(async move {
            self.submitted.lock().unwrap().push((update_id, event));
            self.receipt.clone().map_err(IngressError::RecordingFailed)
        })
    }
}

fn settings() -> Arc<Settings> {
    Arc::new(Settings {
        bot_token: SecretString::from("bot-token"),
        webhook_secret: SecretString::from("correct-secret"),
        challenge_hmac_key: SecretString::from("challenge-key"),
        owner_user_id: 42,
        database_url: "sqlite::memory:".to_owned(),
        destructive_mode: false,
    })
}

fn router(inbox: FakeInbox) -> Router {
    build_router_with_inbox(settings().as_ref(), Arc::new(inbox))
}

fn request(secret: Option<&str>, body: impl Into<Body>) -> Request<Body> {
    let mut builder = Request::builder().method("POST").uri("/telegram/webhook");
    if let Some(secret) = secret {
        builder = builder.header("X-Telegram-Bot-Api-Secret-Token", secret);
    }
    builder.body(body.into()).unwrap()
}

#[tokio::test]
async fn valid_secret_records_before_returning_ok() {
    let inbox = FakeInbox::returning(RecordReceipt::Recorded);
    let submitted = Arc::clone(&inbox.submitted);
    let response = router(inbox)
        .oneshot(request(
            Some("correct-secret"),
            include_str!("fixtures/telegram/inbound_message.json"),
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let submitted = submitted.lock().unwrap();
    assert_eq!(submitted.len(), 1);
    assert_eq!(submitted[0].0, 101);
}

#[tokio::test]
async fn missing_or_wrong_secret_is_forbidden_before_json_parsing() {
    for secret in [None, Some("wrong-secret")] {
        let response = router(FakeInbox::returning(RecordReceipt::Recorded))
            .oneshot(request(secret, "not json"))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn malformed_json_is_bad_request_and_inbox_failure_is_unavailable() {
    let malformed = router(FakeInbox::returning(RecordReceipt::Recorded))
        .oneshot(request(Some("correct-secret"), "not json"))
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::BAD_REQUEST);

    let unavailable = router(FakeInbox::failing())
        .oneshot(request(
            Some("correct-secret"),
            include_str!("fixtures/telegram/inbound_message.json"),
        ))
        .await
        .unwrap();
    assert_eq!(unavailable.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn duplicate_receipts_are_successful() {
    for receipt in [
        RecordReceipt::DuplicateRecorded,
        RecordReceipt::DuplicateApplied,
    ] {
        let response = router(FakeInbox::returning(receipt))
            .oneshot(request(
                Some("correct-secret"),
                include_str!("fixtures/telegram/inbound_message.json"),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn oversized_body_is_rejected() {
    let response = router(FakeInbox::returning(RecordReceipt::Recorded))
        .oneshot(request(Some("correct-secret"), vec![b'x'; 256 * 1024 + 1]))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
