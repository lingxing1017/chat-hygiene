use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chathygiene::app::build_router_with_inbox;
use chathygiene::events::RecordReceipt;
use chathygiene::owner::OwnerIdentityHandle;
use chathygiene::storage::{OwnerChatSource, OwnerIdentity};
use chathygiene::telegram::{
    IngressError, RawBusinessEvent, RawEventKind, WebhookInbox, webhook_router,
};
use chrono::Utc;
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

fn router(inbox: FakeInbox) -> Router {
    build_router_with_inbox(
        SecretString::from("correct-secret"),
        OwnerIdentityHandle::new(OwnerIdentity::Claimed {
            owner_user_id: 42,
            owner_chat_id: 42,
            owner_chat_source: OwnerChatSource::LegacyFallback,
            connection_floor_established_at: None,
            bound_at: Utc::now(),
        }),
        Arc::new(inbox),
    )
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

#[tokio::test]
async fn identity_router_accepts_redacted_unclaimed_owner_claims() {
    let owner = OwnerIdentityHandle::new(OwnerIdentity::Unclaimed);
    let inbox = FakeInbox::returning(RecordReceipt::Recorded);
    let submitted = Arc::clone(&inbox.submitted);
    let claim = serde_json::json!({
        "update_id": 500,
        "message": {
            "message_id": 5,
            "from": {"id": 100},
            "chat": {"id": 500, "type": "private"},
            "date": 1_783_987_270_i64,
            "text": "/claim 1111111111111111111111111111111111111111111111111111111111111111"
        }
    });
    let response = webhook_router(
        SecretString::from("correct-secret"),
        owner.clone(),
        Arc::new(inbox),
    )
    .oneshot(request(
        Some("correct-secret"),
        serde_json::to_vec(&claim).unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    {
        let submitted = submitted.lock().unwrap();
        assert_eq!(submitted[0].1.kind, RawEventKind::OwnerClaim);
        assert!(submitted[0].1.owner_command.is_none());
        assert!(!format!("{:?}", submitted[0].1).contains("1111111111111111"));
    }
}

#[tokio::test]
async fn identity_router_requires_claimed_user_and_chat() {
    let claimed = OwnerIdentityHandle::new(OwnerIdentity::Claimed {
        owner_user_id: 100,
        owner_chat_id: 500,
        owner_chat_source: OwnerChatSource::Claim,
        connection_floor_established_at: Some(1),
        bound_at: Utc::now(),
    });
    let inbox = FakeInbox::returning(RecordReceipt::Recorded);
    let submitted = Arc::clone(&inbox.submitted);
    let command = serde_json::json!({
        "update_id": 501,
        "message": {
            "message_id": 6,
            "from": {"id": 100},
            "chat": {"id": 500, "type": "private"},
            "date": 1_783_987_271_i64,
            "text": "/health"
        }
    });
    let response = webhook_router(
        SecretString::from("correct-secret"),
        claimed.clone(),
        Arc::new(inbox),
    )
    .oneshot(request(
        Some("correct-secret"),
        serde_json::to_vec(&command).unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        submitted.lock().unwrap()[0].1.kind,
        RawEventKind::OwnerCommand
    );

    let inbox = FakeInbox::returning(RecordReceipt::Recorded);
    let submitted = Arc::clone(&inbox.submitted);
    let mismatch_router = webhook_router(
        SecretString::from("correct-secret"),
        claimed,
        Arc::new(inbox),
    );
    for (update_id, from_user_id, chat_id) in [(502, 100, 501), (503, 101, 500)] {
        let mismatch = serde_json::json!({
            "update_id": update_id,
            "message": {
                "message_id": update_id,
                "from": {"id": from_user_id},
                "chat": {"id": chat_id, "type": "private"},
                "date": 1_783_987_270_i64 + update_id,
                "text": "/health"
            }
        });
        let response = mismatch_router
            .clone()
            .oneshot(request(
                Some("correct-secret"),
                serde_json::to_vec(&mismatch).unwrap(),
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert!(
        submitted
            .lock()
            .unwrap()
            .iter()
            .all(|(_, event)| event.kind == RawEventKind::Ignored)
    );
}

#[tokio::test]
async fn identity_router_accepts_legacy_fallback_chat() {
    let fallback = OwnerIdentityHandle::new(OwnerIdentity::Claimed {
        owner_user_id: 100,
        owner_chat_id: 500,
        owner_chat_source: OwnerChatSource::LegacyFallback,
        connection_floor_established_at: None,
        bound_at: Utc::now(),
    });
    let inbox = FakeInbox::returning(RecordReceipt::Recorded);
    let submitted = Arc::clone(&inbox.submitted);
    let fallback_command = serde_json::json!({
        "update_id": 504,
        "message": {
            "message_id": 504,
            "from": {"id": 100},
            "chat": {"id": 700, "type": "private"},
            "date": 1_783_987_774_i64,
            "text": "/health"
        }
    });
    webhook_router(
        SecretString::from("correct-secret"),
        fallback,
        Arc::new(inbox),
    )
    .oneshot(request(
        Some("correct-secret"),
        serde_json::to_vec(&fallback_command).unwrap(),
    ))
    .await
    .unwrap();
    assert_eq!(
        submitted.lock().unwrap()[0].1.kind,
        RawEventKind::OwnerCommand
    );
}

#[test]
fn legacy_routing_symbols_are_absent_from_the_module_surface() {
    let parser = include_str!("../src/telegram/parser.rs");
    let webhook = include_str!("../src/telegram/webhook.rs");
    let module = include_str!("../src/telegram/mod.rs");

    assert!(!parser.contains("pub fn parse_update_with_owner_identity"));
    assert!(!parser.contains("pub fn parse_update(body: &[u8], owner_user_id: i64)"));
    assert!(!webhook.contains("pub fn webhook_router_with_owner_identity"));
    assert!(!webhook.contains("owner_user_id: i64"));
    assert!(!module.contains("parse_update_with_owner_identity"));
    assert!(!module.contains("webhook_router_with_owner_identity"));
}
