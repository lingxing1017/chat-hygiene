use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use chathygiene::app::build_router;
use chathygiene::config::Settings;
use tower::ServiceExt;

#[tokio::test]
async fn liveness_exposes_no_config() {
    let settings = Settings::from_map(&HashMap::from([
        ("CHATHYGIENE_BOT_TOKEN".into(), "secret-bot-token".into()),
        ("CHATHYGIENE_WEBHOOK_SECRET".into(), "secret-webhook".into()),
        (
            "CHATHYGIENE_CHALLENGE_HMAC_KEY".into(),
            "secret-challenge".into(),
        ),
        ("CHATHYGIENE_OWNER_USER_ID".into(), "42".into()),
    ]))
    .expect("valid settings");

    let response = build_router(Arc::new(settings))
        .oneshot(
            Request::builder()
                .uri("/health/live")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request succeeds");

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024)
        .await
        .expect("read body");
    assert_eq!(body.as_ref(), br#"{"status":"ok"}"#);
    assert!(!String::from_utf8_lossy(&body).contains("secret"));
}
