mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use chathygiene::app::{build_router, build_runtime_router};
use chathygiene::config::Settings;
use chathygiene::events::{PreparedEvent, record_prepared_event};
use chathygiene::storage::{connect, migrate};
use chrono::Utc;
use secrecy::SecretString;
use serde_json::json;
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

#[tokio::test]
async fn runtime_recovers_recorded_events_before_readiness() {
    let (_directory, database_url) = common::temporary_database();
    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    let now = Utc::now();
    record_prepared_event(
        &pool,
        &PreparedEvent::new(
            90,
            "lifecycle",
            now,
            json!({
                "connection_id": "recovered-business",
                "chat_id": null,
                "user_id": null,
                "message_id": null,
                "media_group_id": null,
                "occurred_at": now,
                "action": {
                    "kind": "CONNECTION_CHANGED",
                    "owner_user_id": 42,
                    "enabled": true,
                    "rights_json": "{}"
                }
            }),
        ),
    )
    .await
    .unwrap();
    pool.close().await;
    let settings = Arc::new(Settings {
        bot_token: SecretString::from("test-token".to_owned()),
        webhook_secret: SecretString::from("test-webhook".to_owned()),
        challenge_hmac_key: SecretString::from("test-hmac".to_owned()),
        owner_user_id: 42,
        database_url: database_url.clone(),
        destructive_mode: false,
    });

    let router = build_runtime_router(settings).await.unwrap();
    let response = router
        .oneshot(
            Request::builder()
                .uri("/health/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let pool = connect(&database_url).await.unwrap();
    let recovered: (String, String) = sqlx::query_as(
        "SELECT p.status, b.connection_id
         FROM processed_update AS p CROSS JOIN business_connection AS b
         WHERE p.update_id = 90",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        recovered,
        ("APPLIED".to_owned(), "recovered-business".to_owned())
    );
}
