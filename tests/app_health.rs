mod common;

use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use chathygiene::app::{build_router, serve_runtime};
use chathygiene::config::Settings;
use chathygiene::events::{PreparedEvent, record_prepared_event};
use chathygiene::storage::{connect, migrate};
use chrono::Utc;
use secrecy::SecretString;
use serde_json::json;
use tower::ServiceExt;

async fn start_and_stop(settings: Arc<Settings>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    serve_runtime(settings, listener, async {}).await.unwrap();
}

#[tokio::test]
async fn liveness_exposes_no_config() {
    let response = build_router()
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
        owner_user_id: 999_999,
        public_webhook_url: "https://chat.example.net/telegram/webhook".parse().unwrap(),
        database_url: database_url.clone(),
        destructive_mode: false,
    });

    start_and_stop(settings).await;
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
    let owner: (String, Option<i64>) =
        sqlx::query_as("SELECT state, owner_user_id FROM owner_identity WHERE singleton = 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(owner, ("CLAIMED".to_owned(), Some(42)));
    let trace_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE idempotency_key = '90:DRY_RUN_TRACE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trace_count, 0, "legacy events must not gain dry-run traces");
}

#[tokio::test]
async fn runtime_leaves_a_fresh_database_unclaimed_despite_legacy_setting() {
    let (_directory, database_url) = common::temporary_database();
    let settings = Arc::new(Settings {
        bot_token: SecretString::from("test-token".to_owned()),
        webhook_secret: SecretString::from("test-webhook".to_owned()),
        challenge_hmac_key: SecretString::from("test-hmac".to_owned()),
        owner_user_id: 999_999,
        public_webhook_url: "https://chat.example.net/telegram/webhook".parse().unwrap(),
        database_url: database_url.clone(),
        destructive_mode: false,
    });

    start_and_stop(settings).await;

    let pool = connect(&database_url).await.unwrap();
    let owner: (String, Option<i64>, Option<i64>) = sqlx::query_as(
        "SELECT state, owner_user_id, owner_chat_id
         FROM owner_identity WHERE singleton = 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(owner, ("UNCLAIMED".to_owned(), None, None));
}
