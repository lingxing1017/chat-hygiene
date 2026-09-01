use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::get;
use secrecy::{ExposeSecret, SecretSlice};
use serde::Serialize;
use thiserror::Error;

use crate::clock::SystemClock;
use crate::config::Settings;
use crate::detection::{DetectorError, RuleDetector};
use crate::events::{EventError, recover_recorded_events, spawn_outbox_worker};
use crate::processing::{LifecycleHandler, ProcessingEngine, spawn_processing_worker};
use crate::storage::{StorageError, connect, migrate};
use crate::telegram::{
    OutboxDispatcher, TelegramClient, WebhookInbox, spawn_new_contact_notifier, webhook_router,
};
use crate::verification::ArithmeticVerifier;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Detector(#[from] DetectorError),
    #[error(transparent)]
    Event(#[from] EventError),
}

pub fn build_router(_settings: Arc<Settings>) -> Router {
    Router::new().route("/health/live", get(liveness))
}

pub fn build_router_with_inbox<I: WebhookInbox + 'static>(
    settings: &Settings,
    inbox: Arc<I>,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .merge(webhook_router(
            settings.webhook_secret.clone(),
            settings.owner_user_id,
            inbox,
        ))
}

/// Builds the production router and starts its two single-worker pipelines.
///
/// # Errors
///
/// Returns [`AppError`] when storage or the embedded rules cannot initialize.
pub async fn build_runtime_router(settings: Arc<Settings>) -> Result<Router, AppError> {
    let pool = connect(&settings.database_url).await?;
    migrate(&pool).await?;
    recover_recorded_events(&pool, &LifecycleHandler).await?;
    let detector = RuleDetector::from_defaults()?;
    let verifier = ArithmeticVerifier::from_os_rng_with_key_version(
        SecretSlice::from(
            settings
                .challenge_hmac_key
                .expose_secret()
                .as_bytes()
                .to_vec(),
        ),
        0,
    );
    let telegram = TelegramClient::new(settings.bot_token.clone());
    let notifier = spawn_new_contact_notifier(telegram.clone(), 32);
    let engine = ProcessingEngine::new(
        pool.clone(),
        detector,
        verifier,
        SystemClock,
        settings.destructive_mode,
    )
    .with_new_contact_notifier(notifier);
    let inbox = Arc::new(spawn_processing_worker(engine, 128));
    std::mem::drop(spawn_outbox_worker(
        OutboxDispatcher::new(telegram),
        pool,
        std::time::Duration::from_millis(250),
    ));
    Ok(build_router_with_inbox(&settings, inbox).route("/health/ready", get(readiness)))
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn readiness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
