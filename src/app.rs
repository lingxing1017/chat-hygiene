use std::future::Future;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::get;
use chrono::Utc;
use secrecy::{ExposeSecret, SecretSlice};
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;

use crate::clock::SystemClock;
use crate::config::Settings;
use crate::detection::{DetectorError, RuleDetector};
use crate::events::{EventError, recover_recorded_events, spawn_outbox_worker};
use crate::owner::OwnerIdentityHandle;
use crate::processing::{
    LifecycleHandler, ProcessingEngine, ProcessingError, spawn_processing_worker,
};
use crate::runtime_workers::{RuntimeExitError, WorkerGroup, run_server_with_workers};
use crate::storage::{StorageError, connect, initialize_or_load_owner_identity, migrate};
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
    #[error(transparent)]
    Processing(#[from] ProcessingError),
    #[error("HTTP server failed")]
    Server(#[source] std::io::Error),
    #[error("runtime worker shutdown failed")]
    WorkerShutdown,
    #[error("HTTP server failed and runtime worker shutdown also failed")]
    ServerAndWorker(#[source] std::io::Error),
}

pub fn build_router() -> Router {
    Router::new().route("/health/live", get(liveness))
}

pub fn build_router_with_inbox<I: WebhookInbox + 'static>(
    webhook_secret: secrecy::SecretString,
    owner_identity: OwnerIdentityHandle,
    inbox: Arc<I>,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .merge(webhook_router(webhook_secret, owner_identity, inbox))
}

/// Serves the runtime while retaining and cleaning up every worker task.
///
/// # Errors
///
/// Returns [`AppError`] when startup, serving, or bounded worker cleanup fails.
pub async fn serve_runtime<F>(
    settings: Arc<Settings>,
    listener: TcpListener,
    shutdown: F,
) -> Result<(), AppError>
where
    F: Future<Output = ()> + Send + 'static,
{
    let (app, workers) = prepare_runtime(settings).await?;
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
    };
    run_server_with_workers(server, workers)
        .await
        .map_err(|error| match error {
            RuntimeExitError::Server(error) => AppError::Server(error),
            RuntimeExitError::WorkerShutdown => AppError::WorkerShutdown,
            RuntimeExitError::ServerAndWorker(error) => AppError::ServerAndWorker(error),
        })
}

async fn prepare_runtime(settings: Arc<Settings>) -> Result<(Router, WorkerGroup), AppError> {
    let pool = connect(&settings.database_url).await?;
    migrate(&pool).await?;
    recover_recorded_events(&pool, &LifecycleHandler).await?;
    let owner = initialize_or_load_owner_identity(&pool, Utc::now()).await?;
    let owner_identity = OwnerIdentityHandle::new(owner);
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
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        detector,
        verifier,
        SystemClock,
        settings.destructive_mode,
    )
    .with_business_connection_api(telegram.clone())
    .with_owner_claim(None, owner_identity.clone());
    engine.recover_recorded_connection_triggers().await?;
    let notifier = spawn_new_contact_notifier(telegram.clone(), 32);
    engine = engine.with_new_contact_notifier(notifier.handle());
    let processing = spawn_processing_worker(engine, 128);
    let inbox = Arc::new(processing.handle());
    let outbox = spawn_outbox_worker(
        OutboxDispatcher::new(telegram),
        pool,
        std::time::Duration::from_millis(250),
    );
    let app = build_router_with_inbox(settings.webhook_secret.clone(), owner_identity, inbox)
        .route("/health/ready", get(readiness));
    let workers = WorkerGroup::new(vec![
        notifier.into_task(),
        processing.into_task(),
        outbox.into_task(),
    ]);
    Ok((app, workers))
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn readiness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
