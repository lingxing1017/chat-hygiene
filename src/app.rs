use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Serialize;

use crate::config::Settings;
use crate::telegram::{WebhookInbox, webhook_router};

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
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

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
