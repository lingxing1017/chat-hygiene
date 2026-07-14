use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::routing::get;
use serde::Serialize;

use crate::config::Settings;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

pub fn build_router(_settings: Arc<Settings>) -> Router {
    Router::new().route("/health/live", get(liveness))
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}
