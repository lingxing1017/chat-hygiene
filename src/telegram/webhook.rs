use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use thiserror::Error;

use crate::events::RecordReceipt;
use crate::owner::OwnerIdentityHandle;

use super::models::RawBusinessEvent;
use super::parser::{parse_update, parse_update_with_owner_identity};

const MAX_WEBHOOK_BODY_BYTES: usize = 256 * 1024;
const SECRET_HEADER: &str = "X-Telegram-Bot-Api-Secret-Token";

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum IngressError {
    #[error("recording failed: {0}")]
    RecordingFailed(String),
}

pub trait WebhookInbox: Send + Sync {
    /// Durably records one normalized update before the webhook is acknowledged.
    ///
    /// # Errors
    ///
    /// Returns [`IngressError`] when the update cannot be durably accepted.
    fn submit(
        &self,
        update_id: i64,
        event: RawBusinessEvent,
    ) -> Pin<Box<dyn Future<Output = Result<RecordReceipt, IngressError>> + Send + '_>>;
}

struct WebhookState<I> {
    secret: SecretString,
    owner_user_id: i64,
    inbox: Arc<I>,
}

struct IdentityWebhookState<I> {
    secret: SecretString,
    owner_identity: OwnerIdentityHandle,
    inbox: Arc<I>,
}

impl<I> Clone for WebhookState<I> {
    fn clone(&self) -> Self {
        Self {
            secret: self.secret.clone(),
            owner_user_id: self.owner_user_id,
            inbox: Arc::clone(&self.inbox),
        }
    }
}

impl<I> Clone for IdentityWebhookState<I> {
    fn clone(&self) -> Self {
        Self {
            secret: self.secret.clone(),
            owner_identity: self.owner_identity.clone(),
            inbox: Arc::clone(&self.inbox),
        }
    }
}

pub fn webhook_router<I: WebhookInbox + 'static>(
    secret: SecretString,
    owner_user_id: i64,
    inbox: Arc<I>,
) -> Router {
    Router::new()
        .route("/telegram/webhook", post(handle_webhook::<I>))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
        .with_state(WebhookState {
            secret,
            owner_user_id,
            inbox,
        })
}

pub fn webhook_router_with_owner_identity<I: WebhookInbox + 'static>(
    secret: SecretString,
    owner_identity: OwnerIdentityHandle,
    inbox: Arc<I>,
) -> Router {
    Router::new()
        .route("/telegram/webhook", post(handle_identity_webhook::<I>))
        .layer(DefaultBodyLimit::max(MAX_WEBHOOK_BODY_BYTES))
        .with_state(IdentityWebhookState {
            secret,
            owner_identity,
            inbox,
        })
}

async fn handle_webhook<I: WebhookInbox>(
    State(state): State<WebhookState<I>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !secret_is_valid(&headers, &state.secret) {
        return StatusCode::FORBIDDEN;
    }
    let Ok(update) = parse_update(&body, state.owner_user_id) else {
        return StatusCode::BAD_REQUEST;
    };
    match state.inbox.submit(update.update_id, update.event).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn handle_identity_webhook<I: WebhookInbox>(
    State(state): State<IdentityWebhookState<I>>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    if !secret_is_valid(&headers, &state.secret) {
        return StatusCode::FORBIDDEN;
    }
    let owner = state.owner_identity.snapshot().await;
    let Ok(update) = parse_update_with_owner_identity(&body, &owner) else {
        return StatusCode::BAD_REQUEST;
    };
    match state.inbox.submit(update.update_id, update.event).await {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

fn secret_is_valid(headers: &HeaderMap, expected: &SecretString) -> bool {
    let Some(provided) = headers
        .get(SECRET_HEADER)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let expected_hash = Sha256::digest(expected.expose_secret().as_bytes());
    let provided_hash = Sha256::digest(provided.as_bytes());
    bool::from(expected_hash.as_slice().ct_eq(provided_hash.as_slice()))
}
