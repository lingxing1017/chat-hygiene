use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use reqwest::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use super::connection_state::{
    AuthenticatedBot, AuthoritativeBusinessConnection, BotIdentityApi, BoxFuture,
    BusinessConnectionApi,
};
use super::models::BusinessRights;

const TELEGRAM_API_BASE: &str = "https://api.telegram.org/";
const AUTHORITATIVE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const WEBHOOK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const RECOGNIZED_CONNECTION_NOT_FOUND: &str = "Bad Request: business connection not found";
const SAFE_CONNECTION_NOT_FOUND: &str = "recognized business connection not found";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SendAction {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub business_connection_id: Option<String>,
    pub chat_id: i64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EditAction {
    pub business_connection_id: String,
    pub chat_id: i64,
    pub message_id: i64,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReadAction {
    pub business_connection_id: String,
    pub chat_id: i64,
    pub message_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DeleteAction {
    pub business_connection_id: String,
    pub message_ids: Vec<i64>,
}

/// Splits cleanup IDs into Telegram's inclusive 1-to-100 request limit.
#[must_use]
pub fn delete_message_batches(message_ids: &[i64]) -> Vec<Vec<i64>> {
    message_ids.chunks(100).map(<[i64]>::to_vec).collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub struct SentMessage {
    pub message_id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum TelegramError {
    #[error("Telegram request timed out")]
    Timeout,
    #[error("Telegram transport failed")]
    Transport,
    #[error("Telegram API error {error_code}: {description}")]
    Api {
        error_code: u16,
        description: String,
        retry_after: Option<u64>,
    },
    #[error("invalid Telegram response: {0}")]
    Protocol(String),
    #[error("invalid Telegram request: {0}")]
    InvalidRequest(String),
}

pub trait WebhookApi: Send + Sync {
    fn set_webhook<'a>(
        &'a self,
        public_url: &'a Url,
        secret: &'a SecretString,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>>;
}

pub trait BusinessApi: Send + Sync {
    fn send_business_message<'a>(
        &'a self,
        action: &'a SendAction,
    ) -> Pin<Box<dyn Future<Output = Result<SentMessage, TelegramError>> + Send + 'a>>;

    fn edit_business_message<'a>(
        &'a self,
        action: &'a EditAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>>;

    fn read_business_message<'a>(
        &'a self,
        action: &'a ReadAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>>;

    fn delete_business_messages<'a>(
        &'a self,
        action: &'a DeleteAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct TelegramClient {
    http: reqwest::Client,
    token: SecretString,
    base_url: Url,
    webhook_timeout: Duration,
}

impl fmt::Debug for TelegramClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelegramClient")
            .field("token", &"[REDACTED]")
            .field("base_url", &"[REDACTED]")
            .finish_non_exhaustive()
    }
}

impl TelegramClient {
    /// Creates a client for Telegram's public Bot API.
    ///
    /// # Panics
    ///
    /// Panics only when the compile-time Telegram API URL is invalid.
    #[must_use]
    pub fn new(token: SecretString) -> Self {
        Self::with_base_url(
            reqwest::Client::new(),
            token,
            Url::parse(TELEGRAM_API_BASE).expect("static Telegram API URL is valid"),
        )
    }

    #[must_use]
    pub fn with_base_url(http: reqwest::Client, token: SecretString, base_url: Url) -> Self {
        Self::with_base_url_and_webhook_timeout(http, token, base_url, WEBHOOK_REQUEST_TIMEOUT)
    }

    #[must_use]
    pub fn with_base_url_and_webhook_timeout(
        http: reqwest::Client,
        token: SecretString,
        base_url: Url,
        webhook_timeout: Duration,
    ) -> Self {
        Self {
            http,
            token,
            base_url,
            webhook_timeout,
        }
    }

    async fn call<Request, Response>(
        &self,
        method: &str,
        request: &Request,
    ) -> Result<Response, TelegramError>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        self.call_with_timeout(method, request, None).await
    }

    async fn call_with_timeout<Request, Response>(
        &self,
        method: &str,
        request: &Request,
        timeout: Option<Duration>,
    ) -> Result<Response, TelegramError>
    where
        Request: Serialize + ?Sized,
        Response: DeserializeOwned,
    {
        let mut endpoint = self.base_url.clone();
        endpoint
            .path_segments_mut()
            .map_err(|()| TelegramError::InvalidRequest("invalid API base URL".to_owned()))?
            .pop_if_empty()
            .push(&format!("bot{}", self.token.expose_secret()))
            .push(method);
        let mut request_builder = self.http.post(endpoint).json(request);
        if let Some(timeout) = timeout {
            request_builder = request_builder.timeout(timeout);
        }
        let response = request_builder.send().await.map_err(|error| {
            if error.is_timeout() {
                TelegramError::Timeout
            } else {
                TelegramError::Transport
            }
        })?;
        let status = response.status();
        let envelope = response
            .json::<ApiEnvelope<Response>>()
            .await
            .map_err(|_| protocol_error(status))?;
        if envelope.ok {
            return envelope.result.ok_or_else(|| {
                TelegramError::Protocol("successful response omitted result".to_owned())
            });
        }
        Err(TelegramError::Api {
            error_code: envelope.error_code.unwrap_or(status.as_u16()),
            description: envelope
                .description
                .unwrap_or_else(|| "Telegram rejected the request".to_owned()),
            retry_after: envelope.parameters.and_then(|value| value.retry_after),
        })
    }
}

impl WebhookApi for TelegramClient {
    fn set_webhook<'a>(
        &'a self,
        public_url: &'a Url,
        secret: &'a SecretString,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            let result = self
                .call_with_timeout::<_, bool>(
                    "setWebhook",
                    &SetWebhookRequest {
                        url: public_url.as_str(),
                        secret_token: secret.expose_secret(),
                        allowed_updates: [
                            "business_connection",
                            "business_message",
                            "edited_business_message",
                            "deleted_business_messages",
                            "message",
                        ],
                        drop_pending_updates: false,
                    },
                    Some(self.webhook_timeout),
                )
                .await?;
            if !result {
                return Err(TelegramError::Protocol(
                    "setWebhook returned false".to_owned(),
                ));
            }
            Ok(())
        })
    }
}

impl BotIdentityApi for TelegramClient {
    fn get_me(&self) -> BoxFuture<'_, Result<AuthenticatedBot, TelegramError>> {
        Box::pin(async move {
            let raw = self
                .call_with_timeout::<_, RawAuthenticatedBot>(
                    "getMe",
                    &EmptyRequest {},
                    Some(AUTHORITATIVE_REQUEST_TIMEOUT),
                )
                .await
                .map_err(|error| sanitize_authoritative_error(error, false))?;
            if raw.id <= 0 || !raw.is_bot {
                return Err(TelegramError::Protocol(
                    "invalid authoritative bot response".to_owned(),
                ));
            }
            Ok(AuthenticatedBot { id: raw.id })
        })
    }
}

impl BusinessConnectionApi for TelegramClient {
    fn get_business_connection<'a>(
        &'a self,
        connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async move {
            if connection_id.trim().is_empty() {
                return Err(TelegramError::InvalidRequest(
                    "authoritative connection ID is invalid".to_owned(),
                ));
            }
            let raw = self
                .call_with_timeout::<_, RawBusinessConnection>(
                    "getBusinessConnection",
                    &BusinessConnectionRequest {
                        business_connection_id: connection_id,
                    },
                    Some(AUTHORITATIVE_REQUEST_TIMEOUT),
                )
                .await
                .map_err(|error| sanitize_authoritative_error(error, true))?;
            if raw.id.trim().is_empty()
                || raw.id != connection_id
                || raw.user.id <= 0
                || raw.user_chat_id.is_some_and(|chat_id| chat_id <= 0)
                || raw.date <= 0
            {
                return Err(TelegramError::Protocol(
                    "invalid authoritative connection response".to_owned(),
                ));
            }
            Ok(AuthoritativeBusinessConnection {
                connection_id: raw.id,
                business_user_id: raw.user.id,
                user_chat_id: raw.user_chat_id,
                connection_established_at: raw.date,
                rights: BusinessRights {
                    can_reply: raw.rights.can_reply,
                    can_read_messages: raw.rights.can_read_messages,
                    can_delete_sent_messages: raw.rights.can_delete_sent_messages,
                    can_delete_all_messages: raw.rights.can_delete_all_messages,
                },
                enabled: raw.is_enabled,
            })
        })
    }
}

impl BusinessApi for TelegramClient {
    fn send_business_message<'a>(
        &'a self,
        action: &'a SendAction,
    ) -> Pin<Box<dyn Future<Output = Result<SentMessage, TelegramError>> + Send + 'a>> {
        Box::pin(async move { self.call("sendMessage", action).await })
    }

    fn edit_business_message<'a>(
        &'a self,
        action: &'a EditAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.call::<_, bool>("editMessageText", action)
                .await
                .map(|_| ())
        })
    }

    fn read_business_message<'a>(
        &'a self,
        action: &'a ReadAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            self.call::<_, bool>("readBusinessMessage", action)
                .await
                .map(|_| ())
        })
    }

    fn delete_business_messages<'a>(
        &'a self,
        action: &'a DeleteAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
        Box::pin(async move {
            if !(1..=100).contains(&action.message_ids.len()) {
                return Err(TelegramError::InvalidRequest(
                    "delete batch must contain 1 to 100 message IDs".to_owned(),
                ));
            }
            self.call::<_, bool>("deleteBusinessMessages", action)
                .await
                .map(|_| ())
        })
    }
}

#[derive(Deserialize)]
struct ApiEnvelope<T> {
    ok: bool,
    result: Option<T>,
    error_code: Option<u16>,
    description: Option<String>,
    parameters: Option<ResponseParameters>,
}

#[derive(Deserialize)]
struct ResponseParameters {
    retry_after: Option<u64>,
}

#[derive(Serialize)]
struct EmptyRequest {}

#[derive(Serialize)]
struct SetWebhookRequest<'a> {
    url: &'a str,
    secret_token: &'a str,
    allowed_updates: [&'static str; 5],
    drop_pending_updates: bool,
}

#[derive(Deserialize)]
struct RawAuthenticatedBot {
    id: i64,
    is_bot: bool,
}

#[derive(Serialize)]
struct BusinessConnectionRequest<'a> {
    business_connection_id: &'a str,
}

#[derive(Deserialize)]
struct RawBusinessConnection {
    id: String,
    user: RawBusinessUser,
    user_chat_id: Option<i64>,
    date: i64,
    rights: RawBusinessRights,
    is_enabled: bool,
}

#[derive(Deserialize)]
struct RawBusinessUser {
    id: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
struct RawBusinessRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

fn sanitize_authoritative_error(error: TelegramError, recognize_not_found: bool) -> TelegramError {
    match error {
        TelegramError::Api {
            error_code,
            description,
            retry_after: _,
        } if recognize_not_found
            && error_code == 400
            && description == RECOGNIZED_CONNECTION_NOT_FOUND =>
        {
            TelegramError::Protocol(SAFE_CONNECTION_NOT_FOUND.to_owned())
        }
        TelegramError::Api {
            error_code,
            retry_after,
            ..
        } => TelegramError::Api {
            error_code,
            description: "Telegram rejected authoritative lookup".to_owned(),
            retry_after,
        },
        TelegramError::Protocol(_) => {
            TelegramError::Protocol("invalid authoritative response".to_owned())
        }
        TelegramError::InvalidRequest(_) => {
            TelegramError::InvalidRequest("invalid authoritative request".to_owned())
        }
        TelegramError::Timeout => TelegramError::Timeout,
        TelegramError::Transport => TelegramError::Transport,
    }
}

fn protocol_error(status: StatusCode) -> TelegramError {
    if status.is_success() {
        TelegramError::Protocol("response was not a Telegram JSON envelope".to_owned())
    } else {
        TelegramError::Api {
            error_code: status.as_u16(),
            description: "Telegram returned a non-JSON error".to_owned(),
            retry_after: None,
        }
    }
}
