use std::fmt;
use std::future::Future;
use std::pin::Pin;

use reqwest::StatusCode;
use secrecy::{ExposeSecret, SecretString};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

const TELEGRAM_API_BASE: &str = "https://api.telegram.org/";

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
        Self {
            http,
            token,
            base_url,
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
        let mut endpoint = self.base_url.clone();
        endpoint
            .path_segments_mut()
            .map_err(|()| TelegramError::InvalidRequest("invalid API base URL".to_owned()))?
            .pop_if_empty()
            .push(&format!("bot{}", self.token.expose_secret()))
            .push(method);
        let response = self
            .http
            .post(endpoint)
            .json(request)
            .send()
            .await
            .map_err(|error| {
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
