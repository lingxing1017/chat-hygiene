use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use thiserror::Error;

use super::{BusinessRights, TelegramError};

const RETRY_DELAYS: [Duration; 2] = [Duration::from_secs(1), Duration::from_secs(2)];
const MAX_RETRY_AFTER: Duration = Duration::from_secs(5);
const RECOGNIZED_CONNECTION_NOT_FOUND: &str = "Bad Request: business connection not found";
const SAFE_CONNECTION_NOT_FOUND: &str = "recognized business connection not found";

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthenticatedBot {
    pub id: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeBusinessConnection {
    pub connection_id: String,
    pub business_user_id: i64,
    pub user_chat_id: Option<i64>,
    pub connection_established_at: i64,
    pub rights: BusinessRights,
    pub enabled: bool,
}

pub trait BotIdentityApi: Send + Sync {
    fn get_me(&self) -> BoxFuture<'_, Result<AuthenticatedBot, TelegramError>>;
}

pub trait BusinessConnectionApi: Send + Sync {
    fn get_business_connection<'a>(
        &'a self,
        connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum AuthoritativeLookupError {
    #[error("authoritative Business connection was not found")]
    ConnectionNotFound { status: Option<u16> },
    #[error("Telegram rejected bot authentication")]
    BotAuthentication { status: Option<u16> },
    #[error("authoritative Telegram lookup exhausted retries")]
    TransientExhausted { status: Option<u16> },
    #[error("Telegram returned an invalid authoritative response")]
    InvalidResponse { status: Option<u16> },
    #[error("authoritative Telegram lookup was rejected")]
    ClientRejected { status: Option<u16> },
}

impl AuthoritativeLookupError {
    const fn kind(self) -> &'static str {
        match self {
            Self::ConnectionNotFound { .. } => "connection_not_found",
            Self::BotAuthentication { .. } => "bot_authentication",
            Self::TransientExhausted { .. } => "transient_exhausted",
            Self::InvalidResponse { .. } => "invalid_response",
            Self::ClientRejected { .. } => "client_rejected",
        }
    }
}

/// Authenticates the configured bot through a bounded three-attempt policy.
///
/// Consumers must call this without holding a database transaction, pooled
/// connection, or identity lock.
///
/// # Errors
///
/// Returns only a closed, value-free lookup class and optional numeric status.
pub async fn lookup_authenticated_bot<C>(
    api: &C,
) -> Result<AuthenticatedBot, AuthoritativeLookupError>
where
    C: BotIdentityApi + ?Sized,
{
    lookup_authenticated_bot_with_delay(api, &|delay| Box::pin(tokio::time::sleep(delay))).await
}

/// Queries current Business state through a bounded three-attempt policy.
///
/// Consumers must release every transaction and identity lock before this
/// call, then use revision CAS before any permissive state write. A bot-auth
/// failure is global; only `ConnectionNotFound` permits connection-specific
/// disablement or removal.
///
/// # Errors
///
/// Returns only a closed, value-free lookup class and optional numeric status.
pub async fn lookup_business_connection<C>(
    api: &C,
    connection_id: &str,
) -> Result<AuthoritativeBusinessConnection, AuthoritativeLookupError>
where
    C: BusinessConnectionApi + ?Sized,
{
    lookup_business_connection_with_delay(api, connection_id, &|delay| {
        Box::pin(tokio::time::sleep(delay))
    })
    .await
}

#[doc(hidden)]
pub async fn lookup_authenticated_bot_with_delay<C, D>(
    api: &C,
    delay: &D,
) -> Result<AuthenticatedBot, AuthoritativeLookupError>
where
    C: BotIdentityApi + ?Sized,
    D: Fn(Duration) -> BoxFuture<'static, ()> + Sync,
{
    for (attempt, default_delay) in RETRY_DELAYS.into_iter().map(Some).chain([None]).enumerate() {
        match api.get_me().await {
            Ok(bot) => return Ok(bot),
            Err(error) => match classify(error, false) {
                FailureClass::Retry {
                    status,
                    retry_after,
                } if default_delay.is_some() => {
                    let delay_duration = retry_after.unwrap_or_else(|| default_delay.unwrap());
                    warn_retry(attempt, status, delay_duration);
                    delay(delay_duration).await;
                }
                FailureClass::Retry { status, .. } => {
                    return Err(AuthoritativeLookupError::TransientExhausted { status });
                }
                FailureClass::Terminal(error) => return Err(error),
            },
        }
    }
    unreachable!("lookup attempt loop always returns")
}

#[doc(hidden)]
pub async fn lookup_business_connection_with_delay<C, D>(
    api: &C,
    connection_id: &str,
    delay: &D,
) -> Result<AuthoritativeBusinessConnection, AuthoritativeLookupError>
where
    C: BusinessConnectionApi + ?Sized,
    D: Fn(Duration) -> BoxFuture<'static, ()> + Sync,
{
    for (attempt, default_delay) in RETRY_DELAYS.into_iter().map(Some).chain([None]).enumerate() {
        match api.get_business_connection(connection_id).await {
            Ok(connection) => return Ok(connection),
            Err(error) => match classify(error, true) {
                FailureClass::Retry {
                    status,
                    retry_after,
                } if default_delay.is_some() => {
                    let delay_duration = retry_after.unwrap_or_else(|| default_delay.unwrap());
                    warn_retry(attempt, status, delay_duration);
                    delay(delay_duration).await;
                }
                FailureClass::Retry { status, .. } => {
                    return Err(AuthoritativeLookupError::TransientExhausted { status });
                }
                FailureClass::Terminal(error) => return Err(error),
            },
        }
    }
    unreachable!("lookup attempt loop always returns")
}

enum FailureClass {
    Retry {
        status: Option<u16>,
        retry_after: Option<Duration>,
    },
    Terminal(AuthoritativeLookupError),
}

fn classify(error: TelegramError, allow_not_found: bool) -> FailureClass {
    match error {
        TelegramError::Timeout | TelegramError::Transport => FailureClass::Retry {
            status: None,
            retry_after: None,
        },
        TelegramError::Api {
            error_code,
            retry_after,
            ..
        } if error_code == 429 || (500..=599).contains(&error_code) => FailureClass::Retry {
            status: Some(error_code),
            retry_after: retry_after
                .map(Duration::from_secs)
                .map(|delay| delay.min(MAX_RETRY_AFTER)),
        },
        TelegramError::Api { error_code, .. } if matches!(error_code, 401 | 403) => {
            FailureClass::Terminal(AuthoritativeLookupError::BotAuthentication {
                status: Some(error_code),
            })
        }
        TelegramError::Api {
            error_code,
            description,
            ..
        } if allow_not_found
            && error_code == 400
            && description == RECOGNIZED_CONNECTION_NOT_FOUND =>
        {
            FailureClass::Terminal(AuthoritativeLookupError::ConnectionNotFound {
                status: Some(error_code),
            })
        }
        TelegramError::Protocol(description)
            if allow_not_found && description == SAFE_CONNECTION_NOT_FOUND =>
        {
            FailureClass::Terminal(AuthoritativeLookupError::ConnectionNotFound {
                status: Some(400),
            })
        }
        TelegramError::Api { error_code, .. } => {
            FailureClass::Terminal(AuthoritativeLookupError::ClientRejected {
                status: Some(error_code),
            })
        }
        TelegramError::Protocol(_) => {
            FailureClass::Terminal(AuthoritativeLookupError::InvalidResponse { status: None })
        }
        TelegramError::InvalidRequest(_) => {
            FailureClass::Terminal(AuthoritativeLookupError::ClientRejected { status: None })
        }
    }
}

fn warn_retry(attempt: usize, status: Option<u16>, delay: Duration) {
    let error = AuthoritativeLookupError::TransientExhausted { status };
    tracing::warn!(
        attempt = attempt + 1,
        kind = error.kind(),
        status,
        delay_seconds = delay.as_secs(),
        "authoritative Telegram lookup retry scheduled"
    );
}
