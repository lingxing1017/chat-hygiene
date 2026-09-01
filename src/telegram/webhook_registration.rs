use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

use super::{TelegramError, WebhookApi};

const RETRY_SECONDS: [u64; 6] = [1, 2, 4, 8, 16, 30];
const MAX_RETRY_AFTER_SECONDS: u64 = 60;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebhookFailureKind {
    Timeout,
    Transport,
    RateLimited,
    ClientResponse,
    ServerResponse,
    Protocol,
    InvalidRequest,
}

impl fmt::Display for WebhookFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Timeout => "timeout",
            Self::Transport => "transport",
            Self::RateLimited => "rate_limited",
            Self::ClientResponse => "client_response",
            Self::ServerResponse => "server_response",
            Self::Protocol => "protocol",
            Self::InvalidRequest => "invalid_request",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookFailureSummary {
    pub kind: WebhookFailureKind,
    pub status_code: Option<u16>,
}

impl fmt::Display for WebhookFailureSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(formatter)?;
        if let Some(status_code) = self.status_code {
            write!(formatter, ", status {status_code}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum WebhookRegistrationError {
    #[error("Telegram permanently rejected webhook registration ({0})")]
    Permanent(WebhookFailureSummary),
    #[error("Telegram webhook registration failed after 7 attempts ({0})")]
    Exhausted(WebhookFailureSummary),
}

trait Sleeper: Send + Sync {
    fn sleep<'a>(&'a self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;
}

struct TokioSleeper;

impl Sleeper for TokioSleeper {
    fn sleep<'a>(&'a self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        Box::pin(tokio::time::sleep(delay))
    }
}

enum RegistrationFailure {
    Retry {
        summary: WebhookFailureSummary,
        retry_after: Option<Duration>,
    },
    Permanent(WebhookFailureSummary),
}

/// Reconciles Telegram's webhook declaration through at most seven attempts.
///
/// # Errors
///
/// Returns only a closed failure class and optional numeric HTTP status. Raw
/// Telegram response text, request values, and secrets are never retained.
pub async fn reconcile_webhook<A: WebhookApi>(
    api: &A,
    public_url: &Url,
    secret: &SecretString,
) -> Result<(), WebhookRegistrationError> {
    reconcile_webhook_with_sleeper(api, public_url, secret, &TokioSleeper).await
}

async fn reconcile_webhook_with_sleeper<A: WebhookApi, S: Sleeper>(
    api: &A,
    public_url: &Url,
    secret: &SecretString,
    sleeper: &S,
) -> Result<(), WebhookRegistrationError> {
    for (attempt, scheduled_seconds) in RETRY_SECONDS
        .into_iter()
        .map(Some)
        .chain([None])
        .enumerate()
    {
        match api.set_webhook(public_url, secret).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                let failure = classify(&error);
                drop(error);
                match failure {
                    RegistrationFailure::Permanent(summary) => {
                        return Err(WebhookRegistrationError::Permanent(summary));
                    }
                    RegistrationFailure::Retry { summary, .. } if scheduled_seconds.is_none() => {
                        return Err(WebhookRegistrationError::Exhausted(summary));
                    }
                    RegistrationFailure::Retry {
                        summary,
                        retry_after,
                    } => {
                        let scheduled = Duration::from_secs(
                            scheduled_seconds.expect("every non-final attempt has a retry delay"),
                        );
                        let delay = if summary.kind == WebhookFailureKind::RateLimited {
                            retry_after
                                .map(|value| {
                                    value.min(Duration::from_secs(MAX_RETRY_AFTER_SECONDS))
                                })
                                .map_or(scheduled, |value| value.max(scheduled))
                        } else {
                            scheduled
                        };
                        tracing::warn!(
                            attempt = attempt + 1,
                            error_kind = %summary.kind,
                            status_code = summary.status_code,
                            retry_delay_seconds = delay.as_secs(),
                            "Telegram webhook registration retry scheduled"
                        );
                        sleeper.sleep(delay).await;
                    }
                }
            }
        }
    }
    unreachable!("webhook registration attempt loop always returns")
}

fn classify(error: &TelegramError) -> RegistrationFailure {
    match error {
        TelegramError::Timeout => RegistrationFailure::Retry {
            summary: WebhookFailureSummary {
                kind: WebhookFailureKind::Timeout,
                status_code: None,
            },
            retry_after: None,
        },
        TelegramError::Transport => RegistrationFailure::Retry {
            summary: WebhookFailureSummary {
                kind: WebhookFailureKind::Transport,
                status_code: None,
            },
            retry_after: None,
        },
        TelegramError::Api {
            error_code: 429,
            retry_after,
            ..
        } => RegistrationFailure::Retry {
            summary: WebhookFailureSummary {
                kind: WebhookFailureKind::RateLimited,
                status_code: Some(429),
            },
            retry_after: (*retry_after).map(Duration::from_secs),
        },
        TelegramError::Api {
            error_code,
            retry_after: _,
            ..
        } if (500..=599).contains(error_code) => RegistrationFailure::Retry {
            summary: WebhookFailureSummary {
                kind: WebhookFailureKind::ServerResponse,
                status_code: Some(*error_code),
            },
            retry_after: None,
        },
        TelegramError::Api { error_code, .. } => {
            RegistrationFailure::Permanent(WebhookFailureSummary {
                kind: WebhookFailureKind::ClientResponse,
                status_code: Some(*error_code),
            })
        }
        TelegramError::Protocol(_) => RegistrationFailure::Retry {
            summary: WebhookFailureSummary {
                kind: WebhookFailureKind::Protocol,
                status_code: None,
            },
            retry_after: None,
        },
        TelegramError::InvalidRequest(_) => RegistrationFailure::Permanent(WebhookFailureSummary {
            kind: WebhookFailureKind::InvalidRequest,
            status_code: None,
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    use secrecy::{ExposeSecret, SecretString};
    use tracing::instrument::WithSubscriber;

    use super::*;
    use crate::telegram::TelegramError;

    struct FakeWebhookApi {
        responses: Mutex<VecDeque<Result<(), TelegramError>>>,
        expected_url: String,
        expected_secret: String,
        observations: Mutex<Vec<(bool, bool)>>,
    }

    impl FakeWebhookApi {
        fn new(responses: Vec<Result<(), TelegramError>>, url: &Url, secret: &str) -> Self {
            Self {
                responses: Mutex::new(responses.into()),
                expected_url: url.as_str().to_owned(),
                expected_secret: secret.to_owned(),
                observations: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.observations.lock().unwrap().len()
        }
    }

    impl WebhookApi for FakeWebhookApi {
        fn set_webhook<'a>(
            &'a self,
            public_url: &'a Url,
            secret: &'a SecretString,
        ) -> Pin<Box<dyn Future<Output = Result<(), TelegramError>> + Send + 'a>> {
            Box::pin(async move {
                self.observations.lock().unwrap().push((
                    public_url.as_str() == self.expected_url,
                    secret.expose_secret() == self.expected_secret,
                ));
                self.responses.lock().unwrap().pop_front().unwrap()
            })
        }
    }

    #[derive(Default)]
    struct RecordingSleeper {
        delays: Mutex<Vec<Duration>>,
    }

    impl Sleeper for RecordingSleeper {
        fn sleep<'a>(&'a self, delay: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
            Box::pin(async move {
                self.delays.lock().unwrap().push(delay);
            })
        }
    }

    fn fixture() -> (Url, SecretString, String) {
        let url = Url::parse("https://chat.example.net/telegram/webhook").unwrap();
        let secret_text = "s".repeat(64);
        let secret = SecretString::from(secret_text.clone());
        (url, secret, secret_text)
    }

    fn api_error(error_code: u16, description: &str, retry_after: Option<u64>) -> TelegramError {
        TelegramError::Api {
            error_code,
            description: description.to_owned(),
            retry_after,
        }
    }

    #[tokio::test]
    async fn retries_every_transient_class_then_succeeds() {
        let cases = [
            TelegramError::Timeout,
            TelegramError::Transport,
            TelegramError::Protocol("sentinel protocol".to_owned()),
            api_error(500, "sentinel server", None),
            api_error(429, "sentinel rate limit", None),
        ];
        for failure in cases {
            let (url, secret, secret_text) = fixture();
            let api = FakeWebhookApi::new(vec![Err(failure), Ok(())], &url, &secret_text);
            let sleeper = RecordingSleeper::default();

            reconcile_webhook_with_sleeper(&api, &url, &secret, &sleeper)
                .await
                .unwrap();

            assert_eq!(api.call_count(), 2);
            assert_eq!(
                sleeper.delays.lock().unwrap().as_slice(),
                [Duration::from_secs(1)]
            );
        }
    }

    #[tokio::test]
    async fn rejects_local_and_client_failures_permanently() {
        let failures = [
            TelegramError::InvalidRequest("sentinel invalid request".to_owned()),
            api_error(400, "sentinel bad request", None),
            api_error(401, "sentinel unauthorized", None),
        ];
        let expected = [
            WebhookFailureSummary {
                kind: WebhookFailureKind::InvalidRequest,
                status_code: None,
            },
            WebhookFailureSummary {
                kind: WebhookFailureKind::ClientResponse,
                status_code: Some(400),
            },
            WebhookFailureSummary {
                kind: WebhookFailureKind::ClientResponse,
                status_code: Some(401),
            },
        ];
        for (failure, expected) in failures.into_iter().zip(expected) {
            let (url, secret, secret_text) = fixture();
            let api = FakeWebhookApi::new(vec![Err(failure)], &url, &secret_text);
            let sleeper = RecordingSleeper::default();

            let error = reconcile_webhook_with_sleeper(&api, &url, &secret, &sleeper)
                .await
                .unwrap_err();

            assert!(
                matches!(error, WebhookRegistrationError::Permanent(value) if value == expected)
            );
            assert_eq!(api.call_count(), 1);
            assert!(sleeper.delays.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn exhausts_after_exactly_seven_attempts_without_final_sleep() {
        let (url, secret, secret_text) = fixture();
        let api = FakeWebhookApi::new(
            vec![
                Err(TelegramError::Timeout),
                Err(TelegramError::Transport),
                Err(api_error(429, "rate", None)),
                Err(api_error(500, "server", None)),
                Err(TelegramError::Protocol("protocol".to_owned())),
                Err(TelegramError::Timeout),
                Err(api_error(503, "last", None)),
            ],
            &url,
            &secret_text,
        );
        let sleeper = RecordingSleeper::default();

        let error = reconcile_webhook_with_sleeper(&api, &url, &secret, &sleeper)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            WebhookRegistrationError::Exhausted(WebhookFailureSummary {
                kind: WebhookFailureKind::ServerResponse,
                status_code: Some(503)
            })
        ));
        assert_eq!(api.call_count(), 7);
        assert_eq!(
            sleeper.delays.lock().unwrap().as_slice(),
            [1, 2, 4, 8, 16, 30].map(Duration::from_secs)
        );
    }

    #[tokio::test]
    async fn rate_limit_delay_never_shortens_schedule_and_caps_hint() {
        for (retry_after, expected) in [(None, 1), (Some(0), 1), (Some(93), 60)] {
            let (url, secret, secret_text) = fixture();
            let api = FakeWebhookApi::new(
                vec![Err(api_error(429, "rate", retry_after)), Ok(())],
                &url,
                &secret_text,
            );
            let sleeper = RecordingSleeper::default();

            reconcile_webhook_with_sleeper(&api, &url, &secret, &sleeper)
                .await
                .unwrap();

            assert_eq!(
                sleeper.delays.lock().unwrap().as_slice(),
                [Duration::from_secs(expected)]
            );
        }
    }

    #[tokio::test]
    async fn repeated_successes_send_identical_values() {
        let (url, secret, secret_text) = fixture();
        let api = FakeWebhookApi::new(vec![Ok(()), Ok(())], &url, &secret_text);

        reconcile_webhook_with_sleeper(&api, &url, &secret, &RecordingSleeper::default())
            .await
            .unwrap();
        reconcile_webhook_with_sleeper(&api, &url, &secret, &RecordingSleeper::default())
            .await
            .unwrap();

        let observations = api.observations.lock().unwrap();
        assert_eq!(observations.len(), 2);
        assert!(
            observations
                .iter()
                .all(|(same_url, same_secret)| *same_url && *same_secret)
        );
    }

    #[derive(Clone)]
    struct BufferWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for BufferWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn registration_errors_and_warnings_discard_sensitive_values() {
        let token = "123456:sentinel-token";
        let response = "sentinel response text";
        let public_url = Url::parse(
            "https://sentinel-user:sentinel-password@example.net/telegram/webhook?key=sentinel",
        )
        .unwrap();
        let public_url_text = public_url.to_string();
        let secret_text = "sentinel-webhook-secret".repeat(3);
        let secret = SecretString::from(secret_text.clone());
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let writer_buffer = Arc::clone(&buffer);
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_writer(move || BufferWriter(Arc::clone(&writer_buffer)))
            .finish();

        let exhausted_api = FakeWebhookApi::new(
            (0..7)
                .map(|_| {
                    Err(TelegramError::Protocol(format!(
                        "{response} {token} {public_url_text} {secret_text}"
                    )))
                })
                .collect(),
            &public_url,
            &secret_text,
        );
        let exhausted = reconcile_webhook_with_sleeper(
            &exhausted_api,
            &public_url,
            &secret,
            &RecordingSleeper::default(),
        )
        .with_subscriber(subscriber)
        .await
        .unwrap_err();

        let permanent_api = FakeWebhookApi::new(
            vec![Err(TelegramError::InvalidRequest(format!(
                "{response} {token} {public_url_text} {secret_text}"
            )))],
            &public_url,
            &secret_text,
        );
        let permanent = reconcile_webhook_with_sleeper(
            &permanent_api,
            &public_url,
            &secret,
            &RecordingSleeper::default(),
        )
        .await
        .unwrap_err();

        let logs = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        let rendered = format!("{exhausted:?} {exhausted} {permanent:?} {permanent} {logs}");
        for sensitive in [token, response, &public_url_text, &secret_text] {
            assert!(!rendered.contains(sensitive));
        }
        assert!(logs.contains("error_kind=protocol"));
        assert!(logs.contains("retry_delay_seconds=1"));
    }
}
