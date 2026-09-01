use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chathygiene::telegram::{
    AuthenticatedBot, AuthoritativeBusinessConnection, AuthoritativeLookupError, BotIdentityApi,
    BoxFuture, BusinessConnectionApi, BusinessRights, TelegramError,
    lookup_authenticated_bot_with_delay, lookup_business_connection_with_delay,
};

#[derive(Clone)]
struct FakeBotApi {
    outcomes: Arc<Mutex<VecDeque<Result<AuthenticatedBot, TelegramError>>>>,
    calls: Arc<Mutex<usize>>,
}

impl FakeBotApi {
    fn new(outcomes: Vec<Result<AuthenticatedBot, TelegramError>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            calls: Arc::new(Mutex::new(0)),
        }
    }

    fn calls(&self) -> usize {
        *self.calls.lock().unwrap()
    }
}

impl BotIdentityApi for FakeBotApi {
    fn get_me(&self) -> BoxFuture<'_, Result<AuthenticatedBot, TelegramError>> {
        Box::pin(async move {
            *self.calls.lock().unwrap() += 1;
            self.outcomes.lock().unwrap().pop_front().unwrap()
        })
    }
}

#[derive(Clone)]
struct FakeBusinessApi {
    outcomes: Arc<Mutex<VecDeque<Result<AuthoritativeBusinessConnection, TelegramError>>>>,
    calls: Arc<Mutex<Vec<String>>>,
}

impl FakeBusinessApi {
    fn new(outcomes: Vec<Result<AuthoritativeBusinessConnection, TelegramError>>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into())),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }
}

impl BusinessConnectionApi for FakeBusinessApi {
    fn get_business_connection<'a>(
        &'a self,
        connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(connection_id.to_owned());
            self.outcomes.lock().unwrap().pop_front().unwrap()
        })
    }
}

fn bot(id: i64) -> AuthenticatedBot {
    AuthenticatedBot { id }
}

fn connection(connection_id: &str) -> AuthoritativeBusinessConnection {
    AuthoritativeBusinessConnection {
        connection_id: connection_id.to_owned(),
        business_user_id: 42,
        user_chat_id: Some(4200),
        connection_established_at: 1_789_000_000,
        rights: BusinessRights {
            can_reply: true,
            can_read_messages: true,
            can_delete_sent_messages: true,
            can_delete_all_messages: true,
        },
        enabled: true,
    }
}

fn no_delay(
    observed: Arc<Mutex<Vec<Duration>>>,
) -> impl Fn(Duration) -> BoxFuture<'static, ()> + Send + Sync + 'static {
    move |duration| {
        observed.lock().unwrap().push(duration);
        Box::pin(async {})
    }
}

#[tokio::test]
async fn bounded_lookup_returns_first_and_last_attempt_successes_without_real_delays() {
    let delays = Arc::new(Mutex::new(Vec::new()));
    let delay = no_delay(Arc::clone(&delays));
    let first = FakeBotApi::new(vec![Ok(bot(7))]);
    assert_eq!(
        lookup_authenticated_bot_with_delay(&first, &delay)
            .await
            .unwrap(),
        bot(7)
    );
    assert_eq!(first.calls(), 1);
    assert!(delays.lock().unwrap().is_empty());

    let last = FakeBotApi::new(vec![
        Err(TelegramError::Timeout),
        Err(TelegramError::Transport),
        Ok(bot(9)),
    ]);
    assert_eq!(
        lookup_authenticated_bot_with_delay(&last, &delay)
            .await
            .unwrap(),
        bot(9)
    );
    assert_eq!(last.calls(), 3);
    assert_eq!(
        *delays.lock().unwrap(),
        vec![Duration::from_secs(1), Duration::from_secs(2)]
    );

    let business_delays = Arc::new(Mutex::new(Vec::new()));
    let business_delay = no_delay(Arc::clone(&business_delays));
    let business = FakeBusinessApi::new(vec![
        Err(TelegramError::Transport),
        Ok(connection("business-1")),
    ]);
    assert_eq!(
        lookup_business_connection_with_delay(&business, "business-1", &business_delay)
            .await
            .unwrap(),
        connection("business-1")
    );
    assert_eq!(business.calls(), vec!["business-1", "business-1"]);
    assert_eq!(
        *business_delays.lock().unwrap(),
        vec![Duration::from_secs(1)]
    );
}

#[tokio::test]
async fn transient_failures_exhaust_three_attempts_and_cap_retry_after() {
    for (error, expected_status, expected_delays) in [
        (
            TelegramError::Timeout,
            None,
            vec![Duration::from_secs(1), Duration::from_secs(2)],
        ),
        (
            TelegramError::Api {
                error_code: 429,
                description: "sentinel rate limit description".to_owned(),
                retry_after: Some(93),
            },
            Some(429),
            vec![Duration::from_secs(5), Duration::from_secs(5)],
        ),
        (
            TelegramError::Api {
                error_code: 503,
                description: "sentinel server description".to_owned(),
                retry_after: None,
            },
            Some(503),
            vec![Duration::from_secs(1), Duration::from_secs(2)],
        ),
    ] {
        let delays = Arc::new(Mutex::new(Vec::new()));
        let delay = no_delay(Arc::clone(&delays));
        let api = FakeBotApi::new(vec![Err(error.clone()), Err(error.clone()), Err(error)]);
        assert_eq!(
            lookup_authenticated_bot_with_delay(&api, &delay)
                .await
                .unwrap_err(),
            AuthoritativeLookupError::TransientExhausted {
                status: expected_status
            }
        );
        assert_eq!(api.calls(), 3);
        assert_eq!(*delays.lock().unwrap(), expected_delays);
    }
}

#[tokio::test]
async fn terminal_failures_use_closed_connection_and_authentication_classes() {
    let delay = no_delay(Arc::new(Mutex::new(Vec::new())));
    for status in [401, 403] {
        let api = FakeBusinessApi::new(vec![Err(TelegramError::Api {
            error_code: status,
            description: "sentinel auth description".to_owned(),
            retry_after: None,
        })]);
        assert_eq!(
            lookup_business_connection_with_delay(&api, "business-1", &delay)
                .await
                .unwrap_err(),
            AuthoritativeLookupError::BotAuthentication {
                status: Some(status)
            }
        );
        assert_eq!(api.calls().len(), 1);
    }

    let missing = FakeBusinessApi::new(vec![Err(TelegramError::Api {
        error_code: 400,
        description: "Bad Request: business connection not found".to_owned(),
        retry_after: None,
    })]);
    assert_eq!(
        lookup_business_connection_with_delay(&missing, "business-1", &delay)
            .await
            .unwrap_err(),
        AuthoritativeLookupError::ConnectionNotFound { status: Some(400) }
    );

    for status in [400, 404] {
        let rejected = FakeBusinessApi::new(vec![Err(TelegramError::Api {
            error_code: status,
            description: "unknown client rejection".to_owned(),
            retry_after: None,
        })]);
        assert_eq!(
            lookup_business_connection_with_delay(&rejected, "business-1", &delay)
                .await
                .unwrap_err(),
            AuthoritativeLookupError::ClientRejected {
                status: Some(status)
            }
        );
    }

    let malformed = FakeBusinessApi::new(vec![Err(TelegramError::Protocol(
        "sentinel malformed body".to_owned(),
    ))]);
    assert_eq!(
        lookup_business_connection_with_delay(&malformed, "business-1", &delay)
            .await
            .unwrap_err(),
        AuthoritativeLookupError::InvalidResponse { status: None }
    );

    let get_me_not_found = FakeBotApi::new(vec![Err(TelegramError::Api {
        error_code: 400,
        description: "Bad Request: business connection not found".to_owned(),
        retry_after: None,
    })]);
    assert_eq!(
        lookup_authenticated_bot_with_delay(&get_me_not_found, &delay)
            .await
            .unwrap_err(),
        AuthoritativeLookupError::ClientRejected { status: Some(400) }
    );
}

#[tokio::test]
async fn closed_lookup_errors_redact_url_token_id_description_and_body() {
    let sentinel =
        "https://sentinel.invalid/ 123456:secret business-secret response-description body-secret";
    let api = FakeBusinessApi::new(vec![Err(TelegramError::Protocol(sentinel.to_owned()))]);
    let delay = no_delay(Arc::new(Mutex::new(Vec::new())));
    let error = lookup_business_connection_with_delay(&api, "business-secret", &delay)
        .await
        .unwrap_err();
    let rendered = format!("{error:?} {error}");
    for secret in [
        "sentinel.invalid",
        "123456:secret",
        "business-secret",
        "response-description",
        "body-secret",
    ] {
        assert!(!rendered.contains(secret));
    }
}
