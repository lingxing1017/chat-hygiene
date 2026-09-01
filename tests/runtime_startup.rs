mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use chathygiene::app::{AppError, prepare_runtime_with_telegram};
use chathygiene::config::Settings;
use chathygiene::storage::{connect, migrate};
use chathygiene::telegram::{
    AuthenticatedBot, AuthoritativeBusinessConnection, BotIdentityApi, BoxFuture, BusinessApi,
    BusinessConnectionApi, BusinessRights, DeleteAction, EditAction, ReadAction, SendAction,
    SentMessage, TelegramError, WebhookApi,
};
use secrecy::SecretString;
use sqlx::Row;
use url::Url;

#[derive(Clone, Default)]
struct StartupFake {
    get_me_calls: Arc<AtomicUsize>,
    business_calls: Arc<AtomicUsize>,
    webhook_calls: Arc<AtomicUsize>,
    fail_webhook: Arc<AtomicBool>,
}

impl BotIdentityApi for StartupFake {
    fn get_me(&self) -> BoxFuture<'_, Result<AuthenticatedBot, TelegramError>> {
        self.get_me_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(AuthenticatedBot { id: 9001 }) })
    }
}

impl BusinessConnectionApi for StartupFake {
    fn get_business_connection<'a>(
        &'a self,
        connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        self.business_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            Ok(AuthoritativeBusinessConnection {
                connection_id: connection_id.to_owned(),
                business_user_id: 42,
                user_chat_id: Some(42),
                connection_established_at: 1,
                rights: BusinessRights::default(),
                enabled: false,
            })
        })
    }
}

impl WebhookApi for StartupFake {
    fn set_webhook<'a>(
        &'a self,
        _public_url: &'a Url,
        _secret: &'a SecretString,
    ) -> BoxFuture<'a, Result<(), TelegramError>> {
        self.webhook_calls.fetch_add(1, Ordering::SeqCst);
        let fail = self.fail_webhook.load(Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                Err(TelegramError::Api {
                    error_code: 400,
                    description: "response-sentinel token-sentinel url-sentinel".to_owned(),
                    retry_after: None,
                })
            } else {
                Ok(())
            }
        })
    }
}

impl BusinessApi for StartupFake {
    fn send_business_message<'a>(
        &'a self,
        _action: &'a SendAction,
    ) -> BoxFuture<'a, Result<SentMessage, TelegramError>> {
        Box::pin(async { Err(TelegramError::Transport) })
    }

    fn edit_business_message<'a>(
        &'a self,
        _action: &'a EditAction,
    ) -> BoxFuture<'a, Result<(), TelegramError>> {
        Box::pin(async { Err(TelegramError::Transport) })
    }

    fn read_business_message<'a>(
        &'a self,
        _action: &'a ReadAction,
    ) -> BoxFuture<'a, Result<(), TelegramError>> {
        Box::pin(async { Err(TelegramError::Transport) })
    }

    fn delete_business_messages<'a>(
        &'a self,
        _action: &'a DeleteAction,
    ) -> BoxFuture<'a, Result<(), TelegramError>> {
        Box::pin(async { Err(TelegramError::Transport) })
    }
}

fn settings(database_url: String) -> Arc<Settings> {
    Arc::new(Settings {
        bot_token: SecretString::from("token-sentinel".to_owned()),
        webhook_secret: SecretString::from("old-webhook-sentinel".to_owned()),
        challenge_hmac_key: SecretString::from("old-challenge-sentinel".to_owned()),
        owner_user_id: 999_999,
        public_webhook_url: "https://runtime.example/telegram/webhook".parse().unwrap(),
        database_url,
        destructive_mode: false,
    })
}

#[tokio::test]
async fn preparation_commits_managed_state_before_binding() {
    let (directory, database_url) = common::temporary_database();
    let fake = StartupFake::default();
    let _prepared = prepare_runtime_with_telegram(settings(database_url.clone()), fake.clone())
        .await
        .unwrap();

    assert_eq!(fake.get_me_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.business_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.webhook_calls.load(Ordering::SeqCst), 0);
    let pool = connect(&database_url).await.unwrap();
    let key = sqlx::query(
        "SELECT state, length(master_seed) AS seed_len, telegram_bot_id FROM key_material",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(key.get::<String, _>("state"), "READY");
    assert_eq!(key.get::<i64, _>("seed_len"), 32);
    assert_eq!(key.get::<i64, _>("telegram_bot_id"), 9001);
    let global: String = sqlx::query_scalar("SELECT state FROM telegram_reconciliation_state")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(global, "PENDING");
    assert!(directory.path().join("claim-code").is_file());
}

#[tokio::test]
async fn bind_failure_skips_webhook_and_business_calls() {
    let (_directory, database_url) = common::temporary_database();
    let fake = StartupFake::default();
    let prepared = prepare_runtime_with_telegram(settings(database_url), fake.clone())
        .await
        .unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = occupied.local_addr().unwrap();

    let Err(error) = prepared.bind(address).await else {
        panic!("occupied address must fail");
    };
    assert!(matches!(error, AppError::Bind(_)));
    assert_eq!(fake.webhook_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.business_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn registration_failure_is_redacted_and_releases_listener() {
    let (_directory, database_url) = common::temporary_database();
    let fake = StartupFake::default();
    fake.fail_webhook.store(true, Ordering::SeqCst);
    let prepared = prepare_runtime_with_telegram(settings(database_url), fake.clone())
        .await
        .unwrap();
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address: SocketAddr = probe.local_addr().unwrap();
    drop(probe);

    let bound = prepared.bind(address).await.unwrap();
    let Err(error) = bound.reconcile().await else {
        panic!("configured webhook rejection must fail");
    };
    let rendered = format!("{error:?} {error}");
    for sentinel in [
        "response-sentinel",
        "token-sentinel",
        "url-sentinel",
        "runtime.example",
    ] {
        assert!(!rendered.contains(sentinel));
    }
    assert_eq!(fake.webhook_calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.business_calls.load(Ordering::SeqCst), 0);
    let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
    drop(rebound);
}

#[tokio::test]
async fn corrupt_seed_fails_before_telegram_or_bind() {
    let (_directory, database_url) = common::temporary_database();
    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "UPDATE key_material
         SET state = 'READY', master_seed = zeroblob(32), seed_checksum = zeroblob(32),
             initialized_at = '2026-09-01T00:00:00Z'",
    )
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    let fake = StartupFake::default();

    let Err(error) = prepare_runtime_with_telegram(settings(database_url), fake.clone()).await
    else {
        panic!("corrupt seed must fail");
    };
    assert!(matches!(error, AppError::Storage(_)));
    assert_eq!(fake.get_me_calls.load(Ordering::SeqCst), 0);
    assert_eq!(fake.webhook_calls.load(Ordering::SeqCst), 0);
}
