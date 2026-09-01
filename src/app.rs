use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::Json;
use axum::Router;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::Utc;
use rand::rngs::StdRng;
use secrecy::{SecretSlice, SecretString};
use serde::Serialize;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio::sync::{oneshot, watch};

use crate::clock::SystemClock;
use crate::config::Settings;
use crate::detection::{DetectorError, RuleDetector};
use crate::events::{
    EventError, recover_legacy_recorded_connection_events, recover_recorded_events,
    spawn_outbox_worker,
};
use crate::health::classify_readiness;
use crate::installation::{
    InstallationKeyError, derive_bot_independent_keys, derive_webhook_secret,
};
use crate::owner::{ClaimFileError, ClaimFileManager, OwnerIdentityHandle};
use crate::processing::{
    ClaimSetupCapability, FatalRuntimeEvent, FatalRuntimeNotifier, LifecycleHandler,
    ProcessingEngine, ProcessingError, TrustedStartupReconciliation,
    reconcile_trusted_current_state, spawn_processing_worker_after_start,
};
use crate::runtime_workers::{RuntimeExitError, WorkerGroup, run_server_with_workers};
use crate::storage::{
    OwnerIdentity, ServiceDatabaseDescriptor, StorageError, UnitOfWork,
    initialize_or_load_owner_identity, load_or_initialize_master_seed,
    load_persisted_telegram_bot_id, load_single_trusted_connection, migrate,
    normalize_startup_reconciliation, pin_or_verify_telegram_bot_id,
};
use crate::telegram::{
    BotIdentityApi, BusinessApi, BusinessConnectionApi, OutboxDispatcher, TelegramClient,
    WebhookApi, WebhookInbox, WebhookRegistrationError, lookup_authenticated_bot,
    reconcile_webhook, spawn_new_contact_notifier, webhook_router,
};
use crate::verification::{
    ArithmeticVerifier, ChallengeKeyUpgradeError, upgrade_active_challenge_hmacs,
};

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ReadyHealthResponse {
    status: &'static str,
    owner: &'static str,
    connection: &'static str,
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
    #[error(transparent)]
    InstallationKey(#[from] InstallationKeyError),
    #[error(transparent)]
    ChallengeKeyUpgrade(#[from] ChallengeKeyUpgradeError),
    #[error(transparent)]
    ClaimFile(#[from] ClaimFileError),
    #[error("Telegram bot authentication failed during startup")]
    StartupBotAuthentication,
    #[error("Telegram bot identity lookup failed during startup")]
    StartupBotLookup,
    #[error("Telegram Business authentication failed during startup")]
    StartupBusinessAuthentication,
    #[error(transparent)]
    WebhookRegistration(#[from] WebhookRegistrationError),
    #[error("listener bind failed")]
    Bind(#[source] std::io::Error),
    #[error("HTTP server failed")]
    Server(#[source] std::io::Error),
    #[error("runtime worker shutdown failed")]
    WorkerShutdown,
    #[error("HTTP server failed and runtime worker shutdown also failed")]
    ServerAndWorker(#[source] std::io::Error),
    #[error("Telegram authentication failed during runtime")]
    RuntimeTelegramAuthentication,
}

pub struct PreparedRuntime<C> {
    pool: sqlx::SqlitePool,
    detector: RuleDetector,
    verifier: ArithmeticVerifier<StdRng>,
    webhook_secret: SecretString,
    public_webhook_url: url::Url,
    telegram: C,
    owner_identity: OwnerIdentityHandle,
    claim_file_manager: ClaimFileManager,
    claim_setup_capability: ClaimSetupCapability,
    owner_claim_token: Option<SecretSlice<u8>>,
    destructive_mode: bool,
    startup_pending_connection_id: Option<String>,
}

pub struct BoundRuntime<C> {
    prepared: PreparedRuntime<C>,
    listener: TcpListener,
}

pub struct ReconciledRuntime<C> {
    prepared: PreparedRuntime<C>,
    listener: TcpListener,
}

/// Builds the production Telegram client and completes all local preparation.
///
/// # Errors
///
/// Returns [`AppError`] before binding when local or Telegram identity
/// preparation fails.
pub async fn prepare_runtime(
    settings: Arc<Settings>,
) -> Result<PreparedRuntime<TelegramClient>, AppError> {
    let telegram = TelegramClient::new(settings.bot_token.clone());
    prepare_runtime_with_telegram(settings, telegram).await
}

/// Completes transaction-free managed-credential preparation with an injected
/// Telegram API implementation.
///
/// # Errors
///
/// Returns [`AppError`] before binding, webhook registration, or worker launch.
pub async fn prepare_runtime_with_telegram<C>(
    settings: Arc<Settings>,
    telegram: C,
) -> Result<PreparedRuntime<C>, AppError>
where
    C: BotIdentityApi
        + BusinessConnectionApi
        + BusinessApi
        + WebhookApi
        + Clone
        + Send
        + Sync
        + 'static,
{
    let database = ServiceDatabaseDescriptor::parse(&settings.database_url, Path::new("."))?;
    let claim_file_manager = ClaimFileManager::from_database_descriptor(&database)?;
    let pool = database.connect().await?;
    migrate(&pool).await?;

    let master_seed = load_or_initialize_master_seed(&pool, Utc::now()).await?;
    let independent_keys =
        derive_bot_independent_keys(master_seed.key_version, &master_seed.bytes)?;
    claim_file_manager
        .validate_existing_before_reconciliation(&independent_keys.owner_claim_token)?;

    let authenticated = lookup_authenticated_bot(&telegram)
        .await
        .map_err(|error| match error {
            crate::telegram::AuthoritativeLookupError::BotAuthentication { .. } => {
                AppError::StartupBotAuthentication
            }
            _ => AppError::StartupBotLookup,
        })?;
    let mut pin = UnitOfWork::begin_immediate(&pool).await?;
    pin_or_verify_telegram_bot_id(&mut pin, authenticated.id).await?;
    pin.commit().await?;
    let persisted_bot_id = load_persisted_telegram_bot_id(&pool).await?;
    let webhook_secret = derive_webhook_secret(
        master_seed.key_version,
        &master_seed.bytes,
        persisted_bot_id,
    )?;
    drop(master_seed);

    let mut global_gate = UnitOfWork::begin_immediate(&pool).await?;
    normalize_startup_reconciliation(&mut global_gate, Utc::now()).await?;
    global_gate.commit().await?;

    recover_legacy_recorded_connection_events(&pool, &LifecycleHandler).await?;
    let owner = initialize_or_load_owner_identity(&pool, Utc::now()).await?;
    let owner_identity = OwnerIdentityHandle::new(owner.clone());

    let mut trusted_read = UnitOfWork::begin(&pool).await?;
    let trusted = load_single_trusted_connection(&mut trusted_read).await?;
    trusted_read.commit().await?;
    let startup_pending_connection_id = if let Some(trusted) = trusted {
        match reconcile_trusted_current_state(&pool, &telegram, &trusted.connection_id, Utc::now())
            .await?
        {
            TrustedStartupReconciliation::Converged => None,
            TrustedStartupReconciliation::Pending { .. } => Some(trusted.connection_id),
            TrustedStartupReconciliation::AuthenticationFailed => {
                return Err(AppError::StartupBusinessAuthentication);
            }
        }
    } else {
        None
    };

    recover_recorded_events(&pool, &LifecycleHandler).await?;
    let verifier = ArithmeticVerifier::from_os_rng_with_key_version(
        independent_keys.challenge_hmac_key,
        independent_keys.key_version,
    );
    upgrade_active_challenge_hmacs(&pool, &verifier).await?;
    let detector = RuleDetector::from_defaults()?;

    claim_file_manager.reconcile(&owner, &independent_keys.owner_claim_token)?;
    let (owner_claim_token, claim_setup_capability) = match owner {
        OwnerIdentity::Unclaimed => {
            tracing::info!("owner binding required; use the claim-code file");
            (
                Some(independent_keys.owner_claim_token),
                ClaimSetupCapability::Available,
            )
        }
        OwnerIdentity::Claimed { .. } => {
            drop(independent_keys.owner_claim_token);
            (None, ClaimSetupCapability::Unavailable)
        }
    };

    Ok(PreparedRuntime {
        pool,
        detector,
        verifier,
        webhook_secret,
        public_webhook_url: settings.public_webhook_url.clone(),
        telegram,
        owner_identity,
        claim_file_manager,
        claim_setup_capability,
        owner_claim_token,
        destructive_mode: settings.destructive_mode,
        startup_pending_connection_id,
    })
}

impl<C> PreparedRuntime<C> {
    /// Binds the internal listener without starting any worker or HTTP service.
    ///
    /// # Errors
    ///
    /// Returns a redacted bind error and drops prepared state on failure.
    pub async fn bind(self, address: SocketAddr) -> Result<BoundRuntime<C>, AppError> {
        let listener = TcpListener::bind(address).await.map_err(AppError::Bind)?;
        Ok(BoundRuntime {
            prepared: self,
            listener,
        })
    }
}

impl<C> BoundRuntime<C>
where
    C: WebhookApi,
{
    /// Reconciles Telegram while retaining ownership of the bound listener.
    ///
    /// # Errors
    ///
    /// Returns a closed webhook error and releases the listener on failure.
    pub async fn reconcile(self) -> Result<ReconciledRuntime<C>, AppError> {
        reconcile_webhook(
            &self.prepared.telegram,
            &self.prepared.public_webhook_url,
            &self.prepared.webhook_secret,
        )
        .await?;
        Ok(ReconciledRuntime {
            prepared: self.prepared,
            listener: self.listener,
        })
    }
}

#[derive(Clone)]
struct FatalRuntimeSender {
    signal: watch::Sender<bool>,
}

impl FatalRuntimeNotifier for FatalRuntimeSender {
    fn notify(&self, _event: FatalRuntimeEvent) {
        self.signal.send_replace(true);
    }
}

impl<C> ReconciledRuntime<C>
where
    C: BusinessConnectionApi + BusinessApi + Clone + Send + Sync + 'static,
{
    /// Launches the three owned workers and serves the reconciled listener.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] after every worker has been stopped and awaited.
    pub async fn serve<F>(self, shutdown: F) -> Result<(), AppError>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let PreparedRuntime {
            pool,
            detector,
            verifier,
            webhook_secret,
            public_webhook_url: _,
            telegram,
            owner_identity,
            claim_file_manager,
            claim_setup_capability,
            owner_claim_token,
            destructive_mode,
            startup_pending_connection_id,
        } = self.prepared;

        let (fatal_sender, mut fatal_receiver) = watch::channel(false);
        let fatal_seen = Arc::new(AtomicBool::new(false));
        let fatal_seen_by_shutdown = Arc::clone(&fatal_seen);
        let notifier = spawn_new_contact_notifier(telegram.clone(), 32);
        let engine = ProcessingEngine::new(
            pool.clone(),
            detector,
            verifier,
            SystemClock,
            destructive_mode,
        )
        .with_business_connection_api(telegram.clone())
        .with_fatal_runtime_notifier(FatalRuntimeSender {
            signal: fatal_sender,
        })
        .with_owner_claim(owner_claim_token, owner_identity.clone())
        .with_claim_file_manager(claim_file_manager)
        .with_claim_setup_capability(claim_setup_capability)
        .with_startup_pending_connection(startup_pending_connection_id)
        .with_new_contact_notifier(notifier.handle());
        let (start_sender, start_receiver) = oneshot::channel();
        let processing = spawn_processing_worker_after_start(engine, 128, start_receiver);
        let inbox = Arc::new(processing.handle());
        let outbox = spawn_outbox_worker(
            OutboxDispatcher::new(telegram),
            pool.clone(),
            std::time::Duration::from_millis(250),
        );
        let app = build_served_router(pool, webhook_secret, owner_identity, inbox);
        let workers = WorkerGroup::new(vec![
            notifier.into_task(),
            processing.into_task(),
            outbox.into_task(),
        ]);
        let graceful = async move {
            tokio::select! {
                () = shutdown => {}
                changed = fatal_receiver.changed() => {
                    if changed.is_ok() && *fatal_receiver.borrow() {
                        fatal_seen_by_shutdown.store(true, Ordering::SeqCst);
                    }
                }
            }
        };
        let server = async move {
            let _ = start_sender.send(());
            axum::serve(self.listener, app)
                .with_graceful_shutdown(graceful)
                .await
        };
        run_server_with_workers(server, workers)
            .await
            .map_err(|error| match error {
                RuntimeExitError::Server(error) => AppError::Server(error),
                RuntimeExitError::WorkerShutdown => AppError::WorkerShutdown,
                RuntimeExitError::ServerAndWorker(error) => AppError::ServerAndWorker(error),
            })?;
        if fatal_seen.load(Ordering::SeqCst) {
            return Err(AppError::RuntimeTelegramAuthentication);
        }
        Ok(())
    }
}

pub fn build_router() -> Router {
    Router::new().route("/health/live", get(liveness))
}

pub fn build_router_with_inbox<I: WebhookInbox + 'static>(
    webhook_secret: SecretString,
    owner_identity: OwnerIdentityHandle,
    inbox: Arc<I>,
) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .merge(webhook_router(webhook_secret, owner_identity, inbox))
}

fn build_served_router<I: WebhookInbox + 'static>(
    pool: sqlx::SqlitePool,
    webhook_secret: SecretString,
    owner_identity: OwnerIdentityHandle,
    inbox: Arc<I>,
) -> Router {
    build_router_with_inbox(webhook_secret, owner_identity, inbox)
        .route("/health/ready", get(move || readiness(pool.clone())))
}

async fn liveness() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn readiness(pool: sqlx::SqlitePool) -> Response {
    match classify_readiness(&pool, Utc::now()).await {
        Ok(snapshot) => (
            StatusCode::OK,
            Json(ReadyHealthResponse {
                status: "ok",
                owner: snapshot.owner.as_str(),
                connection: snapshot.connection.as_str(),
            }),
        )
            .into_response(),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "unavailable",
            }),
        )
            .into_response(),
    }
}
