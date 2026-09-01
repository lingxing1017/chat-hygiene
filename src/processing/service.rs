use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use secrecy::{ExposeSecret, SecretSlice};
use sqlx::SqlitePool;
use subtle::ConstantTimeEq;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval};

use crate::clock::Clock;
use crate::detection::SpamDetector;
use crate::events::{EventError, RecordReceipt, apply_recorded_event, record_prepared_event};
use crate::owner::{
    ClaimFileManager, LabeledMessageBody, OwnerCommandError, OwnerCommandService,
    OwnerCommandSource, OwnerIdentityHandle, OwnerTelegramAction, ParsedOwnerClaim,
    parse_owner_command,
};
use crate::retention::RetentionService;
use crate::storage::{
    BusinessConnectionCandidate, CandidateGuard, CandidateWrite, ConnectionReconciliationSnapshot,
    ConversationKey, GlobalReconciliationState, NewAuditEvent, NewOutboxAction, OutboxActionKind,
    OwnerChatSource, OwnerIdentity, StorageError, TelegramReconciliationState,
    TrustedConnectionWrite, UnitOfWork, apply_authoritative_candidate, candidates_for_user,
    claim_owner, clear_connection_candidates, connection_reconciliation_snapshot,
    delete_connection_candidates_for_other_users, enqueue_outbox_action, find_business_connection,
    find_conversation, insert_audit_event, list_outbox_actions_for_update, load_candidate_guard,
    load_owner_identity, load_telegram_reconciliation_state, promote_claim_candidate,
    promote_owner_chat, prune_connection_candidates, reconcile_authoritative_trusted_connection,
    retire_trusted_connection_not_found, set_telegram_auth_failed,
};
use crate::telegram::{
    AuthoritativeBusinessConnection, AuthoritativeLookupError, BoxFuture, BusinessConnectionApi,
    IngressError, RawBusinessEvent, RawEventKind, TelegramError, WebhookInbox,
    delete_message_batches, lookup_business_connection,
};
use crate::verification::ChallengeVerifier;

use super::handler::LifecycleHandler;
use super::models::{LifecycleFacts, PreparedAction};
use super::notifications::{NewContactNotice, NewContactNotifier, NoopNewContactNotifier};
use super::preparer::{EventPreparer, OwnerLifecycleState, load_owner_lifecycle_state};
use super::trace::ProcessingTrace;

#[derive(Debug, Error)]
pub enum ProcessingError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Event(#[from] EventError),
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
    #[error("invalid raw Business event: {0}")]
    InvalidEvent(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FatalRuntimeEvent {
    TelegramAuthentication,
}

pub trait FatalRuntimeNotifier: Send + Sync {
    fn notify(&self, event: FatalRuntimeEvent);
}

#[cfg(test)]
trait ClaimRetirementTestSeam: Send + Sync {
    fn retire<'a>(
        &'a self,
        token: &'a SecretSlice<u8>,
    ) -> BoxFuture<'a, Result<(), crate::owner::ClaimFileError>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopFatalRuntimeNotifier;

impl FatalRuntimeNotifier for NoopFatalRuntimeNotifier {
    fn notify(&self, _event: FatalRuntimeEvent) {}
}

#[derive(Debug, Clone, Copy, Default)]
struct UnavailableBusinessConnectionApi;

impl BusinessConnectionApi for UnavailableBusinessConnectionApi {
    fn get_business_connection<'a>(
        &'a self,
        _connection_id: &'a str,
    ) -> BoxFuture<'a, Result<AuthoritativeBusinessConnection, TelegramError>> {
        Box::pin(async {
            Err(TelegramError::InvalidRequest(
                "authoritative Business API is unavailable".to_owned(),
            ))
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ConnectionRetry {
    failures: usize,
    due_at: tokio::time::Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TriggerApplication {
    Commit,
    Retry { transient: bool },
}

#[derive(Debug, Clone, Copy)]
struct ClaimedOwner {
    user_id: i64,
    chat_source: OwnerChatSource,
}

#[derive(Debug, Clone)]
struct OwnerClaimSnapshot {
    owner: OwnerIdentity,
    global: TelegramReconciliationState,
    guard: CandidateGuard,
    candidates: Vec<BusinessConnectionCandidate>,
}

#[derive(Debug)]
struct OwnerClaimLookup {
    authoritative: Option<AuthoritativeBusinessConnection>,
    authentication_failed: bool,
}

#[derive(Debug, Clone, Copy)]
struct ValidOwnerClaim {
    from_user_id: i64,
    owner_chat_id: i64,
    message_date: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ClaimSetupCapability {
    #[default]
    Unavailable,
    Available,
}

pub struct ProcessingEngine<D, V, C> {
    pool: SqlitePool,
    clock: C,
    preparer: EventPreparer<D, V, C>,
    retention: RetentionService<C>,
    handler: LifecycleHandler,
    default_destructive_mode: bool,
    new_contact_notifier: Arc<dyn NewContactNotifier>,
    business_connection_api: Arc<dyn BusinessConnectionApi>,
    fatal_runtime_notifier: Arc<dyn FatalRuntimeNotifier>,
    fatal_notified: bool,
    connection_retries: HashMap<i64, ConnectionRetry>,
    owner_claim_token: Option<SecretSlice<u8>>,
    owner_identity: Option<OwnerIdentityHandle>,
    claim_file_manager: Option<ClaimFileManager>,
    claim_setup_capability: ClaimSetupCapability,
    #[cfg(test)]
    claim_retirement_test_seam: Option<Arc<dyn ClaimRetirementTestSeam>>,
}

struct WorkItem {
    update_id: i64,
    raw: RawBusinessEvent,
    receipt: oneshot::Sender<Result<RecordReceipt, ProcessingError>>,
}

#[derive(Clone)]
pub struct ProcessingHandle {
    sender: mpsc::Sender<WorkItem>,
}

impl<D, V, C> ProcessingEngine<D, V, C>
where
    D: SpamDetector,
    V: ChallengeVerifier,
    C: Clock,
{
    #[must_use]
    pub fn new(pool: SqlitePool, detector: D, verifier: V, clock: C, destructive_mode: bool) -> Self
    where
        C: Clone,
    {
        Self {
            pool,
            clock: clock.clone(),
            preparer: EventPreparer::new(detector, verifier, clock.clone(), destructive_mode),
            retention: RetentionService::new(clock),
            handler: LifecycleHandler,
            default_destructive_mode: destructive_mode,
            new_contact_notifier: Arc::new(NoopNewContactNotifier),
            business_connection_api: Arc::new(UnavailableBusinessConnectionApi),
            fatal_runtime_notifier: Arc::new(NoopFatalRuntimeNotifier),
            fatal_notified: false,
            connection_retries: HashMap::new(),
            owner_claim_token: None,
            owner_identity: None,
            claim_file_manager: None,
            claim_setup_capability: ClaimSetupCapability::Unavailable,
            #[cfg(test)]
            claim_retirement_test_seam: None,
        }
    }

    #[must_use]
    pub fn with_new_contact_notifier<N>(mut self, notifier: N) -> Self
    where
        N: NewContactNotifier + 'static,
    {
        self.new_contact_notifier = Arc::new(notifier);
        self
    }

    #[must_use]
    pub fn with_business_connection_api<A>(mut self, api: A) -> Self
    where
        A: BusinessConnectionApi + 'static,
    {
        self.business_connection_api = Arc::new(api);
        self
    }

    #[must_use]
    pub fn with_fatal_runtime_notifier<N>(mut self, notifier: N) -> Self
    where
        N: FatalRuntimeNotifier + 'static,
    {
        self.fatal_runtime_notifier = Arc::new(notifier);
        self
    }

    #[must_use]
    pub fn with_owner_claim(
        mut self,
        token: Option<SecretSlice<u8>>,
        owner_identity: OwnerIdentityHandle,
    ) -> Self {
        self.owner_claim_token = token;
        self.owner_identity = Some(owner_identity);
        self
    }

    #[must_use]
    pub fn with_claim_file_manager(mut self, manager: ClaimFileManager) -> Self {
        self.claim_file_manager = Some(manager);
        self
    }

    #[must_use]
    pub fn with_claim_setup_capability(mut self, capability: ClaimSetupCapability) -> Self {
        self.claim_setup_capability = capability;
        self
    }

    #[cfg(test)]
    fn with_claim_retirement_test_seam(mut self, seam: Arc<dyn ClaimRetirementTestSeam>) -> Self {
        self.claim_retirement_test_seam = Some(seam);
        self
    }

    /// Serially prepares, records, and atomically applies one raw update.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessingError`] when preparation, durable recording, or
    /// transactional application fails.
    pub async fn process(
        &mut self,
        update_id: i64,
        mut raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        if raw.kind == RawEventKind::OwnerClaim {
            return self.process_owner_claim(update_id, raw).await;
        }
        if raw.kind == RawEventKind::OwnerStart {
            return self.process_owner_start(update_id, raw).await;
        }
        if raw.kind == RawEventKind::OwnerCommand {
            return self.process_owner_command(update_id, raw).await;
        }
        let mut read = UnitOfWork::begin(&self.pool).await?;
        make_preclaim_business_event_inert(&mut raw, &mut read).await?;
        let first_contact_notice = first_contact_notice(&raw, &mut read).await?;
        let prepared = self.preparer.prepare(update_id, raw, &mut read).await?;
        read.rollback().await?;

        let receipt = record_prepared_event(&self.pool, &prepared).await?;
        if receipt == RecordReceipt::DuplicateApplied {
            return Ok(receipt);
        }
        if prepared.event_type == "business_connection_changed" {
            let facts: LifecycleFacts = serde_json::from_value(prepared.facts.clone())?;
            let connection_id = facts.connection_id.as_deref().ok_or_else(|| {
                ProcessingError::InvalidEvent("connection trigger ID is missing".to_owned())
            })?;
            self.reconcile_connection_trigger(update_id, connection_id, prepared.occurred_at)
                .await?;
            return Ok(receipt);
        }
        apply_recorded_event(&self.pool, update_id, &self.handler).await?;
        if let Some(notice) = first_contact_notice {
            let owner_user_id = notice.owner_user_id;
            let contact_chat_id = notice.contact_chat_id;
            if self.new_contact_notifier.try_notify(notice).is_err() {
                tracing::warn!(
                    owner_user_id,
                    contact_chat_id,
                    "new-contact notification was not queued"
                );
            }
        }
        Ok(receipt)
    }

    async fn process_owner_claim(
        &mut self,
        update_id: i64,
        mut raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        if let Some(receipt) = existing_update_receipt(&self.pool, update_id).await? {
            return Ok(receipt);
        }
        let claim = raw.owner_claim.take().ok_or_else(|| {
            ProcessingError::InvalidEvent("owner claim context is missing".to_owned())
        })?;
        match claim {
            ParsedOwnerClaim::NotClaim => Err(ProcessingError::InvalidEvent(
                "owner claim event is not a claim".to_owned(),
            )),
            ParsedOwnerClaim::Ignore => {
                record_owner_claim_rejection(&self.pool, update_id, None, raw.occurred_at).await
            }
            ParsedOwnerClaim::Reject { reply_chat_id } => {
                record_owner_claim_rejection(
                    &self.pool,
                    update_id,
                    Some(reply_chat_id),
                    raw.occurred_at,
                )
                .await
            }
            ParsedOwnerClaim::Candidate {
                from_user_id,
                owner_chat_id,
                message_id: _,
                message_date,
                token,
            } => {
                let Some(owner_identity) = self.owner_identity.clone() else {
                    return record_owner_claim_rejection(
                        &self.pool,
                        update_id,
                        Some(owner_chat_id),
                        raw.occurred_at,
                    )
                    .await;
                };
                let snapshot = self
                    .snapshot_owner_claim(from_user_id, self.clock.now())
                    .await?;
                if !matches!(snapshot.owner, OwnerIdentity::Unclaimed)
                    || !claim_token_matches(self.owner_claim_token.as_ref(), &token)
                {
                    return record_owner_claim_rejection(
                        &self.pool,
                        update_id,
                        Some(owner_chat_id),
                        raw.occurred_at,
                    )
                    .await;
                }
                drop(token);
                let lookup = self.lookup_claim_candidate(&snapshot).await;
                self.commit_owner_claim(
                    update_id,
                    ValidOwnerClaim {
                        from_user_id,
                        owner_chat_id,
                        message_date,
                        occurred_at: raw.occurred_at,
                    },
                    snapshot,
                    lookup,
                    owner_identity,
                )
                .await
            }
        }
    }

    async fn process_owner_start(
        &mut self,
        update_id: i64,
        mut raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        let mut snapshot = raw.owner_command.take().ok_or_else(|| {
            ProcessingError::InvalidEvent("owner Start context is missing".to_owned())
        })?;
        let from_user_id = snapshot
            .from_user_id
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                ProcessingError::InvalidEvent("owner Start user is invalid".to_owned())
            })?;
        let chat_id = raw.chat_id.filter(|value| *value > 0).ok_or_else(|| {
            ProcessingError::InvalidEvent("owner Start chat is invalid".to_owned())
        })?;
        if !snapshot.private_chat || snapshot.text != "/start" {
            return Err(ProcessingError::InvalidEvent(
                "owner Start shape is invalid".to_owned(),
            ));
        }

        let identity_gate = if let Some(owner_identity) = &self.owner_identity {
            Some(owner_identity.write_gate().await)
        } else {
            None
        };
        let mut uow = UnitOfWork::begin_immediate(&self.pool).await?;
        if let Some(receipt) = existing_update_receipt_in(&mut uow, update_id).await? {
            uow.rollback().await?;
            return Ok(receipt);
        }
        let owner = load_owner_identity(&mut uow).await?;
        if matches!(
            owner,
            OwnerIdentity::Claimed {
                owner_user_id,
                owner_chat_id,
                owner_chat_source,
                ..
            } if owner_user_id == from_user_id
                && (owner_chat_source == OwnerChatSource::LegacyFallback
                    || owner_chat_id == chat_id)
        ) {
            uow.rollback().await?;
            drop(identity_gate);
            raw.kind = RawEventKind::OwnerCommand;
            "/help".clone_into(&mut snapshot.text);
            raw.owner_command = Some(snapshot);
            return self.process_owner_command(update_id, raw).await;
        }

        insert_redacted_owner_start_update(&mut uow, update_id, raw.occurred_at).await?;
        if owner == OwnerIdentity::Unclaimed
            && self.claim_setup_capability == ClaimSetupCapability::Available
        {
            enqueue_private_message(
                &mut uow,
                update_id,
                chat_id,
                "OWNER_SETUP_GUIDE",
                raw.occurred_at,
            )
            .await?;
        }
        uow.commit().await?;
        drop(identity_gate);
        Ok(RecordReceipt::Recorded)
    }

    async fn snapshot_owner_claim(
        &mut self,
        claimant_user_id: i64,
        service_now: chrono::DateTime<chrono::Utc>,
    ) -> Result<OwnerClaimSnapshot, ProcessingError> {
        let mut uow = UnitOfWork::begin(&self.pool).await?;
        let owner = load_owner_identity(&mut uow).await?;
        let global = load_telegram_reconciliation_state(&mut uow).await?;
        let guard = load_candidate_guard(&mut uow).await?;
        let mut candidates = candidates_for_user(&mut uow, claimant_user_id).await?;
        uow.rollback().await?;
        let cutoff = service_now - chrono::Duration::days(7);
        candidates.retain(|candidate| candidate.observed_at >= cutoff);
        Ok(OwnerClaimSnapshot {
            owner,
            global,
            guard,
            candidates,
        })
    }

    async fn lookup_claim_candidate(&mut self, snapshot: &OwnerClaimSnapshot) -> OwnerClaimLookup {
        if snapshot.global.state != GlobalReconciliationState::Ready
            || snapshot.guard.overflow_established_at.is_some()
            || snapshot.candidates.len() != 1
        {
            return OwnerClaimLookup {
                authoritative: None,
                authentication_failed: false,
            };
        }
        match lookup_business_connection(
            self.business_connection_api.as_ref(),
            &snapshot.candidates[0].connection_id,
        )
        .await
        {
            Ok(authoritative) => OwnerClaimLookup {
                authoritative: Some(authoritative),
                authentication_failed: false,
            },
            Err(AuthoritativeLookupError::BotAuthentication { .. }) => OwnerClaimLookup {
                authoritative: None,
                authentication_failed: true,
            },
            Err(_) => OwnerClaimLookup {
                authoritative: None,
                authentication_failed: false,
            },
        }
    }

    async fn commit_owner_claim(
        &mut self,
        update_id: i64,
        claim: ValidOwnerClaim,
        snapshot: OwnerClaimSnapshot,
        lookup: OwnerClaimLookup,
        owner_identity: OwnerIdentityHandle,
    ) -> Result<RecordReceipt, ProcessingError> {
        let service_now = self.clock.now();
        let mut identity_gate = owner_identity.write_gate().await;
        let mut uow = UnitOfWork::begin_immediate(&self.pool).await?;
        if let Some(receipt) = existing_update_receipt_in(&mut uow, update_id).await? {
            uow.rollback().await?;
            return Ok(receipt);
        }
        if !matches!(
            load_owner_identity(&mut uow).await?,
            OwnerIdentity::Unclaimed
        ) {
            insert_redacted_owner_update(&mut uow, update_id, claim.occurred_at).await?;
            insert_owner_claim_audit(&mut uow, update_id, false, claim.occurred_at).await?;
            enqueue_private_message(
                &mut uow,
                update_id,
                claim.owner_chat_id,
                "OWNER_CLAIM_REJECTED",
                claim.occurred_at,
            )
            .await?;
            uow.commit().await?;
            return Ok(RecordReceipt::Recorded);
        }

        let global = load_telegram_reconciliation_state(&mut uow).await?;
        let guard_before_prune = load_candidate_guard(&mut uow).await?;
        let candidates_before_prune = candidates_for_user(&mut uow, claim.from_user_id).await?;
        let floor = claim_generation_floor(
            claim.message_date,
            &snapshot,
            &guard_before_prune,
            &candidates_before_prune,
        );
        prune_connection_candidates(&mut uow, service_now - chrono::Duration::days(7), 256).await?;
        let guard = load_candidate_guard(&mut uow).await?;
        let candidates = candidates_for_user(&mut uow, claim.from_user_id).await?;
        let claimed = claim_owner(
            &mut uow,
            claim.from_user_id,
            claim.owner_chat_id,
            floor,
            service_now,
        )
        .await?;
        if lookup.authentication_failed {
            set_telegram_auth_failed(&mut uow, service_now).await?;
        }
        let promoted = try_promote_claim_candidate(
            &mut uow,
            &snapshot,
            &global,
            &guard,
            &candidates,
            lookup.authoritative.as_ref(),
            service_now,
        )
        .await?;
        let connection_state = if promoted {
            claim_connection_state(lookup.authoritative.as_ref())
        } else if guard.overflow_established_at.is_none() && candidates.len() >= 2 {
            delete_connection_candidates_for_other_users(&mut uow, claim.from_user_id).await?;
            "ambiguous"
        } else {
            clear_connection_candidates(&mut uow).await?;
            "missing"
        };
        if promoted {
            clear_connection_candidates(&mut uow).await?;
        }
        insert_redacted_owner_update(&mut uow, update_id, claim.occurred_at).await?;
        insert_owner_claim_audit(&mut uow, update_id, true, claim.occurred_at).await?;
        enqueue_direct_owner_message(
            &mut uow,
            update_id,
            claim.owner_chat_id,
            &format!("claim succeeded connection={connection_state}"),
            claim.occurred_at,
            "OWNER_CLAIM_CONFIRMATION",
        )
        .await?;
        uow.commit().await?;
        *identity_gate = claimed;
        drop(identity_gate);
        #[cfg(test)]
        self.finish_owner_claim(lookup.authentication_failed).await;
        #[cfg(not(test))]
        self.finish_owner_claim(lookup.authentication_failed);
        Ok(RecordReceipt::Recorded)
    }

    #[cfg(not(test))]
    fn finish_owner_claim(&mut self, authentication_failed: bool) {
        let retirement_error = self
            .claim_file_manager
            .as_ref()
            .zip(self.owner_claim_token.as_ref())
            .and_then(|(manager, token)| manager.retire_after_claim(token).err());
        drop(self.owner_claim_token.take());
        if retirement_error.is_some() {
            tracing::error!(
                error_code = "claim_file_retirement_failed",
                "claim-code retirement failed after Owner claim"
            );
        }
        if authentication_failed && !self.fatal_notified {
            self.fatal_notified = true;
            self.fatal_runtime_notifier
                .notify(FatalRuntimeEvent::TelegramAuthentication);
        }
    }

    #[cfg(test)]
    async fn finish_owner_claim(&mut self, authentication_failed: bool) {
        let retirement_error = if let Some(token) = self.owner_claim_token.as_ref() {
            if let Some(seam) = self.claim_retirement_test_seam.as_ref() {
                seam.retire(token).await.err()
            } else {
                self.claim_file_manager
                    .as_ref()
                    .and_then(|manager| manager.retire_after_claim(token).err())
            }
        } else {
            None
        };
        drop(self.owner_claim_token.take());
        if retirement_error.is_some() {
            tracing::error!(
                error_code = "claim_file_retirement_failed",
                "claim-code retirement failed after Owner claim"
            );
        }
        if authentication_failed && !self.fatal_notified {
            self.fatal_notified = true;
            self.fatal_runtime_notifier
                .notify(FatalRuntimeEvent::TelegramAuthentication);
        }
    }

    async fn reconcile_connection_trigger(
        &mut self,
        update_id: i64,
        connection_id: &str,
        occurred_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), ProcessingError> {
        let mut snapshot_uow = UnitOfWork::begin_immediate(&self.pool).await?;
        let owner = match load_owner_lifecycle_state(&mut snapshot_uow).await? {
            OwnerLifecycleState::Pending => {
                snapshot_uow.rollback().await?;
                self.schedule_connection_retry(update_id, false);
                return Ok(());
            }
            OwnerLifecycleState::Ready(owner) => owner,
        };
        let snapshot = connection_reconciliation_snapshot(&mut snapshot_uow, connection_id).await?;
        snapshot_uow.rollback().await?;

        let authoritative =
            match lookup_business_connection(self.business_connection_api.as_ref(), connection_id)
                .await
            {
                Ok(authoritative) => authoritative,
                Err(AuthoritativeLookupError::ConnectionNotFound { .. }) => {
                    self.apply_connection_not_found(update_id, connection_id, owner, &snapshot)
                        .await?;
                    return Ok(());
                }
                Err(AuthoritativeLookupError::BotAuthentication { .. }) => {
                    let mut uow = UnitOfWork::begin_immediate(&self.pool).await?;
                    set_telegram_auth_failed(&mut uow, self.clock.now()).await?;
                    uow.commit().await?;
                    if !self.fatal_notified {
                        self.fatal_notified = true;
                        self.fatal_runtime_notifier
                            .notify(FatalRuntimeEvent::TelegramAuthentication);
                    }
                    return Ok(());
                }
                Err(error) => {
                    self.schedule_connection_retry(
                        update_id,
                        matches!(error, AuthoritativeLookupError::TransientExhausted { .. }),
                    );
                    return Ok(());
                }
            };

        self.apply_authoritative_connection_trigger(
            update_id,
            occurred_at,
            owner,
            snapshot,
            authoritative,
        )
        .await
    }

    async fn apply_connection_not_found(
        &mut self,
        update_id: i64,
        connection_id: &str,
        expected_owner: OwnerIdentity,
        snapshot: &ConnectionReconciliationSnapshot,
    ) -> Result<(), ProcessingError> {
        let mut uow = UnitOfWork::begin_immediate(&self.pool).await?;
        let current_owner = match load_owner_lifecycle_state(&mut uow).await? {
            OwnerLifecycleState::Pending => {
                uow.rollback().await?;
                self.schedule_connection_retry(update_id, false);
                return Ok(());
            }
            OwnerLifecycleState::Ready(owner) => owner,
        };
        if current_owner != expected_owner {
            uow.rollback().await?;
            self.schedule_connection_retry(update_id, true);
            return Ok(());
        }
        let outcome = retire_trusted_connection_not_found(
            &mut uow,
            connection_id,
            snapshot.trusted_revision.unwrap_or_default(),
        )
        .await?;
        match outcome {
            TrustedConnectionWrite::Reconciled => {
                mark_connection_trigger_applied(&mut uow, update_id, self.clock.now()).await?;
                uow.commit().await?;
                self.connection_retries.remove(&update_id);
            }
            TrustedConnectionWrite::RevisionConflict => {
                uow.rollback().await?;
                self.schedule_connection_retry(update_id, true);
            }
            TrustedConnectionWrite::UserConflict | TrustedConnectionWrite::GenerationConflict => {
                uow.rollback().await?;
                self.schedule_connection_retry(update_id, false);
            }
            TrustedConnectionWrite::Installed
            | TrustedConnectionWrite::Replaced
            | TrustedConnectionWrite::Ambiguous => {
                return Err(ProcessingError::InvalidEvent(
                    "unexpected connection-not-found outcome".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn apply_authoritative_connection_trigger(
        &mut self,
        update_id: i64,
        occurred_at: chrono::DateTime<chrono::Utc>,
        expected_owner: OwnerIdentity,
        snapshot: ConnectionReconciliationSnapshot,
        authoritative: AuthoritativeBusinessConnection,
    ) -> Result<(), ProcessingError> {
        let service_now = self.clock.now();
        let candidate = authoritative_candidate(&authoritative, service_now)?;
        let mut uow = UnitOfWork::begin_immediate(&self.pool).await?;
        let current_owner = match load_owner_lifecycle_state(&mut uow).await? {
            OwnerLifecycleState::Pending => {
                uow.rollback().await?;
                self.schedule_connection_retry(update_id, false);
                return Ok(());
            }
            OwnerLifecycleState::Ready(owner) => owner,
        };
        if current_owner != expected_owner {
            uow.rollback().await?;
            self.schedule_connection_retry(update_id, true);
            return Ok(());
        }
        let application = match current_owner {
            OwnerIdentity::Unclaimed => {
                apply_unclaimed_connection_trigger(
                    &mut uow,
                    &candidate,
                    &snapshot,
                    update_id,
                    occurred_at,
                    service_now,
                )
                .await?
            }
            OwnerIdentity::Claimed {
                owner_user_id,
                owner_chat_source,
                ..
            } => {
                apply_claimed_connection_trigger(
                    &mut uow,
                    &authoritative,
                    &candidate,
                    &snapshot,
                    ClaimedOwner {
                        user_id: owner_user_id,
                        chat_source: owner_chat_source,
                    },
                    update_id,
                    occurred_at,
                )
                .await?
            }
        };
        if let TriggerApplication::Retry { transient } = application {
            uow.rollback().await?;
            self.schedule_connection_retry(update_id, transient);
            return Ok(());
        }
        mark_connection_trigger_applied(&mut uow, update_id, service_now).await?;
        uow.commit().await?;
        self.connection_retries.remove(&update_id);
        Ok(())
    }

    fn schedule_connection_retry(&mut self, update_id: i64, transient: bool) {
        const BACKOFF: [u64; 6] = [1, 2, 4, 8, 16, 30];
        let current = self.connection_retries.get(&update_id).copied();
        let failures = current.map_or(0, |retry| retry.failures);
        let delay = if transient {
            BACKOFF[failures.min(BACKOFF.len() - 1)]
        } else {
            300
        };
        self.connection_retries.insert(
            update_id,
            ConnectionRetry {
                failures: failures.saturating_add(1),
                due_at: tokio::time::Instant::now() + Duration::from_secs(delay),
            },
        );
    }

    /// Attempts every recorded connection trigger once without interpreting
    /// update-ID order as lifecycle chronology.
    ///
    /// # Errors
    ///
    /// Returns a processing error for corrupt recorded facts or storage state.
    pub async fn recover_recorded_connection_triggers(&mut self) -> Result<usize, ProcessingError> {
        let triggers = recorded_connection_triggers(&self.pool).await?;
        let mut attempted = 0;
        for (update_id, connection_id, occurred_at) in triggers {
            self.reconcile_connection_trigger(update_id, &connection_id, occurred_at)
                .await?;
            attempted += 1;
        }
        Ok(attempted)
    }

    async fn retry_due_connection_triggers(&mut self) -> Result<(), ProcessingError> {
        let now = tokio::time::Instant::now();
        let due = self
            .connection_retries
            .iter()
            .filter_map(|(update_id, retry)| (retry.due_at <= now).then_some(*update_id))
            .collect::<Vec<_>>();
        if due.is_empty() {
            return Ok(());
        }
        let recorded = recorded_connection_triggers(&self.pool).await?;
        for update_id in due {
            if let Some((_, connection_id, occurred_at)) = recorded
                .iter()
                .find(|(candidate, _, _)| *candidate == update_id)
            {
                self.reconcile_connection_trigger(update_id, connection_id, *occurred_at)
                    .await?;
            } else {
                self.connection_retries.remove(&update_id);
            }
        }
        Ok(())
    }

    async fn process_owner_command(
        &mut self,
        update_id: i64,
        raw: RawBusinessEvent,
    ) -> Result<RecordReceipt, ProcessingError> {
        let mut identity_gate = if let Some(owner_identity) = &self.owner_identity {
            Some(owner_identity.write_gate().await)
        } else {
            None
        };
        let mut uow = UnitOfWork::begin(&self.pool).await?;
        if let Some(receipt) = existing_update_receipt_in(&mut uow, update_id).await? {
            uow.rollback().await?;
            return Ok(receipt);
        }
        let trace_enabled = dry_run_enabled(&mut uow, self.default_destructive_mode).await?;
        insert_redacted_owner_command_update(&mut uow, update_id, raw.occurred_at).await?;

        let snapshot = raw.owner_command.ok_or_else(|| {
            ProcessingError::InvalidEvent("owner command context is missing".to_owned())
        })?;
        let source = OwnerCommandSource {
            from_user_id: snapshot.from_user_id.unwrap_or_default(),
            chat_id: raw.chat_id.unwrap_or_default(),
            private_chat: snapshot.private_chat,
            replied_sample: snapshot.replied_sample.map(|sample| LabeledMessageBody {
                body: sample.body,
                content_type: sample.content_type,
                source_chat_id: sample.source_chat_id,
                source_message_id: sample.source_message_id,
            }),
        };
        let service = OwnerCommandService::at(raw.occurred_at)
            .with_default_destructive_mode(self.default_destructive_mode);
        let owner = match service.authorize(&source, &mut uow).await {
            Ok(owner) => owner,
            Err(OwnerCommandError::Unauthorized) => {
                uow.commit().await?;
                return Ok(RecordReceipt::Recorded);
            }
            Err(error) => return Err(ProcessingError::InvalidEvent(error.to_string())),
        };
        let (response, telegram_actions) = match parse_owner_command(&snapshot.text) {
            Ok(command) => match service
                .execute_authorized(command, source, &owner, &mut uow)
                .await
            {
                Ok(execution) => (execution.response, execution.telegram_actions),
                Err(OwnerCommandError::Storage(error)) => {
                    return Err(ProcessingError::InvalidEvent(error));
                }
                Err(error) => (format!("error={error}"), Vec::new()),
            },
            Err(error) => (format!("error={error}"), Vec::new()),
        };
        enqueue_owner_telegram_actions(&mut uow, update_id, telegram_actions, raw.occurred_at)
            .await?;
        let owner_chat_id = owner.owner_chat_id;
        let owner_message_id = raw.message_id;
        enqueue_direct_owner_message(
            &mut uow,
            update_id,
            owner_chat_id,
            &response,
            raw.occurred_at,
            "OWNER_COMMAND_REPLY",
        )
        .await?;
        if trace_enabled {
            enqueue_owner_command_trace(
                &mut uow,
                update_id,
                owner_chat_id,
                owner_message_id,
                raw.occurred_at,
            )
            .await?;
        }
        uow.commit().await?;
        if let Some(gate) = identity_gate.as_mut() {
            **gate = owner.identity;
        }
        Ok(RecordReceipt::Recorded)
    }
}

fn claim_token_matches(expected: Option<&SecretSlice<u8>>, provided: &SecretSlice<u8>) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    expected.expose_secret().len() == provided.expose_secret().len()
        && bool::from(expected.expose_secret().ct_eq(provided.expose_secret()))
}

async fn existing_update_receipt(
    pool: &SqlitePool,
    update_id: i64,
) -> Result<Option<RecordReceipt>, ProcessingError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = ?")
            .bind(update_id)
            .fetch_optional(pool)
            .await
            .map_err(StorageError::from)?;
    status.map(|status| receipt_for_status(&status)).transpose()
}

async fn existing_update_receipt_in(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
) -> Result<Option<RecordReceipt>, ProcessingError> {
    let status: Option<String> =
        sqlx::query_scalar("SELECT status FROM processed_update WHERE update_id = ?")
            .bind(update_id)
            .fetch_optional(uow.connection())
            .await
            .map_err(StorageError::from)?;
    status.map(|status| receipt_for_status(&status)).transpose()
}

fn receipt_for_status(status: &str) -> Result<RecordReceipt, ProcessingError> {
    match status {
        "RECORDED" => Ok(RecordReceipt::DuplicateRecorded),
        "APPLIED" => Ok(RecordReceipt::DuplicateApplied),
        _ => Err(ProcessingError::Event(EventError::InvalidStatus(
            status.to_owned(),
        ))),
    }
}

async fn record_owner_claim_rejection(
    pool: &SqlitePool,
    update_id: i64,
    reply_chat_id: Option<i64>,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<RecordReceipt, ProcessingError> {
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    if let Some(receipt) = existing_update_receipt_in(&mut uow, update_id).await? {
        uow.rollback().await?;
        return Ok(receipt);
    }
    insert_redacted_owner_update(&mut uow, update_id, occurred_at).await?;
    insert_owner_claim_audit(&mut uow, update_id, false, occurred_at).await?;
    if let Some(owner_chat_id) = reply_chat_id {
        enqueue_private_message(
            &mut uow,
            update_id,
            owner_chat_id,
            "OWNER_CLAIM_REJECTED",
            occurred_at,
        )
        .await?;
    }
    uow.commit().await?;
    Ok(RecordReceipt::Recorded)
}

async fn insert_redacted_owner_start_update(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (?, 'owner_start', ?, 'APPLIED', ?, ?)",
    )
    .bind(update_id)
    .bind(format!(
        "{{\"update_id\":{update_id},\"event_type\":\"owner_start\",\"facts\":{{\"kind\":\"OWNER_START\"}}}}"
    ))
    .bind(occurred_at.to_rfc3339())
    .bind(occurred_at.to_rfc3339())
    .execute(uow.connection())
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

async fn insert_redacted_owner_update(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    let result = sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (?, 'owner_claim', ?, 'APPLIED', ?, ?)",
    )
    .bind(update_id)
    .bind(format!(
        "{{\"update_id\":{update_id},\"event_type\":\"owner_claim\",\"facts\":{{\"kind\":\"OWNER_CLAIM\"}}}}"
    ))
    .bind(occurred_at.to_rfc3339())
    .bind(occurred_at.to_rfc3339())
    .execute(uow.connection())
    .await
    .map_err(StorageError::from)?;
    if result.rows_affected() != 1 {
        return Err(ProcessingError::Event(EventError::InvalidStatus(
            "owner claim update was not inserted".to_owned(),
        )));
    }
    Ok(())
}

async fn insert_redacted_owner_command_update(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    sqlx::query(
        "INSERT INTO processed_update
         (update_id, event_type, event_json, status, received_at, applied_at)
         VALUES (?, 'owner_command', ?, 'APPLIED', ?, ?)",
    )
    .bind(update_id)
    .bind(format!(
        "{{\"update_id\":{update_id},\"event_type\":\"owner_command\",\"facts\":{{\"kind\":\"OWNER_COMMAND\"}}}}"
    ))
    .bind(occurred_at.to_rfc3339())
    .bind(occurred_at.to_rfc3339())
    .execute(uow.connection())
    .await
    .map_err(StorageError::from)?;
    Ok(())
}

async fn insert_owner_claim_audit(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    success: bool,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    insert_audit_event(
        uow,
        &NewAuditEvent {
            source_update_id: update_id,
            key: None,
            event_kind: if success {
                "OWNER_CLAIMED".to_owned()
            } else {
                "SECURITY".to_owned()
            },
            state_before: None,
            state_after: None,
            score: None,
            reasons_json: None,
            rule_ids_json: None,
            normalized_hash: None,
            rule_version: None,
            error_code: (!success).then(|| "OWNER_CLAIM_REJECTED".to_owned()),
            error_message: (!success).then(|| "owner claim rejected".to_owned()),
            occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn enqueue_direct_owner_message(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    owner_chat_id: i64,
    message: &str,
    occurred_at: chrono::DateTime<chrono::Utc>,
    suffix: &str,
) -> Result<(), ProcessingError> {
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: None,
            kind: OutboxActionKind::SendOwnerMessage,
            payload_json: serde_json::json!({
                "message": message,
                "owner_chat_id": owner_chat_id,
            })
            .to_string(),
            idempotency_key: format!("{update_id}:{suffix}"),
            created_at: occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn enqueue_private_message(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    chat_id: i64,
    message_kind: &'static str,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: None,
            kind: OutboxActionKind::SendPrivateMessage,
            payload_json: serde_json::json!({
                "chat_id": chat_id,
                "message_kind": message_kind,
            })
            .to_string(),
            idempotency_key: format!("{update_id}:{message_kind}"),
            created_at: occurred_at,
        },
    )
    .await?;
    Ok(())
}

fn claim_generation_floor(
    message_date: i64,
    snapshot: &OwnerClaimSnapshot,
    current_guard: &CandidateGuard,
    current_candidates: &[BusinessConnectionCandidate],
) -> i64 {
    snapshot
        .candidates
        .iter()
        .chain(current_candidates)
        .map(|candidate| candidate.connection_established_at)
        .chain(snapshot.guard.overflow_established_at)
        .chain(current_guard.overflow_established_at)
        .fold(message_date, i64::max)
}

async fn try_promote_claim_candidate(
    uow: &mut UnitOfWork<'_>,
    snapshot: &OwnerClaimSnapshot,
    current_global: &TelegramReconciliationState,
    current_guard: &CandidateGuard,
    current_candidates: &[BusinessConnectionCandidate],
    authoritative: Option<&AuthoritativeBusinessConnection>,
    service_now: chrono::DateTime<chrono::Utc>,
) -> Result<bool, ProcessingError> {
    if snapshot.global.state != GlobalReconciliationState::Ready
        || current_global.state != GlobalReconciliationState::Ready
        || snapshot.global.state_revision != current_global.state_revision
        || snapshot.guard != *current_guard
        || current_guard.overflow_established_at.is_some()
        || snapshot.candidates.len() != 1
        || current_candidates.len() != 1
    {
        return Ok(false);
    }
    let snapshot_candidate = &snapshot.candidates[0];
    let current_candidate = &current_candidates[0];
    if snapshot_candidate.connection_id != current_candidate.connection_id
        || snapshot_candidate.state_revision != current_candidate.state_revision
    {
        return Ok(false);
    }
    let Some(authoritative) = authoritative else {
        return Ok(false);
    };
    if !authoritative.enabled
        || authoritative.connection_id != current_candidate.connection_id
        || authoritative.business_user_id != current_candidate.business_user_id
        || authoritative.connection_established_at != current_candidate.connection_established_at
    {
        return Ok(false);
    }
    let candidate = authoritative_candidate(authoritative, service_now)?;
    Ok(matches!(
        promote_claim_candidate(
            uow,
            &candidate,
            current_candidate.state_revision,
            current_guard.state_revision,
        )
        .await?,
        TrustedConnectionWrite::Installed
    ))
}

fn claim_connection_state(authoritative: Option<&AuthoritativeBusinessConnection>) -> &'static str {
    let Some(authoritative) = authoritative else {
        return "missing";
    };
    if authoritative.rights.can_reply
        && authoritative.rights.can_read_messages
        && authoritative.rights.can_delete_sent_messages
        && authoritative.rights.can_delete_all_messages
    {
        "enabled"
    } else {
        "rights_incomplete"
    }
}

async fn apply_unclaimed_connection_trigger(
    uow: &mut UnitOfWork<'_>,
    candidate: &BusinessConnectionCandidate,
    snapshot: &ConnectionReconciliationSnapshot,
    update_id: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
    service_now: chrono::DateTime<chrono::Utc>,
) -> Result<TriggerApplication, ProcessingError> {
    match apply_authoritative_candidate(uow, candidate, snapshot.candidate_revision).await? {
        CandidateWrite::RevisionConflict => {
            return Ok(TriggerApplication::Retry { transient: true });
        }
        CandidateWrite::UserConflict | CandidateWrite::GenerationConflict => {
            insert_connection_audit(uow, update_id, "connection_candidate_conflict", occurred_at)
                .await?;
        }
        CandidateWrite::Inserted | CandidateWrite::Reconciled => {}
    }
    prune_connection_candidates(uow, service_now - chrono::Duration::days(7), 256).await?;
    Ok(TriggerApplication::Commit)
}

async fn apply_claimed_connection_trigger(
    uow: &mut UnitOfWork<'_>,
    authoritative: &AuthoritativeBusinessConnection,
    candidate: &BusinessConnectionCandidate,
    snapshot: &ConnectionReconciliationSnapshot,
    owner: ClaimedOwner,
    update_id: i64,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<TriggerApplication, ProcessingError> {
    let matching_trusted =
        snapshot.trusted_connection_id.as_deref() == Some(candidate.connection_id.as_str());
    if authoritative.business_user_id == owner.user_id {
        let outcome = reconcile_authoritative_trusted_connection(
            uow,
            candidate,
            snapshot.trusted_revision,
            snapshot.candidate_revision,
            snapshot.guard_revision,
        )
        .await?;
        match outcome {
            TrustedConnectionWrite::RevisionConflict => {
                return Ok(TriggerApplication::Retry { transient: true });
            }
            TrustedConnectionWrite::UserConflict | TrustedConnectionWrite::GenerationConflict
                if matching_trusted =>
            {
                return Ok(TriggerApplication::Retry { transient: false });
            }
            TrustedConnectionWrite::UserConflict | TrustedConnectionWrite::GenerationConflict => {
                insert_connection_audit(
                    uow,
                    update_id,
                    "connection_generation_rejected",
                    occurred_at,
                )
                .await?;
            }
            TrustedConnectionWrite::Installed
            | TrustedConnectionWrite::Reconciled
            | TrustedConnectionWrite::Replaced
            | TrustedConnectionWrite::Ambiguous => {
                if owner.chat_source == OwnerChatSource::LegacyFallback
                    && let Some(owner_chat_id) = authoritative.user_chat_id
                {
                    promote_owner_chat(
                        uow,
                        owner.user_id,
                        owner_chat_id,
                        OwnerChatSource::BusinessConnection,
                    )
                    .await?;
                }
            }
        }
    } else if matching_trusted {
        return Ok(TriggerApplication::Retry { transient: false });
    } else {
        insert_connection_audit(uow, update_id, "connection_owner_mismatch", occurred_at).await?;
    }
    Ok(TriggerApplication::Commit)
}

fn authoritative_candidate(
    connection: &AuthoritativeBusinessConnection,
    observed_at: chrono::DateTime<chrono::Utc>,
) -> Result<BusinessConnectionCandidate, ProcessingError> {
    let rights_json = serde_json::to_string(&serde_json::json!({
        "can_reply": connection.rights.can_reply,
        "can_read_messages": connection.rights.can_read_messages,
        "can_delete_sent_messages": connection.rights.can_delete_sent_messages,
        "can_delete_all_messages": connection.rights.can_delete_all_messages,
    }))?;
    Ok(BusinessConnectionCandidate {
        connection_id: connection.connection_id.clone(),
        business_user_id: connection.business_user_id,
        user_chat_id: connection.user_chat_id,
        rights_json,
        enabled: connection.enabled,
        connection_established_at: connection.connection_established_at,
        state_revision: 0,
        observed_at,
    })
}

async fn mark_connection_trigger_applied(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    applied_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    let result = sqlx::query(
        "UPDATE processed_update
         SET status = 'APPLIED', applied_at = ?, error_code = NULL,
             error_message = NULL
         WHERE update_id = ? AND status = 'RECORDED'",
    )
    .bind(applied_at.to_rfc3339())
    .bind(update_id)
    .execute(uow.connection())
    .await
    .map_err(StorageError::from)?;
    if result.rows_affected() != 1 {
        return Err(ProcessingError::Event(EventError::InvalidStatus(
            "connection trigger changed during application".to_owned(),
        )));
    }
    Ok(())
}

async fn insert_connection_audit(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    event_kind: &str,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    insert_audit_event(
        uow,
        &NewAuditEvent {
            source_update_id: update_id,
            key: None,
            event_kind: event_kind.to_owned(),
            state_before: None,
            state_after: None,
            score: None,
            reasons_json: None,
            rule_ids_json: None,
            normalized_hash: None,
            rule_version: None,
            error_code: None,
            error_message: None,
            occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn recorded_connection_triggers(
    pool: &SqlitePool,
) -> Result<Vec<(i64, String, chrono::DateTime<chrono::Utc>)>, ProcessingError> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        "SELECT update_id, event_json FROM processed_update
         WHERE status = 'RECORDED' AND event_type = 'business_connection_changed'
         ORDER BY update_id",
    )
    .fetch_all(pool)
    .await
    .map_err(StorageError::from)?;
    let mut triggers = Vec::new();
    for (update_id, event_json) in rows {
        let event: crate::events::PreparedEvent = serde_json::from_str(&event_json)?;
        let facts: LifecycleFacts = serde_json::from_value(event.facts)?;
        if matches!(facts.action, PreparedAction::Ignore) {
            let connection_id = facts.connection_id.ok_or_else(|| {
                ProcessingError::InvalidEvent(
                    "recorded connection trigger ID is missing".to_owned(),
                )
            })?;
            if connection_id.trim().is_empty() {
                return Err(ProcessingError::InvalidEvent(
                    "recorded connection trigger ID is invalid".to_owned(),
                ));
            }
            triggers.push((update_id, connection_id, event.occurred_at));
        }
    }
    Ok(triggers)
}

async fn make_preclaim_business_event_inert(
    raw: &mut RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<(), ProcessingError> {
    if !matches!(
        raw.kind,
        RawEventKind::InboundMessage
            | RawEventKind::EditedInboundMessage
            | RawEventKind::ManualOwnerMessage
            | RawEventKind::BotBusinessMessage
            | RawEventKind::ImplicitOwnerMessage
            | RawEventKind::MessagesDeleted
    ) || !matches!(
        load_owner_lifecycle_state(uow).await?,
        OwnerLifecycleState::Ready(OwnerIdentity::Unclaimed)
    ) {
        return Ok(());
    }
    raw.kind = RawEventKind::Ignored;
    raw.connection_id = None;
    raw.chat_id = None;
    raw.message_id = None;
    raw.media_group_id = None;
    raw.content = None;
    raw.deleted_message_ids.clear();
    raw.connection = None;
    raw.contact_display_name = None;
    raw.contact_username = None;
    Ok(())
}

async fn enqueue_owner_telegram_actions(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    actions: Vec<OwnerTelegramAction>,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    for action in actions {
        match action {
            OwnerTelegramAction::DeleteBusinessMessages { key, message_ids } => {
                for (batch_index, batch) in
                    delete_message_batches(&message_ids).into_iter().enumerate()
                {
                    enqueue_outbox_action(
                        uow,
                        &NewOutboxAction {
                            source_update_id: update_id,
                            key: Some(key.clone()),
                            kind: OutboxActionKind::DeleteBusinessMessages,
                            payload_json: serde_json::json!({"message_ids": batch}).to_string(),
                            idempotency_key: format!(
                                "{update_id}:DELETE_BUSINESS_MESSAGES:{}:{}:{batch_index}",
                                key.connection_id, key.chat_id
                            ),
                            created_at: occurred_at,
                        },
                    )
                    .await?;
                }
            }
        }
    }
    Ok(())
}

async fn enqueue_owner_command_trace(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    owner_chat_id: i64,
    owner_message_id: Option<i64>,
    occurred_at: chrono::DateTime<chrono::Utc>,
) -> Result<(), ProcessingError> {
    let source_actions = list_outbox_actions_for_update(uow, update_id).await?;
    let trace =
        ProcessingTrace::owner_command(update_id, owner_chat_id, owner_message_id, &source_actions);
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: None,
            kind: OutboxActionKind::SendOwnerMessage,
            payload_json: serde_json::json!({
                "message": trace.render(),
                "owner_chat_id": owner_chat_id,
            })
            .to_string(),
            idempotency_key: format!("{update_id}:DRY_RUN_TRACE"),
            created_at: occurred_at,
        },
    )
    .await?;
    Ok(())
}

async fn dry_run_enabled(
    uow: &mut UnitOfWork<'_>,
    default_destructive_mode: bool,
) -> Result<bool, ProcessingError> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_optional(uow.connection())
            .await
            .map_err(StorageError::from)?;
    Ok(!value
        .as_deref()
        .map_or(default_destructive_mode, |value| value == "true"))
}

async fn first_contact_notice(
    raw: &RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<Option<NewContactNotice>, ProcessingError> {
    if raw.kind != RawEventKind::InboundMessage {
        return Ok(None);
    }
    let (Some(connection_id), Some(contact_chat_id)) = (raw.connection_id.as_deref(), raw.chat_id)
    else {
        return Ok(None);
    };
    let Some(connection) = find_business_connection(uow, connection_id).await? else {
        return Ok(None);
    };
    let owner_chat_id = match load_owner_lifecycle_state(uow).await? {
        OwnerLifecycleState::Pending => connection.owner_user_id,
        OwnerLifecycleState::Ready(OwnerIdentity::Claimed {
            owner_user_id,
            owner_chat_id,
            ..
        }) if owner_user_id == connection.owner_user_id => owner_chat_id,
        OwnerLifecycleState::Ready(_) => return Ok(None),
    };
    let key = ConversationKey::new(connection_id, contact_chat_id);
    if find_conversation(uow, &key).await?.is_some() {
        return Ok(None);
    }
    Ok(Some(NewContactNotice {
        owner_user_id: connection.owner_user_id,
        owner_chat_id,
        contact_chat_id,
        username: raw.contact_username.clone(),
    }))
}

/// Starts the single bounded lifecycle worker used by the MVP.
#[must_use]
pub fn spawn_processing_worker<D, V, C>(
    mut engine: ProcessingEngine<D, V, C>,
    capacity: usize,
) -> ProcessingHandle
where
    D: SpamDetector + 'static,
    V: ChallengeVerifier + 'static,
    C: Clock + 'static,
{
    let (sender, mut receiver) = mpsc::channel::<WorkItem>(capacity.max(1));
    tokio::spawn(async move {
        let mut connection_retry_tick = interval(Duration::from_millis(250));
        connection_retry_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut expiry_tick = interval(Duration::from_secs(15));
        expiry_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut purge_tick = interval(Duration::from_hours(1));
        purge_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                item = receiver.recv() => {
                    let Some(item) = item else {
                        break;
                    };
                    let result = engine.process(item.update_id, item.raw).await;
                    let _ = item.receipt.send(result);
                }
                _ = connection_retry_tick.tick() => {
                    if let Err(error) = engine.retry_due_connection_triggers().await {
                        tracing::error!(
                            error_code = "connection_reconciliation_retry_failed",
                            error = %error,
                            "connection reconciliation retry pass failed"
                        );
                    }
                }
                _ = expiry_tick.tick() => {
                    if let Err(error) = engine.retention.expire_due_state(&engine.pool).await {
                        tracing::error!(%error, "state expiry pass failed");
                    }
                }
                _ = purge_tick.tick() => {
                    if let Err(error) = engine.retention.purge_history(&engine.pool).await {
                        tracing::error!(%error, "history retention pass failed");
                    }
                }
            }
        }
    });
    ProcessingHandle { sender }
}

impl WebhookInbox for ProcessingHandle {
    fn submit(
        &self,
        update_id: i64,
        event: RawBusinessEvent,
    ) -> Pin<Box<dyn Future<Output = Result<RecordReceipt, IngressError>> + Send + '_>> {
        Box::pin(async move {
            let (receipt, receiver) = oneshot::channel();
            self.sender
                .send(WorkItem {
                    update_id,
                    raw: event,
                    receipt,
                })
                .await
                .map_err(|_| {
                    IngressError::RecordingFailed("processing worker stopped".to_owned())
                })?;
            receiver
                .await
                .map_err(|_| {
                    IngressError::RecordingFailed("processing receipt was dropped".to_owned())
                })?
                .map_err(|error| IngressError::RecordingFailed(error.to_string()))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use secrecy::SecretSlice;

    use super::*;
    use crate::clock::SystemClock;
    use crate::detection::RuleDetector;
    use crate::owner::ClaimFileError;
    use crate::storage::{
        connect, initialize_or_load_owner_identity, load_owner_identity, migrate,
    };
    use crate::verification::ArithmeticVerifier;

    type TestEngine =
        ProcessingEngine<RuleDetector, ArithmeticVerifier<rand::rngs::StdRng>, SystemClock>;

    #[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
    struct RetirementObservation {
        calls: usize,
        owner_was_claimed: bool,
        confirmation_was_committed: bool,
        token_was_available: bool,
    }

    struct RecordingRetirement {
        pool: SqlitePool,
        owner: OwnerIdentityHandle,
        fail: bool,
        observation: Mutex<RetirementObservation>,
    }

    impl RecordingRetirement {
        fn observation(&self) -> RetirementObservation {
            *self.observation.lock().unwrap()
        }
    }

    impl ClaimRetirementTestSeam for RecordingRetirement {
        fn retire<'a>(
            &'a self,
            token: &'a SecretSlice<u8>,
        ) -> BoxFuture<'a, Result<(), ClaimFileError>> {
            Box::pin(async move {
                let owner_was_claimed = matches!(
                    self.owner.snapshot().await,
                    OwnerIdentity::Claimed {
                        owner_user_id: 100,
                        owner_chat_id: 500,
                        ..
                    }
                );
                let confirmations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action")
                    .fetch_one(&self.pool)
                    .await
                    .unwrap();
                let mut observation = self.observation.lock().unwrap();
                observation.calls += 1;
                observation.owner_was_claimed = owner_was_claimed;
                observation.confirmation_was_committed = confirmations == 1;
                observation.token_was_available = token.expose_secret().len() == 32;
                if self.fail {
                    Err(ClaimFileError::Filesystem("injected retirement failure"))
                } else {
                    Ok(())
                }
            })
        }
    }

    async fn retirement_engine(
        fail: bool,
    ) -> (
        tempfile::TempDir,
        SqlitePool,
        OwnerIdentityHandle,
        TestEngine,
        Arc<RecordingRetirement>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let pool = connect(&format!(
            "sqlite://{}?mode=rwc",
            directory.path().join("service.db").display()
        ))
        .await
        .unwrap();
        migrate(&pool).await.unwrap();
        let owner = initialize_or_load_owner_identity(&pool, chrono::Utc::now())
            .await
            .unwrap();
        let token = SecretSlice::from(vec![0x11; 32]);
        let owner_handle = OwnerIdentityHandle::new(owner);
        let retirement = Arc::new(RecordingRetirement {
            pool: pool.clone(),
            owner: owner_handle.clone(),
            fail,
            observation: Mutex::new(RetirementObservation::default()),
        });
        let engine = ProcessingEngine::new(
            pool.clone(),
            RuleDetector::from_defaults().unwrap(),
            ArithmeticVerifier::from_os_rng_with_key_version(SecretSlice::from(vec![0x33; 32]), 1),
            SystemClock,
            false,
        )
        .with_owner_claim(Some(token), owner_handle.clone())
        .with_claim_retirement_test_seam(retirement.clone());
        (directory, pool, owner_handle, engine, retirement)
    }

    fn claim_event(token_byte: u8) -> RawBusinessEvent {
        RawBusinessEvent {
            kind: RawEventKind::OwnerClaim,
            connection_id: None,
            chat_id: Some(500),
            message_id: Some(1),
            media_group_id: None,
            content: None,
            deleted_message_ids: Vec::new(),
            connection: None,
            owner_claim: Some(ParsedOwnerClaim::Candidate {
                from_user_id: 100,
                owner_chat_id: 500,
                message_id: 1,
                message_date: chrono::Utc::now().timestamp(),
                token: SecretSlice::from(vec![token_byte; 32]),
            }),
            owner_command: None,
            contact_display_name: None,
            contact_username: None,
            occurred_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn claim_commit_and_publication_precede_file_retirement() {
        let (_directory, _pool, _owner, mut engine, retirement) = retirement_engine(false).await;
        engine.process(1, claim_event(0x11)).await.unwrap();
        assert_eq!(
            retirement.observation(),
            RetirementObservation {
                calls: 1,
                owner_was_claimed: true,
                confirmation_was_committed: true,
                token_was_available: true,
            }
        );
        assert!(engine.owner_claim_token.is_none());
    }

    #[tokio::test]
    async fn retirement_failure_cannot_roll_back_committed_claim() {
        let (_directory, pool, owner, mut engine, retirement) = retirement_engine(true).await;
        engine.process(2, claim_event(0x11)).await.unwrap();
        assert!(matches!(
            owner.snapshot().await,
            OwnerIdentity::Claimed { .. }
        ));
        let confirmations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE source_update_id = 2")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(confirmations, 1);
        assert_eq!(retirement.observation().calls, 1);
        assert!(engine.owner_claim_token.is_none());

        engine.process(2, claim_event(0x11)).await.unwrap();
        assert_eq!(retirement.observation().calls, 1);
    }

    #[tokio::test]
    async fn wrong_token_never_retires_claim_file_or_consumes_expected_token() {
        let (_directory, _pool, owner, mut engine, retirement) = retirement_engine(false).await;
        engine.process(3, claim_event(0x22)).await.unwrap();
        assert_eq!(owner.snapshot().await, OwnerIdentity::Unclaimed);
        assert_eq!(retirement.observation().calls, 0);
        assert!(engine.owner_claim_token.is_some());
    }

    #[tokio::test]
    async fn rolled_back_claim_never_retires_or_publishes_owner() {
        let (_directory, pool, owner, mut engine, retirement) = retirement_engine(false).await;
        sqlx::query(
            "CREATE TRIGGER fail_claim_outbox BEFORE INSERT ON outbox_action \
             WHEN NEW.source_update_id = 4 BEGIN SELECT RAISE(ABORT, 'injected'); END",
        )
        .execute(&pool)
        .await
        .unwrap();

        assert!(engine.process(4, claim_event(0x11)).await.is_err());
        assert_eq!(retirement.observation().calls, 0);
        assert!(engine.owner_claim_token.is_some());
        assert_eq!(owner.snapshot().await, OwnerIdentity::Unclaimed);
        let mut read = UnitOfWork::begin(&pool).await.unwrap();
        assert_eq!(
            load_owner_identity(&mut read).await.unwrap(),
            OwnerIdentity::Unclaimed
        );
        read.rollback().await.unwrap();
        let confirmations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_action WHERE source_update_id = 4")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(confirmations, 0);
    }
}
