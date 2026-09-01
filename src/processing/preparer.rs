use std::collections::HashSet;

use chrono::Duration;

use crate::clock::Clock;
use crate::detection::{Decision, DetectionContext, DetectionResult, MessageContent, SpamDetector};
use crate::domain::ConversationState;
use crate::events::PreparedEvent;
use crate::storage::{
    ConversationKey, OwnerIdentity, StorageError, UnitOfWork, active_challenge,
    find_business_connection, find_conversation, find_single_business_connection,
    load_owner_identity,
};
use crate::telegram::{RawBusinessEvent, RawEventKind};
use crate::verification::{AnswerKind, ChallengeVerifier};

use super::models::{
    DetectionFacts, InboundOutcome, LifecycleFacts, PreparedAction, PreparedSender,
};
use super::service::ProcessingError;

pub struct EventPreparer<D, V, C> {
    detector: D,
    verifier: V,
    clock: C,
    destructive_mode: bool,
}

pub(crate) enum OwnerLifecycleState {
    Pending,
    Ready(OwnerIdentity),
}

pub(crate) async fn load_owner_lifecycle_state(
    uow: &mut UnitOfWork<'_>,
) -> Result<OwnerLifecycleState, StorageError> {
    match load_owner_identity(uow).await {
        Ok(identity) => Ok(OwnerLifecycleState::Ready(identity)),
        Err(StorageError::InvalidOwnerIdentity("owner identity is pending initialization")) => {
            Ok(OwnerLifecycleState::Pending)
        }
        Err(error) => Err(error),
    }
}

impl<D, V, C> EventPreparer<D, V, C>
where
    D: SpamDetector,
    V: ChallengeVerifier,
    C: Clock,
{
    #[must_use]
    pub fn new(detector: D, verifier: V, clock: C, destructive_mode: bool) -> Self {
        Self {
            detector,
            verifier,
            clock,
            destructive_mode,
        }
    }

    /// Converts transient content into a body-free durable lifecycle event.
    ///
    /// # Errors
    ///
    /// Returns [`ProcessingError`] for missing Business identities, storage
    /// failures, inconsistent challenge state, or serialization failures.
    pub async fn prepare(
        &mut self,
        update_id: i64,
        raw: RawBusinessEvent,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<PreparedEvent, ProcessingError> {
        let destructive_mode = runtime_destructive_mode(uow, self.destructive_mode).await?;
        let (owner_user_id, owner_chat_id) = trace_owner_identity(&raw, uow).await?;
        let state_before = trace_state_before(&raw, uow).await?;
        let (contact_display_name, contact_username) = if destructive_mode {
            (None, None)
        } else {
            (
                raw.contact_display_name.clone(),
                raw.contact_username.clone(),
            )
        };
        let action = if requires_known_connection(raw.kind)
            && !known_business_connection(&raw, uow).await?
        {
            PreparedAction::Ignore
        } else {
            match raw.kind {
                RawEventKind::InboundMessage | RawEventKind::EditedInboundMessage => {
                    self.prepare_inbound(&raw, destructive_mode, uow).await?
                }
                RawEventKind::ManualOwnerMessage => PreparedAction::ManualOwner,
                RawEventKind::BotBusinessMessage => PreparedAction::BotMessage {
                    sender: PreparedSender::BusinessBot,
                },
                RawEventKind::ImplicitOwnerMessage => PreparedAction::BotMessage {
                    sender: PreparedSender::Implicit,
                },
                RawEventKind::MessagesDeleted => PreparedAction::MessagesDeleted {
                    message_ids: raw.deleted_message_ids.clone(),
                },
                RawEventKind::BusinessConnectionChanged
                | RawEventKind::OwnerClaim
                | RawEventKind::OwnerCommand
                | RawEventKind::Ignored => PreparedAction::Ignore,
            }
        };
        let state_before = state_before.or_else(|| {
            creates_conversation(&action).then(|| ConversationState::New.as_str().to_owned())
        });
        let chat_id = raw.chat_id;
        let facts = LifecycleFacts {
            connection_id: raw.connection_id.clone(),
            chat_id,
            user_id: chat_id,
            message_id: raw.message_id,
            media_group_id: raw.media_group_id,
            contact_display_name,
            contact_username,
            owner_user_id,
            owner_chat_id,
            event_kind: raw_event_name(raw.kind).to_owned(),
            dry_run: !destructive_mode
                && !matches!(raw.kind, RawEventKind::OwnerClaim | RawEventKind::Ignored),
            state_before,
            occurred_at: raw.occurred_at,
            action,
        };
        let event_type = if raw.kind == RawEventKind::BusinessConnectionChanged {
            "business_connection_changed"
        } else {
            "lifecycle"
        };
        Ok(PreparedEvent::new(
            update_id,
            event_type,
            raw.occurred_at,
            serde_json::to_value(facts)?,
        ))
    }

    async fn prepare_inbound(
        &mut self,
        raw: &RawBusinessEvent,
        runtime_destructive_mode: bool,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<PreparedAction, ProcessingError> {
        let key = event_key(raw)?;
        let conversation = find_conversation(uow, &key).await?;
        let state = conversation
            .as_ref()
            .map_or(ConversationState::New, |conversation| conversation.state);
        if state == ConversationState::Active {
            return Ok(PreparedAction::ActiveInbound);
        }
        let availability = business_availability(uow, &key).await?;
        if matches!(
            state,
            ConversationState::TempSoftBlocked | ConversationState::SpamSoftBlocked
        ) {
            return Ok(if runtime_destructive_mode && availability.destructive {
                PreparedAction::BlockedInbound
            } else {
                PreparedAction::BlockedFailOpen
            });
        }

        let content = raw.content.clone().unwrap_or_default();
        let (detection, detector_failed) = match self
            .detector
            .detect(
                &content,
                &DetectionContext {
                    malicious_domains: HashSet::new(),
                    prior_distinct_senders_for_hash: 0,
                },
            )
            .await
        {
            Ok(result) => (DetectionFacts::from(result), false),
            Err(error) => (DetectionFacts::failed(error.to_string()), true),
        };
        let is_spam = detection.decision == "SPAM";
        let destructive_mode = runtime_destructive_mode && availability.destructive;
        let dry_run_spam = is_spam && !destructive_mode;
        let outcome = if is_spam && destructive_mode {
            InboundOutcome::Spam
        } else if (is_spam && state == ConversationState::VerifyPending)
            || (matches!(
                state,
                ConversationState::New | ConversationState::VerifyPending
            ) && !availability.reply)
        {
            InboundOutcome::Retain
        } else {
            self.safe_outcome(raw.kind, state, &key, &content, destructive_mode, uow)
                .await?
        };
        let mut detection = detection;
        if detector_failed && detection.error.is_none() {
            detection.error = Some("detector failed".to_owned());
        }
        Ok(PreparedAction::Inbound {
            detection: Box::new(detection),
            outcome,
            dry_run_spam,
        })
    }

    async fn safe_outcome(
        &mut self,
        raw_kind: RawEventKind,
        state: ConversationState,
        key: &ConversationKey,
        content: &MessageContent,
        destructive_mode: bool,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<InboundOutcome, ProcessingError> {
        match state {
            ConversationState::New => Ok(self.generated_challenge()),
            ConversationState::VerifyPending => {
                let challenge = active_challenge(uow, key).await?.ok_or_else(|| {
                    ProcessingError::InvalidEvent(
                        "VERIFY_PENDING conversation has no active challenge".to_owned(),
                    )
                })?;
                let now = self.clock.now();
                if now >= challenge.expires_at {
                    return Ok(InboundOutcome::Expired {
                        challenge_id: challenge.id,
                    });
                }
                if raw_kind == RawEventKind::EditedInboundMessage {
                    return Ok(InboundOutcome::Retain);
                }
                if challenge.hmac_key_version != self.verifier.key_version() {
                    return Err(ProcessingError::InvalidEvent(
                        "challenge HMAC key version does not match verifier".to_owned(),
                    ));
                }
                let raw_answer = content.text.as_deref().unwrap_or_default();
                Ok(
                    match self.verifier.evaluate(raw_answer, &challenge.answer_hmac) {
                        AnswerKind::Correct => InboundOutcome::Correct {
                            challenge_id: challenge.id,
                        },
                        AnswerKind::Incorrect | AnswerKind::NonNumeric => {
                            let attempts_used = challenge.attempts_used + 1;
                            let exhausted = attempts_used >= challenge.max_attempts;
                            let attempts_remaining =
                                u8::try_from((challenge.max_attempts - attempts_used).max(0))
                                    .unwrap_or_default();
                            InboundOutcome::Incorrect {
                                challenge_id: challenge.id,
                                exhausted,
                                attempts_remaining,
                                block_expires_at: exhausted
                                    .then(|| self.clock.now() + Duration::hours(24))
                                    .filter(|_| destructive_mode),
                            }
                        }
                    },
                )
            }
            ConversationState::VerifiedWaitingOwner => Ok(InboundOutcome::Retain),
            ConversationState::Active
            | ConversationState::TempSoftBlocked
            | ConversationState::SpamSoftBlocked => Err(ProcessingError::InvalidEvent(
                "checked state reached safe inbound preparation".to_owned(),
            )),
        }
    }

    fn generated_challenge(&mut self) -> InboundOutcome {
        let challenge = self.verifier.generate(self.clock.now());
        InboundOutcome::StartChallenge {
            expression: challenge.expression,
            answer_hmac: challenge.answer_hmac,
            hmac_key_version: challenge.hmac_key_version,
            created_at: challenge.created_at,
            expires_at: challenge.expires_at,
            max_attempts: challenge.max_attempts,
        }
    }
}

async fn trace_owner_identity(
    raw: &RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<(Option<i64>, Option<i64>), ProcessingError> {
    if let Some(connection) = raw.connection.as_ref() {
        return Ok((Some(connection.owner_user_id), connection.owner_chat_id));
    }
    if let Some(connection_id) = raw.connection_id.as_deref()
        && let Some(connection) = find_business_connection(uow, connection_id).await?
    {
        let chat_id = owner_chat_for_user(uow, connection.owner_user_id).await?;
        return Ok((Some(connection.owner_user_id), chat_id));
    }
    let Some(connection) = find_single_business_connection(uow).await? else {
        return Ok((None, None));
    };
    let chat_id = owner_chat_for_user(uow, connection.owner_user_id).await?;
    Ok((Some(connection.owner_user_id), chat_id))
}

async fn owner_chat_for_user(
    uow: &mut UnitOfWork<'_>,
    owner_user_id: i64,
) -> Result<Option<i64>, ProcessingError> {
    Ok(match load_owner_lifecycle_state(uow).await? {
        OwnerLifecycleState::Pending => Some(owner_user_id),
        OwnerLifecycleState::Ready(OwnerIdentity::Claimed {
            owner_user_id: stored_user_id,
            owner_chat_id,
            ..
        }) if stored_user_id == owner_user_id => Some(owner_chat_id),
        OwnerLifecycleState::Ready(_) => None,
    })
}

async fn trace_state_before(
    raw: &RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<Option<String>, ProcessingError> {
    let (Some(connection_id), Some(chat_id)) = (raw.connection_id.as_deref(), raw.chat_id) else {
        return Ok(None);
    };
    Ok(
        find_conversation(uow, &ConversationKey::new(connection_id, chat_id))
            .await?
            .map(|conversation| conversation.state.as_str().to_owned()),
    )
}

const fn creates_conversation(action: &PreparedAction) -> bool {
    matches!(
        action,
        PreparedAction::Inbound { .. }
            | PreparedAction::BlockedInbound
            | PreparedAction::BlockedFailOpen
            | PreparedAction::ManualOwner
            | PreparedAction::BotMessage { .. }
            | PreparedAction::MessagesDeleted { .. }
    )
}

const fn raw_event_name(kind: RawEventKind) -> &'static str {
    match kind {
        RawEventKind::BusinessConnectionChanged => "BUSINESS_CONNECTION_CHANGED",
        RawEventKind::InboundMessage => "INBOUND_MESSAGE",
        RawEventKind::EditedInboundMessage => "EDITED_INBOUND_MESSAGE",
        RawEventKind::ManualOwnerMessage => "MANUAL_OWNER_MESSAGE",
        RawEventKind::BotBusinessMessage => "BOT_BUSINESS_MESSAGE",
        RawEventKind::ImplicitOwnerMessage => "IMPLICIT_OWNER_MESSAGE",
        RawEventKind::MessagesDeleted => "MESSAGES_DELETED",
        RawEventKind::OwnerClaim => "OWNER_CLAIM",
        RawEventKind::OwnerCommand => "OWNER_COMMAND",
        RawEventKind::Ignored => "IGNORED",
    }
}

const fn requires_known_connection(kind: RawEventKind) -> bool {
    matches!(
        kind,
        RawEventKind::InboundMessage
            | RawEventKind::EditedInboundMessage
            | RawEventKind::ManualOwnerMessage
            | RawEventKind::BotBusinessMessage
            | RawEventKind::ImplicitOwnerMessage
            | RawEventKind::MessagesDeleted
    )
}

async fn known_business_connection(
    raw: &RawBusinessEvent,
    uow: &mut UnitOfWork<'_>,
) -> Result<bool, ProcessingError> {
    let Some(connection_id) = raw.connection_id.as_deref() else {
        return Ok(false);
    };
    Ok(find_business_connection(uow, connection_id)
        .await?
        .is_some())
}

#[derive(Debug, Default, serde::Deserialize)]
struct StoredRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_all_messages: bool,
}

#[derive(Debug, Clone, Copy, Default)]
struct BusinessAvailability {
    reply: bool,
    destructive: bool,
}

async fn business_availability(
    uow: &mut UnitOfWork<'_>,
    key: &ConversationKey,
) -> Result<BusinessAvailability, ProcessingError> {
    let Some(connection) = find_business_connection(uow, &key.connection_id).await? else {
        return Ok(BusinessAvailability::default());
    };
    if !connection.enabled {
        return Ok(BusinessAvailability::default());
    }
    let rights = serde_json::from_str::<StoredRights>(&connection.rights_json).unwrap_or_default();
    Ok(BusinessAvailability {
        reply: rights.can_reply,
        destructive: rights.can_read_messages && rights.can_delete_all_messages,
    })
}

async fn runtime_destructive_mode(
    uow: &mut UnitOfWork<'_>,
    default: bool,
) -> Result<bool, ProcessingError> {
    let value: Option<String> =
        sqlx::query_scalar("SELECT value FROM runtime_setting WHERE key = 'destructive_mode'")
            .fetch_optional(uow.connection())
            .await
            .map_err(crate::storage::StorageError::from)?;
    Ok(match value.as_deref() {
        None => default,
        Some("true") => true,
        Some(_) => false,
    })
}

impl From<DetectionResult> for DetectionFacts {
    fn from(result: DetectionResult) -> Self {
        Self {
            decision: result.decision.as_str().to_owned(),
            score: result.score,
            reasons: result.reasons,
            matched_rules: result.matched_rules,
            detector_name: result.detector_name,
            detector_version: result.detector_version,
            normalized_hash: result.normalized_hash,
            error: None,
        }
    }
}

impl DetectionFacts {
    fn failed(error: String) -> Self {
        Self {
            decision: Decision::Allow.as_str().to_owned(),
            score: 0,
            reasons: Vec::new(),
            matched_rules: Vec::new(),
            detector_name: "error".to_owned(),
            detector_version: "unavailable".to_owned(),
            normalized_hash: "unavailable".to_owned(),
            error: Some(error),
        }
    }
}

fn event_key(raw: &RawBusinessEvent) -> Result<ConversationKey, ProcessingError> {
    Ok(ConversationKey::new(
        raw.connection_id
            .as_deref()
            .ok_or_else(|| ProcessingError::InvalidEvent("connection ID is missing".to_owned()))?,
        raw.chat_id
            .ok_or_else(|| ProcessingError::InvalidEvent("chat ID is missing".to_owned()))?,
    ))
}
