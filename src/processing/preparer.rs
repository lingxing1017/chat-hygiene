use std::collections::HashSet;

use chrono::Duration;

use crate::clock::Clock;
use crate::detection::{Decision, DetectionContext, DetectionResult, MessageContent, SpamDetector};
use crate::domain::ConversationState;
use crate::events::PreparedEvent;
use crate::storage::{ConversationKey, UnitOfWork, active_challenge, find_conversation};
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
        let action = match raw.kind {
            RawEventKind::BusinessConnectionChanged => {
                let connection = raw.connection.as_ref().ok_or_else(|| {
                    ProcessingError::InvalidEvent("connection snapshot is missing".to_owned())
                })?;
                let rights_json = serde_json::to_string(&serde_json::json!({
                    "can_reply": connection.rights.can_reply,
                    "can_read_messages": connection.rights.can_read_messages,
                    "can_delete_sent_messages": connection.rights.can_delete_sent_messages,
                    "can_delete_all_messages": connection.rights.can_delete_all_messages,
                }))?;
                PreparedAction::ConnectionChanged {
                    owner_user_id: connection.owner_user_id,
                    enabled: connection.enabled,
                    rights_json,
                }
            }
            RawEventKind::InboundMessage | RawEventKind::EditedInboundMessage => {
                self.prepare_inbound(&raw, uow).await?
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
            RawEventKind::OwnerCommand | RawEventKind::Ignored => PreparedAction::Ignore,
        };
        let chat_id = raw.chat_id;
        let facts = LifecycleFacts {
            connection_id: raw.connection_id.clone(),
            chat_id,
            user_id: chat_id,
            message_id: raw.message_id,
            media_group_id: raw.media_group_id,
            occurred_at: raw.occurred_at,
            action,
        };
        Ok(PreparedEvent::new(
            update_id,
            "lifecycle",
            raw.occurred_at,
            serde_json::to_value(facts)?,
        ))
    }

    async fn prepare_inbound(
        &mut self,
        raw: &RawBusinessEvent,
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
        if matches!(
            state,
            ConversationState::TempSoftBlocked | ConversationState::SpamSoftBlocked
        ) {
            return Ok(PreparedAction::BlockedInbound);
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
        let dry_run_spam = is_spam && !self.destructive_mode;
        let outcome = if is_spam && self.destructive_mode {
            InboundOutcome::Spam
        } else {
            self.safe_outcome(raw.kind, state, &key, &content, uow)
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
                let raw_answer = content.text.as_deref().unwrap_or_default();
                Ok(
                    match self.verifier.evaluate(raw_answer, &challenge.answer_hmac) {
                        AnswerKind::Correct => InboundOutcome::Correct {
                            challenge_id: challenge.id,
                        },
                        AnswerKind::Incorrect => {
                            let exhausted = challenge.attempts_used + 1 >= challenge.max_attempts;
                            InboundOutcome::Incorrect {
                                challenge_id: challenge.id,
                                exhausted,
                                block_expires_at: exhausted
                                    .then(|| self.clock.now() + Duration::hours(24))
                                    .filter(|_| self.destructive_mode),
                            }
                        }
                        AnswerKind::NonNumeric => InboundOutcome::NonNumeric,
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
            created_at: challenge.created_at,
            expires_at: challenge.expires_at,
            max_attempts: challenge.max_attempts,
        }
    }
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
