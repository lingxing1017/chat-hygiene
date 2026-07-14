use chrono::Utc;
use serde_json::json;

use crate::domain::{ConversationState, TransitionEvent, transition};
use crate::events::{EventApplier, EventError, PreparedEvent};
use crate::storage::{
    BusinessConnectionRecord, ChallengeRecord, Conversation, ConversationKey, LedgerMessage,
    MessageDirection, NewAuditEvent, NewOutboxAction, OutboxActionKind, SenderKind, UnitOfWork,
    active_owner_reply_ids, close_active_challenge, close_challenge, create_challenge,
    eligible_deletion_ids, enqueue_outbox_action, get_or_create_conversation,
    increment_challenge_attempts, insert_audit_event, mark_message_deleted, record_message,
    save_conversation, upsert_business_connection,
};

use super::models::{
    DetectionFacts, InboundOutcome, LifecycleFacts, PreparedAction, PreparedSender,
};

#[derive(Debug, Clone, Copy, Default)]
pub struct LifecycleHandler;

struct ApplyContext<'a> {
    update_id: i64,
    facts: &'a LifecycleFacts,
    key: &'a ConversationKey,
}

impl EventApplier for LifecycleHandler {
    fn apply<'a>(
        &'a self,
        event: &'a PreparedEvent,
        uow: &'a mut UnitOfWork<'_>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), EventError>> + Send + 'a>>
    {
        Box::pin(async move {
            let facts: LifecycleFacts = serde_json::from_value(event.facts.clone())?;
            match &facts.action {
                PreparedAction::ConnectionChanged {
                    owner_user_id,
                    enabled,
                    rights_json,
                } => {
                    let connection_id = facts.connection_id.as_ref().ok_or_else(|| {
                        EventError::Application("connection ID is missing".to_owned())
                    })?;
                    upsert_business_connection(
                        uow,
                        &BusinessConnectionRecord {
                            connection_id: connection_id.clone(),
                            owner_user_id: *owner_user_id,
                            rights_json: rights_json.clone(),
                            enabled: *enabled,
                            updated_at: facts.occurred_at,
                        },
                    )
                    .await?;
                }
                PreparedAction::Inbound {
                    detection,
                    outcome,
                    dry_run_spam,
                } => {
                    self.apply_inbound(
                        event.update_id,
                        &facts,
                        detection,
                        outcome,
                        *dry_run_spam,
                        uow,
                    )
                    .await?;
                }
                PreparedAction::ActiveInbound | PreparedAction::Ignore => {}
                PreparedAction::BlockedInbound => {
                    let key = facts.key()?;
                    let message_id = facts.message_id()?;
                    let mut conversation =
                        get_or_create_conversation(uow, &key, facts.user_id()?, facts.occurred_at)
                            .await?;
                    record_inbound(uow, &facts, &key).await?;
                    enqueue_read(event.update_id, &key, message_id, facts.occurred_at, uow).await?;
                    enqueue_delete(event.update_id, &key, &[message_id], facts.occurred_at, uow)
                        .await?;
                    conversation.updated_at = facts.occurred_at;
                    let version = conversation.state_version;
                    save_conversation(uow, &mut conversation, version).await?;
                }
                PreparedAction::ManualOwner => {
                    self.apply_manual_owner(&facts, uow).await?;
                }
                PreparedAction::BotMessage { sender } => {
                    self.apply_bot_message(&facts, *sender, uow).await?;
                }
                PreparedAction::MessagesDeleted { message_ids } => {
                    self.apply_deletions(&facts, message_ids, uow).await?;
                }
            }
            Ok(())
        })
    }
}

impl LifecycleHandler {
    async fn apply_inbound(
        self,
        update_id: i64,
        facts: &LifecycleFacts,
        detection: &DetectionFacts,
        outcome: &InboundOutcome,
        dry_run_spam: bool,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let key = facts.key()?;
        let mut conversation =
            get_or_create_conversation(uow, &key, facts.user_id()?, facts.occurred_at).await?;
        let state_before = conversation.state;
        record_inbound(uow, facts, &key).await?;
        if dry_run_spam {
            enqueue_action(
                uow,
                update_id,
                &key,
                OutboxActionKind::ProposedDestructiveAction,
                json!({"reason": "spam", "score": detection.score}),
                0,
                facts.occurred_at,
            )
            .await?;
        }
        if detection.error.is_some() {
            enqueue_action(
                uow,
                update_id,
                &key,
                OutboxActionKind::SendOwnerMessage,
                json!({"alert": "detector_failed"}),
                0,
                facts.occurred_at,
            )
            .await?;
        }

        let context = ApplyContext {
            update_id,
            facts,
            key: &key,
        };
        self.apply_inbound_outcome(&context, &mut conversation, outcome, uow)
            .await?;

        if conversation.state != state_before || conversation.updated_at != facts.occurred_at {
            conversation.updated_at = facts.occurred_at;
            let version = conversation.state_version;
            save_conversation(uow, &mut conversation, version).await?;
        }
        insert_detection_audit(
            uow,
            update_id,
            &key,
            state_before,
            conversation.state,
            detection,
            facts.occurred_at,
        )
        .await?;
        Ok(())
    }

    async fn apply_inbound_outcome(
        self,
        context: &ApplyContext<'_>,
        conversation: &mut Conversation,
        outcome: &InboundOutcome,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        match outcome {
            InboundOutcome::StartChallenge { .. } => {
                self.apply_challenge_start(context, conversation, outcome, uow)
                    .await?;
            }
            InboundOutcome::Retain | InboundOutcome::NonNumeric => {}
            InboundOutcome::Correct { challenge_id } => {
                close_challenge(uow, *challenge_id, context.facts.occurred_at).await?;
                set_transition(
                    conversation,
                    TransitionEvent::VerificationSucceeded,
                    context.facts.occurred_at,
                )?;
                enqueue_challenge_edit(
                    uow,
                    context.update_id,
                    context.key,
                    *challenge_id,
                    "success",
                    None,
                    context.facts.occurred_at,
                )
                .await?;
            }
            InboundOutcome::Incorrect { .. } => {
                self.apply_incorrect_answer(context, conversation, outcome, uow)
                    .await?;
            }
            InboundOutcome::Expired { challenge_id } => {
                close_challenge(uow, *challenge_id, context.facts.occurred_at).await?;
                set_transition(
                    conversation,
                    TransitionEvent::ChallengeExpired,
                    context.facts.occurred_at,
                )?;
                enqueue_challenge_edit(
                    uow,
                    context.update_id,
                    context.key,
                    *challenge_id,
                    "expired",
                    None,
                    context.facts.occurred_at,
                )
                .await?;
            }
            InboundOutcome::Spam => {
                self.apply_spam(context, conversation, uow).await?;
            }
        }
        Ok(())
    }

    async fn apply_challenge_start(
        self,
        context: &ApplyContext<'_>,
        conversation: &mut Conversation,
        outcome: &InboundOutcome,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let InboundOutcome::StartChallenge {
            expression,
            answer_hmac,
            created_at,
            expires_at,
            max_attempts,
        } = outcome
        else {
            return Err(EventError::Application(
                "challenge start outcome is required".to_owned(),
            ));
        };
        if *max_attempts != 3 {
            return Err(EventError::Application(
                "challenge max attempts must be three".to_owned(),
            ));
        }
        let challenge = ChallengeRecord::pending(
            context.key.clone(),
            expression,
            answer_hmac,
            *created_at,
            *expires_at,
        );
        let challenge_id = create_challenge(uow, &challenge).await?;
        set_transition(
            conversation,
            TransitionEvent::SafeFirstInbound,
            context.facts.occurred_at,
        )?;
        enqueue_action(
            uow,
            context.update_id,
            context.key,
            OutboxActionKind::SendChallenge,
            json!({
                "challenge_id": challenge_id,
                "expression": expression,
                "expires_at": expires_at,
                "attempts_remaining": max_attempts,
            }),
            0,
            context.facts.occurred_at,
        )
        .await?;
        Ok(())
    }

    async fn apply_incorrect_answer(
        self,
        context: &ApplyContext<'_>,
        conversation: &mut Conversation,
        outcome: &InboundOutcome,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let InboundOutcome::Incorrect {
            challenge_id,
            exhausted,
            block_expires_at,
        } = outcome
        else {
            return Err(EventError::Application(
                "incorrect-answer outcome is required".to_owned(),
            ));
        };
        let attempts = increment_challenge_attempts(uow, *challenge_id).await?;
        if !exhausted {
            return enqueue_challenge_edit(
                uow,
                context.update_id,
                context.key,
                *challenge_id,
                "incorrect",
                Some(3 - attempts),
                context.facts.occurred_at,
            )
            .await;
        }

        close_challenge(uow, *challenge_id, context.facts.occurred_at).await?;
        if let Some(expires_at) = block_expires_at {
            set_transition(
                conversation,
                TransitionEvent::VerificationExhausted,
                context.facts.occurred_at,
            )?;
            conversation.block_expires_at = Some(*expires_at);
            conversation.block_reason = Some("verification_exhausted".to_owned());
            conversation.block_count += 1;
        } else {
            set_transition(
                conversation,
                TransitionEvent::ChallengeExpired,
                context.facts.occurred_at,
            )?;
            enqueue_action(
                uow,
                context.update_id,
                context.key,
                OutboxActionKind::ProposedDestructiveAction,
                json!({"reason": "verification_exhausted", "hours": 24}),
                1,
                context.facts.occurred_at,
            )
            .await?;
        }
        enqueue_challenge_edit(
            uow,
            context.update_id,
            context.key,
            *challenge_id,
            "exhausted",
            Some(0),
            context.facts.occurred_at,
        )
        .await
    }

    async fn apply_spam(
        self,
        context: &ApplyContext<'_>,
        conversation: &mut Conversation,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        set_transition(
            conversation,
            TransitionEvent::SpamDetected,
            context.facts.occurred_at,
        )?;
        conversation.block_reason = Some("spam".to_owned());
        conversation.block_count += 1;
        close_active_challenge(uow, context.key, context.facts.occurred_at).await?;
        enqueue_read(
            context.update_id,
            context.key,
            context.facts.message_id()?,
            context.facts.occurred_at,
            uow,
        )
        .await?;
        let deletion_ids = eligible_deletion_ids(uow, context.key).await?;
        enqueue_delete(
            context.update_id,
            context.key,
            &deletion_ids,
            context.facts.occurred_at,
            uow,
        )
        .await
    }

    async fn apply_manual_owner(
        self,
        facts: &LifecycleFacts,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let key = facts.key()?;
        let mut conversation =
            get_or_create_conversation(uow, &key, facts.user_id()?, facts.occurred_at).await?;
        let message = LedgerMessage::new(
            key.clone(),
            facts.message_id()?,
            MessageDirection::Outbound,
            SenderKind::Owner,
            true,
            facts.occurred_at,
        );
        record_message(uow, &message).await?;
        close_active_challenge(uow, &key, facts.occurred_at).await?;
        if conversation.state != ConversationState::Active {
            let event = if conversation.state == ConversationState::SpamSoftBlocked {
                TransitionEvent::OwnerInitiatedMessage
            } else {
                TransitionEvent::ManualOwnerReply
            };
            set_transition(&mut conversation, event, facts.occurred_at)?;
        }
        conversation.block_expires_at = None;
        conversation.block_reason = None;
        conversation.updated_at = facts.occurred_at;
        let version = conversation.state_version;
        save_conversation(uow, &mut conversation, version).await?;
        Ok(())
    }

    async fn apply_bot_message(
        self,
        facts: &LifecycleFacts,
        sender: PreparedSender,
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let key = facts.key()?;
        get_or_create_conversation(uow, &key, facts.user_id()?, facts.occurred_at).await?;
        let sender_kind = match sender {
            PreparedSender::BusinessBot => SenderKind::BusinessBot,
            PreparedSender::Implicit => SenderKind::Implicit,
        };
        let message = LedgerMessage::new(
            key,
            facts.message_id()?,
            MessageDirection::Outbound,
            sender_kind,
            false,
            facts.occurred_at,
        );
        record_message(uow, &message).await?;
        Ok(())
    }

    async fn apply_deletions(
        self,
        facts: &LifecycleFacts,
        message_ids: &[i64],
        uow: &mut UnitOfWork<'_>,
    ) -> Result<(), EventError> {
        let key = facts.key()?;
        let mut conversation =
            get_or_create_conversation(uow, &key, facts.user_id()?, facts.occurred_at).await?;
        for message_id in message_ids {
            mark_message_deleted(uow, &key, *message_id, facts.occurred_at).await?;
        }
        if conversation.state == ConversationState::Active
            && active_owner_reply_ids(uow, &key).await?.is_empty()
        {
            set_transition(
                &mut conversation,
                TransitionEvent::AllOwnerRepliesDeleted,
                facts.occurred_at,
            )?;
            let version = conversation.state_version;
            save_conversation(uow, &mut conversation, version).await?;
        }
        Ok(())
    }
}

impl LifecycleFacts {
    fn key(&self) -> Result<ConversationKey, EventError> {
        Ok(ConversationKey::new(
            self.connection_id
                .as_deref()
                .ok_or_else(|| EventError::Application("connection ID is missing".to_owned()))?,
            self.chat_id
                .ok_or_else(|| EventError::Application("chat ID is missing".to_owned()))?,
        ))
    }

    fn user_id(&self) -> Result<i64, EventError> {
        self.user_id
            .ok_or_else(|| EventError::Application("user ID is missing".to_owned()))
    }

    fn message_id(&self) -> Result<i64, EventError> {
        self.message_id
            .ok_or_else(|| EventError::Application("message ID is missing".to_owned()))
    }
}

async fn record_inbound(
    uow: &mut UnitOfWork<'_>,
    facts: &LifecycleFacts,
    key: &ConversationKey,
) -> Result<(), EventError> {
    let mut message = LedgerMessage::new(
        key.clone(),
        facts.message_id()?,
        MessageDirection::Inbound,
        SenderKind::External,
        false,
        facts.occurred_at,
    );
    message.media_group_id.clone_from(&facts.media_group_id);
    record_message(uow, &message).await?;
    Ok(())
}

fn set_transition(
    conversation: &mut Conversation,
    event: TransitionEvent,
    now: chrono::DateTime<Utc>,
) -> Result<(), EventError> {
    conversation.state = transition(conversation.state, event)
        .map_err(|error| EventError::Application(error.to_string()))?;
    conversation.updated_at = now;
    Ok(())
}

async fn enqueue_challenge_edit(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    key: &ConversationKey,
    challenge_id: i64,
    status: &str,
    attempts_remaining: Option<i64>,
    now: chrono::DateTime<Utc>,
) -> Result<(), EventError> {
    enqueue_action(
        uow,
        update_id,
        key,
        OutboxActionKind::EditChallenge,
        json!({
            "challenge_id": challenge_id,
            "status": status,
            "attempts_remaining": attempts_remaining,
        }),
        0,
        now,
    )
    .await
}

async fn enqueue_read(
    update_id: i64,
    key: &ConversationKey,
    message_id: i64,
    now: chrono::DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<(), EventError> {
    enqueue_action(
        uow,
        update_id,
        key,
        OutboxActionKind::ReadBusinessMessage,
        json!({"message_id": message_id}),
        0,
        now,
    )
    .await
}

async fn enqueue_delete(
    update_id: i64,
    key: &ConversationKey,
    message_ids: &[i64],
    now: chrono::DateTime<Utc>,
    uow: &mut UnitOfWork<'_>,
) -> Result<(), EventError> {
    for (batch_index, batch) in message_ids.chunks(100).enumerate() {
        enqueue_action(
            uow,
            update_id,
            key,
            OutboxActionKind::DeleteBusinessMessages,
            json!({"message_ids": batch}),
            batch_index,
            now,
        )
        .await?;
    }
    Ok(())
}

async fn enqueue_action(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    key: &ConversationKey,
    kind: OutboxActionKind,
    payload: serde_json::Value,
    batch_index: usize,
    now: chrono::DateTime<Utc>,
) -> Result<(), EventError> {
    let action_name = kind.as_str();
    enqueue_outbox_action(
        uow,
        &NewOutboxAction {
            source_update_id: update_id,
            key: Some(key.clone()),
            kind,
            payload_json: serde_json::to_string(&payload)?,
            idempotency_key: format!(
                "{update_id}:{action_name}:{}:{}:{batch_index}",
                key.connection_id, key.chat_id
            ),
            created_at: now,
        },
    )
    .await?;
    Ok(())
}

async fn insert_detection_audit(
    uow: &mut UnitOfWork<'_>,
    update_id: i64,
    key: &ConversationKey,
    state_before: ConversationState,
    state_after: ConversationState,
    detection: &DetectionFacts,
    occurred_at: chrono::DateTime<Utc>,
) -> Result<(), EventError> {
    insert_audit_event(
        uow,
        &NewAuditEvent {
            source_update_id: update_id,
            key: Some(key.clone()),
            event_kind: "detection".to_owned(),
            state_before: Some(state_before.as_str().to_owned()),
            state_after: Some(state_after.as_str().to_owned()),
            score: Some(detection.score),
            reasons_json: Some(serde_json::to_string(&detection.reasons)?),
            rule_ids_json: Some(serde_json::to_string(&detection.matched_rules)?),
            normalized_hash: Some(detection.normalized_hash.clone()),
            rule_version: Some(detection.detector_version.clone()),
            error_code: detection
                .error
                .as_ref()
                .map(|_| "detector_failed".to_owned()),
            error_message: detection.error.clone(),
            occurred_at,
        },
    )
    .await?;
    Ok(())
}
