use std::fmt::Write;

use crate::storage::{OutboxActionKind, OutboxActionRecord};

use super::models::{DetectionFacts, InboundOutcome, LifecycleFacts, PreparedAction};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TraceStatus {
    Applied,
    Queued,
    SkippedDryRun,
    Retained,
    Ignored,
}

impl TraceStatus {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "APPLIED",
            Self::Queued => "QUEUED",
            Self::SkippedDryRun => "SKIPPED_DRY_RUN",
            Self::Retained => "RETAINED",
            Self::Ignored => "IGNORED",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TraceAction {
    name: &'static str,
    status: TraceStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TraceDetection {
    decision: String,
    score: u8,
    reasons: Vec<String>,
    matched_rules: Vec<String>,
    failed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProcessingTrace {
    update_id: i64,
    contact_id: Option<i64>,
    message_id: Option<i64>,
    event: String,
    state_before: Option<String>,
    state_after: Option<String>,
    detection: Option<TraceDetection>,
    verification: String,
    actions: Vec<TraceAction>,
}

impl ProcessingTrace {
    pub(crate) fn from_lifecycle(
        update_id: i64,
        facts: &LifecycleFacts,
        state_after: Option<String>,
        source_actions: &[OutboxActionRecord],
    ) -> Self {
        let (detection, verification) = trace_decisions(facts);
        let mut actions = internal_actions(facts, state_after.as_deref());
        append_outbox_actions(&mut actions, source_actions);
        Self {
            update_id,
            contact_id: facts.chat_id,
            message_id: facts.message_id,
            event: facts.event_kind.clone(),
            state_before: facts.state_before.clone(),
            state_after,
            detection,
            verification,
            actions,
        }
    }

    pub(crate) fn owner_command(
        update_id: i64,
        owner_chat_id: i64,
        message_id: Option<i64>,
        source_actions: &[OutboxActionRecord],
    ) -> Self {
        let mut actions = vec![TraceAction {
            name: "EXECUTE_OWNER_COMMAND",
            status: TraceStatus::Applied,
        }];
        append_outbox_actions(&mut actions, source_actions);
        Self {
            update_id,
            contact_id: Some(owner_chat_id),
            message_id,
            event: "OWNER_COMMAND".to_owned(),
            state_before: None,
            state_after: None,
            detection: None,
            verification: "NOT_APPLICABLE".to_owned(),
            actions,
        }
    }

    pub(crate) fn render(&self) -> String {
        let mut output = String::from("[DRY-RUN TRACE]\n\n");
        writeln!(output, "update_id: {}", self.update_id).expect("write trace update ID");
        writeln!(output, "contact: {}", optional_number(self.contact_id))
            .expect("write trace contact ID");
        writeln!(output, "message_id: {}", optional_number(self.message_id))
            .expect("write trace message ID");
        writeln!(output, "event: {}", self.event).expect("write trace event");
        writeln!(
            output,
            "state: {} -> {}",
            self.state_before.as_deref().unwrap_or("none"),
            self.state_after.as_deref().unwrap_or("none")
        )
        .expect("write trace state");
        output.push_str("\ndetection:\n");
        if let Some(detection) = &self.detection {
            writeln!(output, "- decision: {}", detection.decision).expect("write trace decision");
            writeln!(output, "- score: {}", detection.score).expect("write trace score");
            writeln!(output, "- reasons: {}", joined_or_none(&detection.reasons))
                .expect("write trace reasons");
            writeln!(
                output,
                "- rules: {}",
                joined_or_none(&detection.matched_rules)
            )
            .expect("write trace rules");
            writeln!(output, "- failed: {}", detection.failed).expect("write trace failure");
        } else {
            output.push_str(
                "- decision: NOT_APPLICABLE\n- score: none\n- reasons: none\n- rules: none\n- failed: false\n",
            );
        }
        output.push_str("\nverification:\n");
        writeln!(output, "- result: {}", self.verification).expect("write trace verification");
        output.push_str("\nactions:\n");
        for action in &self.actions {
            writeln!(output, "- {}: {}", action.name, action.status.as_str())
                .expect("write trace action");
        }
        output
    }
}

fn trace_decisions(facts: &LifecycleFacts) -> (Option<TraceDetection>, String) {
    let PreparedAction::Inbound {
        detection,
        outcome,
        dry_run_spam,
    } = &facts.action
    else {
        return (None, "NOT_APPLICABLE".to_owned());
    };
    let detection = Some(TraceDetection::from(detection.as_ref()));
    let verification = if *dry_run_spam && facts.state_before.as_deref() == Some("VERIFY_PENDING") {
        "NOT_EVALUATED_SPAM_FIRST".to_owned()
    } else {
        match outcome {
            InboundOutcome::StartChallenge { max_attempts, .. } => {
                format!("CHALLENGE_STARTED attempts_remaining={max_attempts}")
            }
            InboundOutcome::Retain if facts.event_kind == "EDITED_INBOUND_MESSAGE" => {
                "NOT_EVALUATED_EDIT".to_owned()
            }
            InboundOutcome::Retain => "RETAINED".to_owned(),
            InboundOutcome::Correct { .. } => "CORRECT attempts_remaining=0".to_owned(),
            InboundOutcome::Incorrect {
                attempts_remaining, ..
            } => format!("INCORRECT attempts_remaining={attempts_remaining}"),
            InboundOutcome::Expired { .. } => "EXPIRED".to_owned(),
            InboundOutcome::Spam => "NOT_EVALUATED_SPAM_FIRST".to_owned(),
        }
    };
    (detection, verification)
}

fn internal_actions(facts: &LifecycleFacts, state_after: Option<&str>) -> Vec<TraceAction> {
    let mut actions = Vec::new();
    match &facts.action {
        PreparedAction::ConnectionChanged { .. } => {
            push_action(
                &mut actions,
                "UPSERT_BUSINESS_CONNECTION",
                TraceStatus::Applied,
            );
        }
        PreparedAction::Inbound {
            outcome,
            dry_run_spam,
            ..
        } => {
            push_action(&mut actions, "RECORD_MESSAGE", TraceStatus::Applied);
            if *dry_run_spam {
                push_action(&mut actions, "DELETE_MESSAGE", TraceStatus::SkippedDryRun);
                push_action(&mut actions, "SPAM_BLOCK", TraceStatus::SkippedDryRun);
                if facts.state_before.as_deref() == Some("VERIFY_PENDING") {
                    push_action(&mut actions, "KEEP_CHALLENGE_OPEN", TraceStatus::Applied);
                }
            }
            append_inbound_actions(
                &mut actions,
                outcome,
                *dry_run_spam,
                facts.state_before.as_deref(),
                state_after,
            );
        }
        PreparedAction::ActiveInbound => {
            push_action(&mut actions, "ACTIVE_BYPASS", TraceStatus::Ignored);
        }
        PreparedAction::BlockedInbound => {
            push_action(&mut actions, "RECORD_MESSAGE", TraceStatus::Applied);
        }
        PreparedAction::BlockedFailOpen => {
            push_action(&mut actions, "RECORD_MESSAGE", TraceStatus::Applied);
            push_action(
                &mut actions,
                "BLOCK_ENFORCEMENT",
                TraceStatus::SkippedDryRun,
            );
        }
        PreparedAction::ManualOwner => {
            push_action(&mut actions, "RECORD_OWNER_MESSAGE", TraceStatus::Applied);
            push_action(&mut actions, "CLOSE_CHALLENGE", TraceStatus::Applied);
            append_state_change(&mut actions, facts.state_before.as_deref(), state_after);
        }
        PreparedAction::BotMessage { .. } => {
            push_action(&mut actions, "RECORD_BOT_MESSAGE", TraceStatus::Applied);
        }
        PreparedAction::MessagesDeleted { .. } => {
            push_action(&mut actions, "MARK_MESSAGES_DELETED", TraceStatus::Applied);
            append_state_change(&mut actions, facts.state_before.as_deref(), state_after);
        }
        PreparedAction::Ignore => {
            push_action(&mut actions, "IGNORE_EVENT", TraceStatus::Ignored);
        }
    }
    actions
}

fn append_inbound_actions(
    actions: &mut Vec<TraceAction>,
    outcome: &InboundOutcome,
    dry_run_spam: bool,
    state_before: Option<&str>,
    state_after: Option<&str>,
) {
    match outcome {
        InboundOutcome::StartChallenge { .. } => {
            push_action(actions, "CREATE_CHALLENGE", TraceStatus::Applied);
            append_state_change(actions, state_before, state_after);
        }
        InboundOutcome::Retain if !dry_run_spam => {
            push_action(actions, "KEEP_MESSAGE", TraceStatus::Retained);
        }
        InboundOutcome::Retain | InboundOutcome::Spam => {}
        InboundOutcome::Correct { .. } | InboundOutcome::Expired { .. } => {
            push_action(actions, "CLOSE_CHALLENGE", TraceStatus::Applied);
            append_state_change(actions, state_before, state_after);
        }
        InboundOutcome::Incorrect {
            exhausted,
            block_expires_at,
            ..
        } => {
            push_action(
                actions,
                "INCREMENT_CHALLENGE_ATTEMPTS",
                TraceStatus::Applied,
            );
            if *exhausted {
                push_action(actions, "CLOSE_CHALLENGE", TraceStatus::Applied);
                append_state_change(actions, state_before, state_after);
                if block_expires_at.is_none() {
                    push_action(actions, "TEMP_SOFT_BLOCK", TraceStatus::SkippedDryRun);
                }
            }
        }
    }
}

fn append_state_change(
    actions: &mut Vec<TraceAction>,
    state_before: Option<&str>,
    state_after: Option<&str>,
) {
    if state_before != state_after {
        push_action(actions, "UPDATE_CONVERSATION_STATE", TraceStatus::Applied);
    }
}

fn append_outbox_actions(actions: &mut Vec<TraceAction>, source: &[OutboxActionRecord]) {
    for action in source {
        if action.kind == OutboxActionKind::ProposedDestructiveAction {
            let reason = serde_json::from_str::<serde_json::Value>(&action.payload_json)
                .ok()
                .and_then(|payload| payload["reason"].as_str().map(str::to_owned));
            match reason.as_deref() {
                Some("spam") => {
                    push_action(actions, "DELETE_MESSAGE", TraceStatus::SkippedDryRun);
                    push_action(actions, "SPAM_BLOCK", TraceStatus::SkippedDryRun);
                }
                Some("verification_exhausted") => {
                    push_action(actions, "TEMP_SOFT_BLOCK", TraceStatus::SkippedDryRun);
                }
                _ => {}
            }
        } else {
            push_action(actions, action.kind.as_str(), TraceStatus::Queued);
        }
    }
}

fn push_action(actions: &mut Vec<TraceAction>, name: &'static str, status: TraceStatus) {
    if !actions
        .iter()
        .any(|action| action.name == name && action.status == status)
    {
        actions.push(TraceAction { name, status });
    }
}

impl From<&DetectionFacts> for TraceDetection {
    fn from(detection: &DetectionFacts) -> Self {
        Self {
            decision: detection.decision.clone(),
            score: detection.score,
            reasons: detection.reasons.clone(),
            matched_rules: detection.matched_rules.clone(),
            failed: detection.error.is_some(),
        }
    }
}

fn optional_number(value: Option<i64>) -> String {
    value.map_or_else(|| "none".to_owned(), |value| value.to_string())
}

fn joined_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(", ")
    }
}
