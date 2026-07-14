use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ConversationState {
    New,
    VerifyPending,
    VerifiedWaitingOwner,
    Active,
    TempSoftBlocked,
    SpamSoftBlocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransitionEvent {
    SafeFirstInbound,
    SpamDetected,
    VerificationSucceeded,
    ChallengeExpired,
    VerificationExhausted,
    ManualOwnerReply,
    AllOwnerRepliesDeleted,
    TempBlockExpired,
    OwnerUnblocked,
    OwnerInitiatedMessage,
}

#[derive(Debug, Clone, Copy, Error, PartialEq, Eq)]
#[error("event {event:?} is invalid for state {state:?}")]
pub struct InvalidTransition {
    pub state: ConversationState,
    pub event: TransitionEvent,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("unknown conversation state: {0}")]
pub struct InvalidState(String);

/// Applies one permitted conversation-state transition.
///
/// # Errors
///
/// Returns [`InvalidTransition`] when the event is not valid for the current
/// state. In particular, detection and verification events are invalid while
/// the conversation is active.
pub fn transition(
    state: ConversationState,
    event: TransitionEvent,
) -> Result<ConversationState, InvalidTransition> {
    use ConversationState::{
        Active, New, SpamSoftBlocked, TempSoftBlocked, VerifiedWaitingOwner, VerifyPending,
    };
    use TransitionEvent::{
        AllOwnerRepliesDeleted, ChallengeExpired, ManualOwnerReply, OwnerInitiatedMessage,
        OwnerUnblocked, SafeFirstInbound, SpamDetected, TempBlockExpired, VerificationExhausted,
        VerificationSucceeded,
    };

    match (state, event) {
        (New, SafeFirstInbound) => Ok(VerifyPending),
        (New | VerifyPending | VerifiedWaitingOwner, SpamDetected) => Ok(SpamSoftBlocked),
        (New | VerifyPending | VerifiedWaitingOwner | TempSoftBlocked, ManualOwnerReply) => {
            Ok(Active)
        }
        (VerifyPending, VerificationSucceeded) => Ok(VerifiedWaitingOwner),
        (VerifyPending, ChallengeExpired)
        | (Active, AllOwnerRepliesDeleted)
        | (TempSoftBlocked, TempBlockExpired)
        | (SpamSoftBlocked, OwnerUnblocked) => Ok(New),
        (VerifyPending, VerificationExhausted) => Ok(TempSoftBlocked),
        (SpamSoftBlocked, OwnerInitiatedMessage) => Ok(Active),
        _ => Err(InvalidTransition { state, event }),
    }
}

impl ConversationState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::New => "NEW",
            Self::VerifyPending => "VERIFY_PENDING",
            Self::VerifiedWaitingOwner => "VERIFIED_WAITING_OWNER",
            Self::Active => "ACTIVE",
            Self::TempSoftBlocked => "TEMP_SOFT_BLOCKED",
            Self::SpamSoftBlocked => "SPAM_SOFT_BLOCKED",
        }
    }
}

impl fmt::Display for ConversationState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl FromStr for ConversationState {
    type Err = InvalidState;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "NEW" => Ok(Self::New),
            "VERIFY_PENDING" => Ok(Self::VerifyPending),
            "VERIFIED_WAITING_OWNER" => Ok(Self::VerifiedWaitingOwner),
            "ACTIVE" => Ok(Self::Active),
            "TEMP_SOFT_BLOCKED" => Ok(Self::TempSoftBlocked),
            "SPAM_SOFT_BLOCKED" => Ok(Self::SpamSoftBlocked),
            _ => Err(InvalidState(value.to_owned())),
        }
    }
}
