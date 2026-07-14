use std::str::FromStr;

use chathygiene::domain::{ConversationState, InvalidTransition, TransitionEvent, transition};

#[test]
fn applies_every_approved_transition() {
    use ConversationState::{
        Active, New, SpamSoftBlocked, TempSoftBlocked, VerifiedWaitingOwner, VerifyPending,
    };
    use TransitionEvent::{
        AllOwnerRepliesDeleted, ChallengeExpired, ManualOwnerReply, OwnerInitiatedMessage,
        OwnerUnblocked, SafeFirstInbound, SpamDetected, TempBlockExpired, VerificationExhausted,
        VerificationSucceeded,
    };

    let cases = [
        (New, SafeFirstInbound, VerifyPending),
        (New, SpamDetected, SpamSoftBlocked),
        (New, ManualOwnerReply, Active),
        (VerifyPending, VerificationSucceeded, VerifiedWaitingOwner),
        (VerifyPending, ChallengeExpired, New),
        (VerifyPending, VerificationExhausted, TempSoftBlocked),
        (VerifyPending, SpamDetected, SpamSoftBlocked),
        (VerifyPending, ManualOwnerReply, Active),
        (VerifiedWaitingOwner, SpamDetected, SpamSoftBlocked),
        (VerifiedWaitingOwner, ManualOwnerReply, Active),
        (Active, AllOwnerRepliesDeleted, New),
        (TempSoftBlocked, TempBlockExpired, New),
        (TempSoftBlocked, ManualOwnerReply, Active),
        (SpamSoftBlocked, OwnerUnblocked, New),
        (SpamSoftBlocked, OwnerInitiatedMessage, Active),
    ];

    for (from, event, expected) in cases {
        assert_eq!(
            transition(from, event),
            Ok(expected),
            "{from:?} + {event:?}"
        );
    }
}

#[test]
fn active_rejects_detection_and_verification_events() {
    use TransitionEvent::{
        ChallengeExpired, SafeFirstInbound, SpamDetected, VerificationExhausted,
        VerificationSucceeded,
    };

    for event in [
        SafeFirstInbound,
        SpamDetected,
        VerificationSucceeded,
        ChallengeExpired,
        VerificationExhausted,
    ] {
        assert_eq!(
            transition(ConversationState::Active, event),
            Err(InvalidTransition {
                state: ConversationState::Active,
                event,
            })
        );
    }
}

#[test]
fn serializes_states_with_stable_database_names() {
    let json =
        serde_json::to_string(&ConversationState::VerifiedWaitingOwner).expect("serialize state");

    assert_eq!(json, r#""VERIFIED_WAITING_OWNER""#);
    assert_eq!(
        ConversationState::from_str("SPAM_SOFT_BLOCKED"),
        Ok(ConversationState::SpamSoftBlocked)
    );
    assert!(ConversationState::from_str("active").is_err());
}
