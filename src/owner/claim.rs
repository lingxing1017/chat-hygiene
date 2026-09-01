use std::fmt;

use secrecy::{ExposeSecretMut, SecretSlice};

const CLAIM_TOKEN_BYTES: usize = 32;
const CLAIM_TOKEN_HEX_LEN: usize = CLAIM_TOKEN_BYTES * 2;

#[derive(Debug, Clone, Copy)]
pub struct OwnerClaimContext<'a> {
    pub ordinary_message: bool,
    pub private_chat: bool,
    pub from_user_id: Option<i64>,
    pub chat_id: i64,
    pub message_id: i64,
    pub message_date: i64,
    pub text: Option<&'a str>,
}

pub enum ParsedOwnerClaim {
    NotClaim,
    Ignore,
    Reject {
        reply_chat_id: i64,
    },
    Candidate {
        from_user_id: i64,
        owner_chat_id: i64,
        message_id: i64,
        message_date: i64,
        token: SecretSlice<u8>,
    },
}

impl fmt::Debug for ParsedOwnerClaim {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotClaim => formatter.write_str("NotClaim"),
            Self::Ignore => formatter.write_str("Ignore"),
            Self::Reject { reply_chat_id } => formatter
                .debug_struct("Reject")
                .field("reply_chat_id", reply_chat_id)
                .finish(),
            Self::Candidate {
                from_user_id,
                owner_chat_id,
                message_id,
                message_date,
                ..
            } => formatter
                .debug_struct("Candidate")
                .field("from_user_id", from_user_id)
                .field("owner_chat_id", owner_chat_id)
                .field("message_id", message_id)
                .field("message_date", message_date)
                .field("token", &"[REDACTED]")
                .finish(),
        }
    }
}

/// Parses a possible one-time Owner claim without retaining encoded token text.
#[must_use]
pub fn parse_owner_claim(context: OwnerClaimContext<'_>) -> ParsedOwnerClaim {
    let Some(text) = context.text else {
        return ParsedOwnerClaim::NotClaim;
    };
    let trimmed = text.trim();
    if !trimmed.starts_with("/claim") {
        return ParsedOwnerClaim::NotClaim;
    }
    let Some(from_user_id) = context.from_user_id.filter(|value| *value > 0) else {
        return ParsedOwnerClaim::Ignore;
    };
    if !context.ordinary_message
        || !context.private_chat
        || context.chat_id <= 0
        || context.message_id <= 0
        || context.message_date <= 0
    {
        return ParsedOwnerClaim::Ignore;
    }
    let reject = || ParsedOwnerClaim::Reject {
        reply_chat_id: context.chat_id,
    };
    if text != trimmed {
        return reject();
    }
    let Some(encoded) = text.strip_prefix("/claim ") else {
        return reject();
    };
    if encoded.len() != CLAIM_TOKEN_HEX_LEN
        || !encoded
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return reject();
    }
    let mut token = SecretSlice::from(vec![0_u8; CLAIM_TOKEN_BYTES]);
    if hex::decode_to_slice(encoded, token.expose_secret_mut()).is_err() {
        return reject();
    }
    ParsedOwnerClaim::Candidate {
        from_user_id,
        owner_chat_id: context.chat_id,
        message_id: context.message_id,
        message_date: context.message_date,
        token,
    }
}
