use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerCommand {
    Health,
    Inspect { chat_id: i64 },
    Reset { chat_id: i64 },
    Unblock { chat_id: i64 },
    DryRun { enabled: bool },
    Errors { limit: u8 },
    MarkSpam,
    MarkHam,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OwnerCommandParseError {
    #[error("command must start with /")]
    MissingPrefix,
    #[error("unknown owner command")]
    Unknown,
    #[error("invalid command arguments")]
    InvalidArguments,
    #[error("invalid bot username suffix")]
    InvalidSuffix,
}

/// Parses one exact owner command, accepting Telegram's optional bot suffix.
///
/// # Errors
///
/// Returns [`OwnerCommandParseError`] for unknown commands or invalid arity,
/// IDs, limits, dry-run values, and suffixes.
pub fn parse_owner_command(input: &str) -> Result<OwnerCommand, OwnerCommandParseError> {
    let mut parts = input.split_whitespace();
    let token = parts.next().ok_or(OwnerCommandParseError::MissingPrefix)?;
    let token = token
        .strip_prefix('/')
        .ok_or(OwnerCommandParseError::MissingPrefix)?;
    let (name, suffix) = token
        .split_once('@')
        .map_or((token, None), |(name, suffix)| (name, Some(suffix)));
    if suffix.is_some_and(|suffix| {
        suffix.is_empty()
            || !suffix
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
    }) {
        return Err(OwnerCommandParseError::InvalidSuffix);
    }
    let arguments = parts.collect::<Vec<_>>();
    match name.to_ascii_lowercase().as_str() {
        "health" if arguments.is_empty() => Ok(OwnerCommand::Health),
        "inspect" => Ok(OwnerCommand::Inspect {
            chat_id: one_positive_id(&arguments)?,
        }),
        "reset" => Ok(OwnerCommand::Reset {
            chat_id: one_positive_id(&arguments)?,
        }),
        "unblock" => Ok(OwnerCommand::Unblock {
            chat_id: one_positive_id(&arguments)?,
        }),
        "dry_run" if arguments.len() == 1 => match arguments[0] {
            "on" => Ok(OwnerCommand::DryRun { enabled: true }),
            "off" => Ok(OwnerCommand::DryRun { enabled: false }),
            _ => Err(OwnerCommandParseError::InvalidArguments),
        },
        "errors" if arguments.len() <= 1 => {
            let limit = arguments.first().map_or(Ok(10), |value| {
                value
                    .parse::<u8>()
                    .ok()
                    .filter(|limit| (1..=20).contains(limit))
                    .ok_or(OwnerCommandParseError::InvalidArguments)
            })?;
            Ok(OwnerCommand::Errors { limit })
        }
        "mark_spam" if arguments.is_empty() => Ok(OwnerCommand::MarkSpam),
        "mark_ham" if arguments.is_empty() => Ok(OwnerCommand::MarkHam),
        "health" | "dry_run" | "errors" | "mark_spam" | "mark_ham" => {
            Err(OwnerCommandParseError::InvalidArguments)
        }
        _ => Err(OwnerCommandParseError::Unknown),
    }
}

fn one_positive_id(arguments: &[&str]) -> Result<i64, OwnerCommandParseError> {
    if arguments.len() != 1 {
        return Err(OwnerCommandParseError::InvalidArguments);
    }
    arguments[0]
        .parse::<i64>()
        .ok()
        .filter(|chat_id| *chat_id > 0)
        .ok_or(OwnerCommandParseError::InvalidArguments)
}
