use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerCommand {
    Help,
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
        "help" if arguments.is_empty() => Ok(OwnerCommand::Help),
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
        "errors" => Ok(OwnerCommand::Errors {
            limit: normalized_error_limit(&arguments)?,
        }),
        "mark_spam" if arguments.is_empty() => Ok(OwnerCommand::MarkSpam),
        "mark_ham" if arguments.is_empty() => Ok(OwnerCommand::MarkHam),
        "help" | "health" | "dry_run" | "mark_spam" | "mark_ham" => {
            Err(OwnerCommandParseError::InvalidArguments)
        }
        _ => Err(OwnerCommandParseError::Unknown),
    }
}

fn normalized_error_limit(arguments: &[&str]) -> Result<u8, OwnerCommandParseError> {
    if arguments.len() > 1 {
        return Err(OwnerCommandParseError::InvalidArguments);
    }
    let Some(value) = arguments.first() else {
        return Ok(10);
    };
    let Ok(value) = value.parse::<i128>() else {
        return Ok(10);
    };
    if value <= 0 {
        return Ok(10);
    }
    Ok(u8::try_from(value.min(20)).unwrap_or(20))
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
