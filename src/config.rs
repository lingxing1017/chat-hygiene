use std::collections::HashMap;
use std::env;
use std::fmt;

use secrecy::SecretString;
use thiserror::Error;

const BOT_TOKEN: &str = "CHATHYGIENE_BOT_TOKEN";
const WEBHOOK_SECRET: &str = "CHATHYGIENE_WEBHOOK_SECRET";
const CHALLENGE_HMAC_KEY: &str = "CHATHYGIENE_CHALLENGE_HMAC_KEY";
const OWNER_USER_ID: &str = "CHATHYGIENE_OWNER_USER_ID";
const DATABASE_URL: &str = "CHATHYGIENE_DATABASE_URL";
const DESTRUCTIVE_MODE: &str = "CHATHYGIENE_DESTRUCTIVE_MODE";

#[derive(Clone)]
pub struct Settings {
    pub bot_token: SecretString,
    pub webhook_secret: SecretString,
    pub challenge_hmac_key: SecretString,
    pub owner_user_id: i64,
    pub database_url: String,
    pub destructive_mode: bool,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("bot_token", &"[REDACTED]")
            .field("webhook_secret", &"[REDACTED]")
            .field("challenge_hmac_key", &"[REDACTED]")
            .field("owner_user_id", &self.owner_user_id)
            .field("database_url", &self.database_url)
            .field("destructive_mode", &self.destructive_mode)
            .finish()
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("missing environment variable {0}")]
    Missing(&'static str),
    #[error("environment variable {0} cannot be empty")]
    Empty(&'static str),
    #[error("owner user ID must be a positive integer")]
    InvalidOwnerUserId,
    #[error("destructive mode must be true or false")]
    InvalidDestructiveMode,
}

impl Settings {
    /// Loads settings from the current process environment.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required value is missing or invalid.
    pub fn from_env() -> Result<Self, ConfigError> {
        let values = env::vars().collect::<HashMap<_, _>>();
        Self::from_map(&values)
    }

    /// Parses settings from an environment-style key/value map.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required value is missing or invalid.
    pub fn from_map(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let bot_token = required_secret(values, BOT_TOKEN)?;
        let webhook_secret = required_secret(values, WEBHOOK_SECRET)?;
        let challenge_hmac_key = required_secret(values, CHALLENGE_HMAC_KEY)?;
        let owner_user_id = values
            .get(OWNER_USER_ID)
            .ok_or(ConfigError::Missing(OWNER_USER_ID))?
            .parse::<i64>()
            .ok()
            .filter(|id| *id > 0)
            .ok_or(ConfigError::InvalidOwnerUserId)?;
        let database_url = values
            .get(DATABASE_URL)
            .cloned()
            .unwrap_or_else(|| "sqlite://data/chathygiene.db".to_owned());
        let destructive_mode = match values.get(DESTRUCTIVE_MODE).map(String::as_str) {
            None | Some("false") => false,
            Some("true") => true,
            Some(_) => return Err(ConfigError::InvalidDestructiveMode),
        };

        Ok(Self {
            bot_token,
            webhook_secret,
            challenge_hmac_key,
            owner_user_id,
            database_url,
            destructive_mode,
        })
    }
}

fn required_secret(
    values: &HashMap<String, String>,
    name: &'static str,
) -> Result<SecretString, ConfigError> {
    let value = values.get(name).ok_or(ConfigError::Missing(name))?;
    if value.trim().is_empty() {
        return Err(ConfigError::Empty(name));
    }
    Ok(SecretString::from(value.clone()))
}
