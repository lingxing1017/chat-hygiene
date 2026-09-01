use std::collections::HashMap;
use std::env;
use std::fmt;

use secrecy::SecretString;
use thiserror::Error;
use url::Url;

const BOT_TOKEN: &str = "CHATHYGIENE_BOT_TOKEN";
const PUBLIC_WEBHOOK_URL: &str = "CHATHYGIENE_PUBLIC_WEBHOOK_URL";
const DATABASE_URL: &str = "CHATHYGIENE_DATABASE_URL";
const DESTRUCTIVE_MODE: &str = "CHATHYGIENE_DESTRUCTIVE_MODE";

#[derive(Clone)]
pub struct Settings {
    pub bot_token: SecretString,
    pub public_webhook_url: Url,
    pub database_url: String,
    pub destructive_mode: bool,
}

impl fmt::Debug for Settings {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Settings")
            .field("bot_token", &"[REDACTED]")
            .field("public_webhook_url", &"[REDACTED]")
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
    #[error("public webhook URL must be an HTTPS URL with a host and no credentials or fragment")]
    InvalidPublicWebhookUrl,
    #[error("public webhook URL port {0} is unsupported by Telegram; use 443, 80, 88, or 8443")]
    UnsupportedPublicWebhookPort(u16),
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
        let bot_token = required_environment_secret(BOT_TOKEN)?;
        let public_webhook_url =
            parse_public_webhook_url(&required_environment(PUBLIC_WEBHOOK_URL)?)?;
        let database_url = optional_environment(DATABASE_URL)?
            .unwrap_or_else(|| "sqlite://data/chathygiene.db".to_owned());
        let destructive_mode =
            parse_destructive_mode(optional_environment(DESTRUCTIVE_MODE)?.as_deref())?;
        Ok(Self {
            bot_token,
            public_webhook_url,
            database_url,
            destructive_mode,
        })
    }

    /// Parses settings from an environment-style key/value map.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a required value is missing or invalid.
    pub fn from_map(values: &HashMap<String, String>) -> Result<Self, ConfigError> {
        let bot_token = required_secret(values, BOT_TOKEN)?;
        let public_webhook_url = parse_public_webhook_url(
            values
                .get(PUBLIC_WEBHOOK_URL)
                .ok_or(ConfigError::Missing(PUBLIC_WEBHOOK_URL))?,
        )?;
        let database_url = values
            .get(DATABASE_URL)
            .cloned()
            .unwrap_or_else(|| "sqlite://data/chathygiene.db".to_owned());
        let destructive_mode =
            parse_destructive_mode(values.get(DESTRUCTIVE_MODE).map(String::as_str))?;

        Ok(Self {
            bot_token,
            public_webhook_url,
            database_url,
            destructive_mode,
        })
    }
}

fn required_environment(name: &'static str) -> Result<String, ConfigError> {
    let value = optional_environment(name)?.ok_or(ConfigError::Missing(name))?;
    if value.trim().is_empty() {
        return Err(ConfigError::Empty(name));
    }
    Ok(value)
}

fn required_environment_secret(name: &'static str) -> Result<SecretString, ConfigError> {
    required_environment(name).map(SecretString::from)
}

fn optional_environment(name: &'static str) -> Result<Option<String>, ConfigError> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(ConfigError::Empty(name)),
    }
}

fn parse_destructive_mode(value: Option<&str>) -> Result<bool, ConfigError> {
    match value {
        None | Some("false") => Ok(false),
        Some("true") => Ok(true),
        Some(_) => Err(ConfigError::InvalidDestructiveMode),
    }
}

fn parse_public_webhook_url(value: &str) -> Result<Url, ConfigError> {
    let url = Url::parse(value).map_err(|_| ConfigError::InvalidPublicWebhookUrl)?;
    if url.scheme() != "https"
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(ConfigError::InvalidPublicWebhookUrl);
    }
    let port = url
        .port_or_known_default()
        .ok_or(ConfigError::InvalidPublicWebhookUrl)?;
    if !matches!(port, 443 | 80 | 88 | 8443) {
        return Err(ConfigError::UnsupportedPublicWebhookPort(port));
    }
    Ok(url)
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
