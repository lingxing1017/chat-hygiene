mod normalize;
mod rules;

use std::collections::HashSet;

use thiserror::Error;

pub use rules::RuleDetector;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Suspicious,
    Spam,
}

impl Decision {
    #[must_use]
    pub const fn from_score(score: u8) -> Self {
        match score {
            100 => Self::Spam,
            50..=99 => Self::Suspicious,
            _ => Self::Allow,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "ALLOW",
            Self::Suspicious => "SUSPICIOUS",
            Self::Spam => "SPAM",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageEntityKind {
    Url,
    TextLink,
    Mention,
    PhoneNumber,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageEntity {
    pub kind: MessageEntityKind,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    Photo,
    Video,
    Document,
    Voice,
    Sticker,
    Other,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageContent {
    pub text: Option<String>,
    pub caption: Option<String>,
    pub entities: Vec<MessageEntity>,
    pub media_kind: Option<MediaKind>,
    pub document_filename: Option<String>,
    pub forwarded: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DetectionContext {
    pub malicious_domains: HashSet<String>,
    pub prior_distinct_senders_for_hash: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DetectionResult {
    pub decision: Decision,
    pub score: u8,
    pub reasons: Vec<String>,
    pub matched_rules: Vec<String>,
    pub detector_name: String,
    pub detector_version: String,
    pub normalized_hash: String,
}

#[derive(Debug, Error)]
pub enum DetectorError {
    #[error("invalid rule configuration: {0}")]
    InvalidConfig(String),
}

#[allow(async_fn_in_trait)]
pub trait SpamDetector: Send + Sync {
    /// Classifies transient message content without performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`DetectorError`] when the configured rule set is invalid.
    async fn detect(
        &self,
        message: &MessageContent,
        context: &DetectionContext,
    ) -> Result<DetectionResult, DetectorError>;
}
