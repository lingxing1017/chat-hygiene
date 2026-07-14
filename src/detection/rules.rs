use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::normalize::{NormalizedContent, normalize};
use super::{
    Decision, DetectionContext, DetectionResult, DetectorError, MessageContent, SpamDetector,
};

static WALLET_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)(?:\b0x[a-f0-9]{40}\b|\b(?:bc1|[13])[a-z0-9]{25,62}\b|\bT[1-9A-HJ-NP-Za-km-z]{33}\b|paypal\.me/|wallet address|usdt address|钱包地址|收款地址)",
    )
    .expect("static wallet regex must compile")
});

#[derive(Debug, Clone, Deserialize)]
struct RuleConfig {
    version: String,
    weights: RuleWeights,
    solicitation_phrases: Vec<String>,
    task_investment_airdrop_phrases: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RuleWeights {
    malicious_domain_exact: u8,
    repeated_promo_template: u8,
    telegram_invite_link: u8,
    solicitation_phrase: u8,
    wallet_or_payment_destination: u8,
    task_investment_airdrop_phrase: u8,
    more_than_two_links: u8,
    contact_plus_solicitation: u8,
    spacing_or_mixed_script_evasion: u8,
    excessive_mentions_or_emoji: u8,
    ordinary_url: u8,
}

pub struct RuleDetector {
    config: RuleConfig,
    checksum: String,
}

impl RuleDetector {
    /// Loads the embedded, versioned first-release rule configuration.
    ///
    /// # Errors
    ///
    /// Returns [`DetectorError`] when the embedded configuration is malformed.
    pub fn from_defaults() -> Result<Self, DetectorError> {
        Self::from_json(include_str!("defaults.json"))
    }

    /// Loads a versioned rule configuration and calculates its checksum.
    ///
    /// # Errors
    ///
    /// Returns [`DetectorError`] when JSON is malformed or required rules and
    /// phrase groups are missing.
    pub fn from_json(json: &str) -> Result<Self, DetectorError> {
        let config: RuleConfig = serde_json::from_str(json)
            .map_err(|error| DetectorError::InvalidConfig(error.to_string()))?;
        validate_config(&config)?;
        let checksum = hex::encode(Sha256::digest(json.as_bytes()));
        Ok(Self { config, checksum })
    }
}

impl SpamDetector for RuleDetector {
    fn detect<'a>(
        &'a self,
        message: &'a MessageContent,
        context: &'a DetectionContext,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<DetectionResult, DetectorError>> + Send + 'a>,
    > {
        Box::pin(async move {
            let normalized = normalize(message);
            let mut accumulator = Accumulator::default();
            let solicitation = contains_phrase(&normalized.text, &self.config.solicitation_phrases);
            let task_scam = contains_phrase(
                &normalized.text,
                &self.config.task_investment_airdrop_phrases,
            );
            let malicious_link = normalized.links.iter().any(|link| {
                context
                    .malicious_domains
                    .iter()
                    .filter_map(|domain| idna::domain_to_ascii(domain).ok())
                    .any(|domain| domain.eq_ignore_ascii_case(&link.domain))
            });
            let telegram_invite = normalized.links.iter().any(|link| link.telegram_invite);

            if malicious_link {
                accumulator.add(
                    "malicious_domain_exact",
                    self.config.weights.malicious_domain_exact,
                    "message contains a configured malicious domain",
                );
            }
            if context.prior_distinct_senders_for_hash >= 2 && (solicitation || task_scam) {
                accumulator.add(
                    "repeated_promo_template",
                    self.config.weights.repeated_promo_template,
                    "promotional template repeated across distinct senders",
                );
            }
            if telegram_invite {
                accumulator.add(
                    "telegram_invite_link",
                    self.config.weights.telegram_invite_link,
                    "message contains a Telegram invite link",
                );
            }
            if solicitation {
                accumulator.add(
                    "solicitation_phrase",
                    self.config.weights.solicitation_phrase,
                    "message contains solicitation language",
                );
            }
            if WALLET_RE.is_match(&normalized.text) {
                accumulator.add(
                    "wallet_or_payment_destination",
                    self.config.weights.wallet_or_payment_destination,
                    "message contains a wallet or payment destination",
                );
            }
            if task_scam {
                accumulator.add(
                    "task_investment_airdrop_phrase",
                    self.config.weights.task_investment_airdrop_phrase,
                    "message contains task, investment, or airdrop language",
                );
            }
            if normalized.links.len() > 2 {
                accumulator.add(
                    "more_than_two_links",
                    self.config.weights.more_than_two_links,
                    "message contains more than two distinct links",
                );
            }
            if normalized.contact && solicitation {
                accumulator.add(
                    "contact_plus_solicitation",
                    self.config.weights.contact_plus_solicitation,
                    "contact information accompanies solicitation language",
                );
            }
            if normalized.evasion {
                accumulator.add(
                    "spacing_or_mixed_script_evasion",
                    self.config.weights.spacing_or_mixed_script_evasion,
                    "message uses spacing, zero-width, or mixed-script evasion",
                );
            }
            if normalized.mention_count > 3 || normalized.emoji_count >= 8 {
                accumulator.add(
                    "excessive_mentions_or_emoji",
                    self.config.weights.excessive_mentions_or_emoji,
                    "message contains excessive mentions or emoji",
                );
            }
            if normalized
                .links
                .iter()
                .any(|link| !link.telegram_invite && !domain_is_malicious(&link.domain, context))
            {
                accumulator.add(
                    "ordinary_url",
                    self.config.weights.ordinary_url,
                    "message contains an ordinary URL",
                );
            }

            Ok(accumulator.finish(&self.config.version, &self.checksum, &normalized))
        })
    }
}

#[derive(Default)]
struct Accumulator {
    score: u16,
    reasons: Vec<String>,
    matched_rules: Vec<String>,
}

impl Accumulator {
    fn add(&mut self, rule_id: &str, score: u8, reason: &str) {
        self.score = self.score.saturating_add(u16::from(score));
        self.matched_rules.push(rule_id.to_owned());
        self.reasons.push(reason.to_owned());
    }

    fn finish(
        self,
        version: &str,
        checksum: &str,
        normalized: &NormalizedContent,
    ) -> DetectionResult {
        let score = self.score.min(100) as u8;
        DetectionResult {
            decision: Decision::from_score(score),
            score,
            reasons: self.reasons,
            matched_rules: self.matched_rules,
            detector_name: "rule".to_owned(),
            detector_version: format!("{version}:{checksum}"),
            normalized_hash: normalized.normalized_hash.clone(),
        }
    }
}

fn validate_config(config: &RuleConfig) -> Result<(), DetectorError> {
    if config.version.trim().is_empty() {
        return Err(DetectorError::InvalidConfig(
            "version cannot be empty".to_owned(),
        ));
    }
    if config.solicitation_phrases.is_empty() || config.task_investment_airdrop_phrases.is_empty() {
        return Err(DetectorError::InvalidConfig(
            "phrase groups cannot be empty".to_owned(),
        ));
    }
    Ok(())
}

fn contains_phrase(text: &str, phrases: &[String]) -> bool {
    phrases.iter().any(|phrase| text.contains(phrase))
}

fn domain_is_malicious(domain: &str, context: &DetectionContext) -> bool {
    context
        .malicious_domains
        .iter()
        .filter_map(|candidate| idna::domain_to_ascii(candidate).ok())
        .any(|candidate| candidate.eq_ignore_ascii_case(domain))
}
