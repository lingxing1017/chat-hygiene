use std::collections::HashSet;

use chathygiene::detection::{
    Decision, DetectionContext, MediaKind, MessageContent, MessageEntity, MessageEntityKind,
    RuleDetector, SpamDetector,
};
use serde::Deserialize;

fn content(text: &str) -> MessageContent {
    MessageContent {
        text: Some(text.to_owned()),
        ..MessageContent::default()
    }
}

async fn detect(text: &str) -> chathygiene::detection::DetectionResult {
    RuleDetector::from_defaults()
        .expect("valid default rules")
        .detect(&content(text), &DetectionContext::default())
        .await
        .expect("detect message")
}

#[tokio::test]
async fn scores_each_stable_rule_and_caps_spam() {
    let cases = [
        ("https://example.com", "ordinary_url", 10),
        ("https://t.me/+abcdef", "telegram_invite_link", 45),
        ("contact me for promotion", "solicitation_phrase", 70),
        (
            "wallet 0x1234567890abcdef1234567890abcdef12345678",
            "wallet_or_payment_destination",
            45,
        ),
        (
            "guaranteed investment return",
            "task_investment_airdrop_phrase",
            40,
        ),
        (
            "https://a.example https://b.example https://c.example",
            "more_than_two_links",
            40,
        ),
        ("pr\u{200b}omotion", "spacing_or_mixed_script_evasion", 60),
        ("@one @two @three @four", "excessive_mentions_or_emoji", 10),
    ];

    for (raw, rule, score) in cases {
        let result = detect(raw).await;
        assert_eq!(result.score, score, "unexpected score for {raw:?}");
        assert!(result.matched_rules.iter().any(|matched| matched == rule));
    }

    let mut context = DetectionContext::default();
    context.malicious_domains.insert("bad.example".to_owned());
    let malicious = RuleDetector::from_defaults()
        .unwrap()
        .detect(&content("https://bad.example/path"), &context)
        .await
        .unwrap();
    assert_eq!(malicious.score, 100);
    assert_eq!(malicious.decision, Decision::Spam);
    assert!(
        malicious
            .matched_rules
            .contains(&"malicious_domain_exact".to_owned())
    );

    let repeated = RuleDetector::from_defaults()
        .unwrap()
        .detect(
            &content("DM me for advertising"),
            &DetectionContext {
                prior_distinct_senders_for_hash: 2,
                ..DetectionContext::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(repeated.score, 100);
    assert!(
        repeated
            .matched_rules
            .contains(&"repeated_promo_template".to_owned())
    );
}

#[tokio::test]
async fn normalizes_unicode_and_deduplicates_links() {
    let nfkc = detect("ＰＲＯＭＯＴＩＯＮ").await;
    assert!(
        nfkc.matched_rules
            .contains(&"solicitation_phrase".to_owned())
    );

    let mixed_script = detect("prоmotion").await;
    assert_eq!(mixed_script.score, 20);
    assert!(
        mixed_script
            .matched_rules
            .contains(&"spacing_or_mixed_script_evasion".to_owned())
    );

    let mut message = content("See https://example.com");
    message.entities.push(MessageEntity {
        kind: MessageEntityKind::Url,
        value: "https://example.com".to_owned(),
    });
    let result = RuleDetector::from_defaults()
        .unwrap()
        .detect(&message, &DetectionContext::default())
        .await
        .unwrap();
    assert_eq!(result.score, 10);
    assert_eq!(result.matched_rules, vec!["ordinary_url"]);
    assert_eq!(result.normalized_hash.len(), 64);
}

#[test]
fn applies_exact_decision_boundaries() {
    assert_eq!(Decision::from_score(0), Decision::Allow);
    assert_eq!(Decision::from_score(49), Decision::Allow);
    assert_eq!(Decision::from_score(50), Decision::Suspicious);
    assert_eq!(Decision::from_score(99), Decision::Suspicious);
    assert_eq!(Decision::from_score(100), Decision::Spam);
}

#[test]
fn rejects_malformed_or_incomplete_rules() {
    assert!(RuleDetector::from_json("not json").is_err());
    assert!(RuleDetector::from_json(r#"{"version":"incomplete"}"#).is_err());
}

#[derive(Deserialize)]
struct Fixture {
    name: String,
    text: Option<String>,
    #[serde(default)]
    forwarded: bool,
    media_kind: Option<String>,
    #[serde(default)]
    prior_distinct_senders: u32,
    expected: String,
}

fn media_kind(value: Option<&str>) -> Option<MediaKind> {
    match value {
        None => None,
        Some("photo") => Some(MediaKind::Photo),
        Some("document") => Some(MediaKind::Document),
        Some(other) => panic!("unknown media kind {other}"),
    }
}

#[tokio::test]
async fn fixture_regressions_keep_ham_and_flag_spam_without_fetching() {
    let detector = RuleDetector::from_defaults().unwrap();
    let ham: Vec<Fixture> =
        serde_json::from_str(include_str!("fixtures/detection/ham.json")).unwrap();
    let spam: Vec<Fixture> =
        serde_json::from_str(include_str!("fixtures/detection/spam.json")).unwrap();

    for fixture in ham.into_iter().chain(spam) {
        let message = MessageContent {
            text: fixture.text,
            caption: None,
            entities: Vec::new(),
            media_kind: media_kind(fixture.media_kind.as_deref()),
            document_filename: None,
            forwarded: fixture.forwarded,
        };
        let result = detector
            .detect(
                &message,
                &DetectionContext {
                    malicious_domains: HashSet::new(),
                    prior_distinct_senders_for_hash: fixture.prior_distinct_senders,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            result.decision.as_str(),
            fixture.expected,
            "fixture {} failed",
            fixture.name
        );
    }

    let loopback = detect("http://127.0.0.1:9/this-must-not-be-fetched").await;
    assert_eq!(loopback.decision, Decision::Allow);
}
