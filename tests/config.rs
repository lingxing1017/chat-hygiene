use std::collections::HashMap;

use chathygiene::config::{ConfigError, Settings};

fn valid_values() -> HashMap<String, String> {
    HashMap::from([
        ("CHATHYGIENE_BOT_TOKEN".into(), "bot-token".into()),
        (
            "CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(),
            "https://chat.example.net/telegram/webhook".into(),
        ),
    ])
}

#[test]
fn defaults_to_dry_run() {
    let settings = Settings::from_map(&valid_values()).expect("valid settings");

    assert!(!settings.destructive_mode);
    assert_eq!(settings.database_url, "sqlite://data/chathygiene.db");
}

#[test]
fn rejects_empty_bot_token() {
    let mut values = valid_values();
    values.insert("CHATHYGIENE_BOT_TOKEN".into(), "   ".into());

    let error = Settings::from_map(&values).expect_err("empty bot token must fail");

    assert_eq!(error, ConfigError::Empty("CHATHYGIENE_BOT_TOKEN"));
}

#[test]
fn old_installation_variables_are_ignored() {
    let mut values = valid_values();
    values.insert(
        "CHATHYGIENE_WEBHOOK_SECRET".into(),
        "legacy-webhook-sentinel".into(),
    );
    values.insert(
        "CHATHYGIENE_CHALLENGE_HMAC_KEY".into(),
        "legacy-challenge-sentinel".into(),
    );
    values.insert(
        "CHATHYGIENE_OWNER_USER_ID".into(),
        "legacy-owner-sentinel".into(),
    );

    let settings = Settings::from_map(&values).expect("legacy variables are ignored");
    let rendered = format!("{settings:?}");

    for sentinel in [
        "legacy-webhook-sentinel",
        "legacy-challenge-sentinel",
        "legacy-owner-sentinel",
    ] {
        assert!(!rendered.contains(sentinel));
    }
}

#[test]
fn requires_a_structurally_safe_public_webhook_url() {
    let mut missing = valid_values();
    missing.remove("CHATHYGIENE_PUBLIC_WEBHOOK_URL");
    assert_eq!(
        Settings::from_map(&missing).unwrap_err(),
        ConfigError::Missing("CHATHYGIENE_PUBLIC_WEBHOOK_URL")
    );

    for invalid in [
        "",
        "   ",
        "/telegram/webhook",
        "http://chat.example.net/telegram/webhook",
        "https://",
        "https://user@chat.example.net/telegram/webhook",
        "https://user:password@chat.example.net/telegram/webhook",
        "https://chat.example.net/telegram/webhook#fragment",
        "https://chat.example.net:65536/telegram/webhook",
    ] {
        let mut values = valid_values();
        values.insert("CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(), invalid.into());
        assert_eq!(
            Settings::from_map(&values).unwrap_err(),
            ConfigError::InvalidPublicWebhookUrl,
            "accepted invalid public webhook URL shape {invalid:?}"
        );
    }
}

#[test]
fn accepts_only_telegram_webhook_ports() {
    for (value, effective_port) in [
        ("https://chat.example.net/telegram/webhook", 443),
        ("https://chat.example.net:443/telegram/webhook", 443),
        ("https://chat.example.net:80/telegram/webhook", 80),
        ("https://chat.example.net:88/telegram/webhook", 88),
        (
            "https://chat.example.net:8443/telegram/webhook?tenant=one",
            8443,
        ),
        ("https://[2001:db8::1]:8443/telegram/webhook", 8443),
    ] {
        let mut values = valid_values();
        values.insert("CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(), value.into());
        let settings = Settings::from_map(&values).unwrap();
        assert_eq!(
            settings.public_webhook_url.port_or_known_default(),
            Some(effective_port)
        );
        if effective_port == 8443 && value.contains("tenant=one") {
            assert_eq!(settings.public_webhook_url.path(), "/telegram/webhook");
            assert_eq!(settings.public_webhook_url.query(), Some("tenant=one"));
        }
    }

    for unsupported in [8080, 0] {
        let mut values = valid_values();
        values.insert(
            "CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(),
            format!("https://chat.example.net:{unsupported}/telegram/webhook"),
        );
        assert_eq!(
            Settings::from_map(&values).unwrap_err(),
            ConfigError::UnsupportedPublicWebhookPort(unsupported)
        );
    }

    let mut values = valid_values();
    values.insert(
        "CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(),
        "http://chat.example.net:8080/telegram/webhook".into(),
    );
    assert_eq!(
        Settings::from_map(&values).unwrap_err(),
        ConfigError::InvalidPublicWebhookUrl
    );
}

#[test]
fn redacts_the_complete_public_webhook_url() {
    let sentinel_url =
        "https://chat.example.net:8443/sentinel-user/sentinel-password?token=sentinel-query";
    let mut values = valid_values();
    values.insert("CHATHYGIENE_PUBLIC_WEBHOOK_URL".into(), sentinel_url.into());

    let settings = Settings::from_map(&values).unwrap();
    let rendered = format!("{settings:?}");

    assert_eq!(settings.public_webhook_url.as_str(), sentinel_url);
    for sentinel in [
        sentinel_url,
        "sentinel-user",
        "sentinel-password",
        "sentinel-query",
        "bot-token",
    ] {
        assert!(!rendered.contains(sentinel));
    }
    assert!(rendered.contains("public_webhook_url: \"[REDACTED]\""));
}
