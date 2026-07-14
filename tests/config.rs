use std::collections::HashMap;

use chathygiene::config::{ConfigError, Settings};

fn valid_values() -> HashMap<String, String> {
    HashMap::from([
        ("CHATHYGIENE_BOT_TOKEN".into(), "bot-token".into()),
        ("CHATHYGIENE_WEBHOOK_SECRET".into(), "webhook-secret".into()),
        (
            "CHATHYGIENE_CHALLENGE_HMAC_KEY".into(),
            "challenge-key".into(),
        ),
        ("CHATHYGIENE_OWNER_USER_ID".into(), "42".into()),
    ])
}

#[test]
fn defaults_to_dry_run() {
    let settings = Settings::from_map(&valid_values()).expect("valid settings");

    assert!(!settings.destructive_mode);
    assert_eq!(settings.database_url, "sqlite://data/chathygiene.db");
}

#[test]
fn rejects_empty_secret() {
    let mut values = valid_values();
    values.insert("CHATHYGIENE_WEBHOOK_SECRET".into(), "   ".into());

    let error = Settings::from_map(&values).expect_err("empty secret must fail");

    assert_eq!(error, ConfigError::Empty("CHATHYGIENE_WEBHOOK_SECRET"));
}

#[test]
fn rejects_non_positive_owner() {
    let mut values = valid_values();
    values.insert("CHATHYGIENE_OWNER_USER_ID".into(), "0".into());

    let error = Settings::from_map(&values).expect_err("zero owner id must fail");

    assert_eq!(error, ConfigError::InvalidOwnerUserId);
}
