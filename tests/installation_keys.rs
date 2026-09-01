use chathygiene::installation::{
    CURRENT_KEY_VERSION, InstallationKeyError, derive_bot_independent_keys, derive_webhook_secret,
    master_seed_checksum,
};
use secrecy::{ExposeSecret, SecretSlice};

const WEBHOOK_VECTOR: &str = "c51ff32bf59514741663521d0955917c3bbd9d03e54a8e3accfed44d8715653f";
const CHALLENGE_VECTOR: &str = "bd442488308239145955b5d27edefe6f3b3c12e7996f099def53e114477253cb";
const CLAIM_VECTOR: &str = "3e6c2af1c78a08c3d11bdc8212e30ad987a66d55f5889fa5949baf3f06c3e24f";
const CHECKSUM_VECTOR: &str = "c46db4c212eed09ad508a58d2de410a17bb15a4046318b6cd0e2739fd6716801";

fn vector_seed() -> SecretSlice<u8> {
    SecretSlice::from((0_u8..32).collect::<Vec<_>>())
}

#[test]
fn version_one_matches_the_frozen_vectors() {
    let seed = vector_seed();
    let keys = derive_bot_independent_keys(CURRENT_KEY_VERSION, &seed).unwrap();
    let webhook_secret = derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 123_456_789).unwrap();
    assert!(
        webhook_secret.expose_secret() == WEBHOOK_VECTOR,
        "webhook derivation no longer matches the frozen vector"
    );
    assert!(
        hex::encode(keys.challenge_hmac_key.expose_secret()) == CHALLENGE_VECTOR,
        "challenge derivation no longer matches the frozen vector"
    );
    assert!(
        hex::encode(keys.owner_claim_token.expose_secret()) == CLAIM_VECTOR,
        "owner claim derivation no longer matches the frozen vector"
    );
    assert!(
        hex::encode(master_seed_checksum(CURRENT_KEY_VERSION, seed.expose_secret()).unwrap())
            == CHECKSUM_VECTOR,
        "seed checksum no longer matches the frozen vector"
    );
}

#[test]
fn derivation_is_stable_and_domain_separated() {
    let seed = vector_seed();
    let first = derive_bot_independent_keys(CURRENT_KEY_VERSION, &seed).unwrap();
    let second = derive_bot_independent_keys(CURRENT_KEY_VERSION, &seed).unwrap();
    let first_webhook = derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 123_456_789).unwrap();
    let second_webhook = derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 987_654_321).unwrap();
    let first_webhook_bytes = hex::decode(first_webhook.expose_secret()).unwrap();

    assert_ne!(
        first_webhook_bytes,
        first.challenge_hmac_key.expose_secret()
    );
    assert_ne!(first_webhook_bytes, first.owner_claim_token.expose_secret());
    assert_ne!(
        first.challenge_hmac_key.expose_secret(),
        first.owner_claim_token.expose_secret()
    );
    assert_ne!(
        first_webhook.expose_secret(),
        second_webhook.expose_secret()
    );
    assert_eq!(
        first.challenge_hmac_key.expose_secret(),
        second.challenge_hmac_key.expose_secret()
    );
    assert_eq!(
        first.owner_claim_token.expose_secret(),
        second.owner_claim_token.expose_secret()
    );
    assert_eq!(
        first_webhook.expose_secret(),
        derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 123_456_789)
            .unwrap()
            .expose_secret()
    );
}

#[test]
fn derivation_rejects_wrong_seed_lengths_and_unknown_versions() {
    let short = SecretSlice::from(vec![7_u8; 31]);
    assert_eq!(
        derive_bot_independent_keys(CURRENT_KEY_VERSION, &short).unwrap_err(),
        InstallationKeyError::InvalidSeedLength
    );
    assert_eq!(
        derive_webhook_secret(CURRENT_KEY_VERSION, &short, 1).unwrap_err(),
        InstallationKeyError::InvalidSeedLength
    );
    let seed = SecretSlice::from(vec![7_u8; 32]);
    assert_eq!(
        derive_bot_independent_keys(2, &seed).unwrap_err(),
        InstallationKeyError::UnsupportedVersion(2)
    );
    assert_eq!(
        derive_webhook_secret(2, &seed, 1).unwrap_err(),
        InstallationKeyError::UnsupportedVersion(2)
    );
    assert_eq!(
        derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 0).unwrap_err(),
        InstallationKeyError::InvalidBotId
    );
    assert_eq!(
        derive_webhook_secret(CURRENT_KEY_VERSION, &seed, -1).unwrap_err(),
        InstallationKeyError::InvalidBotId
    );
}

#[test]
fn debug_and_errors_do_not_disclose_derived_values() {
    let seed = vector_seed();
    let keys = derive_bot_independent_keys(CURRENT_KEY_VERSION, &seed).unwrap();
    let webhook = derive_webhook_secret(CURRENT_KEY_VERSION, &seed, 123_456_789).unwrap();
    let rendered = format!(
        "{keys:?} {webhook:?} {}",
        InstallationKeyError::InvalidBotId
    );

    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains(WEBHOOK_VECTOR));
    assert!(!rendered.contains(CHALLENGE_VECTOR));
    assert!(!rendered.contains(CLAIM_VECTOR));
}
