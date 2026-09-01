use std::fmt;

use hkdf::Hkdf;
use secrecy::{SecretSlice, SecretString, zeroize::Zeroize};
use sha2::{Digest, Sha256};

pub const LEGACY_CHALLENGE_HMAC_KEY_VERSION: i64 = 0;
pub const CURRENT_KEY_VERSION: i64 = 1;

const MASTER_SEED_LEN: usize = 32;
const DERIVED_KEY_LEN: usize = 32;
const HKDF_SALT_V1: &[u8] = b"chathygiene.hkdf-sha256.v1";
const WEBHOOK_INFO_PREFIX_V1: &[u8] = b"chathygiene.telegram-webhook-secret.v1\0";
const CHALLENGE_INFO_V1: &[u8] = b"chathygiene.arithmetic-challenge-hmac.v1";
const OWNER_CLAIM_INFO_V1: &[u8] = b"chathygiene.owner-claim-token.v1";
const SEED_CHECKSUM_DOMAIN_V1: &[u8] = b"chathygiene.master-seed-checksum.v1";

pub struct BotIndependentKeys {
    pub key_version: i64,
    pub challenge_hmac_key: SecretSlice<u8>,
    pub owner_claim_token: SecretSlice<u8>,
}

impl fmt::Debug for BotIndependentKeys {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BotIndependentKeys")
            .field("key_version", &self.key_version)
            .field("challenge_hmac_key", &"[REDACTED]")
            .field("owner_claim_token", &"[REDACTED]")
            .finish()
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InstallationKeyError {
    #[error("unsupported installation key version {0}")]
    UnsupportedVersion(i64),
    #[error("master seed must contain exactly 32 bytes")]
    InvalidSeedLength,
    #[error("Telegram bot ID must be positive")]
    InvalidBotId,
    #[error("HKDF output length is invalid")]
    InvalidOutputLength,
}

/// Derives the installation keys whose values do not depend on a Telegram bot.
///
/// # Errors
///
/// Returns an error when the protocol version or master-seed length is invalid,
/// or when HKDF cannot produce the fixed-size outputs.
pub fn derive_bot_independent_keys(
    key_version: i64,
    master_seed: &SecretSlice<u8>,
) -> Result<BotIndependentKeys, InstallationKeyError> {
    validate_version_and_seed(key_version, master_seed)?;

    let hkdf = Hkdf::<Sha256>::new(Some(HKDF_SALT_V1), expose(master_seed));
    let mut challenge = [0_u8; DERIVED_KEY_LEN];
    let mut owner_claim = [0_u8; DERIVED_KEY_LEN];

    if hkdf.expand(CHALLENGE_INFO_V1, &mut challenge).is_err() {
        challenge.zeroize();
        owner_claim.zeroize();
        return Err(InstallationKeyError::InvalidOutputLength);
    }
    if hkdf.expand(OWNER_CLAIM_INFO_V1, &mut owner_claim).is_err() {
        challenge.zeroize();
        owner_claim.zeroize();
        return Err(InstallationKeyError::InvalidOutputLength);
    }

    Ok(BotIndependentKeys {
        key_version,
        challenge_hmac_key: move_into_secret_slice(challenge),
        owner_claim_token: move_into_secret_slice(owner_claim),
    })
}

/// Derives the Telegram-bot-bound webhook authentication secret.
///
/// # Errors
///
/// Returns an error when the protocol version, master-seed length, or bot ID is
/// invalid, or when HKDF cannot produce the fixed-size output.
pub fn derive_webhook_secret(
    key_version: i64,
    master_seed: &SecretSlice<u8>,
    telegram_bot_id: i64,
) -> Result<SecretString, InstallationKeyError> {
    validate_version_and_seed(key_version, master_seed)?;
    let bot_id = u64::try_from(telegram_bot_id).map_err(|_| InstallationKeyError::InvalidBotId)?;
    if bot_id == 0 {
        return Err(InstallationKeyError::InvalidBotId);
    }

    let mut info = Vec::with_capacity(WEBHOOK_INFO_PREFIX_V1.len() + 8);
    info.extend_from_slice(WEBHOOK_INFO_PREFIX_V1);
    info.extend_from_slice(&bot_id.to_be_bytes());

    let hkdf = Hkdf::<Sha256>::new(Some(HKDF_SALT_V1), expose(master_seed));
    let mut raw_secret = [0_u8; DERIVED_KEY_LEN];
    if hkdf.expand(&info, &mut raw_secret).is_err() {
        raw_secret.zeroize();
        return Err(InstallationKeyError::InvalidOutputLength);
    }
    let encoded = hex::encode(raw_secret);
    raw_secret.zeroize();
    Ok(SecretString::from(encoded))
}

/// Computes the non-authenticating checksum used to detect seed corruption.
///
/// # Errors
///
/// Returns an error when the protocol version or master-seed length is invalid.
pub fn master_seed_checksum(
    key_version: i64,
    master_seed: &[u8],
) -> Result<[u8; 32], InstallationKeyError> {
    validate_version(key_version)?;
    if master_seed.len() != MASTER_SEED_LEN {
        return Err(InstallationKeyError::InvalidSeedLength);
    }

    let mut hasher = Sha256::new();
    hasher.update(SEED_CHECKSUM_DOMAIN_V1);
    hasher.update(master_seed);
    Ok(hasher.finalize().into())
}

fn validate_version_and_seed(
    key_version: i64,
    master_seed: &SecretSlice<u8>,
) -> Result<(), InstallationKeyError> {
    validate_version(key_version)?;
    if expose(master_seed).len() != MASTER_SEED_LEN {
        return Err(InstallationKeyError::InvalidSeedLength);
    }
    Ok(())
}

fn validate_version(key_version: i64) -> Result<(), InstallationKeyError> {
    if key_version != CURRENT_KEY_VERSION {
        return Err(InstallationKeyError::UnsupportedVersion(key_version));
    }
    Ok(())
}

fn expose(secret: &SecretSlice<u8>) -> &[u8] {
    secrecy::ExposeSecret::expose_secret(secret)
}

fn move_into_secret_slice<const N: usize>(value: [u8; N]) -> SecretSlice<u8> {
    let boxed: Box<[u8]> = Box::new(value);
    SecretSlice::from(boxed)
}
