use rand::RngCore;
use sqlx::SqlitePool;

use crate::installation::{CURRENT_KEY_VERSION, LEGACY_CHALLENGE_HMAC_KEY_VERSION};
use crate::storage::{
    StorageError, UnitOfWork, active_challenges_not_on_version, replace_challenge_hmac,
};

use super::{ArithmeticVerifier, ChallengeExpressionError, ChallengeVerifier};

#[derive(Debug, thiserror::Error)]
pub enum ChallengeKeyUpgradeError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    InvalidExpression(#[from] ChallengeExpressionError),
    #[error("challenge HMAC signer version {0} is not current")]
    WrongSignerVersion(i64),
    #[error("unsupported active challenge HMAC source version {0}")]
    UnknownSourceVersion(i64),
}

/// Re-signs every active legacy challenge in one immediate transaction.
///
/// # Errors
///
/// Returns [`ChallengeKeyUpgradeError`] when the signer or a source version is
/// unsupported, an expression is invalid, or the atomic storage update fails.
pub async fn upgrade_active_challenge_hmacs<R>(
    pool: &SqlitePool,
    verifier: &ArithmeticVerifier<R>,
) -> Result<u64, ChallengeKeyUpgradeError>
where
    R: RngCore + Send,
{
    if verifier.key_version() != CURRENT_KEY_VERSION {
        return Err(ChallengeKeyUpgradeError::WrongSignerVersion(
            verifier.key_version(),
        ));
    }
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    let challenges = active_challenges_not_on_version(&mut uow, CURRENT_KEY_VERSION).await?;
    for challenge in &challenges {
        if challenge.hmac_key_version != LEGACY_CHALLENGE_HMAC_KEY_VERSION {
            return Err(ChallengeKeyUpgradeError::UnknownSourceVersion(
                challenge.hmac_key_version,
            ));
        }
        let answer_hmac = verifier.answer_hmac_for_expression(&challenge.expression)?;
        replace_challenge_hmac(
            &mut uow,
            challenge.id,
            challenge.hmac_key_version,
            CURRENT_KEY_VERSION,
            &answer_hmac,
        )
        .await?;
    }
    let upgraded = u64::try_from(challenges.len())
        .map_err(|_| StorageError::InvalidData("active challenge count exceeds u64".to_owned()))?;
    uow.commit().await?;
    Ok(upgraded)
}
