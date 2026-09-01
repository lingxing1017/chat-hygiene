use chrono::{DateTime, Utc};
use sqlx::{Row, SqliteConnection, SqlitePool};

use super::{OwnerChatSource, OwnerIdentity, StorageError, UnitOfWork};

pub(crate) enum OwnerLifecycleState {
    Pending,
    Ready(OwnerIdentity),
}

struct OwnerMetadata {
    singleton: i64,
    state: String,
    owner_user_type: String,
    owner_user_id: Option<i64>,
    owner_chat_type: String,
    owner_chat_id: Option<i64>,
    source_type: String,
    source: Option<String>,
    floor_type: String,
    floor: Option<i64>,
    bound_at_type: String,
    bound_at: Option<String>,
}

/// Initializes the Owner singleton after legacy event recovery, or strictly
/// loads the already initialized identity.
///
/// # Errors
///
/// Returns [`StorageError`] when the singleton or legacy connection set is
/// missing, corrupt, ambiguous, or cannot be transitioned atomically.
pub async fn initialize_or_load_owner_identity(
    pool: &SqlitePool,
    initialized_at: DateTime<Utc>,
) -> Result<OwnerIdentity, StorageError> {
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    match load_owner_lifecycle_state(&mut uow).await? {
        OwnerLifecycleState::Ready(identity) => {
            uow.commit().await?;
            Ok(identity)
        }
        OwnerLifecycleState::Pending => {
            let connections: Vec<i64> = sqlx::query_scalar(
                "SELECT owner_user_id FROM business_connection ORDER BY connection_id LIMIT 2",
            )
            .fetch_all(uow.connection())
            .await?;
            let result = match connections.as_slice() {
                [] => {
                    sqlx::query(
                        "UPDATE owner_identity SET state = 'UNCLAIMED'
                         WHERE singleton = 1 AND state = 'PENDING'
                           AND owner_user_id IS NULL AND owner_chat_id IS NULL
                           AND owner_chat_source IS NULL
                           AND connection_floor_established_at IS NULL
                           AND bound_at IS NULL",
                    )
                    .execute(uow.connection())
                    .await?
                }
                [owner_user_id] if *owner_user_id > 0 => {
                    sqlx::query(
                        "UPDATE owner_identity
                         SET state = 'CLAIMED', owner_user_id = ?, owner_chat_id = ?,
                             owner_chat_source = 'LEGACY_FALLBACK', bound_at = ?
                         WHERE singleton = 1 AND state = 'PENDING'
                           AND owner_user_id IS NULL AND owner_chat_id IS NULL
                           AND owner_chat_source IS NULL
                           AND connection_floor_established_at IS NULL
                           AND bound_at IS NULL",
                    )
                    .bind(owner_user_id)
                    .bind(owner_user_id)
                    .bind(initialized_at.to_rfc3339())
                    .execute(uow.connection())
                    .await?
                }
                [_] => {
                    return Err(StorageError::InvalidOwnerIdentity(
                        "legacy connection owner id must be positive",
                    ));
                }
                _ => {
                    return Err(StorageError::InvalidOwnerIdentity(
                        "multiple legacy business connections",
                    ));
                }
            };
            if result.rows_affected() != 1 {
                return Err(StorageError::ConcurrentModification);
            }
            let identity = load_owner_identity(&mut uow).await?;
            uow.commit().await?;
            Ok(identity)
        }
    }
}

/// Loads a strictly initialized Owner identity.
///
/// # Errors
///
/// Returns [`StorageError`] when the singleton is pending, missing, or corrupt.
pub async fn load_owner_identity(uow: &mut UnitOfWork<'_>) -> Result<OwnerIdentity, StorageError> {
    match load_owner_lifecycle_state(uow).await? {
        OwnerLifecycleState::Pending => Err(StorageError::InvalidOwnerIdentity(
            "owner identity is pending initialization",
        )),
        OwnerLifecycleState::Ready(identity) => Ok(identity),
    }
}

pub(crate) async fn load_owner_lifecycle_state(
    uow: &mut UnitOfWork<'_>,
) -> Result<OwnerLifecycleState, StorageError> {
    let metadata = load_metadata(uow.connection()).await?;
    decode_metadata(&metadata)
}

/// Atomically binds an unclaimed installation to one immutable Owner.
///
/// # Errors
///
/// Returns [`StorageError`] for invalid IDs, pending/corrupt state, an already
/// claimed installation, or a concurrent transition.
pub async fn claim_owner(
    uow: &mut UnitOfWork<'_>,
    owner_user_id: i64,
    owner_chat_id: i64,
    connection_floor_established_at: i64,
    bound_at: DateTime<Utc>,
) -> Result<OwnerIdentity, StorageError> {
    if owner_user_id <= 0 || owner_chat_id <= 0 || connection_floor_established_at <= 0 {
        return Err(StorageError::InvalidOwnerIdentity(
            "claim identifiers and connection floor must be positive",
        ));
    }
    match load_owner_lifecycle_state(uow).await? {
        OwnerLifecycleState::Pending => {
            return Err(StorageError::InvalidOwnerIdentity(
                "owner identity is pending initialization",
            ));
        }
        OwnerLifecycleState::Ready(OwnerIdentity::Claimed { .. }) => {
            return Err(StorageError::OwnerAlreadyClaimed);
        }
        OwnerLifecycleState::Ready(OwnerIdentity::Unclaimed) => {}
    }
    let result = sqlx::query(
        "UPDATE owner_identity
         SET state = 'CLAIMED', owner_user_id = ?, owner_chat_id = ?,
             owner_chat_source = 'CLAIM', connection_floor_established_at = ?,
             bound_at = ?
         WHERE singleton = 1 AND state = 'UNCLAIMED'
           AND owner_user_id IS NULL AND owner_chat_id IS NULL
           AND owner_chat_source IS NULL
           AND connection_floor_established_at IS NULL AND bound_at IS NULL",
    )
    .bind(owner_user_id)
    .bind(owner_chat_id)
    .bind(connection_floor_established_at)
    .bind(bound_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    load_owner_identity(uow).await
}

/// Promotes only a legacy fallback delivery chat to authoritative metadata.
///
/// # Errors
///
/// Returns [`StorageError`] when the identity is not claimed, the user differs,
/// the requested source is not authoritative, or storage is corrupt.
pub async fn promote_owner_chat(
    uow: &mut UnitOfWork<'_>,
    owner_user_id: i64,
    owner_chat_id: i64,
    source: OwnerChatSource,
) -> Result<OwnerIdentity, StorageError> {
    if owner_user_id <= 0 || owner_chat_id <= 0 {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner user and chat ids must be positive",
        ));
    }
    if !matches!(
        source,
        OwnerChatSource::BusinessConnection | OwnerChatSource::PrivateMessage
    ) {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner chat promotion source must be authoritative",
        ));
    }
    let identity = load_owner_identity(uow).await?;
    let OwnerIdentity::Claimed {
        owner_user_id: stored_user_id,
        owner_chat_source,
        ..
    } = identity
    else {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner chat promotion requires a claimed owner",
        ));
    };
    if stored_user_id != owner_user_id {
        return Err(StorageError::OwnerChatMismatch);
    }
    if owner_chat_source != OwnerChatSource::LegacyFallback {
        return Ok(identity);
    }
    let result = sqlx::query(
        "UPDATE owner_identity
         SET owner_chat_id = ?, owner_chat_source = ?
         WHERE singleton = 1 AND state = 'CLAIMED' AND owner_user_id = ?
           AND owner_chat_source = 'LEGACY_FALLBACK'",
    )
    .bind(owner_chat_id)
    .bind(source.as_str())
    .bind(owner_user_id)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    load_owner_identity(uow).await
}

/// Advances the claimed Owner's monotonic connection-generation floor.
///
/// # Errors
///
/// Returns [`StorageError`] for non-positive input, non-claimed/corrupt state,
/// or a database failure.
pub async fn advance_owner_connection_floor(
    uow: &mut UnitOfWork<'_>,
    retired_generation_floor: i64,
) -> Result<OwnerIdentity, StorageError> {
    if retired_generation_floor <= 0 {
        return Err(StorageError::InvalidOwnerIdentity(
            "connection floor must be positive",
        ));
    }
    if !matches!(
        load_owner_identity(uow).await?,
        OwnerIdentity::Claimed { .. }
    ) {
        return Err(StorageError::InvalidOwnerIdentity(
            "connection floor requires a claimed owner",
        ));
    }
    let result = sqlx::query(
        "UPDATE owner_identity
         SET connection_floor_established_at = CASE
             WHEN connection_floor_established_at IS NULL
               OR connection_floor_established_at < ? THEN ?
             ELSE connection_floor_established_at END
         WHERE singleton = 1 AND state = 'CLAIMED'",
    )
    .bind(retired_generation_floor)
    .bind(retired_generation_floor)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    load_owner_identity(uow).await
}

impl OwnerChatSource {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LegacyFallback => "LEGACY_FALLBACK",
            Self::Claim => "CLAIM",
            Self::BusinessConnection => "BUSINESS_CONNECTION",
            Self::PrivateMessage => "PRIVATE_MESSAGE",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "LEGACY_FALLBACK" => Ok(Self::LegacyFallback),
            "CLAIM" => Ok(Self::Claim),
            "BUSINESS_CONNECTION" => Ok(Self::BusinessConnection),
            "PRIVATE_MESSAGE" => Ok(Self::PrivateMessage),
            _ => Err(StorageError::InvalidOwnerIdentity(
                "owner chat source is unknown",
            )),
        }
    }
}

async fn load_metadata(connection: &mut SqliteConnection) -> Result<OwnerMetadata, StorageError> {
    let rows = sqlx::query(
        "SELECT
             typeof(singleton) AS singleton_type,
             CASE WHEN typeof(singleton) = 'integer' THEN singleton END AS singleton_value,
             typeof(state) AS state_type,
             CASE WHEN typeof(state) = 'text' THEN state END AS state_value,
             typeof(owner_user_id) AS owner_user_type,
             CASE WHEN typeof(owner_user_id) = 'integer' THEN owner_user_id END AS owner_user_value,
             typeof(owner_chat_id) AS owner_chat_type,
             CASE WHEN typeof(owner_chat_id) = 'integer' THEN owner_chat_id END AS owner_chat_value,
             typeof(owner_chat_source) AS source_type,
             CASE WHEN typeof(owner_chat_source) = 'text' THEN owner_chat_source END AS source_value,
             typeof(connection_floor_established_at) AS floor_type,
             CASE WHEN typeof(connection_floor_established_at) = 'integer'
                  THEN connection_floor_established_at END AS floor_value,
             typeof(bound_at) AS bound_at_type,
             CASE WHEN typeof(bound_at) = 'text' THEN bound_at END AS bound_at_value
         FROM owner_identity ORDER BY singleton",
    )
    .fetch_all(&mut *connection)
    .await?;
    if rows.is_empty() {
        return Err(StorageError::OwnerIdentityMissing);
    }
    if rows.len() != 1 {
        return Err(StorageError::InvalidOwnerIdentity(
            "expected exactly one owner singleton row",
        ));
    }
    let row = &rows[0];
    if row.try_get::<String, _>("singleton_type")? != "integer" {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner singleton id must be an integer",
        ));
    }
    let singleton = row.try_get::<Option<i64>, _>("singleton_value")?.ok_or(
        StorageError::InvalidOwnerIdentity("owner singleton id must be an integer"),
    )?;
    if singleton != 1 {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner singleton row must have id 1",
        ));
    }
    if row.try_get::<String, _>("state_type")? != "text" {
        return Err(StorageError::InvalidOwnerIdentity(
            "owner state must be text",
        ));
    }
    let state = row.try_get::<Option<String>, _>("state_value")?.ok_or(
        StorageError::InvalidOwnerIdentity("owner state must be text"),
    )?;
    Ok(OwnerMetadata {
        singleton,
        state,
        owner_user_type: row.try_get("owner_user_type")?,
        owner_user_id: row.try_get("owner_user_value")?,
        owner_chat_type: row.try_get("owner_chat_type")?,
        owner_chat_id: row.try_get("owner_chat_value")?,
        source_type: row.try_get("source_type")?,
        source: row.try_get("source_value")?,
        floor_type: row.try_get("floor_type")?,
        floor: row.try_get("floor_value")?,
        bound_at_type: row.try_get("bound_at_type")?,
        bound_at: row.try_get("bound_at_value")?,
    })
}

fn decode_metadata(metadata: &OwnerMetadata) -> Result<OwnerLifecycleState, StorageError> {
    debug_assert_eq!(metadata.singleton, 1);
    match metadata.state.as_str() {
        "PENDING" | "UNCLAIMED" => {
            if metadata.owner_user_type != "null"
                || metadata.owner_user_id.is_some()
                || metadata.owner_chat_type != "null"
                || metadata.owner_chat_id.is_some()
                || metadata.source_type != "null"
                || metadata.source.is_some()
                || metadata.floor_type != "null"
                || metadata.floor.is_some()
                || metadata.bound_at_type != "null"
                || metadata.bound_at.is_some()
            {
                return Err(StorageError::InvalidOwnerIdentity(
                    "unbound owner state contains identity data",
                ));
            }
            if metadata.state == "PENDING" {
                Ok(OwnerLifecycleState::Pending)
            } else {
                Ok(OwnerLifecycleState::Ready(OwnerIdentity::Unclaimed))
            }
        }
        "CLAIMED" => {
            let owner_user_id = positive_integer(
                &metadata.owner_user_type,
                metadata.owner_user_id,
                "owner user id must be a positive integer",
            )?;
            let owner_chat_id = positive_integer(
                &metadata.owner_chat_type,
                metadata.owner_chat_id,
                "owner chat id must be a positive integer",
            )?;
            if metadata.source_type != "text" {
                return Err(StorageError::InvalidOwnerIdentity(
                    "owner chat source must be text",
                ));
            }
            let owner_chat_source = OwnerChatSource::parse(metadata.source.as_deref().ok_or(
                StorageError::InvalidOwnerIdentity("owner chat source is missing"),
            )?)?;
            let connection_floor_established_at = match metadata.floor_type.as_str() {
                "null" if metadata.floor.is_none() => None,
                "integer" => Some(positive_integer(
                    &metadata.floor_type,
                    metadata.floor,
                    "connection floor must be a positive integer",
                )?),
                _ => {
                    return Err(StorageError::InvalidOwnerIdentity(
                        "connection floor must be a positive integer",
                    ));
                }
            };
            if metadata.bound_at_type != "text" {
                return Err(StorageError::InvalidOwnerIdentity(
                    "owner bound timestamp must be text",
                ));
            }
            let bound_at = metadata
                .bound_at
                .as_deref()
                .ok_or(StorageError::InvalidOwnerIdentity(
                    "owner bound timestamp is missing",
                ))?
                .parse::<DateTime<Utc>>()
                .map_err(|_| {
                    StorageError::InvalidOwnerIdentity("owner bound timestamp must be RFC3339")
                })?;
            Ok(OwnerLifecycleState::Ready(OwnerIdentity::Claimed {
                owner_user_id,
                owner_chat_id,
                owner_chat_source,
                connection_floor_established_at,
                bound_at,
            }))
        }
        _ => Err(StorageError::InvalidOwnerIdentity("owner state is unknown")),
    }
}

fn positive_integer(
    value_type: &str,
    value: Option<i64>,
    error: &'static str,
) -> Result<i64, StorageError> {
    match (value_type, value) {
        ("integer", Some(value)) if value > 0 => Ok(value),
        _ => Err(StorageError::InvalidOwnerIdentity(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};

    async fn test_pool() -> (tempfile::TempDir, SqlitePool) {
        let directory = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", directory.path().join("owner.db").display());
        let pool = connect(&url).await.unwrap();
        migrate(&pool).await.unwrap();
        (directory, pool)
    }

    #[tokio::test]
    async fn lifecycle_decoder_distinguishes_pending_unclaimed_and_claimed() {
        let (_directory, pool) = test_pool().await;
        let mut pending = UnitOfWork::begin(&pool).await.unwrap();
        assert!(matches!(
            load_owner_lifecycle_state(&mut pending).await.unwrap(),
            OwnerLifecycleState::Pending
        ));
        pending.rollback().await.unwrap();

        assert_eq!(
            initialize_or_load_owner_identity(&pool, "2026-08-30T00:00:00Z".parse().unwrap())
                .await
                .unwrap(),
            OwnerIdentity::Unclaimed
        );
        let mut unclaimed = UnitOfWork::begin_immediate(&pool).await.unwrap();
        assert!(matches!(
            load_owner_lifecycle_state(&mut unclaimed).await.unwrap(),
            OwnerLifecycleState::Ready(OwnerIdentity::Unclaimed)
        ));
        claim_owner(
            &mut unclaimed,
            42,
            420,
            1,
            "2026-08-30T00:00:01Z".parse().unwrap(),
        )
        .await
        .unwrap();
        assert!(matches!(
            load_owner_lifecycle_state(&mut unclaimed).await.unwrap(),
            OwnerLifecycleState::Ready(OwnerIdentity::Claimed {
                owner_user_id: 42,
                owner_chat_id: 420,
                ..
            })
        ));
        unclaimed.commit().await.unwrap();
    }
}
