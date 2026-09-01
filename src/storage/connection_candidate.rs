use std::collections::HashSet;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Row;

use super::{
    BusinessConnectionRecord, OwnerIdentity, ReconciliationState, StorageError, UnitOfWork,
    advance_owner_connection_floor, load_business_connection_for_reconciliation,
    load_owner_identity, load_single_trusted_connection, upsert_business_connection,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessConnectionCandidate {
    pub connection_id: String,
    pub business_user_id: i64,
    pub user_chat_id: Option<i64>,
    pub rights_json: String,
    pub enabled: bool,
    pub connection_established_at: i64,
    pub state_revision: i64,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CandidateGuard {
    pub overflow_established_at: Option<i64>,
    pub state_revision: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionReconciliationSnapshot {
    pub candidate_revision: Option<i64>,
    pub trusted_connection_id: Option<String>,
    pub trusted_revision: Option<i64>,
    pub guard_revision: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CandidateWrite {
    Inserted,
    Reconciled,
    RevisionConflict,
    UserConflict,
    GenerationConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrustedConnectionWrite {
    Installed,
    Reconciled,
    Replaced,
    Ambiguous,
    RevisionConflict,
    UserConflict,
    GenerationConflict,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GlobalReconciliationState {
    Pending,
    Ready,
    AuthFailed,
}

impl GlobalReconciliationState {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Ready => "READY",
            Self::AuthFailed => "AUTH_FAILED",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "READY" => Ok(Self::Ready),
            "AUTH_FAILED" => Ok(Self::AuthFailed),
            _ => Err(StorageError::InvalidTelegramReconciliationState(
                "global state is unknown",
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelegramReconciliationState {
    pub state: GlobalReconciliationState,
    pub state_revision: i64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
struct CanonicalRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[allow(clippy::struct_excessive_bools)]
struct TrustedRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

/// Inserts or revision-CAS refreshes one authoritative untrusted snapshot.
///
/// # Errors
///
/// Returns a value-free storage error for invalid input, corrupt state, or a
/// database failure.
pub async fn apply_authoritative_candidate(
    uow: &mut UnitOfWork<'_>,
    candidate: &BusinessConnectionCandidate,
    expected_revision: Option<i64>,
) -> Result<CandidateWrite, StorageError> {
    validate_candidate(candidate)?;
    let rights_json = canonical_rights(&candidate.rights_json)?;
    let existing = load_candidate(uow, &candidate.connection_id).await?;
    let Some(existing) = existing else {
        if expected_revision.is_some() {
            advance_overflow_guard_if_active(uow, candidate).await?;
            return Ok(CandidateWrite::RevisionConflict);
        }
        sqlx::query(
            "INSERT INTO business_connection_candidate
             (connection_id, business_user_id, user_chat_id, rights_json, enabled,
              connection_established_at, state_revision, observed_at)
             VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        )
        .bind(&candidate.connection_id)
        .bind(candidate.business_user_id)
        .bind(candidate.user_chat_id)
        .bind(rights_json)
        .bind(candidate.enabled)
        .bind(candidate.connection_established_at)
        .bind(candidate.observed_at.to_rfc3339())
        .execute(uow.connection())
        .await?;
        advance_overflow_guard_if_active(uow, candidate).await?;
        return Ok(CandidateWrite::Inserted);
    };
    if existing.business_user_id != candidate.business_user_id {
        advance_overflow_guard_if_active(uow, candidate).await?;
        delete_exact_candidate(uow, &existing).await?;
        return Ok(CandidateWrite::UserConflict);
    }
    if existing.connection_established_at != candidate.connection_established_at {
        advance_overflow_guard_if_active(uow, candidate).await?;
        delete_exact_candidate(uow, &existing).await?;
        return Ok(CandidateWrite::GenerationConflict);
    }
    if expected_revision != Some(existing.state_revision) {
        advance_overflow_guard_if_active(uow, candidate).await?;
        return Ok(CandidateWrite::RevisionConflict);
    }
    let result = sqlx::query(
        "UPDATE business_connection_candidate
         SET user_chat_id = ?, rights_json = ?, enabled = ?,
             state_revision = state_revision + 1, observed_at = ?
         WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(candidate.user_chat_id)
    .bind(rights_json)
    .bind(candidate.enabled)
    .bind(candidate.observed_at.to_rfc3339())
    .bind(&candidate.connection_id)
    .bind(existing.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Ok(CandidateWrite::RevisionConflict);
    }
    advance_overflow_guard_if_active(uow, candidate).await?;
    Ok(CandidateWrite::Reconciled)
}

/// Applies one authoritative response to the trusted connection through captured revisions.
///
/// # Errors
///
/// Returns a value-free storage error for invalid/corrupt state, a failed
/// transactional mutation, or a database failure.
#[allow(clippy::too_many_arguments)]
pub async fn reconcile_authoritative_trusted_connection(
    uow: &mut UnitOfWork<'_>,
    authoritative: &BusinessConnectionCandidate,
    expected_trusted_revision: Option<i64>,
    expected_candidate_revision: Option<i64>,
    expected_guard_revision: i64,
) -> Result<TrustedConnectionWrite, StorageError> {
    validate_candidate(authoritative)?;
    let canonical_rights = canonical_rights(&authoritative.rights_json)?;
    let guard = load_candidate_guard(uow).await?;
    if guard.state_revision != expected_guard_revision {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    let retained_candidate = load_candidate(uow, &authoritative.connection_id).await?;
    if retained_candidate
        .as_ref()
        .map(|candidate| candidate.state_revision)
        != expected_candidate_revision
    {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    let owner = load_owner_identity(uow).await?;
    let OwnerIdentity::Claimed {
        owner_user_id,
        owner_chat_id,
        ..
    } = owner
    else {
        return Ok(TrustedConnectionWrite::UserConflict);
    };
    if owner_user_id != authoritative.business_user_id {
        return Ok(TrustedConnectionWrite::UserConflict);
    }
    let trusted = load_single_trusted_connection(uow).await?;
    let Some(trusted) = trusted else {
        if expected_trusted_revision.is_some() {
            return Ok(TrustedConnectionWrite::RevisionConflict);
        }
        let floor = generation_floor_excluding(uow, &authoritative.connection_id).await?;
        if floor.is_some_and(|floor| authoritative.connection_established_at <= floor) {
            return Ok(TrustedConnectionWrite::GenerationConflict);
        }
        install_trusted(
            uow,
            authoritative,
            canonical_rights,
            expected_candidate_revision.map_or(0, |revision| revision + 1),
        )
        .await?;
        delete_connection_candidate(uow, &authoritative.connection_id).await?;
        return Ok(TrustedConnectionWrite::Installed);
    };
    if trusted.connection_id == authoritative.connection_id {
        return reconcile_same_trusted(
            uow,
            &trusted,
            authoritative,
            canonical_rights,
            expected_trusted_revision,
        )
        .await;
    }
    if expected_trusted_revision != Some(trusted.state_revision) {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    if trusted.owner_user_id != authoritative.business_user_id {
        return Ok(TrustedConnectionWrite::UserConflict);
    }
    let Some(trusted_established_at) = trusted.connection_established_at else {
        return Ok(TrustedConnectionWrite::GenerationConflict);
    };
    if authoritative.connection_established_at == trusted_established_at {
        retain_ambiguous_generations(
            uow,
            &trusted,
            authoritative,
            canonical_rights,
            owner_chat_id,
        )
        .await?;
        return Ok(TrustedConnectionWrite::Ambiguous);
    }
    let floor = generation_floor_excluding(uow, &authoritative.connection_id).await?;
    if authoritative.connection_established_at < trusted_established_at
        || floor.is_some_and(|floor| authoritative.connection_established_at <= floor)
    {
        return Ok(TrustedConnectionWrite::GenerationConflict);
    }
    install_trusted(
        uow,
        authoritative,
        canonical_rights,
        expected_candidate_revision.map_or(0, |revision| revision + 1),
    )
    .await?;
    delete_connection_candidate(uow, &authoritative.connection_id).await?;
    Ok(TrustedConnectionWrite::Replaced)
}

/// Retires an exact trusted connection after an authoritative not-found result.
///
/// # Errors
///
/// Returns a value-free storage error for invalid/corrupt Owner or trusted
/// state, revision conflict, or a database failure.
pub async fn retire_trusted_connection_not_found(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
    expected_revision: i64,
) -> Result<TrustedConnectionWrite, StorageError> {
    let Some(trusted) = load_business_connection_for_reconciliation(uow, connection_id).await?
    else {
        delete_connection_candidate(uow, connection_id).await?;
        return Ok(TrustedConnectionWrite::Reconciled);
    };
    if trusted.state_revision != expected_revision {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    let owner = load_owner_identity(uow).await?;
    let OwnerIdentity::Claimed {
        owner_user_id,
        connection_floor_established_at,
        bound_at,
        ..
    } = owner
    else {
        return Ok(TrustedConnectionWrite::UserConflict);
    };
    if owner_user_id != trusted.owner_user_id {
        return Ok(TrustedConnectionWrite::UserConflict);
    }
    let retired_floor = trusted
        .connection_established_at
        .unwrap_or_else(|| bound_at.timestamp().max(1));
    let retired_floor =
        connection_floor_established_at.map_or(retired_floor, |floor| floor.max(retired_floor));
    advance_owner_connection_floor(uow, retired_floor).await?;
    let result = sqlx::query(
        "DELETE FROM business_connection WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(connection_id)
    .bind(expected_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    Ok(TrustedConnectionWrite::Reconciled)
}

/// Loads every retained candidate for one Business user.
///
/// # Errors
///
/// Returns a value-free storage error for an invalid user, corrupt row, or a
/// database failure.
pub async fn candidates_for_user(
    uow: &mut UnitOfWork<'_>,
    business_user_id: i64,
) -> Result<Vec<BusinessConnectionCandidate>, StorageError> {
    if business_user_id <= 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "business user id must be positive",
        ));
    }
    let rows = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                business_user_id, typeof(business_user_id) AS business_user_id_type,
                user_chat_id, typeof(user_chat_id) AS user_chat_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS established_at_type,
                state_revision, typeof(state_revision) AS revision_type,
                observed_at, typeof(observed_at) AS observed_at_type
         FROM business_connection_candidate
         WHERE business_user_id = ?
         ORDER BY connection_established_at, connection_id",
    )
    .bind(business_user_id)
    .fetch_all(uow.connection())
    .await?;
    rows.iter().map(decode_candidate).collect()
}

/// Applies the caller's retention cutoff and deterministic capacity limit.
///
/// # Errors
///
/// Returns a value-free storage error for an invalid limit, corrupt guard, or
/// a database failure.
pub async fn prune_connection_candidates(
    uow: &mut UnitOfWork<'_>,
    cutoff: DateTime<Utc>,
    maximum_rows: i64,
) -> Result<u64, StorageError> {
    if maximum_rows < 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "candidate row limit must be nonnegative",
        ));
    }
    load_candidate_guard(uow).await?;
    let expired = sqlx::query("DELETE FROM business_connection_candidate WHERE observed_at < ?")
        .bind(cutoff.to_rfc3339())
        .execute(uow.connection())
        .await?
        .rows_affected();
    let retained: Vec<(String, i64)> = sqlx::query_as(
        "SELECT connection_id, connection_established_at
         FROM business_connection_candidate
         ORDER BY connection_established_at DESC, connection_id DESC",
    )
    .fetch_all(uow.connection())
    .await?;
    let limit = usize::try_from(maximum_rows).map_err(|_| {
        StorageError::InvalidConnectionCandidate("candidate row limit is too large")
    })?;
    if retained.len() <= limit {
        return Ok(expired);
    }
    let overflow = retained
        .iter()
        .map(|(_, established_at)| *established_at)
        .max()
        .ok_or(StorageError::InvalidConnectionCandidate(
            "candidate overflow set is empty",
        ))?;
    update_overflow_guard(uow, overflow, cutoff).await?;
    let evicted_ids = retained
        .into_iter()
        .skip(limit)
        .map(|(connection_id, _)| connection_id)
        .collect::<Vec<_>>();
    let mut evicted = 0;
    for connection_id in evicted_ids {
        evicted += sqlx::query("DELETE FROM business_connection_candidate WHERE connection_id = ?")
            .bind(connection_id)
            .execute(uow.connection())
            .await?
            .rows_affected();
    }
    Ok(expired + evicted)
}

/// Deletes every untrusted connection candidate.
///
/// # Errors
///
/// Returns a storage error when `SQLite` rejects the deletion.
pub async fn clear_connection_candidates(uow: &mut UnitOfWork<'_>) -> Result<u64, StorageError> {
    Ok(sqlx::query("DELETE FROM business_connection_candidate")
        .execute(uow.connection())
        .await?
        .rows_affected())
}

/// Deletes one untrusted candidate by connection ID.
///
/// # Errors
///
/// Returns a storage error when `SQLite` rejects the deletion.
pub async fn delete_connection_candidate(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
) -> Result<u64, StorageError> {
    Ok(
        sqlx::query("DELETE FROM business_connection_candidate WHERE connection_id = ?")
            .bind(connection_id)
            .execute(uow.connection())
            .await?
            .rows_affected(),
    )
}

/// Deletes candidates whose Business user differs from the supplied user.
///
/// # Errors
///
/// Returns a value-free storage error for an invalid user or database failure.
pub async fn delete_connection_candidates_for_other_users(
    uow: &mut UnitOfWork<'_>,
    business_user_id: i64,
) -> Result<u64, StorageError> {
    if business_user_id <= 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "business user id must be positive",
        ));
    }
    Ok(
        sqlx::query("DELETE FROM business_connection_candidate WHERE business_user_id != ?")
            .bind(business_user_id)
            .execute(uow.connection())
            .await?
            .rows_affected(),
    )
}

/// Retains only the supplied same-user candidate IDs.
///
/// # Errors
///
/// Returns a value-free storage error for invalid/corrupt state or a database
/// failure.
pub async fn retain_connection_candidates(
    uow: &mut UnitOfWork<'_>,
    business_user_id: i64,
    connection_ids: &[String],
) -> Result<u64, StorageError> {
    let retained = connection_ids.iter().collect::<HashSet<_>>();
    let candidates = candidates_for_user(uow, business_user_id).await?;
    let mut deleted = 0;
    for candidate in candidates {
        if !retained.contains(&candidate.connection_id) {
            deleted += delete_connection_candidate(uow, &candidate.connection_id).await?;
        }
    }
    Ok(deleted)
}

/// Strictly loads the singleton candidate-overflow guard.
///
/// # Errors
///
/// Returns a value-free storage error for missing/corrupt state or a database
/// failure.
pub async fn load_candidate_guard(
    uow: &mut UnitOfWork<'_>,
) -> Result<CandidateGuard, StorageError> {
    let rows = sqlx::query(
        "SELECT singleton, typeof(singleton) AS singleton_type,
                overflow_established_at,
                typeof(overflow_established_at) AS overflow_type,
                state_revision, typeof(state_revision) AS revision_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM business_connection_candidate_guard",
    )
    .fetch_all(uow.connection())
    .await?;
    if rows.len() != 1 {
        return Err(StorageError::InvalidCandidateGuard(
            "expected exactly one guard row",
        ));
    }
    let row = &rows[0];
    if row.try_get::<String, _>("singleton_type")? != "integer"
        || row.try_get::<i64, _>("singleton")? != 1
    {
        return Err(StorageError::InvalidCandidateGuard(
            "guard singleton is invalid",
        ));
    }
    let overflow_type = row.try_get::<String, _>("overflow_type")?;
    let overflow = match overflow_type.as_str() {
        "null" => None,
        "integer" => Some(row.try_get::<i64, _>("overflow_established_at")?),
        _ => {
            return Err(StorageError::InvalidCandidateGuard(
                "guard overflow date must be a positive integer or null",
            ));
        }
    };
    if overflow.is_some_and(|value| value <= 0) {
        return Err(StorageError::InvalidCandidateGuard(
            "guard overflow date must be positive",
        ));
    }
    let revision = strict_nonnegative_integer(
        row,
        "revision_type",
        "state_revision",
        StorageError::InvalidCandidateGuard("guard revision must be nonnegative"),
    )?;
    if row.try_get::<String, _>("updated_at_type")? != "text"
        || parse_timestamp(
            &row.try_get::<String, _>("updated_at")?,
            StorageError::InvalidCandidateGuard("guard timestamp must be RFC3339"),
        )
        .is_err()
    {
        return Err(StorageError::InvalidCandidateGuard(
            "guard timestamp must be RFC3339",
        ));
    }
    Ok(CandidateGuard {
        overflow_established_at: overflow,
        state_revision: revision,
    })
}

/// Captures the strict revisions needed around one external authoritative lookup.
///
/// # Errors
///
/// Returns a value-free storage error for corrupt candidate, trusted, or guard
/// state, or for a database failure.
pub async fn connection_reconciliation_snapshot(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
) -> Result<ConnectionReconciliationSnapshot, StorageError> {
    if connection_id.trim().is_empty() {
        return Err(StorageError::InvalidConnectionCandidate(
            "connection id must not be empty",
        ));
    }
    let candidate_revision = load_candidate(uow, connection_id)
        .await?
        .map(|candidate| candidate.state_revision);
    let trusted = load_single_trusted_connection(uow).await?;
    let guard_revision = load_candidate_guard(uow).await?.state_revision;
    Ok(ConnectionReconciliationSnapshot {
        candidate_revision,
        trusted_connection_id: trusted
            .as_ref()
            .map(|connection| connection.connection_id.clone()),
        trusted_revision: trusted.map(|connection| connection.state_revision),
        guard_revision,
    })
}

/// Strictly loads the singleton Telegram reconciliation state.
///
/// # Errors
///
/// Returns a value-free storage error for missing/corrupt state or a database
/// failure.
pub async fn load_telegram_reconciliation_state(
    uow: &mut UnitOfWork<'_>,
) -> Result<TelegramReconciliationState, StorageError> {
    let rows = sqlx::query(
        "SELECT singleton, typeof(singleton) AS singleton_type,
                state, typeof(state) AS state_type,
                state_revision, typeof(state_revision) AS revision_type,
                updated_at, typeof(updated_at) AS updated_at_type
         FROM telegram_reconciliation_state",
    )
    .fetch_all(uow.connection())
    .await?;
    if rows.is_empty() {
        return Err(StorageError::TelegramReconciliationStateMissing);
    }
    if rows.len() != 1 {
        return Err(StorageError::InvalidTelegramReconciliationState(
            "expected exactly one global state row",
        ));
    }
    let row = &rows[0];
    if row.try_get::<String, _>("singleton_type")? != "integer"
        || row.try_get::<i64, _>("singleton")? != 1
        || row.try_get::<String, _>("state_type")? != "text"
    {
        return Err(StorageError::InvalidTelegramReconciliationState(
            "global singleton metadata is invalid",
        ));
    }
    let state = GlobalReconciliationState::parse(&row.try_get::<String, _>("state")?)?;
    let state_revision = strict_nonnegative_integer(
        row,
        "revision_type",
        "state_revision",
        StorageError::InvalidTelegramReconciliationState(
            "global state revision must be nonnegative",
        ),
    )?;
    if row.try_get::<String, _>("updated_at_type")? != "text" {
        return Err(StorageError::InvalidTelegramReconciliationState(
            "global state timestamp must be text",
        ));
    }
    let updated_at = parse_timestamp(
        &row.try_get::<String, _>("updated_at")?,
        StorageError::InvalidTelegramReconciliationState("global state timestamp must be RFC3339"),
    )?;
    Ok(TelegramReconciliationState {
        state,
        state_revision,
        updated_at,
    })
}

/// Idempotently enters the startup `PENDING` gate and gates trusted state.
///
/// # Errors
///
/// Returns a value-free storage error for illegal cardinality, corrupt state,
/// revision conflict, or a database failure.
pub async fn normalize_startup_reconciliation(
    uow: &mut UnitOfWork<'_>,
    now: DateTime<Utc>,
) -> Result<TelegramReconciliationState, StorageError> {
    let global = load_telegram_reconciliation_state(uow).await?;
    load_single_trusted_connection(uow).await.map_err(|_| {
        StorageError::InvalidTelegramReconciliationState("trusted connection state is invalid")
    })?;
    if global.state != GlobalReconciliationState::Pending {
        let result = sqlx::query(
            "UPDATE telegram_reconciliation_state
             SET state = 'PENDING', state_revision = state_revision + 1, updated_at = ?
             WHERE singleton = 1 AND state_revision = ? AND state = ?",
        )
        .bind(now.to_rfc3339())
        .bind(global.state_revision)
        .bind(global.state.as_str())
        .execute(uow.connection())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::ConcurrentModification);
        }
    }
    let trusted = sqlx::query(
        "UPDATE business_connection
         SET reconciliation_state = 'PENDING', state_revision = state_revision + 1,
             updated_at = ?
         WHERE reconciliation_state = 'CONFIRMED'",
    )
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    if trusted.rows_affected() > 1 {
        return Err(StorageError::InvalidTelegramReconciliationState(
            "multiple trusted connections are not supported",
        ));
    }
    load_telegram_reconciliation_state(uow).await
}

/// Persists a Business API authentication failure and disables trusted state.
///
/// # Errors
///
/// Returns a value-free storage error for corrupt state, revision conflict, or
/// a database failure.
pub async fn set_telegram_auth_failed(
    uow: &mut UnitOfWork<'_>,
    now: DateTime<Utc>,
) -> Result<TelegramReconciliationState, StorageError> {
    let global = load_telegram_reconciliation_state(uow).await?;
    load_single_trusted_connection(uow).await.map_err(|_| {
        StorageError::InvalidTelegramReconciliationState("trusted connection state is invalid")
    })?;
    if global.state != GlobalReconciliationState::AuthFailed {
        let result = sqlx::query(
            "UPDATE telegram_reconciliation_state
             SET state = 'AUTH_FAILED', state_revision = state_revision + 1, updated_at = ?
             WHERE singleton = 1 AND state_revision = ?",
        )
        .bind(now.to_rfc3339())
        .bind(global.state_revision)
        .execute(uow.connection())
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::ConcurrentModification);
        }
    }
    sqlx::query(
        "UPDATE business_connection
         SET enabled = 0, reconciliation_state = 'PENDING',
             state_revision = state_revision + 1, updated_at = ?
         WHERE enabled != 0 OR reconciliation_state != 'PENDING'",
    )
    .bind(now.to_rfc3339())
    .execute(uow.connection())
    .await?;
    load_telegram_reconciliation_state(uow).await
}

/// CAS-transitions a fully reconciled global gate from `PENDING` to `READY`.
///
/// # Errors
///
/// Returns a value-free storage error while trust is pending, on revision
/// conflict, for corrupt state, or for a database failure.
pub async fn transition_telegram_reconciliation_ready(
    uow: &mut UnitOfWork<'_>,
    expected_revision: i64,
    now: DateTime<Utc>,
) -> Result<TelegramReconciliationState, StorageError> {
    let global = load_telegram_reconciliation_state(uow).await?;
    if global.state != GlobalReconciliationState::Pending
        || global.state_revision != expected_revision
    {
        return Err(StorageError::ConcurrentModification);
    }
    if load_single_trusted_connection(uow)
        .await
        .map_err(|_| {
            StorageError::InvalidTelegramReconciliationState("trusted connection state is invalid")
        })?
        .is_some_and(|trusted| trusted.reconciliation_state != ReconciliationState::Confirmed)
    {
        return Err(StorageError::InvalidTelegramReconciliationState(
            "trusted connection reconciliation is pending",
        ));
    }
    let result = sqlx::query(
        "UPDATE telegram_reconciliation_state
         SET state = 'READY', state_revision = state_revision + 1, updated_at = ?
         WHERE singleton = 1 AND state = 'PENDING' AND state_revision = ?",
    )
    .bind(now.to_rfc3339())
    .bind(expected_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    load_telegram_reconciliation_state(uow).await
}

async fn install_trusted(
    uow: &mut UnitOfWork<'_>,
    authoritative: &BusinessConnectionCandidate,
    canonical_rights: String,
    state_revision: i64,
) -> Result<(), StorageError> {
    upsert_business_connection(
        uow,
        &BusinessConnectionRecord {
            connection_id: authoritative.connection_id.clone(),
            owner_user_id: authoritative.business_user_id,
            rights_json: canonical_rights,
            enabled: authoritative.enabled,
            connection_established_at: Some(authoritative.connection_established_at),
            state_revision,
            reconciliation_state: ReconciliationState::Confirmed,
            updated_at: authoritative.observed_at,
        },
    )
    .await
}

async fn reconcile_same_trusted(
    uow: &mut UnitOfWork<'_>,
    trusted: &BusinessConnectionRecord,
    authoritative: &BusinessConnectionCandidate,
    canonical_rights: String,
    expected_trusted_revision: Option<i64>,
) -> Result<TrustedConnectionWrite, StorageError> {
    if expected_trusted_revision != Some(trusted.state_revision) {
        apply_safety_intersection(uow, trusted, authoritative).await?;
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    if trusted.owner_user_id != authoritative.business_user_id {
        gate_conflicting_trusted(uow, trusted, authoritative.observed_at).await?;
        return Ok(TrustedConnectionWrite::UserConflict);
    }
    if trusted
        .connection_established_at
        .is_some_and(|date| date != authoritative.connection_established_at)
    {
        gate_conflicting_trusted(uow, trusted, authoritative.observed_at).await?;
        return Ok(TrustedConnectionWrite::GenerationConflict);
    }
    let result = sqlx::query(
        "UPDATE business_connection
         SET owner_user_id = ?, rights_json = ?, enabled = ?,
             connection_established_at = ?, state_revision = state_revision + 1,
             reconciliation_state = 'CONFIRMED', updated_at = ?
         WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(authoritative.business_user_id)
    .bind(canonical_rights)
    .bind(authoritative.enabled)
    .bind(authoritative.connection_established_at)
    .bind(authoritative.observed_at.to_rfc3339())
    .bind(&authoritative.connection_id)
    .bind(trusted.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Ok(TrustedConnectionWrite::RevisionConflict);
    }
    delete_connection_candidate(uow, &authoritative.connection_id).await?;
    Ok(TrustedConnectionWrite::Reconciled)
}

async fn generation_floor_excluding(
    uow: &mut UnitOfWork<'_>,
    excluded_connection_id: &str,
) -> Result<Option<i64>, StorageError> {
    let owner_floor = match load_owner_identity(uow).await? {
        OwnerIdentity::Claimed {
            connection_floor_established_at,
            ..
        } => connection_floor_established_at,
        OwnerIdentity::Unclaimed => None,
    };
    let trusted_floor: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(connection_established_at) FROM business_connection
         WHERE connection_id != ?",
    )
    .bind(excluded_connection_id)
    .fetch_one(uow.connection())
    .await?;
    let candidate_floor: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(connection_established_at) FROM business_connection_candidate
         WHERE connection_id != ?",
    )
    .bind(excluded_connection_id)
    .fetch_one(uow.connection())
    .await?;
    let overflow = load_candidate_guard(uow).await?.overflow_established_at;
    Ok([owner_floor, trusted_floor, candidate_floor, overflow]
        .into_iter()
        .flatten()
        .max())
}

async fn gate_conflicting_trusted(
    uow: &mut UnitOfWork<'_>,
    trusted: &BusinessConnectionRecord,
    observed_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "UPDATE business_connection
         SET enabled = 0, reconciliation_state = 'PENDING',
             state_revision = state_revision + 1, updated_at = ?
         WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(observed_at.to_rfc3339())
    .bind(&trusted.connection_id)
    .bind(trusted.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(())
}

async fn apply_safety_intersection(
    uow: &mut UnitOfWork<'_>,
    trusted: &BusinessConnectionRecord,
    authoritative: &BusinessConnectionCandidate,
) -> Result<(), StorageError> {
    let current = trusted_rights(&trusted.rights_json)?;
    let incoming = trusted_rights(&authoritative.rights_json)?;
    let intersection = TrustedRights {
        can_reply: current.can_reply && incoming.can_reply,
        can_read_messages: current.can_read_messages && incoming.can_read_messages,
        can_delete_sent_messages: current.can_delete_sent_messages
            && incoming.can_delete_sent_messages,
        can_delete_all_messages: current.can_delete_all_messages
            && incoming.can_delete_all_messages,
    };
    let enabled = trusted.enabled && authoritative.enabled;
    if enabled == trusted.enabled && intersection == current {
        return Ok(());
    }
    let rights_json = serde_json::to_string(&intersection)
        .map_err(|_| StorageError::InvalidData("trusted rights cannot be serialized".to_owned()))?;
    let result = sqlx::query(
        "UPDATE business_connection
         SET enabled = ?, rights_json = ?, state_revision = state_revision + 1,
             updated_at = ?
         WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(enabled)
    .bind(rights_json)
    .bind(authoritative.observed_at.to_rfc3339())
    .bind(&trusted.connection_id)
    .bind(trusted.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(())
}

async fn retain_ambiguous_generations(
    uow: &mut UnitOfWork<'_>,
    trusted: &BusinessConnectionRecord,
    authoritative: &BusinessConnectionCandidate,
    authoritative_rights: String,
    owner_chat_id: i64,
) -> Result<(), StorageError> {
    let established_at = authoritative.connection_established_at;
    let trusted_rights = serde_json::to_string(&trusted_rights(&trusted.rights_json)?)
        .map_err(|_| StorageError::InvalidData("trusted rights cannot be serialized".to_owned()))?;
    let result = sqlx::query(
        "DELETE FROM business_connection WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(&trusted.connection_id)
    .bind(trusted.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    sqlx::query(
        "INSERT INTO business_connection_candidate
         (connection_id, business_user_id, user_chat_id, rights_json, enabled,
          connection_established_at, state_revision, observed_at)
         VALUES (?, ?, ?, ?, ?, ?, 0, ?)
         ON CONFLICT(connection_id) DO UPDATE SET
           business_user_id = excluded.business_user_id,
           user_chat_id = excluded.user_chat_id,
           rights_json = excluded.rights_json,
           enabled = excluded.enabled,
           connection_established_at = excluded.connection_established_at,
           state_revision = business_connection_candidate.state_revision + 1,
           observed_at = excluded.observed_at",
    )
    .bind(&trusted.connection_id)
    .bind(trusted.owner_user_id)
    .bind(owner_chat_id)
    .bind(trusted_rights)
    .bind(trusted.enabled)
    .bind(established_at)
    .bind(authoritative.observed_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    sqlx::query(
        "INSERT INTO business_connection_candidate
         (connection_id, business_user_id, user_chat_id, rights_json, enabled,
          connection_established_at, state_revision, observed_at)
         VALUES (?, ?, ?, ?, ?, ?, 0, ?)
         ON CONFLICT(connection_id) DO UPDATE SET
           business_user_id = excluded.business_user_id,
           user_chat_id = excluded.user_chat_id,
           rights_json = excluded.rights_json,
           enabled = excluded.enabled,
           connection_established_at = excluded.connection_established_at,
           state_revision = business_connection_candidate.state_revision + 1,
           observed_at = excluded.observed_at",
    )
    .bind(&authoritative.connection_id)
    .bind(authoritative.business_user_id)
    .bind(authoritative.user_chat_id)
    .bind(authoritative_rights)
    .bind(authoritative.enabled)
    .bind(established_at)
    .bind(authoritative.observed_at.to_rfc3339())
    .execute(uow.connection())
    .await?;
    advance_owner_connection_floor(uow, established_at).await?;
    advance_overflow_guard_if_active(uow, authoritative).await?;
    Ok(())
}

fn trusted_rights(value: &str) -> Result<TrustedRights, StorageError> {
    serde_json::from_str(value)
        .map_err(|_| StorageError::InvalidData("trusted rights snapshot is invalid".to_owned()))
}

async fn load_candidate(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
) -> Result<Option<BusinessConnectionCandidate>, StorageError> {
    let row = sqlx::query(
        "SELECT connection_id, typeof(connection_id) AS connection_id_type,
                business_user_id, typeof(business_user_id) AS business_user_id_type,
                user_chat_id, typeof(user_chat_id) AS user_chat_id_type,
                rights_json, typeof(rights_json) AS rights_json_type,
                enabled, typeof(enabled) AS enabled_type,
                connection_established_at,
                typeof(connection_established_at) AS established_at_type,
                state_revision, typeof(state_revision) AS revision_type,
                observed_at, typeof(observed_at) AS observed_at_type
         FROM business_connection_candidate WHERE connection_id = ?",
    )
    .bind(connection_id)
    .fetch_optional(uow.connection())
    .await?;
    row.as_ref().map(decode_candidate).transpose()
}

fn decode_candidate(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<BusinessConnectionCandidate, StorageError> {
    if row.try_get::<String, _>("connection_id_type")? != "text" {
        return Err(StorageError::InvalidConnectionCandidate(
            "connection id must be text",
        ));
    }
    let connection_id = row.try_get::<String, _>("connection_id")?;
    if connection_id.trim().is_empty() {
        return Err(StorageError::InvalidConnectionCandidate(
            "connection id must not be empty",
        ));
    }
    let business_user_id = strict_positive_integer(
        row,
        "business_user_id_type",
        "business_user_id",
        "business user id must be positive",
    )?;
    let user_chat_id = match row.try_get::<String, _>("user_chat_id_type")?.as_str() {
        "null" => None,
        "integer" => Some(strict_positive_integer(
            row,
            "user_chat_id_type",
            "user_chat_id",
            "user chat id must be positive",
        )?),
        _ => {
            return Err(StorageError::InvalidConnectionCandidate(
                "user chat id must be a positive integer or null",
            ));
        }
    };
    if row.try_get::<String, _>("rights_json_type")? != "text" {
        return Err(StorageError::InvalidConnectionCandidate(
            "rights snapshot must be text",
        ));
    }
    let rights_json = canonical_rights(&row.try_get::<String, _>("rights_json")?)?;
    if row.try_get::<String, _>("enabled_type")? != "integer" {
        return Err(StorageError::InvalidConnectionCandidate(
            "enabled flag must be an integer boolean",
        ));
    }
    let enabled_value = row.try_get::<i64, _>("enabled")?;
    let enabled = match enabled_value {
        0 => false,
        1 => true,
        _ => {
            return Err(StorageError::InvalidConnectionCandidate(
                "enabled flag must be zero or one",
            ));
        }
    };
    let connection_established_at = strict_positive_integer(
        row,
        "established_at_type",
        "connection_established_at",
        "connection establishment date must be positive",
    )?;
    let state_revision = strict_nonnegative_integer(
        row,
        "revision_type",
        "state_revision",
        StorageError::InvalidConnectionCandidate("candidate revision must be nonnegative"),
    )?;
    if row.try_get::<String, _>("observed_at_type")? != "text" {
        return Err(StorageError::InvalidConnectionCandidate(
            "candidate observation timestamp must be text",
        ));
    }
    let observed_at = parse_timestamp(
        &row.try_get::<String, _>("observed_at")?,
        StorageError::InvalidConnectionCandidate("candidate observation timestamp must be RFC3339"),
    )?;
    Ok(BusinessConnectionCandidate {
        connection_id,
        business_user_id,
        user_chat_id,
        rights_json,
        enabled,
        connection_established_at,
        state_revision,
        observed_at,
    })
}

fn validate_candidate(candidate: &BusinessConnectionCandidate) -> Result<(), StorageError> {
    if candidate.connection_id.trim().is_empty() {
        return Err(StorageError::InvalidConnectionCandidate(
            "connection id must not be empty",
        ));
    }
    if candidate.business_user_id <= 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "business user id must be positive",
        ));
    }
    if candidate.user_chat_id.is_some_and(|value| value <= 0) {
        return Err(StorageError::InvalidConnectionCandidate(
            "user chat id must be positive",
        ));
    }
    if candidate.connection_established_at <= 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "connection establishment date must be positive",
        ));
    }
    if candidate.state_revision < 0 {
        return Err(StorageError::InvalidConnectionCandidate(
            "candidate revision must be nonnegative",
        ));
    }
    canonical_rights(&candidate.rights_json)?;
    Ok(())
}

fn canonical_rights(value: &str) -> Result<String, StorageError> {
    let rights: CanonicalRights = serde_json::from_str(value).map_err(|_| {
        StorageError::InvalidConnectionCandidate("rights snapshot must contain four booleans")
    })?;
    serde_json::to_string(&rights).map_err(|_| {
        StorageError::InvalidConnectionCandidate("rights snapshot cannot be serialized")
    })
}

async fn delete_exact_candidate(
    uow: &mut UnitOfWork<'_>,
    candidate: &BusinessConnectionCandidate,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "DELETE FROM business_connection_candidate
         WHERE connection_id = ? AND state_revision = ?",
    )
    .bind(&candidate.connection_id)
    .bind(candidate.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(())
}

async fn advance_overflow_guard_if_active(
    uow: &mut UnitOfWork<'_>,
    candidate: &BusinessConnectionCandidate,
) -> Result<(), StorageError> {
    let guard = load_candidate_guard(uow).await?;
    if guard.overflow_established_at.is_some() {
        update_overflow_guard(
            uow,
            candidate.connection_established_at,
            candidate.observed_at,
        )
        .await?;
    }
    Ok(())
}

async fn update_overflow_guard(
    uow: &mut UnitOfWork<'_>,
    establishment: i64,
    updated_at: DateTime<Utc>,
) -> Result<(), StorageError> {
    let guard = load_candidate_guard(uow).await?;
    let result = sqlx::query(
        "UPDATE business_connection_candidate_guard
         SET overflow_established_at = CASE
               WHEN overflow_established_at IS NULL OR overflow_established_at < ? THEN ?
               ELSE overflow_established_at END,
             state_revision = state_revision + 1,
             updated_at = ?
         WHERE singleton = 1 AND state_revision = ?",
    )
    .bind(establishment)
    .bind(establishment)
    .bind(updated_at.to_rfc3339())
    .bind(guard.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(())
}

fn strict_positive_integer(
    row: &sqlx::sqlite::SqliteRow,
    type_column: &str,
    value_column: &str,
    error: &'static str,
) -> Result<i64, StorageError> {
    if row.try_get::<String, _>(type_column)? != "integer" {
        return Err(StorageError::InvalidConnectionCandidate(error));
    }
    let value = row.try_get::<i64, _>(value_column)?;
    if value <= 0 {
        return Err(StorageError::InvalidConnectionCandidate(error));
    }
    Ok(value)
}

fn strict_nonnegative_integer(
    row: &sqlx::sqlite::SqliteRow,
    type_column: &str,
    value_column: &str,
    error: StorageError,
) -> Result<i64, StorageError> {
    if row.try_get::<String, _>(type_column)? != "integer" {
        return Err(error);
    }
    let value = row.try_get::<i64, _>(value_column)?;
    if value < 0 {
        return Err(error);
    }
    Ok(value)
}

fn parse_timestamp(value: &str, error: StorageError) -> Result<DateTime<Utc>, StorageError> {
    value.parse().map_err(|_| error)
}

/// Gates only the trusted row matching a lifecycle trigger ID.
///
/// # Errors
///
/// Returns a value-free storage error for corrupt state, revision conflict, or
/// a database failure.
pub async fn gate_matching_trusted_for_reconciliation(
    uow: &mut UnitOfWork<'_>,
    connection_id: &str,
    now: DateTime<Utc>,
) -> Result<Option<i64>, StorageError> {
    let Some(current) = load_business_connection_for_reconciliation(uow, connection_id).await?
    else {
        return Ok(None);
    };
    if current.reconciliation_state == ReconciliationState::Pending {
        return Ok(Some(current.state_revision));
    }
    let result = sqlx::query(
        "UPDATE business_connection
         SET reconciliation_state = 'PENDING', state_revision = state_revision + 1,
             updated_at = ?
         WHERE connection_id = ? AND state_revision = ?
           AND reconciliation_state = 'CONFIRMED'",
    )
    .bind(now.to_rfc3339())
    .bind(connection_id)
    .bind(current.state_revision)
    .execute(uow.connection())
    .await?;
    if result.rows_affected() != 1 {
        return Err(StorageError::ConcurrentModification);
    }
    Ok(Some(current.state_revision + 1))
}

/// Computes the monotonic maximum of trusted, Owner, candidate, and overflow generations.
///
/// # Errors
///
/// Returns a value-free storage error for corrupt state or a database failure.
pub async fn owner_generation_floor(uow: &mut UnitOfWork<'_>) -> Result<Option<i64>, StorageError> {
    let owner_floor = match load_owner_identity(uow).await {
        Ok(OwnerIdentity::Claimed {
            connection_floor_established_at,
            ..
        }) => connection_floor_established_at,
        Ok(OwnerIdentity::Unclaimed)
        | Err(StorageError::InvalidOwnerIdentity("owner identity is pending initialization")) => {
            None
        }
        Err(error) => return Err(error),
    };
    let trusted_floor: Option<i64> =
        sqlx::query_scalar("SELECT MAX(connection_established_at) FROM business_connection")
            .fetch_one(uow.connection())
            .await?;
    let candidate_floor: Option<i64> = sqlx::query_scalar(
        "SELECT MAX(connection_established_at) FROM business_connection_candidate",
    )
    .fetch_one(uow.connection())
    .await?;
    let overflow = load_candidate_guard(uow).await?.overflow_established_at;
    Ok([owner_floor, trusted_floor, candidate_floor, overflow]
        .into_iter()
        .flatten()
        .max())
}
