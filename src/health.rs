use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;

use crate::storage::{
    GlobalReconciliationState, OwnerIdentity, ReconciliationState, StorageError, UnitOfWork,
    list_connection_candidates, load_candidate_guard, load_owner_identity,
    load_single_trusted_connection, load_telegram_reconciliation_state,
};

const CANDIDATE_RETENTION: Duration = Duration::days(7);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OwnerHealth {
    Claimed,
    Unclaimed,
}

impl OwnerHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::Unclaimed => "unclaimed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionHealth {
    Missing,
    Candidate,
    Enabled,
    Disabled,
    RightsIncomplete,
    Ambiguous,
}

impl ConnectionHealth {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Candidate => "candidate",
            Self::Enabled => "enabled",
            Self::Disabled => "disabled",
            Self::RightsIncomplete => "rights_incomplete",
            Self::Ambiguous => "ambiguous",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct HealthSnapshot {
    pub owner: OwnerHealth,
    pub connection: ConnectionHealth,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)]
struct PersistedRights {
    can_reply: bool,
    can_read_messages: bool,
    can_delete_sent_messages: bool,
    can_delete_all_messages: bool,
}

impl PersistedRights {
    const fn complete(&self) -> bool {
        self.can_reply
            && self.can_read_messages
            && self.can_delete_sent_messages
            && self.can_delete_all_messages
    }
}

/// Captures and classifies readiness in one read-only `SQLite` transaction.
///
/// # Errors
///
/// Returns [`StorageError`] when reconciliation or persisted state is not a
/// legal ready snapshot.
pub async fn classify_readiness(
    pool: &SqlitePool,
    now: DateTime<Utc>,
) -> Result<HealthSnapshot, StorageError> {
    let mut uow = UnitOfWork::begin(pool).await?;
    let snapshot = classify_readiness_in(&mut uow, now).await;
    match snapshot {
        Ok(snapshot) => {
            uow.commit().await?;
            Ok(snapshot)
        }
        Err(error) => {
            let _ = uow.rollback().await;
            Err(error)
        }
    }
}

pub(crate) async fn classify_readiness_in(
    uow: &mut UnitOfWork<'_>,
    now: DateTime<Utc>,
) -> Result<HealthSnapshot, StorageError> {
    let global = load_telegram_reconciliation_state(uow).await?;
    if global.state != GlobalReconciliationState::Ready {
        return Err(unavailable());
    }

    let owner = load_owner_identity(uow).await?;
    let trusted = load_single_trusted_connection(uow).await?;
    let candidates = list_connection_candidates(uow).await?;
    let guard = load_candidate_guard(uow).await?;
    let cutoff = now - CANDIDATE_RETENTION;
    let retained = candidates
        .iter()
        .filter(|candidate| candidate.observed_at >= cutoff)
        .collect::<Vec<_>>();

    match (owner, trusted) {
        (OwnerIdentity::Unclaimed, Some(_)) => Err(unavailable()),
        (OwnerIdentity::Unclaimed, None) => {
            let connection = if guard.overflow_established_at.is_some() || retained.len() > 1 {
                ConnectionHealth::Ambiguous
            } else if retained.len() == 1 {
                ConnectionHealth::Candidate
            } else {
                ConnectionHealth::Missing
            };
            Ok(HealthSnapshot {
                owner: OwnerHealth::Unclaimed,
                connection,
            })
        }
        (OwnerIdentity::Claimed { owner_user_id, .. }, Some(trusted)) => {
            if trusted.owner_user_id != owner_user_id
                || trusted.reconciliation_state != ReconciliationState::Confirmed
                || !retained.is_empty()
                || guard.overflow_established_at.is_some()
            {
                return Err(unavailable());
            }
            let rights: PersistedRights =
                serde_json::from_str(&trusted.rights_json).map_err(|_| unavailable())?;
            let connection = if !trusted.enabled {
                ConnectionHealth::Disabled
            } else if rights.complete() {
                ConnectionHealth::Enabled
            } else {
                ConnectionHealth::RightsIncomplete
            };
            Ok(HealthSnapshot {
                owner: OwnerHealth::Claimed,
                connection,
            })
        }
        (OwnerIdentity::Claimed { owner_user_id, .. }, None) => {
            if retained
                .iter()
                .any(|candidate| candidate.business_user_id != owner_user_id)
            {
                return Err(unavailable());
            }
            let connection = if guard.overflow_established_at.is_some() || retained.len() > 1 {
                ConnectionHealth::Ambiguous
            } else if retained.len() == 1 {
                ConnectionHealth::Candidate
            } else {
                ConnectionHealth::Missing
            };
            Ok(HealthSnapshot {
                owner: OwnerHealth::Claimed,
                connection,
            })
        }
    }
}

fn unavailable() -> StorageError {
    StorageError::InvalidTelegramReconciliationState("readiness is unavailable")
}
