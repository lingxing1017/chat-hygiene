mod common;

use chathygiene::storage::{
    BusinessConnectionCandidate, CandidateWrite, GlobalReconciliationState, OwnerIdentity,
    StorageError, TrustedConnectionWrite, UnitOfWork, apply_authoritative_candidate,
    candidates_for_user, claim_owner, clear_connection_candidates, connect,
    delete_connection_candidate, delete_connection_candidates_for_other_users,
    disable_business_connection, find_business_connection, find_single_business_connection,
    gate_matching_trusted_for_reconciliation, initialize_or_load_owner_identity,
    load_candidate_guard, load_owner_identity, load_single_trusted_connection,
    load_telegram_reconciliation_state, migrate, normalize_startup_reconciliation,
    prune_connection_candidates, reconcile_authoritative_trusted_connection,
    retain_connection_candidates, retire_trusted_connection_not_found, set_telegram_auth_failed,
    transition_telegram_reconciliation_ready,
};
use chrono::{DateTime, Duration, Utc};

const RIGHTS: &str = r#"{"can_reply":true,"can_read_messages":false,"can_delete_sent_messages":true,"can_delete_all_messages":false}"#;

async fn database() -> (tempfile::TempDir, sqlx::SqlitePool) {
    let (directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    (directory, pool)
}

fn candidate(
    connection_id: &str,
    business_user_id: i64,
    established_at: i64,
    observed_at: DateTime<Utc>,
) -> BusinessConnectionCandidate {
    BusinessConnectionCandidate {
        connection_id: connection_id.to_owned(),
        business_user_id,
        user_chat_id: Some(business_user_id * 100),
        rights_json: RIGHTS.to_owned(),
        enabled: true,
        connection_established_at: established_at,
        state_revision: 0,
        observed_at,
    }
}

async fn claim_test_owner(pool: &sqlx::SqlitePool, now: DateTime<Utc>, floor: i64) {
    initialize_or_load_owner_identity(pool, now).await.unwrap();
    let mut uow = UnitOfWork::begin_immediate(pool).await.unwrap();
    claim_owner(&mut uow, 42, 4200, floor, now).await.unwrap();
    uow.commit().await.unwrap();
}

#[tokio::test]
async fn authoritative_candidate_write_is_revisioned_and_conflicts_delete_identity() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let initial = candidate("candidate-1", 42, 100, now);
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &initial, None)
            .await
            .unwrap(),
        CandidateWrite::Inserted
    );
    uow.commit().await.unwrap();

    let mut refreshed = initial.clone();
    refreshed.user_chat_id = Some(4200);
    refreshed.enabled = false;
    refreshed.rights_json = r#"{ "can_delete_all_messages": false, "can_reply": true, "can_read_messages": false, "can_delete_sent_messages": true }"#.to_owned();
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &refreshed, Some(0))
            .await
            .unwrap(),
        CandidateWrite::Reconciled
    );
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &refreshed, Some(0))
            .await
            .unwrap(),
        CandidateWrite::RevisionConflict
    );
    uow.commit().await.unwrap();
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let stored = candidates_for_user(&mut read, 42).await.unwrap();
    read.rollback().await.unwrap();
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].state_revision, 1);
    assert!(!stored[0].enabled);
    assert_eq!(stored[0].rights_json, RIGHTS);

    let different_user = candidate("candidate-1", 99, 100, now);
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &different_user, Some(1))
            .await
            .unwrap(),
        CandidateWrite::UserConflict
    );
    uow.commit().await.unwrap();
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    assert!(candidates_for_user(&mut read, 42).await.unwrap().is_empty());
    read.rollback().await.unwrap();

    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    apply_authoritative_candidate(&mut uow, &initial, None)
        .await
        .unwrap();
    uow.commit().await.unwrap();
    let different_generation = candidate("candidate-1", 42, 101, now);
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &different_generation, Some(0))
            .await
            .unwrap(),
        CandidateWrite::GenerationConflict
    );
    uow.commit().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn candidate_repository_rejects_invalid_authoritative_input_without_writes() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let valid = candidate("candidate", 42, 100, now);
    let mut invalid = Vec::new();
    let mut empty_id = valid.clone();
    empty_id.connection_id = " ".to_owned();
    invalid.push(empty_id);
    let mut invalid_user = valid.clone();
    invalid_user.business_user_id = 0;
    invalid.push(invalid_user);
    let mut invalid_chat = valid.clone();
    invalid_chat.user_chat_id = Some(0);
    invalid.push(invalid_chat);
    let mut invalid_date = valid.clone();
    invalid_date.connection_established_at = 0;
    invalid.push(invalid_date);
    let mut invalid_revision = valid.clone();
    invalid_revision.state_revision = -1;
    invalid.push(invalid_revision);
    for rights_json in [
        "{}",
        r#"{"can_reply":true,"can_read_messages":false,"can_delete_sent_messages":true,"can_delete_all_messages":false,"extra":true}"#,
    ] {
        let mut invalid_rights = valid.clone();
        invalid_rights.rights_json = rights_json.to_owned();
        invalid.push(invalid_rights);
    }

    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    for snapshot in invalid {
        assert!(matches!(
            apply_authoritative_candidate(&mut uow, &snapshot, None)
                .await
                .unwrap_err(),
            StorageError::InvalidConnectionCandidate(_)
        ));
    }
    uow.commit().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn candidates_coexist_but_remain_isolated_from_trusted_state() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    for snapshot in [
        candidate("same-user-a", 42, 100, now),
        candidate("same-user-b", 42, 101, now),
        candidate("other-user", 99, 102, now),
    ] {
        apply_authoritative_candidate(&mut uow, &snapshot, None)
            .await
            .unwrap();
    }
    uow.commit().await.unwrap();
    assert_eq!(
        initialize_or_load_owner_identity(&pool, now).await.unwrap(),
        OwnerIdentity::Unclaimed
    );
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('trusted', 42, '{}', 1, ?)",
    )
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();

    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    assert_eq!(candidates_for_user(&mut read, 42).await.unwrap().len(), 2);
    assert!(
        find_business_connection(&mut read, "same-user-a")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        find_single_business_connection(&mut read)
            .await
            .unwrap()
            .unwrap()
            .connection_id,
        "trusted"
    );
    read.rollback().await.unwrap();
    assert!(
        sqlx::query(
            "INSERT INTO conversation
             (connection_id, chat_id, user_id, state, created_at, updated_at)
             VALUES ('same-user-a', 1, 1, 'NEW', ?, ?)",
        )
        .bind(now.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .is_err()
    );

    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        delete_connection_candidates_for_other_users(&mut uow, 42)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        retain_connection_candidates(&mut uow, 42, &["same-user-b".to_owned()])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        delete_connection_candidate(&mut uow, "same-user-b")
            .await
            .unwrap(),
        1
    );
    assert_eq!(clear_connection_candidates(&mut uow).await.unwrap(), 0);
    uow.commit().await.unwrap();
}

#[tokio::test]
async fn pruning_uses_service_time_boundary_and_records_overflow_floor() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let cutoff = now - Duration::days(7);
    let snapshots = [
        candidate("expired", 42, 900, cutoff - Duration::milliseconds(1)),
        candidate("cutoff", 42, 100, cutoff),
        candidate("tie-a", 42, 500, now),
        candidate("tie-b", 42, 500, now),
        candidate("newest", 42, 600, now),
    ];
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    for snapshot in snapshots {
        apply_authoritative_candidate(&mut uow, &snapshot, None)
            .await
            .unwrap();
    }
    assert_eq!(
        prune_connection_candidates(&mut uow, cutoff, 2)
            .await
            .unwrap(),
        3
    );
    let retained = candidates_for_user(&mut uow, 42).await.unwrap();
    assert_eq!(
        retained
            .iter()
            .map(|candidate| candidate.connection_id.as_str())
            .collect::<Vec<_>>(),
        vec!["tie-b", "newest"]
    );
    let guard = load_candidate_guard(&mut uow).await.unwrap();
    assert_eq!(guard.overflow_established_at, Some(600));
    assert_eq!(guard.state_revision, 1);
    uow.commit().await.unwrap();

    let later = candidate("later-observation", 42, 550, now + Duration::hours(1));
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    apply_authoritative_candidate(&mut uow, &later, None)
        .await
        .unwrap();
    let guard = load_candidate_guard(&mut uow).await.unwrap();
    assert_eq!(guard.overflow_established_at, Some(600));
    assert_eq!(guard.state_revision, 2);

    let missing_revision = candidate("missing-revision", 42, 700, now + Duration::hours(2));
    assert_eq!(
        apply_authoritative_candidate(&mut uow, &missing_revision, Some(9))
            .await
            .unwrap(),
        CandidateWrite::RevisionConflict
    );
    let guard = load_candidate_guard(&mut uow).await.unwrap();
    assert_eq!(guard.overflow_established_at, Some(700));
    assert_eq!(guard.state_revision, 3);
    assert_eq!(
        prune_connection_candidates(&mut uow, cutoff, 256)
            .await
            .unwrap(),
        0
    );
    uow.commit().await.unwrap();
}

#[tokio::test]
async fn strict_candidate_decoder_rejects_corruption_without_repair() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    let initial = candidate("candidate-1", 42, 100, now);
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    apply_authoritative_candidate(&mut uow, &initial, None)
        .await
        .unwrap();
    uow.commit().await.unwrap();
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&pool)
        .await
        .unwrap();

    for (column, value) in [
        ("connection_id", "X'37'"),
        ("user_chat_id", "X'31'"),
        ("rights_json", "X'7B7D'"),
        ("rights_json", "'{}'"),
        ("enabled", "X'31'"),
        ("enabled", "2"),
        ("connection_established_at", "X'31'"),
        ("connection_established_at", "0"),
        ("state_revision", "X'30'"),
        ("state_revision", "-1"),
        ("observed_at", "X'31'"),
        ("observed_at", "'not-a-timestamp'"),
    ] {
        sqlx::query(&format!(
            "UPDATE business_connection_candidate SET {column} = {value}
             WHERE connection_id = 'candidate-1'"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let mut read = UnitOfWork::begin(&pool).await.unwrap();
        let error = candidates_for_user(&mut read, 42).await.unwrap_err();
        assert!(matches!(error, StorageError::InvalidConnectionCandidate(_)));
        read.rollback().await.unwrap();
        sqlx::query("DELETE FROM business_connection_candidate")
            .execute(&pool)
            .await
            .unwrap();
        let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
        apply_authoritative_candidate(&mut uow, &initial, None)
            .await
            .unwrap();
        uow.commit().await.unwrap();
    }
    let rows_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rows_before, 1);

    for value in ["X'31'", "0"] {
        sqlx::query(&format!(
            "UPDATE business_connection_candidate SET business_user_id = {value}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let mut read = UnitOfWork::begin_immediate(&pool).await.unwrap();
        assert!(matches!(
            apply_authoritative_candidate(&mut read, &initial, Some(0))
                .await
                .unwrap_err(),
            StorageError::InvalidConnectionCandidate(_)
        ));
        read.rollback().await.unwrap();
        sqlx::query("DELETE FROM business_connection_candidate")
            .execute(&pool)
            .await
            .unwrap();
        let mut restore = UnitOfWork::begin_immediate(&pool).await.unwrap();
        apply_authoritative_candidate(&mut restore, &initial, None)
            .await
            .unwrap();
        restore.commit().await.unwrap();
    }
}

#[tokio::test]
async fn global_reconciliation_transitions_are_strict_and_restart_idempotent() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('trusted', 42, '{}', 1, ?)",
    )
    .bind(now.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();

    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let pending = normalize_startup_reconciliation(&mut uow, now)
        .await
        .unwrap();
    assert_eq!(pending.state, GlobalReconciliationState::Pending);
    assert_eq!(pending.state_revision, 1);
    uow.commit().await.unwrap();
    let trusted: (String, i64) =
        sqlx::query_as("SELECT reconciliation_state, state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(trusted, ("PENDING".to_owned(), 1));

    let mut restart = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let unchanged = normalize_startup_reconciliation(&mut restart, now + Duration::seconds(1))
        .await
        .unwrap();
    restart.commit().await.unwrap();
    assert_eq!(unchanged.state_revision, 1);
    let trusted_revision: i64 =
        sqlx::query_scalar("SELECT state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(trusted_revision, 1);

    let mut auth = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let failed = set_telegram_auth_failed(&mut auth, now + Duration::seconds(2))
        .await
        .unwrap();
    assert_eq!(failed.state, GlobalReconciliationState::AuthFailed);
    auth.commit().await.unwrap();
    let trusted: (bool, String, i64) = sqlx::query_as(
        "SELECT enabled, reconciliation_state, state_revision FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(trusted, (false, "PENDING".to_owned(), 2));

    let mut repair = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let pending = normalize_startup_reconciliation(&mut repair, now + Duration::seconds(3))
        .await
        .unwrap();
    repair.commit().await.unwrap();
    assert_eq!(pending.state_revision, 3);
    sqlx::query(
        "UPDATE business_connection SET reconciliation_state = 'CONFIRMED',
         state_revision = state_revision + 1",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut barrier = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let ready = transition_telegram_reconciliation_ready(
        &mut barrier,
        pending.state_revision,
        now + Duration::seconds(4),
    )
    .await
    .unwrap();
    barrier.commit().await.unwrap();
    assert_eq!(ready.state, GlobalReconciliationState::Ready);
    assert_eq!(ready.state_revision, 4);

    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    assert_eq!(
        load_telegram_reconciliation_state(&mut read).await.unwrap(),
        ready
    );
    read.rollback().await.unwrap();
}

#[tokio::test]
async fn trusted_reconciliation_uses_revisions_generations_and_safety_intersection() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    claim_test_owner(&pool, now, 1).await;
    let first = candidate("trusted-a", 42, 100, now);
    let mut install = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut install, &first, None, None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Installed
    );
    install.commit().await.unwrap();

    let mut refresh = first.clone();
    refresh.rights_json = r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#.to_owned();
    let mut reconcile = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut reconcile, &refresh, Some(0), None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Reconciled
    );
    reconcile.commit().await.unwrap();
    let revision: i64 = sqlx::query_scalar("SELECT state_revision FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(revision, 1);

    let mut stale = refresh.clone();
    stale.enabled = false;
    stale.rights_json = r#"{"can_reply":false,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#.to_owned();
    let mut conflict = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut conflict, &stale, Some(0), None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::RevisionConflict
    );
    conflict.commit().await.unwrap();
    let stricter: (bool, String, i64) =
        sqlx::query_as("SELECT enabled, rights_json, state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!stricter.0);
    assert!(stricter.1.contains(r#""can_reply":false"#));
    assert_eq!(stricter.2, 2);

    let replacement = candidate("trusted-b", 42, 200, now + Duration::seconds(1));
    let mut replace = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut replace, &replacement, Some(2), None, 0,)
            .await
            .unwrap(),
        TrustedConnectionWrite::Replaced
    );
    replace.commit().await.unwrap();
    let trusted: (String, Option<i64>, i64, String) = sqlx::query_as(
        "SELECT connection_id, connection_established_at, state_revision,
                reconciliation_state FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        trusted,
        ("trusted-b".to_owned(), Some(200), 0, "CONFIRMED".to_owned())
    );
}

#[tokio::test]
async fn candidate_promotion_and_conflicting_generations_preserve_trust_boundaries() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    claim_test_owner(&pool, now, 1).await;
    let retained = candidate("retained", 42, 300, now);
    let mut stage = UnitOfWork::begin_immediate(&pool).await.unwrap();
    apply_authoritative_candidate(&mut stage, &retained, None)
        .await
        .unwrap();
    stage.commit().await.unwrap();

    let mut promote = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut promote, &retained, None, Some(0), 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Installed
    );
    promote.commit().await.unwrap();
    let promoted: (String, i64, String) = sqlx::query_as(
        "SELECT connection_id, state_revision, reconciliation_state
         FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(promoted, ("retained".to_owned(), 1, "CONFIRMED".to_owned()));
    let candidate_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM business_connection_candidate")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(candidate_count, 0);

    for (snapshot, expected) in [
        (
            candidate("cross-user", 99, 400, now + Duration::seconds(1)),
            TrustedConnectionWrite::UserConflict,
        ),
        (
            candidate("older", 42, 299, now + Duration::seconds(2)),
            TrustedConnectionWrite::GenerationConflict,
        ),
    ] {
        let mut reconcile = UnitOfWork::begin_immediate(&pool).await.unwrap();
        assert_eq!(
            reconcile_authoritative_trusted_connection(
                &mut reconcile,
                &snapshot,
                Some(1),
                None,
                0,
            )
            .await
            .unwrap(),
            expected
        );
        reconcile.commit().await.unwrap();
        let unchanged: (String, i64, String, bool) = sqlx::query_as(
            "SELECT connection_id, state_revision, reconciliation_state, enabled
             FROM business_connection",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            unchanged,
            ("retained".to_owned(), 1, "CONFIRMED".to_owned(), true)
        );
    }
}

#[tokio::test]
async fn delayed_authoritative_response_cannot_undo_local_disable_or_pending_gate() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    claim_test_owner(&pool, now, 1).await;
    let mut full = candidate("trusted", 42, 300, now);
    full.rights_json = r#"{"can_reply":true,"can_read_messages":true,"can_delete_sent_messages":true,"can_delete_all_messages":true}"#.to_owned();
    let mut install = UnitOfWork::begin_immediate(&pool).await.unwrap();
    reconcile_authoritative_trusted_connection(&mut install, &full, None, None, 0)
        .await
        .unwrap();
    install.commit().await.unwrap();

    let mut disable = UnitOfWork::begin_immediate(&pool).await.unwrap();
    disable_business_connection(&mut disable, "trusted", now + Duration::seconds(1))
        .await
        .unwrap();
    disable.commit().await.unwrap();
    let mut delayed = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut delayed, &full, Some(0), None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::RevisionConflict
    );
    delayed.commit().await.unwrap();
    let disabled: (bool, i64, String) = sqlx::query_as(
        "SELECT enabled, state_revision, reconciliation_state FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(disabled, (false, 1, "CONFIRMED".to_owned()));

    let mut gate = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        gate_matching_trusted_for_reconciliation(&mut gate, "trusted", now + Duration::seconds(2),)
            .await
            .unwrap(),
        Some(2)
    );
    gate.commit().await.unwrap();
    let mut in_flight_disable = UnitOfWork::begin_immediate(&pool).await.unwrap();
    disable_business_connection(
        &mut in_flight_disable,
        "trusted",
        now + Duration::seconds(3),
    )
    .await
    .unwrap();
    in_flight_disable.commit().await.unwrap();
    let pending: (bool, i64, String) = sqlx::query_as(
        "SELECT enabled, state_revision, reconciliation_state FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(pending, (false, 3, "PENDING".to_owned()));
}

#[tokio::test]
async fn equal_generation_becomes_ambiguous_and_floor_survives_marker_pruning() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    claim_test_owner(&pool, now, 1).await;
    let first = candidate("equal-a", 42, 100, now);
    let mut install = UnitOfWork::begin_immediate(&pool).await.unwrap();
    reconcile_authoritative_trusted_connection(&mut install, &first, None, None, 0)
        .await
        .unwrap();
    install.commit().await.unwrap();
    let second = candidate("equal-b", 42, 100, now + Duration::seconds(1));
    let mut ambiguous = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut ambiguous, &second, Some(0), None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Ambiguous
    );
    assert!(
        load_single_trusted_connection(&mut ambiguous)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        candidates_for_user(&mut ambiguous, 42).await.unwrap().len(),
        2
    );
    ambiguous.commit().await.unwrap();
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            connection_floor_established_at: Some(100),
            ..
        }
    ));

    let mut prune = UnitOfWork::begin_immediate(&pool).await.unwrap();
    clear_connection_candidates(&mut prune).await.unwrap();
    prune.commit().await.unwrap();
    let delayed = candidate("delayed", 42, 100, now + Duration::days(1));
    let mut reject = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut reject, &delayed, None, None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::GenerationConflict
    );
    reject.rollback().await.unwrap();
    let later = candidate("later", 42, 101, now + Duration::days(1));
    let mut accept = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        reconcile_authoritative_trusted_connection(&mut accept, &later, None, None, 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Installed
    );
    accept.commit().await.unwrap();
}

#[tokio::test]
async fn matching_gate_and_not_found_retirement_are_revision_guarded() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    claim_test_owner(&pool, now, 1).await;
    let trusted = candidate("trusted", 42, 300, now);
    let mut install = UnitOfWork::begin_immediate(&pool).await.unwrap();
    reconcile_authoritative_trusted_connection(&mut install, &trusted, None, None, 0)
        .await
        .unwrap();
    install.commit().await.unwrap();

    let mut other = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        gate_matching_trusted_for_reconciliation(&mut other, "other", now)
            .await
            .unwrap(),
        None
    );
    other.commit().await.unwrap();
    let untouched: (String, i64) =
        sqlx::query_as("SELECT reconciliation_state, state_revision FROM business_connection")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(untouched, ("CONFIRMED".to_owned(), 0));

    let mut matching = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        gate_matching_trusted_for_reconciliation(&mut matching, "trusted", now)
            .await
            .unwrap(),
        Some(1)
    );
    matching.commit().await.unwrap();
    let mut restart = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        gate_matching_trusted_for_reconciliation(&mut restart, "trusted", now)
            .await
            .unwrap(),
        Some(1)
    );
    restart.commit().await.unwrap();

    let mut stale = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        retire_trusted_connection_not_found(&mut stale, "trusted", 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::RevisionConflict
    );
    stale.rollback().await.unwrap();
    let mut retire = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        retire_trusted_connection_not_found(&mut retire, "trusted", 1)
            .await
            .unwrap(),
        TrustedConnectionWrite::Reconciled
    );
    retire.commit().await.unwrap();
    let trusted_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM business_connection")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(trusted_count, 0);
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            connection_floor_established_at: Some(300),
            ..
        }
    ));
}

#[tokio::test]
async fn corrupt_singletons_fail_closed_without_repair() {
    let (_directory, pool) = database().await;
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE business_connection_candidate_guard
         SET overflow_established_at = 0, state_revision = -1, updated_at = 'bad'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut guard_read = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_candidate_guard(&mut guard_read).await.unwrap_err(),
        StorageError::InvalidCandidateGuard(_)
    ));
    guard_read.rollback().await.unwrap();
    let guard_after: (Option<i64>, i64, String) = sqlx::query_as(
        "SELECT overflow_established_at, state_revision, updated_at
         FROM business_connection_candidate_guard",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(guard_after, (Some(0), -1, "bad".to_owned()));

    sqlx::query(
        "UPDATE telegram_reconciliation_state
         SET state = 'UNKNOWN', state_revision = -1, updated_at = 'bad'",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut global_read = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_telegram_reconciliation_state(&mut global_read)
            .await
            .unwrap_err(),
        StorageError::InvalidTelegramReconciliationState(_)
    ));
    global_read.rollback().await.unwrap();
    let global_after: (String, i64, String) = sqlx::query_as(
        "SELECT state, state_revision, updated_at FROM telegram_reconciliation_state",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(global_after, ("UNKNOWN".to_owned(), -1, "bad".to_owned()));

    sqlx::query("DELETE FROM business_connection_candidate_guard")
        .execute(&pool)
        .await
        .unwrap();
    let mut missing_guard = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_candidate_guard(&mut missing_guard).await.unwrap_err(),
        StorageError::InvalidCandidateGuard(_)
    ));
    missing_guard.rollback().await.unwrap();
    sqlx::query(
        "INSERT INTO business_connection_candidate_guard VALUES
         (1, NULL, 0, '2026-07-14T00:00:00Z'),
         (2, NULL, 0, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut extra_guard = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_candidate_guard(&mut extra_guard).await.unwrap_err(),
        StorageError::InvalidCandidateGuard(_)
    ));
    extra_guard.rollback().await.unwrap();

    sqlx::query("DELETE FROM telegram_reconciliation_state")
        .execute(&pool)
        .await
        .unwrap();
    let mut missing_global = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_telegram_reconciliation_state(&mut missing_global)
            .await
            .unwrap_err(),
        StorageError::TelegramReconciliationStateMissing
    ));
    missing_global.rollback().await.unwrap();
    sqlx::query(
        "INSERT INTO telegram_reconciliation_state VALUES
         (1, 'READY', 0, '2026-07-14T00:00:00Z'),
         (2, 'READY', 0, '2026-07-14T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let mut extra_global = UnitOfWork::begin(&pool).await.unwrap();
    assert!(matches!(
        load_telegram_reconciliation_state(&mut extra_global)
            .await
            .unwrap_err(),
        StorageError::InvalidTelegramReconciliationState(_)
    ));
    extra_global.rollback().await.unwrap();
}

#[tokio::test]
async fn telegram_auth_failure_is_fail_closed_from_every_legal_state() {
    for global_state in ["READY", "PENDING", "AUTH_FAILED"] {
        let (_directory, pool) = database().await;
        let now = common::at("2026-07-14T00:00:00Z");
        sqlx::query(
            "UPDATE telegram_reconciliation_state
             SET state = ?, state_revision = 5, updated_at = ?",
        )
        .bind(global_state)
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO business_connection
             (connection_id, owner_user_id, rights_json, enabled,
              connection_established_at, state_revision, reconciliation_state, updated_at)
             VALUES ('trusted', 42, '{}', 1, 100, 7, 'CONFIRMED', ?)",
        )
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();

        let mut fail = UnitOfWork::begin_immediate(&pool).await.unwrap();
        let state = set_telegram_auth_failed(&mut fail, now + Duration::seconds(1))
            .await
            .unwrap();
        fail.commit().await.unwrap();
        assert_eq!(state.state, GlobalReconciliationState::AuthFailed);
        assert_eq!(
            state.state_revision,
            if global_state == "AUTH_FAILED" { 5 } else { 6 }
        );
        let trusted: (bool, i64, String) = sqlx::query_as(
            "SELECT enabled, state_revision, reconciliation_state FROM business_connection",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(trusted, (false, 8, "PENDING".to_owned()));
    }
}

#[tokio::test]
async fn corrupt_trusted_storage_classes_fail_closed_without_repair() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    sqlx::query("PRAGMA ignore_check_constraints = ON")
        .execute(&pool)
        .await
        .unwrap();
    for (column, value) in [
        ("connection_id", "X'37'"),
        ("connection_id", "''"),
        ("owner_user_id", "X'31'"),
        ("owner_user_id", "0"),
        ("rights_json", "X'7B7D'"),
        ("rights_json", "'not-json'"),
        ("enabled", "X'31'"),
        ("enabled", "2"),
        ("connection_established_at", "X'31'"),
        ("connection_established_at", "0"),
        ("state_revision", "X'30'"),
        ("state_revision", "-1"),
        ("reconciliation_state", "X'50454E44494E47'"),
        ("reconciliation_state", "'UNKNOWN'"),
        ("updated_at", "X'31'"),
        ("updated_at", "'not-a-timestamp'"),
    ] {
        sqlx::query("DELETE FROM business_connection")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO business_connection
             (connection_id, owner_user_id, rights_json, enabled,
              connection_established_at, state_revision, reconciliation_state, updated_at)
             VALUES ('trusted', 42, '{}', 1, 100, 0, 'CONFIRMED', ?)",
        )
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            "UPDATE business_connection SET {column} = {value}"
        ))
        .execute(&pool)
        .await
        .unwrap();
        let mut read = UnitOfWork::begin(&pool).await.unwrap();
        let error = load_single_trusted_connection(&mut read).await.unwrap_err();
        assert!(matches!(error, StorageError::InvalidData(_)));
        read.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn legacy_not_found_uses_imported_owner_bound_time_as_floor() {
    let (_directory, pool) = database().await;
    let bound_at = common::at("2026-07-14T00:00:00Z");
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('legacy', 42, '{}', 1, ?)",
    )
    .bind(bound_at.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    initialize_or_load_owner_identity(&pool, bound_at)
        .await
        .unwrap();

    let mut retire = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert_eq!(
        retire_trusted_connection_not_found(&mut retire, "legacy", 0)
            .await
            .unwrap(),
        TrustedConnectionWrite::Reconciled
    );
    retire.commit().await.unwrap();
    let mut read = UnitOfWork::begin(&pool).await.unwrap();
    let owner = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        owner,
        OwnerIdentity::Claimed {
            connection_floor_established_at: Some(floor),
            ..
        } if floor == bound_at.timestamp()
    ));
}

#[tokio::test]
async fn startup_normalization_rejects_multiple_trusted_rows_without_repair() {
    let (_directory, pool) = database().await;
    let now = common::at("2026-07-14T00:00:00Z");
    for (connection_id, owner_user_id) in [("one", 42_i64), ("two", 84_i64)] {
        sqlx::query(
            "INSERT INTO business_connection
             (connection_id, owner_user_id, rights_json, enabled, updated_at)
             VALUES (?, ?, '{}', 1, ?)",
        )
        .bind(connection_id)
        .bind(owner_user_id)
        .bind(now.to_rfc3339())
        .execute(&pool)
        .await
        .unwrap();
    }
    let mut normalize = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert!(matches!(
        normalize_startup_reconciliation(&mut normalize, now)
            .await
            .unwrap_err(),
        StorageError::InvalidTelegramReconciliationState(_)
    ));
    normalize.rollback().await.unwrap();
    let global: (String, i64) =
        sqlx::query_as("SELECT state, state_revision FROM telegram_reconciliation_state")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(global, ("READY".to_owned(), 0));
    let trusted: Vec<(String, i64)> = sqlx::query_as(
        "SELECT reconciliation_state, state_revision
         FROM business_connection ORDER BY connection_id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        trusted,
        vec![("CONFIRMED".to_owned(), 0), ("CONFIRMED".to_owned(), 0)]
    );
}
