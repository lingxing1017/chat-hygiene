mod common;

use chathygiene::storage::{
    OwnerChatSource, OwnerIdentity, StorageError, UnitOfWork, advance_owner_connection_floor,
    claim_owner, connect, initialize_or_load_owner_identity, load_owner_identity, migrate,
    promote_owner_chat,
};

#[tokio::test]
async fn fresh_database_initializes_unclaimed_and_stays_stable() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    let first = initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    assert_eq!(first, OwnerIdentity::Unclaimed);
    pool.close().await;
    let reopened = connect(&url).await.unwrap();
    let second = initialize_or_load_owner_identity(&reopened, common::at("2026-08-30T00:00:01Z"))
        .await
        .unwrap();
    assert_eq!(second, OwnerIdentity::Unclaimed);
}

#[tokio::test]
async fn one_legacy_connection_imports_without_changing_the_connection() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{\"can_reply\":true}', 1,
                 '2026-08-30T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    let before: (String, i64, String, i64, String) = sqlx::query_as(
        "SELECT connection_id, owner_user_id, rights_json, enabled, updated_at
         FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let identity = initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:01Z"))
        .await
        .unwrap();
    assert_eq!(
        identity,
        OwnerIdentity::Claimed {
            owner_user_id: 42,
            owner_chat_id: 42,
            owner_chat_source: OwnerChatSource::LegacyFallback,
            connection_floor_established_at: None,
            bound_at: common::at("2026-08-30T00:00:01Z"),
        }
    );
    let after: (String, i64, String, i64, String) = sqlx::query_as(
        "SELECT connection_id, owner_user_id, rights_json, enabled, updated_at
         FROM business_connection",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(after, before);
}

#[tokio::test]
async fn ambiguous_legacy_connections_leave_pending_unchanged() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    for (connection_id, owner_user_id) in [("one", 1_i64), ("two", 2_i64)] {
        sqlx::query(
            "INSERT INTO business_connection
             (connection_id, owner_user_id, rights_json, enabled, updated_at)
             VALUES (?, ?, '{}', 1, '2026-08-30T00:00:00Z')",
        )
        .bind(connection_id)
        .bind(owner_user_id)
        .execute(&pool)
        .await
        .unwrap();
    }
    let before = owner_snapshot(&pool).await;
    assert!(matches!(
        initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:01Z")).await,
        Err(StorageError::InvalidOwnerIdentity(_))
    ));
    assert_eq!(owner_snapshot(&pool).await, before);
}

#[tokio::test]
async fn strict_decoder_rejects_corruption_without_repair() {
    let corruptions = [
        "DELETE FROM owner_identity",
        "INSERT INTO owner_identity
         (singleton, state, owner_user_id, owner_chat_id, owner_chat_source,
          connection_floor_established_at, bound_at)
         VALUES (2, 'PENDING', NULL, NULL, NULL, NULL, NULL)",
        "UPDATE owner_identity SET state = 'UNKNOWN'",
        "UPDATE owner_identity SET owner_chat_source = 'UNKNOWN'",
        "UPDATE owner_identity SET owner_user_id = 'user'",
        "UPDATE owner_identity SET owner_chat_id = 'chat'",
        "UPDATE owner_identity SET owner_user_id = 0",
        "UPDATE owner_identity SET owner_chat_id = -1",
        "UPDATE owner_identity SET owner_chat_id = NULL",
        "UPDATE owner_identity SET state = 'UNCLAIMED'",
        "UPDATE owner_identity SET bound_at = 'not-rfc3339'",
        "UPDATE owner_identity SET connection_floor_established_at = 0",
        "UPDATE owner_identity SET connection_floor_established_at = 'floor'",
    ];
    for (index, statement) in corruptions.into_iter().enumerate() {
        let (_directory, url) = common::temporary_database();
        let pool = connect(&url).await.unwrap();
        migrate(&pool).await.unwrap();
        initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:00Z"))
            .await
            .unwrap();
        let mut claim = UnitOfWork::begin_immediate(&pool).await.unwrap();
        claim_owner(&mut claim, 42, 420, 1, common::at("2026-08-30T00:00:01Z"))
            .await
            .unwrap();
        claim.commit().await.unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = ON")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(statement).execute(&pool).await.unwrap();
        sqlx::query("PRAGMA ignore_check_constraints = OFF")
            .execute(&pool)
            .await
            .unwrap();
        let before = owner_snapshot(&pool).await;
        let mut uow = UnitOfWork::begin(&pool).await.unwrap();
        let result = load_owner_identity(&mut uow).await;
        uow.rollback().await.unwrap();
        if index == 0 {
            assert!(matches!(result, Err(StorageError::OwnerIdentityMissing)));
        } else {
            assert!(matches!(result, Err(StorageError::InvalidOwnerIdentity(_))));
        }
        assert_eq!(owner_snapshot(&pool).await, before);
    }
}

#[tokio::test]
async fn concurrent_claims_bind_exactly_one_owner() {
    let (_directory, url) = common::temporary_database();
    let first_pool = connect(&url).await.unwrap();
    migrate(&first_pool).await.unwrap();
    initialize_or_load_owner_identity(&first_pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let second_pool = connect(&url).await.unwrap();
    let (first, second) = tokio::join!(
        claim_in_own_transaction(&first_pool, 10, 100),
        claim_in_own_transaction(&second_pool, 20, 200)
    );
    assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
    let loser = if first.is_err() { first } else { second };
    assert!(matches!(loser, Err(StorageError::OwnerAlreadyClaimed)));
    let mut read = UnitOfWork::begin(&first_pool).await.unwrap();
    let identity = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        identity,
        OwnerIdentity::Claimed {
            owner_user_id: 10,
            owner_chat_id: 100,
            ..
        } | OwnerIdentity::Claimed {
            owner_user_id: 20,
            owner_chat_id: 200,
            ..
        }
    ));
}

#[tokio::test]
async fn legacy_chat_promotes_once_and_authoritative_sources_do_not_drift() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-08-30T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    let promoted = promote_owner_chat(&mut uow, 42, 420, OwnerChatSource::BusinessConnection)
        .await
        .unwrap();
    let retained = promote_owner_chat(&mut uow, 42, 999, OwnerChatSource::PrivateMessage)
        .await
        .unwrap();
    assert_eq!(retained, promoted);
    assert!(matches!(
        promote_owner_chat(&mut uow, 43, 430, OwnerChatSource::BusinessConnection).await,
        Err(StorageError::OwnerChatMismatch)
    ));
    uow.commit().await.unwrap();
    assert!(matches!(
        promoted,
        OwnerIdentity::Claimed {
            owner_chat_id: 420,
            owner_chat_source: OwnerChatSource::BusinessConnection,
            ..
        }
    ));
}

#[tokio::test]
async fn connection_floor_is_monotonic_and_persists_across_restart() {
    let (_directory, url) = common::temporary_database();
    let pool = connect(&url).await.unwrap();
    migrate(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO business_connection
         (connection_id, owner_user_id, rights_json, enabled, updated_at)
         VALUES ('business-1', 42, '{}', 1, '2026-08-30T00:00:00Z')",
    )
    .execute(&pool)
    .await
    .unwrap();
    initialize_or_load_owner_identity(&pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    for floor in [10, 5, 10, 20] {
        advance_owner_connection_floor(&mut uow, floor)
            .await
            .unwrap();
    }
    uow.commit().await.unwrap();
    pool.close().await;
    let reopened = connect(&url).await.unwrap();
    let mut read = UnitOfWork::begin(&reopened).await.unwrap();
    let identity = load_owner_identity(&mut read).await.unwrap();
    read.rollback().await.unwrap();
    assert!(matches!(
        identity,
        OwnerIdentity::Claimed {
            connection_floor_established_at: Some(20),
            ..
        }
    ));
}

#[tokio::test]
async fn floor_rejects_pending_unclaimed_and_nonpositive_values() {
    let (_pending_directory, pending_url) = common::temporary_database();
    let pending_pool = connect(&pending_url).await.unwrap();
    migrate(&pending_pool).await.unwrap();
    let mut pending = UnitOfWork::begin_immediate(&pending_pool).await.unwrap();
    assert!(matches!(
        advance_owner_connection_floor(&mut pending, 1).await,
        Err(StorageError::InvalidOwnerIdentity(_))
    ));
    pending.rollback().await.unwrap();

    let (_unclaimed_directory, unclaimed_url) = common::temporary_database();
    let unclaimed_pool = connect(&unclaimed_url).await.unwrap();
    migrate(&unclaimed_pool).await.unwrap();
    initialize_or_load_owner_identity(&unclaimed_pool, common::at("2026-08-30T00:00:00Z"))
        .await
        .unwrap();
    let mut unclaimed = UnitOfWork::begin_immediate(&unclaimed_pool).await.unwrap();
    assert!(matches!(
        advance_owner_connection_floor(&mut unclaimed, 1).await,
        Err(StorageError::InvalidOwnerIdentity(_))
    ));
    assert!(matches!(
        advance_owner_connection_floor(&mut unclaimed, 0).await,
        Err(StorageError::InvalidOwnerIdentity(_))
    ));
    unclaimed.rollback().await.unwrap();
}

async fn claim_in_own_transaction(
    pool: &sqlx::SqlitePool,
    owner_user_id: i64,
    owner_chat_id: i64,
) -> Result<OwnerIdentity, StorageError> {
    let mut uow = UnitOfWork::begin_immediate(pool).await?;
    let result = claim_owner(
        &mut uow,
        owner_user_id,
        owner_chat_id,
        1,
        common::at("2026-08-30T00:00:01Z"),
    )
    .await;
    match result {
        Ok(identity) => {
            uow.commit().await?;
            Ok(identity)
        }
        Err(error) => {
            uow.rollback().await?;
            Err(error)
        }
    }
}

async fn owner_snapshot(pool: &sqlx::SqlitePool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT quote(singleton) || '|' || quote(state) || '|' || quote(owner_user_id) ||
                '|' || quote(owner_chat_id) || '|' || quote(owner_chat_source) ||
                '|' || quote(connection_floor_established_at) || '|' || quote(bound_at)
         FROM owner_identity ORDER BY singleton",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}
