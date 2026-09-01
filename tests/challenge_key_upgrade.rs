mod common;

use chathygiene::storage::{StorageError, UnitOfWork, connect, migrate, replace_challenge_hmac};
use chathygiene::verification::{
    ArithmeticVerifier, ChallengeKeyUpgradeError, upgrade_active_challenge_hmacs,
};
use rand::SeedableRng;
use rand::rngs::StdRng;
use secrecy::SecretSlice;
use sqlx::SqlitePool;

fn current_verifier() -> ArithmeticVerifier<StdRng> {
    ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(1),
        SecretSlice::from(
            hex::decode("bd442488308239145955b5d27edefe6f3b3c12e7996f099def53e114477253cb")
                .unwrap(),
        ),
        1,
    )
}

async fn database() -> (tempfile::TempDir, String, SqlitePool) {
    let (directory, url) = common::temporary_database();
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
    (directory, url, pool)
}

async fn insert_challenge(
    pool: &SqlitePool,
    chat_id: i64,
    expression: &str,
    answer_hmac: &str,
    version: i64,
    closed: bool,
) -> i64 {
    sqlx::query(
        "INSERT OR IGNORE INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES ('business-1', ?, ?, 'VERIFY_PENDING',
                 '2026-08-30T00:00:00Z', '2026-08-30T00:00:00Z')",
    )
    .bind(chat_id)
    .bind(chat_id)
    .execute(pool)
    .await
    .unwrap();
    let result = sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, hmac_key_version,
          created_at, expires_at, attempts_used, max_attempts, delivery_status, closed_at)
         VALUES ('business-1', ?, ?, ?, ?, '2026-08-30T00:00:00Z',
                 '2026-08-30T00:02:00Z', 0, 3, ?, ?)",
    )
    .bind(chat_id)
    .bind(expression)
    .bind(answer_hmac)
    .bind(version)
    .bind(if closed { "CLOSED" } else { "SENT" })
    .bind(closed.then_some("2026-08-30T00:01:00Z"))
    .execute(pool)
    .await
    .unwrap();
    result.last_insert_rowid()
}

#[tokio::test]
async fn upgrades_only_open_legacy_challenges_and_is_idempotent() {
    let (_directory, _url, pool) = database().await;
    let first = insert_challenge(&pool, 100, "7 + 5 - 3", "legacy-1", 0, false).await;
    let second = insert_challenge(&pool, 101, "2 + 3 × 4", "legacy-2", 0, false).await;
    let closed = insert_challenge(&pool, 102, "7 + 5 - 3", "closed", 0, true).await;
    let current = insert_challenge(&pool, 103, "7 + 5 - 3", "current", 1, false).await;
    let verifier = current_verifier();

    assert_eq!(
        upgrade_active_challenge_hmacs(&pool, &verifier)
            .await
            .unwrap(),
        2
    );
    assert_eq!(
        upgrade_active_challenge_hmacs(&pool, &verifier)
            .await
            .unwrap(),
        0
    );
    let rows: Vec<(i64, String, i64, Option<String>)> = sqlx::query_as(
        "SELECT id, answer_hmac, hmac_key_version, closed_at FROM challenge ORDER BY id",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows[0].0, first);
    assert_eq!(rows[0].2, 1);
    assert!(
        rows[0].1 == verifier.answer_hmac_for_expression("7 + 5 - 3").unwrap(),
        "first challenge was not re-signed with the current key"
    );
    assert_eq!(rows[1].0, second);
    assert_eq!(rows[1].2, 1);
    assert_eq!(
        rows[2],
        (
            closed,
            "closed".to_owned(),
            0,
            Some("2026-08-30T00:01:00Z".to_owned())
        )
    );
    assert_eq!(rows[3], (current, "current".to_owned(), 1, None));
}

#[tokio::test]
async fn malformed_or_unknown_source_rolls_back_the_complete_batch() {
    for (expression, version, expected_unknown) in
        [("not canonical", 0, None), ("7 + 5 - 3", 2, Some(2))]
    {
        let (_directory, _url, pool) = database().await;
        insert_challenge(&pool, 200, "7 + 5 - 3", "first", 0, false).await;
        insert_challenge(&pool, 201, expression, "second", version, false).await;
        let error = upgrade_active_challenge_hmacs(&pool, &current_verifier())
            .await
            .unwrap_err();
        match expected_unknown {
            Some(expected) => assert!(matches!(
                error,
                ChallengeKeyUpgradeError::UnknownSourceVersion(actual) if actual == expected
            )),
            None => assert!(matches!(
                error,
                ChallengeKeyUpgradeError::InvalidExpression(_)
            )),
        }
        let rows: Vec<(String, i64)> =
            sqlx::query_as("SELECT answer_hmac, hmac_key_version FROM challenge ORDER BY id")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(
            rows,
            vec![("first".to_owned(), 0), ("second".to_owned(), version)]
        );
    }
}

#[tokio::test]
async fn legacy_signer_and_stale_compare_and_swap_are_rejected() {
    let (_directory, _url, pool) = database().await;
    let challenge = insert_challenge(&pool, 300, "7 + 5 - 3", "current", 1, false).await;
    let legacy = ArithmeticVerifier::new_with_key_version(
        StdRng::seed_from_u64(3),
        SecretSlice::from(b"legacy".to_vec()),
        0,
    );
    assert!(matches!(
        upgrade_active_challenge_hmacs(&pool, &legacy).await,
        Err(ChallengeKeyUpgradeError::WrongSignerVersion(0))
    ));

    let mut uow = UnitOfWork::begin_immediate(&pool).await.unwrap();
    assert!(matches!(
        replace_challenge_hmac(&mut uow, challenge, 0, 1, "replacement").await,
        Err(StorageError::ConcurrentModification)
    ));
    uow.rollback().await.unwrap();
}

#[tokio::test]
async fn second_update_failure_rolls_back_the_first() {
    let (_directory, _url, pool) = database().await;
    let first = insert_challenge(&pool, 400, "7 + 5 - 3", "first", 0, false).await;
    let second = insert_challenge(&pool, 401, "2 + 3 × 4", "second", 0, false).await;
    assert!(second > first);
    sqlx::query(&format!(
        "CREATE TRIGGER abort_second_upgrade BEFORE UPDATE OF answer_hmac ON challenge
         WHEN OLD.id = {second} BEGIN SELECT RAISE(ABORT, 'rejected'); END"
    ))
    .execute(&pool)
    .await
    .unwrap();

    assert!(matches!(
        upgrade_active_challenge_hmacs(&pool, &current_verifier()).await,
        Err(ChallengeKeyUpgradeError::Storage(StorageError::Database(_)))
    ));
    let rows: Vec<(String, i64)> =
        sqlx::query_as("SELECT answer_hmac, hmac_key_version FROM challenge ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        vec![("first".to_owned(), 0), ("second".to_owned(), 0)]
    );
}

#[tokio::test]
async fn concurrent_post_migration_upgrades_serialize() {
    let (_directory, url, first_pool) = database().await;
    insert_challenge(&first_pool, 500, "7 + 5 - 3", "first", 0, false).await;
    insert_challenge(&first_pool, 501, "2 + 3 × 4", "second", 0, false).await;
    let second_pool = connect(&url).await.unwrap();
    let first_verifier = current_verifier();
    let second_verifier = current_verifier();

    let (first, second) = tokio::join!(
        upgrade_active_challenge_hmacs(&first_pool, &first_verifier),
        upgrade_active_challenge_hmacs(&second_pool, &second_verifier)
    );
    let mut counts = [first.unwrap(), second.unwrap()];
    counts.sort_unstable();
    assert_eq!(counts, [0, 2]);
}
