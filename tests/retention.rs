mod common;

use chathygiene::processing::ProcessingEngine;
use chathygiene::retention::RetentionService;
use chrono::Duration;

#[tokio::test]
async fn expires_challenges_and_temporary_blocks_at_exact_boundary() {
    let (_directory, pool) = common::processing_database().await;
    let expires_at = common::at("2026-07-14T00:02:00Z");
    let created_at = expires_at - Duration::minutes(2);
    let mut engine = ProcessingEngine::new(
        pool.clone(),
        common::MutableDetector::new(common::DetectorMode::Allow),
        common::FixedVerifier,
        common::TestClock::new(created_at),
        true,
    );
    engine
        .process(1, common::inbound(1001, 10, Some("hello"), created_at))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE challenge SET prompt_message_id = 900, delivery_status = 'SENT'
         WHERE chat_id = 1001",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at,
          block_expires_at, block_reason)
         VALUES ('business-1', 1002, 1002, 'TEMP_SOFT_BLOCKED', ?, ?, ?, 'verification_exhausted')",
    )
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO conversation
         (connection_id, chat_id, user_id, state, created_at, updated_at)
         VALUES ('business-1', 1003, 1003, 'ACTIVE', ?, ?)",
    )
    .bind(created_at.to_rfc3339())
    .bind(created_at.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO challenge
         (connection_id, chat_id, expression, answer_hmac, created_at, expires_at,
          attempts_used, max_attempts, delivery_status)
         VALUES ('business-1', 1003, '1 + 1 - 1', 'hash', ?, ?, 0, 3, 'PENDING')",
    )
    .bind(created_at.to_rfc3339())
    .bind(expires_at.to_rfc3339())
    .execute(&pool)
    .await
    .unwrap();

    let clock = common::TestClock::new(expires_at - Duration::nanoseconds(1));
    let service = RetentionService::new(clock.clone());
    let before = service.expire_due_state(&pool).await.unwrap();
    assert_eq!(before.expired_challenges, 0);
    assert_eq!(before.expired_temporary_blocks, 0);

    clock.set(expires_at);
    let at_boundary = service.expire_due_state(&pool).await.unwrap();
    assert_eq!(at_boundary.expired_challenges, 2);
    assert_eq!(at_boundary.expired_temporary_blocks, 1);
    let states: Vec<(i64, String)> =
        sqlx::query_as("SELECT chat_id, state FROM conversation ORDER BY chat_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        states,
        vec![
            (1001, "NEW".to_owned()),
            (1002, "NEW".to_owned()),
            (1003, "ACTIVE".to_owned())
        ]
    );
    let edits: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM outbox_action WHERE action_type = 'EDIT_CHALLENGE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(edits, 1);
    let repeated = service.expire_due_state(&pool).await.unwrap();
    assert_eq!(repeated.expired_challenges, 0);
    assert_eq!(repeated.expired_temporary_blocks, 0);
}

#[tokio::test]
async fn purges_bounded_history_without_losing_protected_state() {
    let (_directory, pool) = common::processing_database().await;
    let now = common::at("2026-07-14T12:00:00Z");
    seed_retention_history(&pool, now).await;
    let service = RetentionService::new(common::TestClock::new(now));

    let report = service.purge_history(&pool).await.unwrap();
    assert_eq!(report.purged_ledger_messages, 1);
    assert_eq!(report.purged_audit_events, 2);
    assert_eq!(report.purged_outbox_actions, 3);
    assert_eq!(report.purged_processed_updates, 3);

    let ledger_ids: Vec<i64> =
        sqlx::query_scalar("SELECT message_id FROM message_ledger ORDER BY message_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(ledger_ids, vec![2, 3, 4]);
    let audit_kinds: Vec<String> =
        sqlx::query_scalar("SELECT event_kind FROM audit_event ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(audit_kinds, vec!["PERSISTENT_KEEP", "RECENT"]);
    let outbox_statuses: Vec<String> =
        sqlx::query_scalar("SELECT status FROM outbox_action ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(outbox_statuses, vec!["PENDING", "RETRY", "SUCCEEDED"]);
    let update_ids: Vec<i64> =
        sqlx::query_scalar("SELECT update_id FROM processed_update ORDER BY update_id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(update_ids, vec![2, 5, 6, 7, 8]);
    let sample_count: i64 = sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM spam_sample) + (SELECT COUNT(*) FROM ham_sample)",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(sample_count, 2);
    let rule_set_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rule_set")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(rule_set_count, 1);
    let persistent: (String, String) =
        sqlx::query_as("SELECT state, block_reason FROM conversation WHERE chat_id = 2002")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        persistent,
        ("SPAM_SOFT_BLOCKED".to_owned(), "spam".to_owned())
    );

    let repeated = service.purge_history(&pool).await.unwrap();
    assert_eq!(repeated.total(), 0);
}

async fn seed_retention_history(pool: &sqlx::SqlitePool, now: chrono::DateTime<chrono::Utc>) {
    let old_ledger = now - Duration::hours(72);
    let recent_ledger = old_ledger + Duration::nanoseconds(1);
    for (chat_id, state, reason) in [
        (2001, "ACTIVE", None),
        (2002, "SPAM_SOFT_BLOCKED", Some("spam")),
    ] {
        sqlx::query(
            "INSERT INTO conversation
             (connection_id, chat_id, user_id, state, created_at, updated_at, block_reason)
             VALUES ('business-1', ?, ?, ?, ?, ?, ?)",
        )
        .bind(chat_id)
        .bind(chat_id)
        .bind(state)
        .bind((now - Duration::days(365)).to_rfc3339())
        .bind(now.to_rfc3339())
        .bind(reason)
        .execute(pool)
        .await
        .unwrap();
    }
    for (message_id, sender, manual, sent_at) in [
        (1, "EXTERNAL", false, old_ledger),
        (2, "EXTERNAL", false, old_ledger),
        (3, "EXTERNAL", false, recent_ledger),
        (4, "OWNER", true, now - Duration::days(365)),
    ] {
        sqlx::query(
            "INSERT INTO message_ledger
             (connection_id, chat_id, message_id, direction, sender_kind,
              manual_owner_reply, sent_at)
             VALUES ('business-1', 2001, ?, ?, ?, ?, ?)",
        )
        .bind(message_id)
        .bind(if sender == "EXTERNAL" {
            "INBOUND"
        } else {
            "OUTBOUND"
        })
        .bind(sender)
        .bind(manual)
        .bind(sent_at.to_rfc3339())
        .execute(pool)
        .await
        .unwrap();
    }
    seed_updates_and_outbox(pool, now).await;
    seed_audits_and_samples(pool, now).await;
}

async fn seed_updates_and_outbox(pool: &sqlx::SqlitePool, now: chrono::DateTime<chrono::Utc>) {
    let old_update = now - Duration::days(7);
    for update_id in 1..=8 {
        let received_at = if update_id == 8 {
            old_update + Duration::nanoseconds(1)
        } else {
            old_update
        };
        sqlx::query(
            "INSERT INTO processed_update
             (update_id, event_type, event_json, status, received_at, applied_at)
             VALUES (?, 'test', '{}', 'APPLIED', ?, ?)",
        )
        .bind(update_id)
        .bind(received_at.to_rfc3339())
        .bind(received_at.to_rfc3339())
        .execute(pool)
        .await
        .unwrap();
    }
    let outbox_rows = [
        (1, 1, "SUCCEEDED", now - Duration::days(30), "{}"),
        (
            2,
            2,
            "PENDING",
            now - Duration::days(90),
            "{\"message_ids\":[2]}",
        ),
        (3, 3, "PERMANENT_FAILURE", now - Duration::days(90), "{}"),
        (4, 4, "UNCERTAIN", now - Duration::days(90), "{}"),
        (
            5,
            5,
            "RETRY",
            now - Duration::days(90),
            "{\"message_ids\":[]}",
        ),
        (
            6,
            6,
            "SUCCEEDED",
            now - Duration::days(30) + Duration::nanoseconds(1),
            "{}",
        ),
    ];
    for (id, update_id, status, updated_at, payload) in outbox_rows {
        sqlx::query(
            "INSERT INTO outbox_action
             (id, source_update_id, connection_id, chat_id, action_type,
              payload_json, idempotency_key, status, attempts, created_at, updated_at)
             VALUES (?, ?, 'business-1', 2001, 'DELETE_BUSINESS_MESSAGES',
                     ?, ?, ?, 0, ?, ?)",
        )
        .bind(id)
        .bind(update_id)
        .bind(payload)
        .bind(format!("retention-{id}"))
        .bind(status)
        .bind(updated_at.to_rfc3339())
        .bind(updated_at.to_rfc3339())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_audits_and_samples(pool: &sqlx::SqlitePool, now: chrono::DateTime<chrono::Utc>) {
    let old_audit = now - Duration::days(90);
    for (source, chat_id, kind, occurred_at) in [
        (
            Some(7),
            Some(2002),
            "PERSISTENT_OLD",
            old_audit - Duration::seconds(1),
        ),
        (Some(7), Some(2002), "PERSISTENT_KEEP", old_audit),
        (None, Some(2001), "ORDINARY_OLD", old_audit),
        (
            None,
            Some(2001),
            "RECENT",
            old_audit + Duration::nanoseconds(1),
        ),
    ] {
        sqlx::query(
            "INSERT INTO audit_event
             (source_update_id, connection_id, chat_id, event_kind, occurred_at)
             VALUES (?, 'business-1', ?, ?, ?)",
        )
        .bind(source)
        .bind(chat_id)
        .bind(kind)
        .bind(occurred_at.to_rfc3339())
        .execute(pool)
        .await
        .unwrap();
    }
    for table in ["spam_sample", "ham_sample"] {
        let query = format!(
            "INSERT INTO {table}
             (body, content_type, normalized_hash, labeled_at)
             VALUES ('body', 'text', 'hash', ?)"
        );
        sqlx::query(&query)
            .bind((now - Duration::days(365)).to_rfc3339())
            .execute(pool)
            .await
            .unwrap();
    }
    sqlx::query(
        "INSERT INTO rule_set
         (version, config_json, checksum_sha256, enabled, created_at)
         VALUES ('retention-test', '[]', 'checksum', 1, ?)",
    )
    .bind((now - Duration::days(365)).to_rfc3339())
    .execute(pool)
    .await
    .unwrap();
}
