CREATE TEMP TABLE outbox_action_sequence (
    value INTEGER NOT NULL
);

INSERT INTO outbox_action_sequence (value)
SELECT seq FROM sqlite_sequence WHERE name = 'outbox_action';

DROP INDEX outbox_due_idx;
ALTER TABLE outbox_action RENAME TO outbox_action_before_private_message;

CREATE TABLE outbox_action (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_update_id INTEGER NOT NULL,
    connection_id TEXT,
    chat_id INTEGER,
    action_type TEXT NOT NULL CHECK (
        action_type IN (
            'SEND_CHALLENGE',
            'EDIT_CHALLENGE',
            'READ_BUSINESS_MESSAGE',
            'DELETE_BUSINESS_MESSAGES',
            'SEND_OWNER_MESSAGE',
            'SEND_PRIVATE_MESSAGE',
            'PROPOSED_DESTRUCTIVE_ACTION'
        )
    ),
    payload_json TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (
        status IN ('PENDING', 'RETRY', 'SUCCEEDED', 'UNCERTAIN', 'PERMANENT_FAILURE')
    ),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    claimed_at TEXT,
    FOREIGN KEY (source_update_id) REFERENCES processed_update(update_id)
        ON DELETE CASCADE
);

INSERT INTO outbox_action (
    id, source_update_id, connection_id, chat_id, action_type, payload_json,
    idempotency_key, status, attempts, next_attempt_at, last_error, created_at,
    updated_at, claimed_at
)
SELECT
    id, source_update_id, connection_id, chat_id, action_type, payload_json,
    idempotency_key, status, attempts, next_attempt_at, last_error, created_at,
    updated_at, claimed_at
FROM outbox_action_before_private_message;

DROP TABLE outbox_action_before_private_message;

DELETE FROM sqlite_sequence WHERE name = 'outbox_action';
INSERT INTO sqlite_sequence (name, seq)
SELECT 'outbox_action', value FROM outbox_action_sequence;
DROP TABLE outbox_action_sequence;

CREATE INDEX outbox_due_idx
    ON outbox_action(status, next_attempt_at, id);
