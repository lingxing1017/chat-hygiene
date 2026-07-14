CREATE TABLE business_connection (
    connection_id TEXT PRIMARY KEY,
    owner_user_id INTEGER NOT NULL UNIQUE CHECK (owner_user_id > 0),
    rights_json TEXT NOT NULL DEFAULT '{}',
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    updated_at TEXT NOT NULL
);

CREATE TABLE conversation (
    connection_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'NEW' CHECK (
        state IN (
            'NEW',
            'VERIFY_PENDING',
            'VERIFIED_WAITING_OWNER',
            'ACTIVE',
            'TEMP_SOFT_BLOCKED',
            'SPAM_SOFT_BLOCKED'
        )
    ),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    block_expires_at TEXT,
    block_reason TEXT,
    block_count INTEGER NOT NULL DEFAULT 0 CHECK (block_count >= 0),
    state_version INTEGER NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    PRIMARY KEY (connection_id, chat_id),
    FOREIGN KEY (connection_id) REFERENCES business_connection(connection_id)
        ON DELETE CASCADE
);

CREATE INDEX conversation_user_idx
    ON conversation(connection_id, user_id);
CREATE INDEX conversation_state_idx
    ON conversation(state, block_expires_at);

CREATE TABLE message_ledger (
    connection_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('INBOUND', 'OUTBOUND')),
    sender_kind TEXT NOT NULL CHECK (
        sender_kind IN ('EXTERNAL', 'OWNER', 'BUSINESS_BOT', 'IMPLICIT')
    ),
    manual_owner_reply INTEGER NOT NULL DEFAULT 0
        CHECK (manual_owner_reply IN (0, 1)),
    sent_at TEXT NOT NULL,
    deleted_at TEXT,
    eligible_for_deletion INTEGER NOT NULL DEFAULT 1
        CHECK (eligible_for_deletion IN (0, 1)),
    media_group_id TEXT,
    PRIMARY KEY (connection_id, chat_id, message_id),
    FOREIGN KEY (connection_id, chat_id)
        REFERENCES conversation(connection_id, chat_id) ON DELETE CASCADE
);

CREATE INDEX message_ledger_cleanup_idx
    ON message_ledger(connection_id, chat_id, deleted_at, eligible_for_deletion);
CREATE INDEX message_ledger_owner_reply_idx
    ON message_ledger(connection_id, chat_id, manual_owner_reply, deleted_at);

CREATE TABLE challenge (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    connection_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    expression TEXT NOT NULL,
    answer_hmac TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    attempts_used INTEGER NOT NULL DEFAULT 0 CHECK (attempts_used BETWEEN 0 AND 3),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts = 3),
    prompt_message_id INTEGER,
    delivery_status TEXT NOT NULL CHECK (
        delivery_status IN ('PENDING', 'SENT', 'UNCERTAIN', 'CLOSED')
    ),
    closed_at TEXT,
    FOREIGN KEY (connection_id, chat_id)
        REFERENCES conversation(connection_id, chat_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX challenge_one_active_idx
    ON challenge(connection_id, chat_id)
    WHERE closed_at IS NULL;
CREATE INDEX challenge_expiry_idx ON challenge(expires_at, closed_at);

CREATE TABLE processed_update (
    update_id INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    event_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('RECORDED', 'APPLIED', 'FAILED')),
    received_at TEXT NOT NULL,
    applied_at TEXT,
    error_code TEXT,
    error_message TEXT
);

CREATE INDEX processed_update_status_idx
    ON processed_update(status, update_id);

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
    FOREIGN KEY (source_update_id) REFERENCES processed_update(update_id)
        ON DELETE CASCADE
);

CREATE INDEX outbox_due_idx
    ON outbox_action(status, next_attempt_at, id);

CREATE TABLE audit_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_update_id INTEGER,
    connection_id TEXT,
    chat_id INTEGER,
    event_kind TEXT NOT NULL,
    state_before TEXT,
    state_after TEXT,
    score INTEGER CHECK (score BETWEEN 0 AND 100),
    reasons_json TEXT,
    rule_ids_json TEXT,
    normalized_hash TEXT,
    rule_version TEXT,
    error_code TEXT,
    error_message TEXT,
    occurred_at TEXT NOT NULL,
    FOREIGN KEY (source_update_id) REFERENCES processed_update(update_id)
        ON DELETE SET NULL
);

CREATE INDEX audit_event_chat_idx
    ON audit_event(connection_id, chat_id, occurred_at);
CREATE INDEX audit_event_hash_idx
    ON audit_event(normalized_hash, occurred_at);

CREATE TABLE spam_sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    body TEXT NOT NULL,
    content_type TEXT NOT NULL,
    normalized_hash TEXT NOT NULL,
    source_chat_id INTEGER,
    source_message_id INTEGER,
    labeled_at TEXT NOT NULL
);

CREATE TABLE ham_sample (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    body TEXT NOT NULL,
    content_type TEXT NOT NULL,
    normalized_hash TEXT NOT NULL,
    source_chat_id INTEGER,
    source_message_id INTEGER,
    labeled_at TEXT NOT NULL
);

CREATE TABLE rule_set (
    version TEXT PRIMARY KEY,
    config_json TEXT NOT NULL,
    checksum_sha256 TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 0 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX rule_set_one_enabled_idx
    ON rule_set(enabled) WHERE enabled = 1;

CREATE TABLE runtime_setting (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

INSERT INTO runtime_setting(key, value, updated_at)
VALUES ('destructive_mode', 'false', CURRENT_TIMESTAMP);
