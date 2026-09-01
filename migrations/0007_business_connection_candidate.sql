ALTER TABLE business_connection
ADD COLUMN connection_established_at INTEGER
CHECK (
    connection_established_at IS NULL
    OR (
        typeof(connection_established_at) = 'integer'
        AND connection_established_at > 0
    )
);

ALTER TABLE business_connection
ADD COLUMN state_revision INTEGER NOT NULL DEFAULT 0
CHECK (typeof(state_revision) = 'integer' AND state_revision >= 0);

ALTER TABLE business_connection
ADD COLUMN reconciliation_state TEXT NOT NULL DEFAULT 'CONFIRMED'
CHECK (reconciliation_state IN ('PENDING', 'CONFIRMED'));

CREATE TABLE business_connection_candidate (
    connection_id TEXT PRIMARY KEY
        CHECK (typeof(connection_id) = 'text' AND length(connection_id) > 0),
    business_user_id INTEGER NOT NULL
        CHECK (typeof(business_user_id) = 'integer' AND business_user_id > 0),
    user_chat_id INTEGER
        CHECK (
            user_chat_id IS NULL
            OR (typeof(user_chat_id) = 'integer' AND user_chat_id > 0)
        ),
    rights_json TEXT NOT NULL
        CHECK (
            typeof(rights_json) = 'text'
            AND json_valid(rights_json)
            AND json_type(rights_json, '$.can_reply') IN ('true', 'false')
            AND json_type(rights_json, '$.can_read_messages') IN ('true', 'false')
            AND json_type(rights_json, '$.can_delete_sent_messages') IN ('true', 'false')
            AND json_type(rights_json, '$.can_delete_all_messages') IN ('true', 'false')
        ),
    enabled INTEGER NOT NULL
        CHECK (typeof(enabled) = 'integer' AND enabled IN (0, 1)),
    connection_established_at INTEGER NOT NULL
        CHECK (
            typeof(connection_established_at) = 'integer'
            AND connection_established_at > 0
        ),
    state_revision INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(state_revision) = 'integer' AND state_revision >= 0),
    observed_at TEXT NOT NULL CHECK (typeof(observed_at) = 'text')
);

CREATE INDEX business_connection_candidate_user_idx
    ON business_connection_candidate(
        business_user_id,
        enabled,
        connection_established_at
    );

CREATE INDEX business_connection_candidate_retention_idx
    ON business_connection_candidate(observed_at, connection_established_at);

CREATE TABLE business_connection_candidate_guard (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    overflow_established_at INTEGER
        CHECK (
            overflow_established_at IS NULL
            OR (
                typeof(overflow_established_at) = 'integer'
                AND overflow_established_at > 0
            )
        ),
    state_revision INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(state_revision) = 'integer' AND state_revision >= 0),
    updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text')
);

INSERT INTO business_connection_candidate_guard (
    singleton,
    overflow_established_at,
    state_revision,
    updated_at
)
VALUES (1, NULL, 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

CREATE TABLE telegram_reconciliation_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'READY', 'AUTH_FAILED')),
    state_revision INTEGER NOT NULL DEFAULT 0
        CHECK (typeof(state_revision) = 'integer' AND state_revision >= 0),
    updated_at TEXT NOT NULL CHECK (typeof(updated_at) = 'text')
);

INSERT INTO telegram_reconciliation_state (
    singleton,
    state,
    state_revision,
    updated_at
)
VALUES (1, 'READY', 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));
