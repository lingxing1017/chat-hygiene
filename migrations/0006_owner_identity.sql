CREATE TABLE owner_identity (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'UNCLAIMED', 'CLAIMED')),
    owner_user_id INTEGER,
    owner_chat_id INTEGER,
    owner_chat_source TEXT,
    connection_floor_established_at INTEGER,
    bound_at TEXT,
    CHECK (
        (
            state IN ('PENDING', 'UNCLAIMED')
            AND owner_user_id IS NULL
            AND owner_chat_id IS NULL
            AND owner_chat_source IS NULL
            AND connection_floor_established_at IS NULL
            AND bound_at IS NULL
        )
        OR
        (
            state = 'CLAIMED'
            AND typeof(owner_user_id) = 'integer'
            AND owner_user_id > 0
            AND typeof(owner_chat_id) = 'integer'
            AND owner_chat_id > 0
            AND owner_chat_source IN (
                'LEGACY_FALLBACK', 'CLAIM', 'BUSINESS_CONNECTION', 'PRIVATE_MESSAGE'
            )
            AND (
                connection_floor_established_at IS NULL
                OR (
                    typeof(connection_floor_established_at) = 'integer'
                    AND connection_floor_established_at > 0
                )
            )
            AND typeof(bound_at) = 'text'
        )
    )
);

INSERT INTO owner_identity (
    singleton,
    state,
    owner_user_id,
    owner_chat_id,
    owner_chat_source,
    connection_floor_established_at,
    bound_at
)
VALUES (1, 'PENDING', NULL, NULL, NULL, NULL, NULL);
