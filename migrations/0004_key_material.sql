CREATE TABLE key_material (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    key_version INTEGER NOT NULL CHECK (key_version > 0),
    state TEXT NOT NULL CHECK (state IN ('PENDING', 'READY')),
    master_seed BLOB,
    seed_checksum BLOB,
    initialized_at TEXT,
    telegram_bot_id INTEGER CHECK (
        telegram_bot_id IS NULL
        OR (typeof(telegram_bot_id) = 'integer' AND telegram_bot_id > 0)
    ),
    CHECK (
        (
            state = 'PENDING'
            AND master_seed IS NULL
            AND seed_checksum IS NULL
            AND initialized_at IS NULL
            AND telegram_bot_id IS NULL
        )
        OR
        (
            state = 'READY'
            AND typeof(master_seed) = 'blob'
            AND length(master_seed) = 32
            AND typeof(seed_checksum) = 'blob'
            AND length(seed_checksum) = 32
            AND initialized_at IS NOT NULL
        )
    )
);

INSERT INTO key_material (
    singleton,
    key_version,
    state,
    master_seed,
    seed_checksum,
    initialized_at,
    telegram_bot_id
)
VALUES (1, 1, 'PENDING', NULL, NULL, NULL, NULL);
