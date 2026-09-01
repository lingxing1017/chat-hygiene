ALTER TABLE challenge
ADD COLUMN hmac_key_version INTEGER NOT NULL DEFAULT 0
CHECK (hmac_key_version >= 0);
