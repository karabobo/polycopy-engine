-- Phase 1 baseline: one copied account and its enabled leader aliases.
-- See docs/COPY_ENGINE_BLUEPRINT.md section 6 ("Core schema requirements").

CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL UNIQUE,
    -- The signing EOA's address, always lowercase (see leader_wallet_aliases
    -- below for why addresses are normalized this way throughout).
    signing_address TEXT NOT NULL CHECK (signing_address = LOWER(signing_address)),
    -- NULL only for signature_type='eoa', where the signer funds itself.
    funder_address TEXT CHECK (funder_address IS NULL OR funder_address = LOWER(funder_address)),
    signature_type TEXT NOT NULL
        CHECK (signature_type IN ('eoa', 'proxy', 'gnosis_safe', 'poly1271')),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE leader_config (
    id INTEGER PRIMARY KEY,
    label TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- A leader is watched under one or more on-chain addresses. Addresses are
-- stored lowercase (Ethereum addresses are case-insensitive; checksum
-- casing is a display convention only) so the uniqueness constraint below
-- can't be bypassed by casing alone.
CREATE TABLE leader_wallet_aliases (
    id INTEGER PRIMARY KEY,
    leader_id INTEGER NOT NULL REFERENCES leader_config(id),
    address TEXT NOT NULL CHECK (address = LOWER(address)),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX leader_wallet_aliases_leader_id ON leader_wallet_aliases(leader_id);

-- "Alias writes must reject an address that belongs to two enabled leaders
-- before persisting the configuration": at most one row for a given address
-- may be enabled at a time, so the same address can never be simultaneously
-- enabled under two different leaders (or twice under the same one).
CREATE UNIQUE INDEX leader_wallet_aliases_unique_enabled_address
    ON leader_wallet_aliases(address)
    WHERE enabled = 1;
