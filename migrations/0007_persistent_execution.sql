-- Persistent copy-execution mode. This state is separate from the bounded
-- copy_run path: it adds an account-level execution fuse plus an append-only
-- rolling-budget ledger that is written before any persistent HTTP order
-- submission may cross the venue boundary.

ALTER TABLE copy_intents ADD COLUMN planned_notional_usdc TEXT;

CREATE TABLE persistent_execution_config (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    enabled INTEGER NOT NULL CHECK (enabled IN (0, 1)),
    allowed_leader_ids TEXT NOT NULL,
    max_order_notional_usdc TEXT NOT NULL,
    rolling_budget_usdc TEXT NOT NULL,
    budget_window_seconds INTEGER NOT NULL,
    tick_seconds INTEGER NOT NULL,
    backfill_every_seconds INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE persistent_budget_reservations (
    id INTEGER PRIMARY KEY,
    order_attempt_id INTEGER NOT NULL UNIQUE REFERENCES order_attempts(id),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    amount_usdc TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'reserved' CHECK (state IN ('reserved', 'released_pre_boundary')),
    release_reason TEXT,
    released_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX persistent_budget_reservations_account_window
    ON persistent_budget_reservations(account_id, state, reserved_at);

CREATE TABLE persistent_execution_fuse (
    account_id INTEGER PRIMARY KEY REFERENCES accounts(id),
    paused_at TEXT NOT NULL,
    reason TEXT NOT NULL,
    actor_source TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
