-- The copy-intent/order-attempt state machine, virtual lots, reconciliation
-- cases, and the durable scheduler record. See
-- docs/COPY_ENGINE_BLUEPRINT.md section 6 and section 3.1 (state model).
--
-- All decimal quantities are stored as exact text, never REAL, for the same
-- reason as leader_events (see migration 0002): SQLite has no
-- arbitrary-precision numeric type, and float storage would reintroduce the
-- rounding-error class OrderReceipt (src/venue/receipt.rs) exists to avoid.

-- One planned copy action derived from exactly one leader_event, for one
-- account. The unique index is what makes "insert-or-ignore
-- copy_intents(event_id, account_id)" (blueprint section 8) safe: replaying
-- an event must create no second intent.
CREATE TABLE copy_intents (
    id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES leader_events(id),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    leader_id INTEGER NOT NULL REFERENCES leader_config(id),
    token_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    -- The full policy snapshot this decision was made under, immutable once
    -- written, plus a hash for quick audit/equality checks without
    -- re-parsing the JSON. A later config change never retroactively
    -- changes an already-planned intent's behavior.
    config_snapshot_json TEXT NOT NULL,
    config_snapshot_hash TEXT NOT NULL,
    -- Set once by the lane (not the planner) because they depend on live
    -- account state and market data at execution time, not planning time.
    planned_qty TEXT,
    planned_price TEXT,
    tick_size TEXT,
    time_in_force TEXT CHECK (time_in_force IS NULL OR time_in_force IN ('FAK')),
    decision_deadline_at TEXT,
    -- A durable reservation, not a display field (blueprint section 3.2):
    -- the lane subtracts other intents' reserved_qty from the strict
    -- account balance before sizing a SELL, and excludes its own
    -- reservation so recovery doesn't shrink its original sale.
    reserved_qty TEXT NOT NULL DEFAULT '0',
    shard_scheme_version INTEGER NOT NULL,
    lane_count INTEGER NOT NULL,
    shard_id INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN (
        'pending', 'in_progress', 'partially_filled', 'completed',
        'rejected', 'cancelled', 'needs_reconcile', 'dead_letter'
    )),
    rejection_reason TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX copy_intents_unique_event_account ON copy_intents(event_id, account_id);
CREATE INDEX copy_intents_account_leader_token ON copy_intents(account_id, leader_id, token_id);
CREATE INDEX copy_intents_status ON copy_intents(status);

-- One signed-envelope attempt to fulfill one copy_intent.
--
-- Phase 0.5's live canary (docs/PHASE_0_5_CANARY_REPORT.md) confirmed two
-- things that directly shape this table:
--   1. `taking_amount`/`making_amount` (this project's filled_qty source)
--      are precise, but a BUY's requested `size` behaved as a notional cap,
--      not a literal share count -- filled_qty must always be read from the
--      venue's actual matched-quantity field, never assumed equal to
--      requested_qty. accounted_filled_qty exists specifically so a
--      duplicate receipt read can never double-apply that delta.
--   2. GET /data/order/{id} 404's immediately after a match and only
--      succeeds after an unmeasured indexing delay, and the field-based
--      listing fallback does not find a matched order at all. Until that
--      gap is closed, an `uncertain` attempt can only be queried and parked
--      -- never auto-resubmitted -- which is exactly what the
--      `status` values below encode (see blueprint section 3.1's
--      submission recovery matrix).
CREATE TABLE order_attempts (
    id INTEGER PRIMARY KEY,
    intent_id INTEGER NOT NULL REFERENCES copy_intents(id),
    attempt_number INTEGER NOT NULL,
    -- The exact serialized envelope for this attempt: salt, timestamp,
    -- expiration, payload, and signature, generated once and never rebuilt
    -- (blueprint invariant #5).
    envelope_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'prepared' CHECK (status IN (
        'prepared', 'submitting', 'uncertain', 'accepted', 'rejected', 'finalized', 'error'
    )),
    receipt_json TEXT,
    venue_order_id TEXT,
    venue_status TEXT,
    requested_qty TEXT NOT NULL,
    accepted_qty TEXT,
    filled_qty TEXT,
    remaining_qty TEXT,
    -- The durable, idempotent filled-quantity delta already applied to
    -- position_lots for this attempt. Only a newly accounted increase over
    -- this value may ever change a lot.
    accounted_filled_qty TEXT NOT NULL DEFAULT '0',
    failure_detail TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- Concurrent calls for one (intent_id, attempt_number) must persist one
-- identical envelope (blueprint Phase 5 acceptance).
CREATE UNIQUE INDEX order_attempts_unique_intent_attempt ON order_attempts(intent_id, attempt_number);
CREATE INDEX order_attempts_venue_order_id ON order_attempts(venue_order_id) WHERE venue_order_id IS NOT NULL;
CREATE INDEX order_attempts_status ON order_attempts(status);

-- The virtual lot for one (account, leader, token). Changed only by an
-- idempotent receipt-accounting transaction -- never by an aggregate
-- balance read (blueprint section 3.2: actual_balance == 0 is not proof a
-- particular SELL completed).
CREATE TABLE position_lots (
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    leader_id INTEGER NOT NULL REFERENCES leader_config(id),
    token_id TEXT NOT NULL,
    qty TEXT NOT NULL DEFAULT '0',
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (account_id, leader_id, token_id)
);

-- Drift, unknown-submission, and manual-resolution history. A strict venue
-- query error or an unresolved submission opens a case here and blocks
-- further work for that (account, token) key; it is never auto-resolved
-- from an aggregate balance.
CREATE TABLE reconciliation_cases (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    token_id TEXT NOT NULL,
    intent_id INTEGER REFERENCES copy_intents(id),
    order_attempt_id INTEGER REFERENCES order_attempts(id),
    case_type TEXT NOT NULL CHECK (case_type IN (
        'unknown_submission', 'strict_query_failure', 'balance_drift', 'retry_exhausted', 'other'
    )),
    detail TEXT,
    opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    resolved_at TEXT,
    resolution TEXT
);

CREATE INDEX reconciliation_cases_account_token ON reconciliation_cases(account_id, token_id);
CREATE INDEX reconciliation_cases_open_by_account_token
    ON reconciliation_cases(account_id, token_id)
    WHERE resolved_at IS NULL;

-- The durable scheduler configuration: a singleton row (id is fixed to 1).
-- A startup with a different lane count, shard algorithm, or scheme version
-- must refuse to run while any older non-terminal intent exists -- that
-- check belongs to the executor's startup code (not built yet; this table
-- only persists what it needs to check against).
CREATE TABLE execution_schedule (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    shard_scheme_version INTEGER NOT NULL,
    shard_algorithm TEXT NOT NULL,
    lane_count INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
