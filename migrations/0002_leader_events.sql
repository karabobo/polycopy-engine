-- The canonical leader-event ledger and its raw source observations.
-- See docs/COPY_ENGINE_BLUEPRINT.md section 6 and section 3.1
-- ("leader_events is immutable canonical input").

CREATE TABLE leader_events (
    id INTEGER PRIMARY KEY,
    -- 'activity:' || activity_trade_id. The sole dedup key: Activity WS and
    -- Activity REST backfill both resolve to this same key for one trade,
    -- so INSERT OR IGNORE on this column is what makes replay/backfill
    -- overlap safe (see blueprint section 3.1 and Phase 2's acceptance).
    canonical_event_key TEXT NOT NULL UNIQUE,
    leader_id INTEGER NOT NULL REFERENCES leader_config(id),
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    outcome_index INTEGER NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    -- Decimal quantities are stored as exact-text, never REAL/FLOAT: SQLite
    -- has no arbitrary-precision numeric type, and float storage would
    -- silently reintroduce the exact rounding-error class this project's
    -- OrderReceipt (src/venue/receipt.rs) exists to avoid.
    size TEXT NOT NULL,
    price TEXT NOT NULL,
    tx_hash TEXT,
    -- When the trade actually happened (venue/on-chain timestamp), distinct
    -- from observed_at (when this system first saw it) -- both are needed
    -- for the activation/signal-age checks in later phases.
    occurred_at TEXT NOT NULL,
    observed_at TEXT NOT NULL,
    onchain_confirmed INTEGER NOT NULL DEFAULT 0 CHECK (onchain_confirmed IN (0, 1)),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX leader_events_leader_id ON leader_events(leader_id);
CREATE INDEX leader_events_occurred_at ON leader_events(occurred_at);

-- Every raw source payload is retained verbatim, never overwritten. Activity
-- WS and Activity backfill may each produce their own observation for the
-- same canonical event (multiple rows may share one leader_event_id); an
-- on-chain observation may stay unlinked (leader_event_id NULL) if it
-- cannot be related to a canonical event by an explicit source identifier
-- -- never by fuzzy field matching, which could silently create a second
-- executable event from confirmation-only data.
CREATE TABLE leader_event_observations (
    id INTEGER PRIMARY KEY,
    leader_event_id INTEGER REFERENCES leader_events(id),
    source TEXT NOT NULL CHECK (source IN ('activity_ws', 'activity_backfill', 'onchain_ws')),
    -- The trade/log identifier this observation's source assigns, used to
    -- explicitly link it to a canonical event -- not for fuzzy matching.
    source_identifier TEXT,
    payload TEXT NOT NULL,
    observed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX leader_event_observations_leader_event_id
    ON leader_event_observations(leader_event_id);

-- Prevents the same source from recording the identical trade/log twice
-- (e.g. a WS reconnect replaying recent messages), without blocking
-- multiple distinct sources from each observing the same canonical event.
CREATE UNIQUE INDEX leader_event_observations_unique_source_identifier
    ON leader_event_observations(source, source_identifier)
    WHERE source_identifier IS NOT NULL;
