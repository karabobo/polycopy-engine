-- Phase 3: transactional intent planning. See
-- docs/COPY_ENGINE_BLUEPRINT.md section 8.

-- "Every policy must define: maximum signal age and decision deadline;
-- price formula and side-specific tolerance bound; tick rounding direction,
-- valid price range, and FAK-only behavior in v1; maximum order notional
-- and leader minimum trade size; and market closed/resolved/invalid
-- handling." One row per leader (v1 is a single account, so policy is not
-- separately scoped per account).
CREATE TABLE leader_policy (
    leader_id INTEGER PRIMARY KEY REFERENCES leader_config(id),
    -- An event older than this (occurred_at vs. now) at planning time is a
    -- durable rejection, never a current-market order.
    max_signal_age_seconds INTEGER NOT NULL CHECK (max_signal_age_seconds > 0),
    -- How long after an intent is created the lane may still act on it.
    decision_window_seconds INTEGER NOT NULL CHECK (decision_window_seconds > 0),
    -- Allowed price movement from the leader's own trade price, in basis
    -- points, before the lane's later price-bound calculation (Phase 4).
    price_tolerance_bps INTEGER NOT NULL CHECK (price_tolerance_bps >= 0),
    tick_size TEXT NOT NULL,
    min_price TEXT NOT NULL DEFAULT '0.01',
    max_price TEXT NOT NULL DEFAULT '0.99',
    max_order_notional TEXT NOT NULL,
    -- A leader trade smaller than this is not copied at all.
    min_leader_trade_size TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

-- The durable planner cursor: "insert-or-ignore copy_intents(event_id,
-- account_id) ... update the planner cursor ... commit" happens in one
-- transaction per forwarded event (blueprint section 8), so a crash between
-- intent insertion and cursor update rolls back both together. One
-- singleton row per account; v1 has exactly one account.
CREATE TABLE planner_cursor (
    account_id INTEGER PRIMARY KEY REFERENCES accounts(id),
    last_event_id INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
