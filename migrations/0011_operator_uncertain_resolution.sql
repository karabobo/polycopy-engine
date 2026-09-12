-- An operator may close an uncertain persistent submission only after a
-- fresh authenticated trade-history lookup found no exact prepared envelope
-- and the operator explicitly records that no-fill decision.  This state is
-- intentionally distinct from released_pre_boundary: the HTTP request may
-- have crossed the venue boundary, so it must not be represented as a local
-- pre-submit failure.
--
-- SQLite cannot extend the CHECK constraint in place.  Preserve every
-- existing reservation (including the per-leader denormalization from 0010),
-- then recreate both indexed access paths.

CREATE TABLE persistent_budget_reservations_new (
    id INTEGER PRIMARY KEY,
    order_attempt_id INTEGER NOT NULL UNIQUE REFERENCES order_attempts(id),
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    amount_usdc TEXT NOT NULL,
    reserved_at TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'reserved' CHECK (state IN (
        'reserved',
        'released_pre_boundary',
        'released_operator_no_fill'
    )),
    release_reason TEXT,
    released_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    leader_id INTEGER REFERENCES leader_config(id)
);

INSERT INTO persistent_budget_reservations_new (
    id, order_attempt_id, account_id, amount_usdc, reserved_at, state,
    release_reason, released_at, created_at, leader_id
)
SELECT
    id, order_attempt_id, account_id, amount_usdc, reserved_at, state,
    release_reason, released_at, created_at, leader_id
FROM persistent_budget_reservations;

DROP TABLE persistent_budget_reservations;
ALTER TABLE persistent_budget_reservations_new RENAME TO persistent_budget_reservations;

CREATE INDEX persistent_budget_reservations_account_window
    ON persistent_budget_reservations(account_id, state, reserved_at);

CREATE INDEX persistent_budget_reservations_leader_window
    ON persistent_budget_reservations(leader_id, state, reserved_at);
