-- Per-Leader rolling budget.
--
-- The account-level budget is first-come-first-served: whichever Leader fires
-- first consumes it, and the rest of the day's signals are dropped. That makes
-- the clock, not the operator, choose which Leaders actually trade -- and the
-- resulting fills are the sample used to judge those same Leaders. A run that
-- exhausted its budget mid-day was measuring its own scheduling.
--
-- Per-Leader limits are optional. A Leader with no budget of its own stays
-- bound only by the account ceiling, so applying this migration changes no
-- existing behaviour until an operator sets one.

ALTER TABLE leader_policy ADD COLUMN rolling_budget_usdc TEXT;
ALTER TABLE leader_policy ADD COLUMN budget_window_seconds INTEGER;

-- Denormalized onto the reservation rather than reached through
-- order_attempts -> copy_intents. The existing
-- persistent_budget_reservations_account_window index shows the shape this
-- lookup is expected to have: it runs inside the BEGIN IMMEDIATE transaction
-- before every submission, so it must stay an indexed range scan.
ALTER TABLE persistent_budget_reservations ADD COLUMN leader_id INTEGER
    REFERENCES leader_config(id);

UPDATE persistent_budget_reservations
SET leader_id = (
    SELECT ci.leader_id
    FROM order_attempts oa
    JOIN copy_intents ci ON ci.id = oa.intent_id
    WHERE oa.id = persistent_budget_reservations.order_attempt_id
)
WHERE leader_id IS NULL;

CREATE INDEX persistent_budget_reservations_leader_window
    ON persistent_budget_reservations(leader_id, state, reserved_at);
