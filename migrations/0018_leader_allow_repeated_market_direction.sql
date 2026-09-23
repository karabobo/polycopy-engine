-- 0018_leader_allow_repeated_market_direction.sql
--
-- Opt-out for the account-wide market_direction_order_locks safety backstop,
-- scoped per leader. Same pattern as migrations/0013_leader_balance_within_market.
-- Default 0 leaves every existing leader on the original behaviour: a copy of
-- YES makes any later copy of YES for the same account/condition auditable as
-- "market direction already has a copy order for this account".
--
-- When 1, plan.rs's plan_next_batch_with_limit skips the
-- market_direction_order_locks INSERT for events from this leader. The lock
-- is account-wide by design -- removing it for one leader is a real safety
-- backstop removal, not a per-leader relaxation: every same-direction signal
-- from that leader becomes a new, separately-sized copy order. The account
-- rolling_budget_usdc / budget_window_seconds cap still bounds total spend,
-- but a leader who fires several same-direction legs in one market now
-- consumes ~(legs) x (their own size x ratio) of the budget window on a
-- single market rather than one fixed bite. Any leader that flips this on
-- must re-confirm rolling_budget_usdc / budget_window_seconds still fit the
-- new behaviour; this comment is the audit entry point.

ALTER TABLE leader_policy
    ADD COLUMN allow_repeated_market_direction INTEGER NOT NULL DEFAULT 0
    CHECK (allow_repeated_market_direction IN (0, 1));
