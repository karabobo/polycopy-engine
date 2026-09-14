-- Optional fixed-shares BUY sizing, per Leader.
--
-- Some Leaders (fast, back-and-forth "crossing" traders on the same token)
-- are better copied by matching a constant share count per trade than by a
-- USDC notional cap: at varying prices, a notional cap produces a different
-- share count on every fill, so a Leader who nets flat over repeated
-- buy/sell cycles leaves this account with residual, unbalanced shares. A
-- fixed share target keeps every copied fill numerically comparable, so the
-- account's own position tracks the Leader's flat/imbalanced state instead
-- of drifting from it.
--
-- Optional and additive: a Leader with no value here is unaffected and
-- keeps using max_order_notional exactly as before. Sizing (which field
-- wins, and how a fixed-shares budget still respects available collateral
-- and the per-order USDC ceiling) is execute.rs's decision, not this
-- migration's -- this column only makes the value available to size with.

ALTER TABLE leader_policy ADD COLUMN max_order_shares TEXT;
