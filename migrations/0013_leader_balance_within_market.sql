-- Optional same-market balancing, per Leader.
--
-- Some Leaders trade both outcomes of the same market (buy "Up", later buy
-- "Down" in the same condition) rather than a clean buy/sell cycle on one
-- token. When this account already holds a position in that market's other
-- outcome for this Leader, sizing the new side to match the held quantity
-- (rather than a fresh notional- or fixed-shares-derived amount) keeps this
-- account's own two-sided exposure numerically balanced, mirroring however
-- balanced or lopsided the Leader's own crossing behavior is.
--
-- Optional and additive: a Leader with this unset (0) is unaffected and
-- keeps sizing from max_order_shares / max_order_notional exactly as
-- before. Whether/how it takes priority over those is execute.rs's
-- decision, not this migration's -- see
-- docs/fixed-shares-sizing-handoff.md.

ALTER TABLE leader_policy ADD COLUMN balance_within_market INTEGER NOT NULL DEFAULT 0
    CHECK (balance_within_market IN (0, 1));
