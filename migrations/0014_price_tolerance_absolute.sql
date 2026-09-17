-- Slippage as an absolute price offset, not a fraction of the leader's price.
--
-- price_tolerance_bps scales with the price, which is the wrong shape for a
-- book quoted in fixed ticks. On a 0.01 tick a 2% tolerance is 0.006 at a
-- price of 0.29 -- not enough to move the limit by even one tick, so the
-- order is identical to a zero-tolerance order -- while the same 2% is 0.013
-- at 0.66. The tolerance therefore did the least exactly where the copied
-- leaders trade most. Measured 2026-09-17 across 30 FAK buys rejected for no
-- matching liquidity, a 2% tolerance could move the limit one tick on 5 of
-- them; a flat 0.02 moves it two ticks on all 30.
--
-- The effective tolerance is the larger of the two, so a configuration that
-- sets only bps keeps exactly the behaviour it had before this column
-- existed, and '0' leaves every existing row unchanged.
ALTER TABLE leader_policy
    ADD COLUMN price_tolerance_abs TEXT NOT NULL DEFAULT '0';
