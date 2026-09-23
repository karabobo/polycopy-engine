-- 0019_leader_maker_only.sql
--
-- Opt-in "skip the FAK, go straight to post-only GTD" execution mode,
-- scoped per leader. Same shape as
-- migrations/0013_leader_balance_within_market.
--
-- When 1, execute_one_intent_with_marker skips the FAK path entirely
-- (orchestrate/mod.rs fresh intent path AND prepare_new_attempt's retry
-- path, both of which currently call envelopes.prepare) and prepares a
-- post-only GTD BUY with a real-time best-ask price cap. Same pattern as
-- balance_within_market: default 0 leaves every existing leader on the
-- FAK-first path exactly as before.
--
-- Read by execute.rs's size_and_reserve (which loads PolicySnapshot per
-- intent) and copied onto SizedDecision so orchestrate/mod.rs branches on
-- a single already-in-scope field read rather than a second policy
-- round-trip.

ALTER TABLE leader_policy
    ADD COLUMN maker_only INTEGER NOT NULL DEFAULT 0
    CHECK (maker_only IN (0, 1));
