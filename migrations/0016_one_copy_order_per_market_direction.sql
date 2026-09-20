-- 0015 was intentionally conservative and keyed its lock only by condition.
-- A binary market's two outcome tokens are distinct directions, however: a
-- copy of YES must not silently prohibit a later copy of NO. Keep the old
-- applied migration immutable and introduce the correctly-scoped lock table.
CREATE TABLE market_direction_order_locks (
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    intent_id INTEGER NOT NULL UNIQUE REFERENCES copy_intents(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (account_id, condition_id, token_id)
);

-- Preserve every already-submitted or still-runnable direction, rather than
-- inheriting 0015's one-token-per-condition historical collapse.
INSERT OR IGNORE INTO market_direction_order_locks (account_id, condition_id, token_id, intent_id)
SELECT ci.account_id, le.condition_id, le.token_id, MIN(ci.id)
FROM copy_intents ci
JOIN leader_events le ON le.id = ci.event_id
WHERE EXISTS (SELECT 1 FROM order_attempts oa WHERE oa.intent_id = ci.id)
   OR ci.status IN ('pending', 'in_progress', 'partially_filled', 'needs_reconcile')
GROUP BY ci.account_id, le.condition_id, le.token_id;
