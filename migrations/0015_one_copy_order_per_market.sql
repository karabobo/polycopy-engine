-- An account may submit at most one copy intent for one immutable Polymarket
-- condition. The lock is acquired by the planner with the first executable
-- intent, before a signed envelope can be prepared. YES and NO are one market.
CREATE TABLE market_order_locks (
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    condition_id TEXT NOT NULL,
    intent_id INTEGER NOT NULL UNIQUE REFERENCES copy_intents(id),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (account_id, condition_id)
);

-- Preserve the invariant for existing orders. Planner-only rejections never
-- submitted an order and do not lock a market; attempts and runnable intents do.
INSERT OR IGNORE INTO market_order_locks (account_id, condition_id, intent_id)
SELECT ci.account_id, le.condition_id, MIN(ci.id)
FROM copy_intents ci
JOIN leader_events le ON le.id = ci.event_id
WHERE EXISTS (SELECT 1 FROM order_attempts oa WHERE oa.intent_id = ci.id)
   OR ci.status IN ('pending', 'in_progress', 'partially_filled', 'needs_reconcile')
GROUP BY ci.account_id, le.condition_id;
