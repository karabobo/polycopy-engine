-- Expand the `reconciliation_cases.case_type` CHECK constraint to accept
-- the orchestrator branches that have always written values outside the
-- original allowlist (migration 0003):
--
--   * `'local_submission_failure'` (orchestrate.rs:419, the SubmitError::Local
--     branch)
--   * `'blocked_recovery'` (orchestrate.rs:329, the walk_existing_attempt
--     RecoveryAction::Blocked branch)
--
-- Both branches were written by the orchestrator before this migration
-- existed, so any end-to-end test driving them failed at INSERT time
-- with a CHECK constraint violation. SQLite cannot ALTER an existing
-- CHECK in place, so the standard rename-and-copy dance is used: build
-- a new table with the expanded allowlist, copy every row, swap the
-- names, recreate the indexes.
--
-- This migration must be applied before any end-to-end test exercises
-- the SubmitError::Local orchestrator path.

CREATE TABLE reconciliation_cases_new (
    id INTEGER PRIMARY KEY,
    account_id INTEGER NOT NULL REFERENCES accounts(id),
    token_id TEXT NOT NULL,
    intent_id INTEGER REFERENCES copy_intents(id),
    order_attempt_id INTEGER REFERENCES order_attempts(id),
    case_type TEXT NOT NULL CHECK (case_type IN (
        'unknown_submission',
        'strict_query_failure',
        'balance_drift',
        'retry_exhausted',
        'local_submission_failure',
        'blocked_recovery',
        'other'
    )),
    detail TEXT,
    opened_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    resolved_at TEXT,
    resolution TEXT
);

INSERT INTO reconciliation_cases_new (
    id, account_id, token_id, intent_id, order_attempt_id,
    case_type, detail, opened_at, resolved_at, resolution
)
SELECT
    id, account_id, token_id, intent_id, order_attempt_id,
    case_type, detail, opened_at, resolved_at, resolution
FROM reconciliation_cases;

DROP TABLE reconciliation_cases;

ALTER TABLE reconciliation_cases_new RENAME TO reconciliation_cases;

CREATE INDEX reconciliation_cases_account_token
    ON reconciliation_cases(account_id, token_id);

CREATE INDEX reconciliation_cases_open_by_account_token
    ON reconciliation_cases(account_id, token_id)
    WHERE resolved_at IS NULL;