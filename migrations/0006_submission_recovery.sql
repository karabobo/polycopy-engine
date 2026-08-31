-- Phase 5: the instant an immutable signed envelope is about to cross the
-- order HTTP boundary must itself be durable. A later read-only recovery uses
-- this timestamp only to bound the authenticated trade-history query; it does
-- not turn an empty result into proof that no order was sent.
ALTER TABLE order_attempts ADD COLUMN submission_started_at TEXT;

CREATE INDEX order_attempts_submission_recovery
    ON order_attempts(status, submission_started_at)
    WHERE status IN ('submitting', 'uncertain');
