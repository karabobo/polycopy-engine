-- "All paths reject occurred_at < activation_at" (blueprint section 7). A
-- leader with no activation_at yet is not being watched: every ingestion
-- path must reject its events until this is explicitly set, not treat NULL
-- as "no lower bound".
ALTER TABLE leader_config ADD COLUMN activation_at TEXT;
