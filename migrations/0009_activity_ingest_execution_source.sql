-- Activity REST is an audit/recovery source, not a realtime order trigger.
-- Existing rows remain executable for backwards compatibility: they have
-- already either advanced the planner cursor or are protected by their signal
-- age. New REST-created rows explicitly set this to zero; a later matching WS
-- observation promotes the one canonical event to realtime-observed.
ALTER TABLE leader_events ADD COLUMN realtime_observed INTEGER NOT NULL DEFAULT 1
    CHECK (realtime_observed IN (0, 1));

CREATE INDEX leader_events_planner_realtime ON leader_events(id, realtime_observed);
