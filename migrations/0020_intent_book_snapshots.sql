-- Diagnostic-only maker order-book snapshot. No foreign key changes to execution.
CREATE TABLE intent_book_snapshots (
    intent_id INTEGER NOT NULL REFERENCES copy_intents(id),
    fetched_at TEXT NOT NULL,
    best_bid TEXT,
    best_ask TEXT NOT NULL,
    ask_size_at_best TEXT NOT NULL,
    ask_size_leader_0 TEXT NOT NULL,
    ask_size_leader_2 TEXT NOT NULL,
    ask_size_leader_4 TEXT NOT NULL,
    ask_size_leader_6 TEXT NOT NULL,
    ask_size_leader_10 TEXT NOT NULL,
    leader_price TEXT NOT NULL,
    limit_price TEXT NOT NULL,
    maker_price TEXT NOT NULL,
    PRIMARY KEY (intent_id, fetched_at)
);
