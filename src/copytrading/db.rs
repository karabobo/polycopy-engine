//! SQLite connection and migration setup for Phase 1's durable state.
//!
//! Every pooled connection is configured per the blueprint: foreign key
//! enforcement on, WAL journal mode, a 5-second busy timeout, and FULL
//! synchronous durability. Migrations run through sqlx's built-in,
//! checksum-protected migrator: it records each applied migration's checksum
//! in its own `_sqlx_migrations` table and refuses to run if an
//! already-applied migration's file content no longer matches what was
//! recorded. This is the same protection the blueprint's
//! `copy_schema_migrations(version, name, checksum, applied_at)` ledger
//! describes; this project uses sqlx's built-in table for it rather than a
//! hand-rolled equivalent, to avoid re-implementing already-solved,
//! well-tested migration bookkeeping.
//!
//! This is a single-host, single-writer SQLite deployment (see
//! [`crate::engine_lock::EngineLock`] for the process-level enforcement of
//! "single writer"). `database_path` must resolve to local block storage,
//! never NFS/SMB.

use std::{fmt, path::Path, time::Duration};

use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    SqlitePool,
};

/// The blueprint's required busy timeout for every pooled connection.
pub const BUSY_TIMEOUT: Duration = Duration::from_millis(5000);

/// Opens a pooled connection to the copy-engine database at `database_path`
/// and runs every pending migration.
pub async fn open_and_migrate(database_path: impl AsRef<Path>) -> Result<SqlitePool, DbError> {
    let pool = open(database_path).await?;
    // Resolved relative to CARGO_MANIFEST_DIR (the crate root), not this file.
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(DbError::Migration)?;
    Ok(pool)
}

/// Opens the pool without running migrations, for callers (tests, tools)
/// that need to control migration timing explicitly.
pub async fn open(database_path: impl AsRef<Path>) -> Result<SqlitePool, DbError> {
    let options = SqliteConnectOptions::new()
        .filename(database_path.as_ref())
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(BUSY_TIMEOUT);

    SqlitePoolOptions::new()
        .connect_with(options)
        .await
        .map_err(DbError::Connect)
}

#[derive(Debug)]
pub enum DbError {
    Connect(sqlx::Error),
    Migration(sqlx::migrate::MigrateError),
}

impl fmt::Display for DbError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Connect(source) => {
                write!(formatter, "unable to open the copy database: {source}")
            }
            Self::Migration(source) => {
                write!(formatter, "unable to apply database migrations: {source}")
            }
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect(source) => Some(source),
            Self::Migration(source) => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs, process,
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use sqlx::Row as _;

    use super::*;

    // Wall-clock nanos alone collided under `cargo test`'s default thread
    // parallelism (the OS clock's actual resolution is coarser than the
    // scheduling gap between two `#[tokio::test]`s starting), which let two
    // tests share one SQLite file and interfere with each other. An
    // in-process counter is always unique regardless of clock resolution.
    fn unique_temp_db_path() -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        env::temp_dir().join(format!(
            "polycopy-engine-db-test-{}-{nonce}-{counter}.sqlite",
            process::id()
        ))
    }

    /// A migrated pool at a unique temp path, whose backing files (including
    /// WAL/SHM siblings) are removed when the guard drops -- so a test that
    /// panics mid-assertion still cleans up instead of leaking a file.
    struct TestDb {
        pool: SqlitePool,
        path: std::path::PathBuf,
    }

    impl TestDb {
        async fn new() -> Self {
            let path = unique_temp_db_path();
            let pool = open_and_migrate(&path)
                .await
                .expect("migrations must apply to a fresh database");
            Self { pool, path }
        }
    }

    impl std::ops::Deref for TestDb {
        type Target = SqlitePool;

        fn deref(&self) -> &SqlitePool {
            &self.pool
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
            let _ = fs::remove_file(format!("{}-shm", self.path.display()));
            let _ = fs::remove_file(format!("{}-wal", self.path.display()));
        }
    }

    #[tokio::test]
    async fn a_fresh_pool_enforces_foreign_keys_and_wal_mode() {
        let db = TestDb::new().await;

        let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&*db)
            .await
            .expect("PRAGMA foreign_keys must be queryable")
            .get(0);
        assert_eq!(foreign_keys, 1);

        let journal_mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&*db)
            .await
            .expect("PRAGMA journal_mode must be queryable")
            .get(0);
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");
    }

    #[tokio::test]
    async fn foreign_keys_are_actually_enforced_not_just_reported_on() {
        let db = TestDb::new().await;

        // leader_wallet_aliases.leader_id references leader_config(id); a
        // row pointing at a nonexistent leader must be rejected, not
        // silently accepted, or the pragma above is cosmetic.
        let result = sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (999, '0xabc')",
        )
        .execute(&*db)
        .await;
        assert!(result.is_err(), "a dangling foreign key must be rejected");
    }

    #[tokio::test]
    async fn one_account_can_follow_many_leaders_at_once() {
        // v1's whole scope is "one account, multiple leaders" (see
        // COPY_ENGINE_BLUEPRINT.md section 1): the uniqueness constraint
        // below only stops the *same leader address* from being claimed
        // twice, never how many distinct leaders one account follows.
        let db = TestDb::new().await;

        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&*db)
        .await
        .expect("the single v1 account must insert");

        sqlx::query(
            "INSERT INTO leader_config (id, label) VALUES \
             (1, 'leader-alpha'), (2, 'leader-beta'), (3, 'leader-gamma')",
        )
        .execute(&*db)
        .await
        .expect("multiple leaders must insert");

        for (leader_id, address) in [
            (1, "0x1111111111111111111111111111111111111111"),
            (2, "0x2222222222222222222222222222222222222222"),
            (3, "0x3333333333333333333333333333333333333333"),
        ] {
            sqlx::query("INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (?, ?)")
                .bind(leader_id)
                .bind(address)
                .execute(&*db)
                .await
                .unwrap_or_else(|error| {
                    panic!("leader {leader_id}'s distinct address must enable cleanly: {error}")
                });
        }

        let enabled_leader_count: i64 =
            sqlx::query("SELECT COUNT(*) FROM leader_config WHERE enabled = 1")
                .fetch_one(&*db)
                .await
                .expect("leader count must be queryable")
                .get(0);
        assert_eq!(
            enabled_leader_count, 3,
            "the one v1 account has three simultaneously enabled leaders"
        );
    }

    #[tokio::test]
    async fn an_address_cannot_be_enabled_under_two_leaders_at_once() {
        let db = TestDb::new().await;

        sqlx::query(
            "INSERT INTO leader_config (id, label) VALUES (1, 'leader-one'), (2, 'leader-two')",
        )
        .execute(&*db)
        .await
        .expect("both leaders must insert");

        sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (1, '0x1111111111111111111111111111111111111111')",
        )
        .execute(&*db)
        .await
        .expect("the first enabled alias must insert");

        let duplicate = sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (2, '0x1111111111111111111111111111111111111111')",
        )
        .execute(&*db)
        .await;
        assert!(
            duplicate.is_err(),
            "the same address must not be enabled under a second leader"
        );
    }

    #[tokio::test]
    async fn migrating_twice_is_idempotent() {
        let path = unique_temp_db_path();
        open_and_migrate(&path).await.expect("first migration run");
        let pool = open_and_migrate(&path).await.expect(
            "a second migration run against the same database must be a no-op, not an error",
        );

        pool.close().await;
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
    }

    async fn insert_leader(db: &TestDb, id: i64, label: &str) {
        sqlx::query("INSERT INTO leader_config (id, label) VALUES (?, ?)")
            .bind(id)
            .bind(label)
            .execute(&**db)
            .await
            .expect("leader must insert");
    }

    #[tokio::test]
    async fn replaying_the_same_activity_trade_creates_no_second_canonical_event() {
        let db = TestDb::new().await;
        insert_leader(&db, 1, "leader-one").await;

        let insert_once = "INSERT OR IGNORE INTO leader_events \
            (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
             price, occurred_at, observed_at) \
            VALUES ('activity:trade-1', 1, '0xcond', '123', 0, 'BUY', '5', '0.5', \
            '2026-08-31T00:00:00Z', '2026-08-31T00:00:01Z')";

        sqlx::query(insert_once)
            .execute(&*db)
            .await
            .expect("first observation of this trade must insert");
        // Simulates Activity WS and Activity REST backfill both delivering
        // the same trade: the canonical_event_key is the same, so a second
        // INSERT OR IGNORE must be a no-op, not a duplicate row.
        sqlx::query(insert_once)
            .execute(&*db)
            .await
            .expect("a replayed observation of the same trade must not error");

        let event_count: i64 = sqlx::query("SELECT COUNT(*) FROM leader_events")
            .fetch_one(&*db)
            .await
            .expect("event count must be queryable")
            .get(0);
        assert_eq!(
            event_count, 1,
            "replay must not create a second canonical event"
        );
    }

    #[tokio::test]
    async fn an_onchain_observation_can_stay_unlinked_from_any_canonical_event() {
        let db = TestDb::new().await;

        // "An on-chain observation may stay unlinked if it cannot be related
        // by an explicit source identifier" -- leader_event_id is nullable
        // precisely so a confirmation-only observation is never forced into
        // fuzzy-matching its way onto some canonical event.
        sqlx::query(
            "INSERT INTO leader_event_observations (leader_event_id, source, payload) \
             VALUES (NULL, 'onchain_ws', '{\"raw\":\"unrelated-log\"}')",
        )
        .execute(&*db)
        .await
        .expect("an unlinked on-chain observation must insert");
    }

    #[tokio::test]
    async fn activity_ws_and_backfill_can_both_observe_one_canonical_event() {
        let db = TestDb::new().await;
        insert_leader(&db, 1, "leader-one").await;

        sqlx::query(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
              price, occurred_at, observed_at) \
             VALUES ('activity:trade-1', 1, '0xcond', '123', 0, 'BUY', '5', '0.5', \
             '2026-08-31T00:00:00Z', '2026-08-31T00:00:01Z')",
        )
        .execute(&*db)
        .await
        .expect("the canonical event must insert");

        for source in ["activity_ws", "activity_backfill"] {
            sqlx::query(
                "INSERT INTO leader_event_observations (leader_event_id, source, source_identifier, payload) \
                 VALUES (1, ?, 'trade-1', '{}')",
            )
            .bind(source)
            .execute(&*db)
            .await
            .unwrap_or_else(|error| panic!("{source}'s observation must insert: {error}"));
        }

        let observation_count: i64 =
            sqlx::query("SELECT COUNT(*) FROM leader_event_observations WHERE leader_event_id = 1")
                .fetch_one(&*db)
                .await
                .expect("observation count must be queryable")
                .get(0);
        assert_eq!(
            observation_count, 2,
            "both sources' observations of the same event must be retained"
        );
    }

    #[tokio::test]
    async fn the_same_source_cannot_record_the_identical_trade_twice() {
        let db = TestDb::new().await;
        insert_leader(&db, 1, "leader-one").await;
        sqlx::query(
            "INSERT INTO leader_events \
             (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, \
              price, occurred_at, observed_at) \
             VALUES ('activity:trade-1', 1, '0xcond', '123', 0, 'BUY', '5', '0.5', \
             '2026-08-31T00:00:00Z', '2026-08-31T00:00:01Z')",
        )
        .execute(&*db)
        .await
        .expect("the canonical event must insert");

        let insert_observation = "INSERT INTO leader_event_observations \
            (leader_event_id, source, source_identifier, payload) \
            VALUES (1, 'activity_ws', 'trade-1', '{}')";
        sqlx::query(insert_observation)
            .execute(&*db)
            .await
            .expect("the first observation must insert");

        // e.g. a WS reconnect replaying its recent message buffer.
        let replay = sqlx::query(insert_observation).execute(&*db).await;
        assert!(
            replay.is_err(),
            "the same source recording the same trade twice must be rejected"
        );
    }

    /// Seeds one account, one leader, and one leader_event, returning the
    /// event id -- the minimum fixture every copy_intents test needs.
    async fn seed_account_leader_event(db: &TestDb) -> i64 {
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&**db)
        .await
        .expect("account must insert");
        insert_leader(db, 1, "leader-one").await;
        sqlx::query(
            "INSERT INTO leader_events \
             (id, canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, \
              size, price, occurred_at, observed_at) \
             VALUES (1, 'activity:trade-1', 1, '0xcond', '123', 0, 'BUY', '5', '0.5', \
             '2026-08-31T00:00:00Z', '2026-08-31T00:00:01Z')",
        )
        .execute(&**db)
        .await
        .expect("event must insert");
        1
    }

    async fn seed_intent(db: &TestDb) -> i64 {
        let event_id = seed_account_leader_event(db).await;
        sqlx::query(
            "INSERT INTO copy_intents \
             (event_id, account_id, leader_id, token_id, side, config_snapshot_json, \
              config_snapshot_hash, shard_scheme_version, lane_count, shard_id) \
             VALUES (?, 1, 1, '123', 'BUY', '{}', 'hash-1', 1, 4, 0)",
        )
        .bind(event_id)
        .execute(&**db)
        .await
        .expect("intent must insert");
        1
    }

    #[tokio::test]
    async fn replaying_an_event_for_the_same_account_creates_no_second_intent() {
        let db = TestDb::new().await;
        let event_id = seed_account_leader_event(&db).await;

        let insert_intent = "INSERT OR IGNORE INTO copy_intents \
            (event_id, account_id, leader_id, token_id, side, config_snapshot_json, \
             config_snapshot_hash, shard_scheme_version, lane_count, shard_id) \
             VALUES (?, 1, 1, '123', 'BUY', '{}', 'hash-1', 1, 4, 0)";

        sqlx::query(insert_intent)
            .bind(event_id)
            .execute(&*db)
            .await
            .expect("first planning of this event must insert");
        // A planner replay (e.g. after a crash between insert and cursor
        // update, per blueprint section 8's acceptance) must not create a
        // second intent for the same (event, account).
        sqlx::query(insert_intent)
            .bind(event_id)
            .execute(&*db)
            .await
            .expect("replaying the same event must not error");

        let intent_count: i64 = sqlx::query("SELECT COUNT(*) FROM copy_intents")
            .fetch_one(&*db)
            .await
            .expect("intent count must be queryable")
            .get(0);
        assert_eq!(intent_count, 1, "replay must not create a second intent");
    }

    #[tokio::test]
    async fn concurrent_attempt_numbers_for_one_intent_persist_only_once_each() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;

        sqlx::query(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, requested_qty) \
             VALUES (?, 1, '{}', '5')",
        )
        .bind(intent_id)
        .execute(&*db)
        .await
        .expect("the first attempt must insert");

        // "Concurrent calls for one (intent_id, attempt_number) persist one
        // identical envelope" (blueprint Phase 5 acceptance) -- modeled here
        // as a second writer racing to prepare the same attempt number.
        let racing_writer = sqlx::query(
            "INSERT INTO order_attempts (intent_id, attempt_number, envelope_json, requested_qty) \
             VALUES (?, 1, '{}', '5')",
        )
        .bind(intent_id)
        .execute(&*db)
        .await;
        assert!(
            racing_writer.is_err(),
            "a second writer must not persist a second row for the same attempt number"
        );
    }

    #[tokio::test]
    async fn two_leaders_buying_one_token_retain_distinct_virtual_lots() {
        // Direct check of the blueprint Phase 4 acceptance criterion by the
        // same name: one account, two leaders, one token -- two lot rows,
        // not one shared balance.
        let db = TestDb::new().await;
        sqlx::query(
            "INSERT INTO accounts (id, label, signing_address, signature_type) \
             VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
        )
        .execute(&*db)
        .await
        .expect("account must insert");
        insert_leader(&db, 1, "leader-one").await;
        insert_leader(&db, 2, "leader-two").await;

        for leader_id in [1, 2] {
            sqlx::query(
                "INSERT INTO position_lots (account_id, leader_id, token_id, qty) \
                 VALUES (1, ?, '123', '5')",
            )
            .bind(leader_id)
            .execute(&*db)
            .await
            .unwrap_or_else(|error| panic!("leader {leader_id}'s lot must insert: {error}"));
        }

        let lot_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM position_lots WHERE account_id = 1 AND token_id = '123'",
        )
        .fetch_one(&*db)
        .await
        .expect("lot count must be queryable")
        .get(0);
        assert_eq!(
            lot_count, 2,
            "each leader must keep its own lot for the same token"
        );
    }

    #[tokio::test]
    async fn a_reconciliation_case_can_reference_the_attempt_that_opened_it() {
        let db = TestDb::new().await;
        let intent_id = seed_intent(&db).await;
        sqlx::query(
            "INSERT INTO order_attempts (id, intent_id, attempt_number, envelope_json, requested_qty, status) \
             VALUES (1, ?, 1, '{}', '5', 'uncertain')",
        )
        .bind(intent_id)
        .execute(&*db)
        .await
        .expect("attempt must insert");

        sqlx::query(
            "INSERT INTO reconciliation_cases (account_id, token_id, intent_id, order_attempt_id, case_type, detail) \
             VALUES (1, '123', ?, 1, 'unknown_submission', 'response lost after HTTP request left the host')",
        )
        .bind(intent_id)
        .execute(&*db)
        .await
        .expect("the case must insert");

        let open_case_count: i64 = sqlx::query(
            "SELECT COUNT(*) FROM reconciliation_cases WHERE account_id = 1 AND token_id = '123' AND resolved_at IS NULL",
        )
        .fetch_one(&*db)
        .await
        .expect("open case count must be queryable")
        .get(0);
        assert_eq!(
            open_case_count, 1,
            "the case must be visible as open until resolved"
        );
    }

    #[tokio::test]
    async fn execution_schedule_is_a_singleton() {
        let db = TestDb::new().await;
        sqlx::query(
            "INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) \
             VALUES (1, 1, 'mod_lane_count', 4)",
        )
        .execute(&*db)
        .await
        .expect("the one schedule row must insert");

        let second_row = sqlx::query(
            "INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) \
             VALUES (2, 1, 'mod_lane_count', 4)",
        )
        .execute(&*db)
        .await;
        assert!(
            second_row.is_err(),
            "execution_schedule must never hold more than its one singleton row"
        );
    }
}
