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
            Self::Connect(source) => write!(formatter, "unable to open the copy database: {source}"),
            Self::Migration(source) => write!(formatter, "unable to apply database migrations: {source}"),
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

    #[tokio::test]
    async fn a_fresh_pool_enforces_foreign_keys_and_wal_mode() {
        let path = unique_temp_db_path();
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply to a fresh database");

        let foreign_keys: i64 = sqlx::query("PRAGMA foreign_keys")
            .fetch_one(&pool)
            .await
            .expect("PRAGMA foreign_keys must be queryable")
            .get(0);
        assert_eq!(foreign_keys, 1);

        let journal_mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .expect("PRAGMA journal_mode must be queryable")
            .get(0);
        assert_eq!(journal_mode.to_ascii_lowercase(), "wal");

        pool.close().await;
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
    }

    #[tokio::test]
    async fn foreign_keys_are_actually_enforced_not_just_reported_on() {
        let path = unique_temp_db_path();
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply to a fresh database");

        // leader_wallet_aliases.leader_id references leader_config(id); a
        // row pointing at a nonexistent leader must be rejected, not
        // silently accepted, or the pragma above is cosmetic.
        let result = sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (999, '0xabc')",
        )
        .execute(&pool)
        .await;
        assert!(result.is_err(), "a dangling foreign key must be rejected");

        pool.close().await;
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
    }

    #[tokio::test]
    async fn an_address_cannot_be_enabled_under_two_leaders_at_once() {
        let path = unique_temp_db_path();
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply to a fresh database");

        sqlx::query("INSERT INTO leader_config (id, label) VALUES (1, 'leader-one'), (2, 'leader-two')")
            .execute(&pool)
            .await
            .expect("both leaders must insert");

        sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (1, '0x1111111111111111111111111111111111111111')",
        )
        .execute(&pool)
        .await
        .expect("the first enabled alias must insert");

        let duplicate = sqlx::query(
            "INSERT INTO leader_wallet_aliases (leader_id, address) VALUES (2, '0x1111111111111111111111111111111111111111')",
        )
        .execute(&pool)
        .await;
        assert!(
            duplicate.is_err(),
            "the same address must not be enabled under a second leader"
        );

        pool.close().await;
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
    }

    #[tokio::test]
    async fn migrating_twice_is_idempotent() {
        let path = unique_temp_db_path();
        open_and_migrate(&path).await.expect("first migration run");
        let pool = open_and_migrate(&path)
            .await
            .expect("a second migration run against the same database must be a no-op, not an error");

        pool.close().await;
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-shm", path.display()));
        let _ = fs::remove_file(format!("{}-wal", path.display()));
    }
}
