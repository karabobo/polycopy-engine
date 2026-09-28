//! Throwaway migrated SQLite database for the panel's tests (same pattern as
//! `setup`'s own test harness).

use std::{
    env, process,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use sqlx::SqlitePool;

pub(crate) struct TestDb {
    pub(crate) pool: SqlitePool,
    path: std::path::PathBuf,
}

impl TestDb {
    pub(crate) async fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "polycopy-engine-ops-test-{}-{nonce}-{counter}.sqlite",
            process::id()
        ));
        let pool = crate::copytrading::db::open_and_migrate(&path)
            .await
            .expect("migrations must apply to a fresh database");
        Self { pool, path }
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
    }
}
