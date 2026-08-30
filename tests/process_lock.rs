use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{self, Command},
    time::{SystemTime, UNIX_EPOCH},
};

use polycopy_engine::EngineLock;

#[test]
fn only_one_process_can_own_a_database_lock() {
    let temp_dir = unique_temp_dir();
    let database_path = temp_dir.join("copy.sqlite");
    let lock_path = EngineLock::path_for_database(&database_path);

    let first_owner = EngineLock::acquire_for_database(&database_path)
        .expect("first engine process should acquire its database lock");
    assert_eq!(first_owner.path(), lock_path);

    assert_eq!(
        run_lock_probe(&lock_path),
        10,
        "a second process must fail fast"
    );

    drop(first_owner);
    assert_eq!(
        run_lock_probe(&lock_path),
        0,
        "the lock must be available after the owning process releases it"
    );

    fs::remove_dir_all(temp_dir).expect("test lock directory should be removable");
}

fn run_lock_probe(lock_path: &Path) -> i32 {
    let output = Command::new(env!("CARGO_BIN_EXE_lock_probe"))
        .arg(lock_path)
        .output()
        .expect("lock probe must start");

    if !output.stderr.is_empty() {
        eprintln!(
            "lock probe stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    output.status.code().expect("lock probe must exit normally")
}

fn unique_temp_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after the Unix epoch")
        .as_nanos();
    let path = env::temp_dir().join(format!(
        "polycopy-engine-lock-test-{}-{nonce}",
        process::id()
    ));
    fs::create_dir(&path).expect("test lock directory must be created");
    path
}
