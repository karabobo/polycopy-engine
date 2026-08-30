use std::{env, process::ExitCode};

use polycopy_engine::{EngineLock, EngineLockError};

fn main() -> ExitCode {
    let Some(lock_path) = env::args_os().nth(1) else {
        eprintln!("usage: lock_probe <lock-path>");
        return ExitCode::from(2);
    };

    match EngineLock::try_acquire(lock_path) {
        Ok(_guard) => ExitCode::SUCCESS,
        Err(EngineLockError::AlreadyHeld { .. }) => ExitCode::from(10),
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(1)
        }
    }
}
