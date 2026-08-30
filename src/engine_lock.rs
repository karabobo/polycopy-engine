use std::{
    error::Error,
    ffi::OsString,
    fmt,
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs2::FileExt;

/// Process-scoped exclusive ownership of an engine database.
///
/// The lock is advisory, so every process that can execute this engine must
/// acquire it before opening its database pool. The guard keeps the lock file
/// descriptor open for its lifetime; the operating system releases the lock if
/// the process exits unexpectedly.
pub struct EngineLock {
    file: File,
    path: PathBuf,
}

impl EngineLock {
    /// Acquires the lock stored beside `database_path`.
    ///
    /// For example, `/var/lib/polycopy/copy.sqlite` uses
    /// `/var/lib/polycopy/copy.sqlite.lock`.
    pub fn acquire_for_database(database_path: impl AsRef<Path>) -> Result<Self, EngineLockError> {
        Self::try_acquire(Self::path_for_database(database_path))
    }

    /// Returns the lock file path that belongs to `database_path`.
    pub fn path_for_database(database_path: impl AsRef<Path>) -> PathBuf {
        let mut path = OsString::from(database_path.as_ref().as_os_str());
        path.push(".lock");
        PathBuf::from(path)
    }

    /// Tries to acquire `lock_path` without waiting.
    ///
    /// A second instance is a startup error, rather than a wait condition: a
    /// fixed-lane scheduler is only safe when one process owns the database.
    pub fn try_acquire(lock_path: impl AsRef<Path>) -> Result<Self, EngineLockError> {
        let path = lock_path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .map_err(|source| EngineLockError::Io {
                path: path.clone(),
                source,
            })?;

        match file.try_lock_exclusive() {
            Ok(()) => Ok(Self { file, path }),
            Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                Err(EngineLockError::AlreadyHeld { path })
            }
            Err(source) => Err(EngineLockError::Io { path, source }),
        }
    }

    /// The path that this guard currently owns.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for EngineLock {
    fn drop(&mut self) {
        // Explicitly unlock to make release deterministic in tests. The OS also
        // releases an advisory lock when this descriptor or process is dropped.
        let _ = FileExt::unlock(&self.file);
    }
}

#[derive(Debug)]
pub enum EngineLockError {
    /// Another engine already owns this database's execution lock.
    AlreadyHeld { path: PathBuf },
    /// Opening or locking the lock file failed for another reason.
    Io { path: PathBuf, source: io::Error },
}

impl fmt::Display for EngineLockError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyHeld { path } => write!(
                formatter,
                "copy-engine process lock is already held: {}",
                path.display()
            ),
            Self::Io { path, source } => write!(
                formatter,
                "cannot acquire copy-engine process lock {}: {source}",
                path.display()
            ),
        }
    }
}

impl Error for EngineLockError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AlreadyHeld { .. } => None,
            Self::Io { source, .. } => Some(source),
        }
    }
}
