//! Testable core of the Chinese operator panel (`ops_panel`).
//!
//! The panel is an operations tool, not part of the order path: it reads the
//! database read-only, reads the non-secret runtime env file, asks systemd for
//! service state, and (only on an operator's explicit confirmation, from the
//! binary) starts/stops/restarts services. Nothing here prepares, signs,
//! submits, or cancels an order.
//!
//! Every user-facing string is Chinese on purpose: the panel is meant for the
//! account owner, not for engine developers. Internal identifiers (leader
//! labels, env variable names) are shown only where the owner needs them to
//! find the matching line in a file.
//!
//! IO that needs a real terminal or a real systemd lives in
//! `src/bin/ops_panel.rs`; everything that decides *what* to show lives here
//! and is unit-tested (the render functions against ratatui's `TestBackend`,
//! the same approach `dashboard` uses).

pub mod checks;
pub mod env_file;
pub mod labels;
pub mod live_config;
pub mod services;
pub mod stats;
pub mod ui;

#[cfg(test)]
pub(crate) mod test_db;
