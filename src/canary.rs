//! Pure, network-free data for the Phase 0.5 CLOB submission-safety canary.
//!
//! This module never touches the SDK, signs anything, or calls the venue; see
//! [`crate::canary_run`] for the orchestration that actually builds, signs,
//! and submits the one canary order. Keeping the plain spec and its
//! persistence here means the validation and the "never silently overwrite a
//! persisted attempt" guard are testable without a live venue.

use std::{
    error::Error,
    fmt,
    fs::OpenOptions,
    io::{self, Read as _, Write as _},
    path::Path,
};

use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};

/// The fixed parameters of one canary order, validated before any network call.
///
/// Phase 0.5 is FAK-only: this project has no cancel-order client, so a GTC
/// canary could rest on the book and get filled later with nothing able to
/// close it. `price`/`size` are deliberately the operator's choice (read from
/// the environment by `canary_run`, never hardcoded) so the human running the
/// canary sets the actual risk-bounded numbers.
#[derive(Clone, Debug, PartialEq)]
pub struct CanaryOrderSpec {
    token_id: String,
    side: CanarySide,
    price: Decimal,
    size: Decimal,
}

impl CanaryOrderSpec {
    pub fn new(
        token_id: String,
        side: CanarySide,
        price: Decimal,
        size: Decimal,
    ) -> Result<Self, CanarySpecError> {
        let token_id = token_id.trim().to_owned();
        if token_id.is_empty() || !token_id.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(CanarySpecError::InvalidTokenId);
        }
        if price <= Decimal::ZERO || price >= Decimal::ONE {
            return Err(CanarySpecError::PriceOutOfRange { price });
        }
        if size <= Decimal::ZERO {
            return Err(CanarySpecError::NonPositiveSize { size });
        }

        Ok(Self {
            token_id,
            side,
            price,
            size,
        })
    }

    pub fn token_id(&self) -> &str {
        &self.token_id
    }

    pub fn side(&self) -> CanarySide {
        self.side
    }

    pub fn price(&self) -> Decimal {
        self.price
    }

    pub fn size(&self) -> Decimal {
        self.size
    }

    /// BUY canaries are maker-USDC-budget requests, matching the production
    /// market-FAK path. The environment field remains named `SIZE` for
    /// compatibility, but BUY callers must provide a cent-denominated budget.
    pub fn buy_maker_budget(&self) -> Result<Decimal, CanarySpecError> {
        if self.side != CanarySide::Buy {
            return Err(CanarySpecError::BuyBudgetForSell);
        }
        if self.size.scale() > 2 {
            return Err(CanarySpecError::BuyBudgetPrecision { budget: self.size });
        }
        Ok(self
            .size
            .round_dp_with_strategy(2, RoundingStrategy::ToZero))
    }

    /// The plain, serializable form persisted to `canary-artifacts/` before
    /// this spec is ever built into a signable order.
    pub fn to_record(&self, label: &str, prepared_at_utc: &str) -> CanarySpecRecord {
        let provenance = canary_build_provenance();
        CanarySpecRecord {
            label: label.to_owned(),
            prepared_at_utc: prepared_at_utc.to_owned(),
            token_id: self.token_id.clone(),
            side: self.side.as_str().to_owned(),
            price: self.price.to_string(),
            size: self.size.to_string(),
            order_type: "FAK".to_owned(),
            build_git_commit: provenance.git_commit.to_owned(),
            construction_fingerprint: provenance.construction_fingerprint.to_owned(),
        }
    }
}

/// Identifies the source used to construct this canary. The fingerprint is a
/// build-time change detector, not a cryptographic integrity assertion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CanaryBuildProvenance {
    pub git_commit: &'static str,
    pub construction_fingerprint: &'static str,
}

pub fn canary_build_provenance() -> CanaryBuildProvenance {
    CanaryBuildProvenance {
        git_commit: option_env!("POLYCOPY_BUILD_GIT_COMMIT").unwrap_or("unknown"),
        construction_fingerprint: option_env!("POLYCOPY_CONSTRUCTION_FINGERPRINT")
            .unwrap_or("unknown"),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CanarySide {
    Buy,
    Sell,
}

impl CanarySide {
    fn as_str(self) -> &'static str {
        match self {
            Self::Buy => "BUY",
            Self::Sell => "SELL",
        }
    }
}

impl std::str::FromStr for CanarySide {
    type Err = CanarySpecError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_uppercase().as_str() {
            "BUY" => Ok(Self::Buy),
            "SELL" => Ok(Self::Sell),
            _ => Err(CanarySpecError::InvalidSide),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CanarySpecError {
    InvalidTokenId,
    InvalidSide,
    PriceOutOfRange { price: Decimal },
    NonPositiveSize { size: Decimal },
    BuyBudgetForSell,
    BuyBudgetPrecision { budget: Decimal },
}

impl fmt::Display for CanarySpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTokenId => write!(formatter, "canary token ID must be a positive integer"),
            Self::InvalidSide => write!(formatter, "canary side must be BUY or SELL"),
            Self::PriceOutOfRange { price } => write!(
                formatter,
                "canary price ({price}) must be strictly between 0 and 1"
            ),
            Self::NonPositiveSize { size } => {
                write!(formatter, "canary size ({size}) must be positive")
            }
            Self::BuyBudgetForSell => write!(formatter, "a SELL canary has no BUY maker budget"),
            Self::BuyBudgetPrecision { budget } => write!(
                formatter,
                "BUY canary USDC budget ({budget}) must have at most two decimals"
            ),
        }
    }
}

impl Error for CanarySpecError {}

/// The wire form of [`CanaryOrderSpec`] persisted before this attempt is ever
/// built into a signable order. Every field is a plain string so this record
/// never depends on SDK types that may not round-trip through JSON.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanarySpecRecord {
    pub label: String,
    pub prepared_at_utc: String,
    pub token_id: String,
    pub side: String,
    pub price: String,
    pub size: String,
    pub order_type: String,
    /// Commit embedded by `build.rs`; `unknown` means this binary was built
    /// outside Git and without a release-provided commit.
    #[serde(default = "unknown_provenance")]
    pub build_git_commit: String,
    /// Build-time source fingerprint for both construction paths and their
    /// shared value contract. Historical artifacts lack this value.
    #[serde(default = "unknown_provenance")]
    pub construction_fingerprint: String,
}

fn unknown_provenance() -> String {
    "unknown".to_owned()
}

/// A redacted-safe summary of one `post_order` response, persisted after a
/// live submission. Deliberately narrower than the SDK's own response type:
/// it carries only the fields this project's receipt/reconciliation reports
/// need, not every field the venue happens to return.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanarySubmissionRecord {
    pub label: String,
    pub submitted_at_utc: String,
    pub order_id: String,
    pub status: String,
    pub success: bool,
    pub making_amount: String,
    pub taking_amount: String,
    pub transaction_hash_count: usize,
    pub trade_id_count: usize,
    /// This project's own offline prediction of `order_id`, computed from
    /// the signed order before submission (see `crate::venue::order_hash`).
    /// `None` only if that computation itself failed (e.g. a neg-risk query
    /// error) -- never a stand-in for "not equal". Comparing this against
    /// `order_id` is exactly the live proof
    /// `docs/PHASE_0_5_CANARY_REPORT.md` still needs.
    pub expected_order_id: Option<String>,
}

/// A redacted-safe summary of one order lookup, persisted for the Phase 0.5
/// "deterministic lookup" question.
///
/// A lookup failure is itself a finding (Phase 0.5 confirmed the venue
/// returns 404 for an order the moment after it matches), never a reason to
/// abort the remaining probe steps. [`Self::query_failed`] is the only path
/// that may produce a record from an `Err`, and it always leaves
/// `found_order_id`/`size_matched` as `None` rather than guessing.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanaryLookupRecord {
    pub label: String,
    pub looked_up_at_utc: String,
    pub method: String,
    pub found_order_id: Option<String>,
    pub status: Option<String>,
    pub size_matched: Option<String>,
}

impl CanaryLookupRecord {
    pub fn found(
        label: &str,
        looked_up_at_utc: &str,
        method: &str,
        order_id: String,
        status: String,
        size_matched: String,
    ) -> Self {
        Self {
            label: label.to_owned(),
            looked_up_at_utc: looked_up_at_utc.to_owned(),
            method: method.to_owned(),
            found_order_id: Some(order_id),
            status: Some(status),
            size_matched: Some(size_matched),
        }
    }

    /// A lookup that completed but did not find the order (e.g. absent from
    /// a listing page), as distinct from a query that failed outright.
    pub fn not_found(label: &str, looked_up_at_utc: &str, method: &str) -> Self {
        Self {
            label: label.to_owned(),
            looked_up_at_utc: looked_up_at_utc.to_owned(),
            method: method.to_owned(),
            found_order_id: None,
            status: None,
            size_matched: None,
        }
    }

    /// A lookup call that itself failed (network error, non-2xx status,
    /// etc). Never a zero/absent result: the failure reason is preserved in
    /// `status` and the record is still safe to persist and continue past.
    pub fn query_failed(
        label: &str,
        looked_up_at_utc: &str,
        method: &str,
        error: impl fmt::Display,
    ) -> Self {
        Self {
            label: label.to_owned(),
            looked_up_at_utc: looked_up_at_utc.to_owned(),
            method: method.to_owned(),
            found_order_id: None,
            status: Some(format!("query_failed: {error}")),
            size_matched: None,
        }
    }

    /// `true` only for a lookup that failed outright, as opposed to one that
    /// completed and simply found nothing.
    pub fn is_query_failure(&self) -> bool {
        matches!(&self.status, Some(status) if status.starts_with("query_failed: "))
    }
}

/// Writes `contents` to `path`, failing if `path` already exists.
///
/// A persisted canary attempt must never be silently rebuilt or overwritten;
/// this applies the same "create, don't replace" discipline `EngineLock`
/// applies to its lock file.
pub fn write_new_record(path: &Path, contents: &str) -> Result<(), CanaryRecordError> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|source| CanaryRecordError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|source| {
            if source.kind() == io::ErrorKind::AlreadyExists {
                CanaryRecordError::AlreadyExists {
                    path: path.to_path_buf(),
                }
            } else {
                CanaryRecordError::Io {
                    path: path.to_path_buf(),
                    source,
                }
            }
        })?;

    file.write_all(contents.as_bytes())
        .map_err(|source| CanaryRecordError::Io {
            path: path.to_path_buf(),
            source,
        })?;

    // P2-9: fsync the data file so the canary dedup record survives a
    // crash before the OS flushes its page cache. Without this, a crash
    // here leaves the canary record invisible at restart, indistinguishable
    // from "never attempted", and a duplicate-submission dedup invariant
    // (blueprint §0.5) is silently broken.
    file.sync_all().map_err(|source| CanaryRecordError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    // P2-9 (continued): fsync the parent directory so the file entry
    // itself is durable. POSIX requires a directory fsync after creating
    // a file so the entry and its metadata are flushed; Windows has no
    // directory-fsync concept and surfaces it as `Unsupported` (Windows
    // ERROR_INVALID_FUNCTION) or sometimes `PermissionDenied`. We treat
    // those as best-effort (the file fsync above already guards the
    // data-loss window on every platform); any other I/O error -- e.g.
    // the parent directory being unlinked between create_dir_all and
    // open here -- still propagates as CanaryRecordError::Io so a real
    // disk problem is not swallowed.
    if let Some(parent) = path.parent() {
        match std::fs::File::open(parent) {
            Ok(dir) => {
                if let Err(source) = dir.sync_all() {
                    if !is_unsupported_directory_sync(&source) {
                        return Err(CanaryRecordError::Io {
                            path: parent.to_path_buf(),
                            source,
                        });
                    }
                }
            }
            Err(source) if source.kind() == io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(CanaryRecordError::Io {
                    path: parent.to_path_buf(),
                    source,
                });
            }
        }
    }

    Ok(())
}

/// Returns true when `sync_all()` on a directory fd fails with the
/// platform-specific "this OS does not support directory fsync" error,
/// so callers can degrade to best-effort. Windows surfaces this as
/// `ERROR_INVALID_FUNCTION`; libstd maps it to `ErrorKind::Unsupported`
/// on recent versions and `ErrorKind::PermissionDenied` on others.
fn is_unsupported_directory_sync(source: &io::Error) -> bool {
    matches!(
        source.kind(),
        io::ErrorKind::Unsupported | io::ErrorKind::PermissionDenied
    )
}

/// Reads and returns the contents of an already-persisted record.
pub fn read_record(path: &Path) -> Result<String, CanaryRecordError> {
    let mut file = std::fs::File::open(path).map_err(|source| CanaryRecordError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)
        .map_err(|source| CanaryRecordError::Io {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(contents)
}

#[derive(Debug)]
pub enum CanaryRecordError {
    AlreadyExists {
        path: std::path::PathBuf,
    },
    Io {
        path: std::path::PathBuf,
        source: io::Error,
    },
}

impl fmt::Display for CanaryRecordError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { path } => write!(
                formatter,
                "refusing to overwrite an already-persisted canary record: {}",
                path.display()
            ),
            Self::Io { path, source } => {
                write!(
                    formatter,
                    "canary record I/O error at {}: {source}",
                    path.display()
                )
            }
        }
    }
}

impl Error for CanaryRecordError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AlreadyExists { .. } => None,
            Self::Io { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        env, fs,
        path::PathBuf,
        process,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn unique_temp_path(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        env::temp_dir().join(format!(
            "polycopy-engine-canary-test-{}-{nonce}-{name}",
            process::id()
        ))
    }

    #[test]
    fn spec_rejects_a_price_at_or_outside_the_open_unit_interval() {
        assert!(matches!(
            CanaryOrderSpec::new(
                "123".to_owned(),
                CanarySide::Buy,
                Decimal::ZERO,
                Decimal::ONE
            ),
            Err(CanarySpecError::PriceOutOfRange { .. })
        ));
        assert!(matches!(
            CanaryOrderSpec::new(
                "123".to_owned(),
                CanarySide::Buy,
                Decimal::ONE,
                Decimal::ONE
            ),
            Err(CanarySpecError::PriceOutOfRange { .. })
        ));
    }

    #[test]
    fn spec_rejects_a_non_positive_size() {
        assert!(matches!(
            CanaryOrderSpec::new(
                "123".to_owned(),
                CanarySide::Sell,
                Decimal::new(1, 1),
                Decimal::ZERO
            ),
            Err(CanarySpecError::NonPositiveSize { .. })
        ));
    }

    #[test]
    fn spec_rejects_a_non_numeric_token_id() {
        assert!(matches!(
            CanaryOrderSpec::new(
                "0xabc".to_owned(),
                CanarySide::Buy,
                Decimal::new(1, 1),
                Decimal::ONE
            ),
            Err(CanarySpecError::InvalidTokenId)
        ));
    }

    #[test]
    fn side_round_trips_through_its_string_form() {
        assert_eq!("BUY".parse::<CanarySide>().unwrap(), CanarySide::Buy);
        assert_eq!("sell".parse::<CanarySide>().unwrap(), CanarySide::Sell);
        assert!("hold".parse::<CanarySide>().is_err());
    }

    #[test]
    fn spec_record_round_trips_through_json() {
        let spec = CanaryOrderSpec::new(
            "123456".to_owned(),
            CanarySide::Buy,
            Decimal::new(5, 2),
            Decimal::ONE,
        )
        .expect("valid canary spec");
        let record = spec.to_record("test-attempt", "2026-08-30T00:00:00Z");

        let json = serde_json::to_string(&record).expect("record must serialize");
        let restored: CanarySpecRecord =
            serde_json::from_str(&json).expect("record must deserialize");

        assert_eq!(restored, record);
        assert_eq!(restored.order_type, "FAK");
        assert_eq!(
            restored.build_git_commit,
            canary_build_provenance().git_commit
        );
        assert_eq!(
            restored.construction_fingerprint,
            canary_build_provenance().construction_fingerprint
        );
    }

    #[test]
    fn historical_spec_record_without_provenance_is_marked_unknown() {
        let record: CanarySpecRecord = serde_json::from_value(serde_json::json!({
            "label": "historical-attempt",
            "prepared_at_utc": "2026-09-01T00:00:00Z",
            "token_id": "123456",
            "side": "BUY",
            "price": "0.55",
            "size": "5",
            "order_type": "FAK"
        }))
        .expect("historical records remain readable");

        assert_eq!(record.build_git_commit, "unknown");
        assert_eq!(record.construction_fingerprint, "unknown");
    }

    #[test]
    fn write_new_record_refuses_to_overwrite_an_existing_attempt() {
        let path = unique_temp_path("spec.json");

        write_new_record(&path, "first").expect("first write must succeed");
        let second_attempt = write_new_record(&path, "second");

        assert!(matches!(
            second_attempt,
            Err(CanaryRecordError::AlreadyExists { .. })
        ));
        assert_eq!(
            read_record(&path).expect("record must be readable"),
            "first"
        );

        fs::remove_file(path).expect("test artifact must be removable");
    }

    #[test]
    fn write_new_record_persists_a_freshly_created_parent_directory() {
        // P2-9 pin: the parent directory is created on demand by
        // write_new_record itself, and dir.sync_all() is then issued
        // against that just-opened directory. This test exercises that
        // path (parent did not exist when write_new_record was called)
        // and verifies the record round-trips through the create_dir_all
        // + file.sync_all + dir.sync_all pipeline.
        //
        // KNOWN LIMITATION (audit finding #2): std::fs offers no
        // cross-platform hook for asserting that sync_all() actually
        // reached the kernel, so a regression that silently removes
        // either sync_all call would not fail this test. The test is
        // therefore a path-coverage pin, not a syscall-observation pin:
        // it guarantees the fsync-on-parent-freshly-created code path is
        // exercised by CI, and the durability guarantee itself rests on
        // review + the explicit test for the write+read round-trip across
        // a real directory fd. A future change that drops fsync must be
        // caught in review.
        let parent = unique_temp_path("nested/canary/spec.json")
            .parent()
            .expect("temp path must have a parent")
            .to_path_buf();
        let _ = fs::remove_dir_all(&parent);

        let path = parent.join("spec.json");
        write_new_record(&path, "fresh").expect("write into a brand-new parent must succeed");
        assert_eq!(
            read_record(&path).expect("record must be readable from a freshly-created parent"),
            "fresh"
        );

        let _ = fs::remove_dir_all(&parent);
    }

    // Phase 0.5 confirmed live that a fully matched order can 404 from
    // `GET /data/order/{id}` immediately afterward, and that the field-based
    // fallback listing does not find a matched order at all. Neither is
    // grounds to abort the probe; both must turn into a persistable,
    // non-panicking record. These tests pin that behavior so a future change
    // cannot silently reintroduce a crash-on-lookup-failure regression.

    #[test]
    fn a_lookup_query_failure_is_recorded_not_a_reason_to_abort() {
        let record = CanaryLookupRecord::query_failed(
            "regression-test",
            "2026-08-31T00:00:00Z",
            "order_id",
            "Status: error(404 Not Found)",
        );

        assert!(record.is_query_failure());
        assert_eq!(record.found_order_id, None);
        assert_eq!(record.size_matched, None);
        assert!(record
            .status
            .as_deref()
            .is_some_and(|status| status.contains("404")));

        // The record itself must still serialize: a lookup failure has to be
        // writable to canary-artifacts/, not just printed and discarded.
        serde_json::to_string(&record).expect("a query-failed record must still serialize");
    }

    #[test]
    fn a_lookup_that_completes_but_finds_nothing_is_distinct_from_a_query_failure() {
        let empty_listing = CanaryLookupRecord::not_found(
            "regression-test",
            "2026-08-31T00:00:00Z",
            "asset_id_field_match",
        );

        assert!(!empty_listing.is_query_failure());
        assert_eq!(empty_listing.found_order_id, None);
        assert_eq!(empty_listing.status, None);
    }

    #[test]
    fn a_successful_lookup_is_never_classified_as_a_query_failure() {
        let found = CanaryLookupRecord::found(
            "regression-test",
            "2026-08-31T00:00:00Z",
            "order_id",
            "0xabc".to_owned(),
            "Matched".to_owned(),
            "5.28846".to_owned(),
        );

        assert!(!found.is_query_failure());
        assert_eq!(found.found_order_id.as_deref(), Some("0xabc"));
    }

    #[test]
    fn unsupported_directory_sync_errors_are_classified_as_best_effort() {
        // P2-9 audit finding #1 pin: dir.sync_all on Windows surfaces
        // ErrorKind::Unsupported (ERROR_INVALID_FUNCTION) or
        // PermissionDenied depending on libstd version; both must be
        // tolerated as best-effort. Any other kind (e.g. Storage full,
        // Io) must NOT be tolerated.
        use std::io;
        let unsupported = io::Error::new(io::ErrorKind::Unsupported, "ERROR_INVALID_FUNCTION");
        assert!(is_unsupported_directory_sync(&unsupported));
        let permission_denied =
            io::Error::new(io::ErrorKind::PermissionDenied, "ERROR_INVALID_FUNCTION");
        assert!(is_unsupported_directory_sync(&permission_denied));
        for kind in [
            io::ErrorKind::Other,
            io::ErrorKind::BrokenPipe,
            io::ErrorKind::InvalidInput,
        ] {
            let other = io::Error::new(kind, "real disk error");
            assert!(
                !is_unsupported_directory_sync(&other),
                "{kind:?} must not be classified as best-effort"
            );
        }
    }
}
