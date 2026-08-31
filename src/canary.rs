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

use rust_decimal::Decimal;
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

    /// The plain, serializable form persisted to `canary-artifacts/` before
    /// this spec is ever built into a signable order.
    pub fn to_record(&self, label: &str, prepared_at_utc: &str) -> CanarySpecRecord {
        CanarySpecRecord {
            label: label.to_owned(),
            prepared_at_utc: prepared_at_utc.to_owned(),
            token_id: self.token_id.clone(),
            side: self.side.as_str().to_owned(),
            price: self.price.to_string(),
            size: self.size.to_string(),
            order_type: "FAK".to_owned(),
        }
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
        })
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
    AlreadyExists { path: std::path::PathBuf },
    Io { path: std::path::PathBuf, source: io::Error },
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
                write!(formatter, "canary record I/O error at {}: {source}", path.display())
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
            CanaryOrderSpec::new("123".to_owned(), CanarySide::Buy, Decimal::ZERO, Decimal::ONE),
            Err(CanarySpecError::PriceOutOfRange { .. })
        ));
        assert!(matches!(
            CanaryOrderSpec::new("123".to_owned(), CanarySide::Buy, Decimal::ONE, Decimal::ONE),
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
        assert_eq!(read_record(&path).expect("record must be readable"), "first");

        fs::remove_file(path).expect("test artifact must be removable");
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
}
