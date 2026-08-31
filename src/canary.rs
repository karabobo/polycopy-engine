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
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CanaryLookupRecord {
    pub label: String,
    pub looked_up_at_utc: String,
    pub method: String,
    pub found_order_id: Option<String>,
    pub status: Option<String>,
    pub size_matched: Option<String>,
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
}
