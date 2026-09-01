//! Live `CopyExecution` adapter for the Polymarket Intl CLOB.
//!
//! Read methods delegate to [`IntlClobReadAdapter`]. `submit_exact_envelope`
//! reconstructs the persisted signed order and calls `post_order` once. A
//! transport error is [`SubmitError::Transport`]; a 4xx venue refusal is
//! [`SubmitError::Rejected`]. Tests in this module never contact the venue.

use std::{collections::HashMap, fs, path::PathBuf, str::FromStr};

use alloy::primitives::Address;
use alloy::signers::local::PrivateKeySigner;
use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Credentials, LocalSigner, Normal, Signer as _},
    clob::{types::SignatureType, Client, Config},
    types::Decimal,
    POLYGON,
};

use crate::{
    copytrading::reconcile::{
        lookup_prepared_fak_in_trade_history, CopyExecution, OrderId, PreparedOrderEnvelope,
        SubmitError, TradeHistoryLookup, TradeHistoryWindow, VenueOrderState,
    },
    venue::{
        intl_clob::{IntlClobReadAdapter, OutcomeTokenId, StrictTokenBalanceReader},
        signed_order::reconstruct_signed_order,
        OrderReceipt,
    },
};

const CLOB_HOST: &str = "https://clob.polymarket.com";
const PRIVATE_KEY_ENV: &str = "POLYCOPY_CLOB_PRIVATE_KEY";
const API_KEY_ENV: &str = "POLYCOPY_CLOB_L2_API_KEY";
const API_SECRET_ENV: &str = "POLYCOPY_CLOB_L2_API_SECRET";
const API_PASSPHRASE_ENV: &str = "POLYCOPY_CLOB_L2_API_PASSPHRASE";
const API_NONCE_ENV: &str = "POLYCOPY_CLOB_L2_NONCE";
const SIGNATURE_TYPE_ENV: &str = "POLYCOPY_CLOB_SIGNATURE_TYPE";
const FUNDER_ENV: &str = "POLYCOPY_CLOB_FUNDER";
const SYSTEMD_CREDENTIALS_DIRECTORY_ENV: &str = "CREDENTIALS_DIRECTORY";
const SYSTEMD_SECRET_CREDENTIAL_NAME: &str = "polycopy-copy-secrets";

/// Authenticated CLOB client plus the read-only adapter used for balances
/// and trade-history recovery.
#[derive(Clone, Debug)]
pub struct IntlClobCopyAdapter {
    client: Client<Authenticated<Normal>>,
    read: IntlClobReadAdapter,
    signer: PrivateKeySigner,
}

impl IntlClobCopyAdapter {
    pub async fn from_env() -> Result<Self, CopyAdapterError> {
        let systemd_secrets = load_systemd_secret_credential()?;
        let lookup = |name: &str| match &systemd_secrets {
            // When started by systemd, secret material must come exclusively
            // from the service-private credential file. Public runtime bounds
            // (funder, signature type, limits) may remain in EnvironmentFile.
            Some(secrets) if secret_environment_name(name).is_some() => secrets.get(name).cloned(),
            _ => std::env::var(name).ok(),
        };
        let private_key = required(&lookup, PRIVATE_KEY_ENV)?;
        let signer = LocalSigner::from_str(&private_key)
            .map_err(|_| CopyAdapterError::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON));

        let unauthenticated = Client::new(CLOB_HOST, Config::default())
            .map_err(CopyAdapterError::ClientInitialization)?;
        let credentials = match credential_source_from_lookup(&lookup)? {
            CredentialSource::Existing {
                api_key,
                api_secret,
                api_passphrase,
            } => {
                let key_uuid = api_key
                    .parse()
                    .map_err(|_| CopyAdapterError::InvalidApiKey)?;
                Credentials::new(key_uuid, api_secret, api_passphrase)
            }
            CredentialSource::Derive { nonce } => unauthenticated
                .derive_api_key(&signer, nonce)
                .await
                .map_err(CopyAdapterError::CredentialDerivation)?,
        };

        let signature_type = match optional(&lookup, SIGNATURE_TYPE_ENV) {
            Some(raw) => raw
                .parse::<ClobSignatureType>()
                .map_err(|_| CopyAdapterError::InvalidSignatureType)?
                .as_sdk(),
            None => SignatureType::Eoa,
        };
        let funder = match optional(&lookup, FUNDER_ENV) {
            Some(raw) => Some(
                Address::from_str(raw.trim())
                    .map_err(|_| CopyAdapterError::InvalidFunderAddress)?,
            ),
            None => None,
        };
        match (&signature_type, funder) {
            (SignatureType::Eoa, Some(_)) => return Err(CopyAdapterError::UnexpectedFunder),
            (SignatureType::Poly1271 | SignatureType::Proxy | SignatureType::GnosisSafe, None) => {
                return Err(CopyAdapterError::MissingFunder)
            }
            (
                SignatureType::Poly1271 | SignatureType::Proxy | SignatureType::GnosisSafe,
                Some(addr),
            ) if addr.is_zero() => return Err(CopyAdapterError::InvalidFunderAddress),
            _ => {}
        }

        let builder = unauthenticated
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(signature_type);
        let builder = match funder {
            Some(address) => builder.funder(address),
            None => builder,
        };
        let client = builder
            .authenticate()
            .await
            .map_err(CopyAdapterError::Authentication)?;
        let read = IntlClobReadAdapter::new(client.clone());
        Ok(Self {
            client,
            read,
            signer,
        })
    }

    pub fn read_adapter(&self) -> &IntlClobReadAdapter {
        &self.read
    }

    pub fn client(&self) -> &Client<Authenticated<Normal>> {
        &self.client
    }

    pub fn signer(&self) -> &PrivateKeySigner {
        &self.signer
    }
}

/// Builds a FAK receipt from the persisted request size and the venue's
/// maker/taker amounts. `requested_qty` is always `envelope.size`.
///
/// Shares filled:
/// - BUY: `taking_amount` (Phase 0.5 Result 3)
/// - SELL: `making_amount` (SDK `order_builder` swaps maker/taker by side)
pub fn receipt_from_submitted_envelope(
    envelope: &PreparedOrderEnvelope,
    making_amount: Decimal,
    taking_amount: Decimal,
) -> Result<OrderReceipt, String> {
    let requested = Decimal::from_str(&envelope.size)
        .map_err(|_| format!("invalid envelope size: {}", envelope.size))?;
    match envelope.side.as_str() {
        "BUY" => OrderReceipt::from_fak_buy_budget(requested, requested, taking_amount)
            .map_err(|error| error.to_string()),
        "SELL" => OrderReceipt::from_fak_sell_shares(requested, requested, making_amount)
            .map_err(|error| error.to_string()),
        other => Err(format!("unsupported envelope side: {other}")),
    }
}

/// Classifies an SDK `post_order` error. HTTP 4xx is a definitive rejection;
/// anything else (5xx, transport, internal) is treated as possibly-sent.
pub fn classify_sdk_submit_error(error: &polymarket_client_sdk_v2::error::Error) -> SubmitError {
    if let Some(status) = error.downcast_ref::<polymarket_client_sdk_v2::error::Status>() {
        if status.status_code.is_client_error() {
            return SubmitError::Rejected(error.to_string());
        }
    }
    SubmitError::Transport(error.to_string())
}

#[allow(clippy::manual_async_fn)]
impl CopyExecution for IntlClobCopyAdapter {
    fn position_for_token_strict(
        &self,
        token_id: &str,
    ) -> impl std::future::Future<Output = Result<Decimal, String>> + Send {
        let token = OutcomeTokenId::from_str(token_id);
        let read = self.read.clone();
        async move {
            let token = token.map_err(|error| error.to_string())?;
            read.position_for_token_strict(&token)
                .await
                .map_err(|error| error.to_string())
        }
    }

    fn order_for_receipt(
        &self,
        order_id: &OrderId,
    ) -> impl std::future::Future<Output = Result<VenueOrderState, String>> + Send {
        let client = self.client.clone();
        let order_id_owned = order_id.0.clone();
        async move {
            let response = client
                .order(&order_id_owned)
                .await
                .map_err(|error| error.to_string())?;
            Ok(VenueOrderState {
                order_id: OrderId(response.id),
                status: format!("{}", response.status),
                size_matched: response.size_matched,
            })
        }
    }

    fn query_prepared_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<Option<OrderReceipt>, String>> + Send {
        let read = self.read.clone();
        let envelope = envelope.clone();
        async move {
            let now = chrono::Utc::now();
            let window = TradeHistoryWindow::new(now - chrono::Duration::hours(24), now)
                .map_err(|error| error.to_string())?;
            match lookup_prepared_fak_in_trade_history(&read, &envelope, window).await {
                Ok(TradeHistoryLookup::Recovered { filled_qty, .. }) => {
                    let size = Decimal::from_str(&envelope.size).unwrap_or(Decimal::ZERO);
                    let receipt = match envelope.side.as_str() {
                        "BUY" => OrderReceipt::from_fak_buy_budget(size, size, filled_qty),
                        "SELL" => OrderReceipt::from_fak_sell_shares(size, size, filled_qty),
                        other => return Err(format!("unsupported envelope side: {other}")),
                    }
                    .map_err(|error| error.to_string())?;
                    Ok(Some(receipt))
                }
                Ok(TradeHistoryLookup::NotFound) => Ok(None),
                Err(error) => Err(error.to_string()),
            }
        }
    }

    fn submit_exact_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<OrderReceipt, SubmitError>> + Send {
        let client = self.client.clone();
        let envelope = envelope.clone();
        async move {
            let signed_order = reconstruct_signed_order(&envelope.signed_order_json)
                .map_err(SubmitError::Local)?;
            let response = match client.post_order(signed_order).await {
                Ok(response) => response,
                Err(error) => return Err(classify_sdk_submit_error(&error)),
            };
            if !response.success {
                return Err(SubmitError::Rejected(
                    response.error_msg.unwrap_or_default(),
                ));
            }
            receipt_from_submitted_envelope(
                &envelope,
                response.making_amount,
                response.taking_amount,
            )
            .map_err(SubmitError::Local)
        }
    }
}

#[derive(Clone, Debug)]
enum CredentialSource {
    Existing {
        api_key: String,
        api_secret: String,
        api_passphrase: String,
    },
    Derive {
        nonce: Option<u32>,
    },
}

fn credential_source_from_lookup<F>(lookup: &F) -> Result<CredentialSource, CopyAdapterError>
where
    F: Fn(&str) -> Option<String>,
{
    let api_key = optional(lookup, API_KEY_ENV);
    let api_secret = optional(lookup, API_SECRET_ENV);
    let api_passphrase = optional(lookup, API_PASSPHRASE_ENV);
    let supplied = [
        api_key.is_some(),
        api_secret.is_some(),
        api_passphrase.is_some(),
    ]
    .into_iter()
    .filter(|flag| *flag)
    .count();
    let nonce = optional(lookup, API_NONCE_ENV)
        .map(|raw| {
            raw.parse::<u32>()
                .map_err(|_| CopyAdapterError::InvalidL2Nonce)
        })
        .transpose()?;
    match supplied {
        0 => Ok(CredentialSource::Derive { nonce }),
        3 if nonce.is_none() => Ok(CredentialSource::Existing {
            api_key: api_key.expect("three supplied values include API key"),
            api_secret: api_secret.expect("three supplied values include API secret"),
            api_passphrase: api_passphrase.expect("three supplied values include API passphrase"),
        }),
        3 => Err(CopyAdapterError::NonceWithExistingCredentials),
        _ => Err(CopyAdapterError::IncompleteL2Credentials),
    }
}

fn required<F>(lookup: &F, name: &'static str) -> Result<String, CopyAdapterError>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or(CopyAdapterError::MissingPrivateKey)
}

fn optional<F>(lookup: &F, name: &str) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name).filter(|value| !value.trim().is_empty())
}

type SystemdSecretValues = HashMap<&'static str, String>;

fn load_systemd_secret_credential() -> Result<Option<SystemdSecretValues>, CopyAdapterError> {
    let Some(directory) = std::env::var_os(SYSTEMD_CREDENTIALS_DIRECTORY_ENV) else {
        return Ok(None);
    };
    if directory.is_empty() {
        return Err(CopyAdapterError::EmptySystemdCredentialsDirectory);
    }
    let path = PathBuf::from(directory).join(SYSTEMD_SECRET_CREDENTIAL_NAME);
    let contents =
        fs::read_to_string(&path).map_err(|source| CopyAdapterError::SystemdCredentialRead {
            path: path.clone(),
            source,
        })?;
    parse_systemd_secret_credential(&contents).map(Some)
}

fn parse_systemd_secret_credential(
    contents: &str,
) -> Result<SystemdSecretValues, CopyAdapterError> {
    let mut values = SystemdSecretValues::new();
    for (index, raw_line) in contents.lines().enumerate() {
        let line = index + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let (raw_name, raw_value) = raw_line
            .split_once('=')
            .ok_or(CopyAdapterError::InvalidSystemdCredentialEntry { line })?;
        let raw_name = raw_name.trim();
        let name = secret_environment_name(raw_name).ok_or_else(|| {
            CopyAdapterError::UnexpectedSystemdCredentialName {
                line,
                name: raw_name.to_owned(),
            }
        })?;
        if values.insert(name, raw_value.to_owned()).is_some() {
            return Err(CopyAdapterError::DuplicateSystemdCredentialName { line, name });
        }
    }
    Ok(values)
}

fn secret_environment_name(name: &str) -> Option<&'static str> {
    match name {
        PRIVATE_KEY_ENV => Some(PRIVATE_KEY_ENV),
        API_KEY_ENV => Some(API_KEY_ENV),
        API_SECRET_ENV => Some(API_SECRET_ENV),
        API_PASSPHRASE_ENV => Some(API_PASSPHRASE_ENV),
        API_NONCE_ENV => Some(API_NONCE_ENV),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ClobSignatureType {
    Eoa,
    Proxy,
    GnosisSafe,
    Poly1271,
}

impl ClobSignatureType {
    fn as_sdk(self) -> SignatureType {
        match self {
            Self::Eoa => SignatureType::Eoa,
            Self::Proxy => SignatureType::Proxy,
            Self::GnosisSafe => SignatureType::GnosisSafe,
            Self::Poly1271 => SignatureType::Poly1271,
        }
    }
}

impl std::str::FromStr for ClobSignatureType {
    type Err = CopyAdapterError;

    fn from_str(raw: &str) -> Result<Self, CopyAdapterError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eoa" => Ok(Self::Eoa),
            "proxy" => Ok(Self::Proxy),
            "gnosis_safe" => Ok(Self::GnosisSafe),
            "poly1271" => Ok(Self::Poly1271),
            _ => Err(CopyAdapterError::InvalidSignatureType),
        }
    }
}

#[derive(Debug)]
pub enum CopyAdapterError {
    MissingPrivateKey,
    EmptySystemdCredentialsDirectory,
    SystemdCredentialRead {
        path: PathBuf,
        source: std::io::Error,
    },
    InvalidSystemdCredentialEntry {
        line: usize,
    },
    UnexpectedSystemdCredentialName {
        line: usize,
        name: String,
    },
    DuplicateSystemdCredentialName {
        line: usize,
        name: &'static str,
    },
    InvalidPrivateKey,
    InvalidApiKey,
    InvalidFunderAddress,
    UnexpectedFunder,
    MissingFunder,
    InvalidSignatureType,
    IncompleteL2Credentials,
    InvalidL2Nonce,
    NonceWithExistingCredentials,
    ClientInitialization(polymarket_client_sdk_v2::error::Error),
    CredentialDerivation(polymarket_client_sdk_v2::error::Error),
    Authentication(polymarket_client_sdk_v2::error::Error),
}

impl std::fmt::Display for CopyAdapterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPrivateKey => write!(formatter, "missing POLYCOPY_CLOB_PRIVATE_KEY"),
            Self::EmptySystemdCredentialsDirectory => write!(
                formatter,
                "CREDENTIALS_DIRECTORY is set but empty; copy-engine secret credential cannot be read"
            ),
            Self::SystemdCredentialRead { path, source } => write!(
                formatter,
                "unable to read systemd copy-engine secret credential at {}: {source}",
                path.display()
            ),
            Self::InvalidSystemdCredentialEntry { line } => write!(
                formatter,
                "systemd copy-engine secret credential line {line} must use NAME=VALUE"
            ),
            Self::UnexpectedSystemdCredentialName { line, name } => write!(
                formatter,
                "systemd copy-engine secret credential line {line} has unsupported name {name}"
            ),
            Self::DuplicateSystemdCredentialName { line, name } => write!(
                formatter,
                "systemd copy-engine secret credential line {line} repeats {name}"
            ),
            Self::InvalidPrivateKey => write!(formatter, "invalid CLOB signing key"),
            Self::InvalidApiKey => write!(formatter, "invalid CLOB L2 API key UUID"),
            Self::InvalidFunderAddress => write!(formatter, "invalid CLOB funder address"),
            Self::UnexpectedFunder => write!(formatter, "EOA mode must not set a funder"),
            Self::MissingFunder => write!(formatter, "non-EOA mode requires a funder"),
            Self::InvalidSignatureType => write!(
                formatter,
                "CLOB signature type must be eoa, proxy, gnosis_safe, or poly1271"
            ),
            Self::IncompleteL2Credentials => write!(
                formatter,
                "supply all three L2 credential variables or none to derive an existing credential"
            ),
            Self::InvalidL2Nonce => write!(formatter, "invalid L2 credential derivation nonce"),
            Self::NonceWithExistingCredentials => {
                write!(
                    formatter,
                    "L2 credential nonce is only valid when deriving credentials"
                )
            }
            Self::ClientInitialization(source) => {
                write!(formatter, "unable to initialize the CLOB client: {source}")
            }
            Self::CredentialDerivation(source) => {
                write!(
                    formatter,
                    "unable to derive an existing CLOB L2 API credential: {source}"
                )
            }
            Self::Authentication(source) => {
                write!(
                    formatter,
                    "unable to construct an authenticated CLOB client: {source}"
                )
            }
        }
    }
}

impl std::error::Error for CopyAdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ClientInitialization(source)
            | Self::CredentialDerivation(source)
            | Self::Authentication(source) => Some(source),
            Self::SystemdCredentialRead { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal as RustDecimal;

    fn envelope(side: &str, size: &str) -> PreparedOrderEnvelope {
        PreparedOrderEnvelope {
            token_id: "123456".to_owned(),
            side: side.to_owned(),
            price: "0.55".to_owned(),
            size: size.to_owned(),
            salt: 1,
            order_type: "FAK".to_owned(),
            expected_taker_order_id: "0xabc".to_owned(),
            signed_order_json: "{}".to_owned(),
        }
    }

    #[test]
    fn buy_filled_qty_is_taking_amount_not_requested_size() {
        let receipt = receipt_from_submitted_envelope(
            &envelope("BUY", "5"),
            RustDecimal::new(2749999, 6),
            RustDecimal::new(528846, 5),
        )
        .expect("BUY receipt");
        assert_eq!(receipt.requested_qty(), RustDecimal::new(5, 0));
        assert_eq!(receipt.filled_qty(), RustDecimal::new(528846, 5));
        assert_ne!(receipt.filled_qty(), receipt.requested_qty());
    }

    #[test]
    fn sell_filled_qty_is_making_amount_not_taking_amount() {
        let usdc_received = RustDecimal::new(275, 2);
        let shares_sold = RustDecimal::new(5, 0);
        let receipt =
            receipt_from_submitted_envelope(&envelope("SELL", "5"), shares_sold, usdc_received)
                .expect("SELL receipt");
        assert_eq!(receipt.requested_qty(), shares_sold);
        assert_eq!(receipt.filled_qty(), shares_sold);
        assert_ne!(receipt.filled_qty(), usdc_received);
    }

    #[test]
    fn requested_qty_comes_from_the_persisted_envelope_not_the_response() {
        let receipt = receipt_from_submitted_envelope(
            &envelope("BUY", "5"),
            RustDecimal::new(1, 0),
            RustDecimal::new(2, 0),
        )
        .expect("BUY receipt");
        assert_eq!(receipt.requested_qty(), RustDecimal::new(5, 0));
        assert_ne!(receipt.requested_qty(), RustDecimal::new(1, 0));
        assert_ne!(receipt.requested_qty(), RustDecimal::new(2, 0));
    }

    #[test]
    fn classify_treats_http_400_as_definitive_rejection() {
        let error = polymarket_client_sdk_v2::error::Error::status(
            polymarket_client_sdk_v2::error::StatusCode::BAD_REQUEST,
            polymarket_client_sdk_v2::error::Method::POST,
            "/order".to_owned(),
            r#"{"error":"invalid. Duplicated."}"#,
        );
        assert!(matches!(
            classify_sdk_submit_error(&error),
            SubmitError::Rejected(_)
        ));
    }

    #[test]
    fn classify_treats_http_503_as_transport_uncertain() {
        let error = polymarket_client_sdk_v2::error::Error::status(
            polymarket_client_sdk_v2::error::StatusCode::SERVICE_UNAVAILABLE,
            polymarket_client_sdk_v2::error::Method::POST,
            "/order".to_owned(),
            r#"{"error":"trading is disabled"}"#,
        );
        assert!(matches!(
            classify_sdk_submit_error(&error),
            SubmitError::Transport(_)
        ));
    }

    #[test]
    fn systemd_copy_secret_allows_only_signing_and_l2_fields() {
        let values = parse_systemd_secret_credential(
            "POLYCOPY_CLOB_PRIVATE_KEY=not-a-real-key\nPOLYCOPY_CLOB_L2_NONCE=7\n",
        )
        .expect("allowed credential fields");
        assert_eq!(
            values.get(PRIVATE_KEY_ENV).map(String::as_str),
            Some("not-a-real-key")
        );
        assert_eq!(values.get(API_NONCE_ENV).map(String::as_str), Some("7"));
    }

    #[test]
    fn systemd_copy_secret_rejects_public_runtime_configuration() {
        let error = parse_systemd_secret_credential("POLYCOPY_ENGINE_EXECUTE=yes\n")
            .expect_err("execution guard must remain public service configuration");
        assert!(matches!(
            error,
            CopyAdapterError::UnexpectedSystemdCredentialName { .. }
        ));
    }

    #[test]
    fn systemd_copy_secret_rejects_duplicate_keys() {
        let error = parse_systemd_secret_credential(
            "POLYCOPY_CLOB_PRIVATE_KEY=one\nPOLYCOPY_CLOB_PRIVATE_KEY=two\n",
        )
        .expect_err("duplicate secret material is ambiguous");
        assert!(matches!(
            error,
            CopyAdapterError::DuplicateSystemdCredentialName { .. }
        ));
    }
}
