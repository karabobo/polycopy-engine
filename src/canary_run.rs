//! Orchestration for the Phase 0.5 CLOB submission-safety canary.
//!
//! This is the only place in the project that calls an order-writing venue
//! method. It exists solely to answer the three Phase 0.5 questions in
//! `docs/COPY_ENGINE_BLUEPRINT.md` with one deliberately tiny, FAK-only
//! order; it is not a reusable trading API and nothing else in this crate
//! depends on it. `src/bin/canary_probe.rs` is a dry run unless the operator
//! explicitly opts in via environment variables — see that file's doc
//! comment for the exact gate.
//!
//! Credential handling mirrors `ghost_run.rs` (same environment variable
//! names, same derive-only-by-default policy, same "never `Debug`, never
//! logged" discipline for secrets) but is kept as a separate copy rather than
//! a shared refactor, so this new, higher-risk code path cannot regress the
//! already-verified Phase 0 GHOST tool.

use std::{collections::HashMap, env, error::Error, fmt, fs, path::PathBuf, str::FromStr};

use polymarket_client_sdk_v2::{
    auth::{state::Authenticated, Credentials, LocalSigner, Normal, Signer},
    clob::{
        types::{
            response::{OpenOrderResponse, PostOrderResponse},
            OrderPayload, OrderType, Side, SignableOrder, SignatureType, SignedOrder,
        },
        Client, Config,
    },
    contract_config,
    error::Error as SdkError,
    types::{Address, Decimal, B256, U256},
    POLYGON,
};

use crate::canary::{CanaryOrderSpec, CanarySide};
use crate::venue::order_hash::{self, ExchangeAddresses, OrderHashError};

type AuthenticatedClient = Client<Authenticated<Normal>>;

const CLOB_HOST: &str = "https://clob.polymarket.com";

const PRIVATE_KEY_ENV: &str = "POLYCOPY_CLOB_PRIVATE_KEY";
const API_KEY_ENV: &str = "POLYCOPY_CLOB_L2_API_KEY";
const API_SECRET_ENV: &str = "POLYCOPY_CLOB_L2_API_SECRET";
const API_PASSPHRASE_ENV: &str = "POLYCOPY_CLOB_L2_API_PASSPHRASE";
const API_NONCE_ENV: &str = "POLYCOPY_CLOB_L2_NONCE";
const SIGNATURE_TYPE_ENV: &str = "POLYCOPY_CLOB_SIGNATURE_TYPE";
const FUNDER_ENV: &str = "POLYCOPY_CLOB_FUNDER";

const TOKEN_ID_ENV: &str = "POLYCOPY_CANARY_TOKEN_ID";
const SIDE_ENV: &str = "POLYCOPY_CANARY_SIDE";
const PRICE_ENV: &str = "POLYCOPY_CANARY_PRICE";
const SIZE_ENV: &str = "POLYCOPY_CANARY_SIZE";
const LABEL_ENV: &str = "POLYCOPY_CANARY_LABEL";
const SYSTEMD_CREDENTIALS_DIRECTORY_ENV: &str = "CREDENTIALS_DIRECTORY";
const SYSTEMD_SECRET_CREDENTIAL_NAME: &str = "polycopy-canary-secrets";

/// Set to exactly `yes` to allow the one live `post_order` call. Any other
/// value, including unset, keeps the probe a dry run.
pub const CONFIRM_SUBMIT_ENV: &str = "POLYCOPY_CANARY_CONFIRM_SUBMIT";
/// Set to exactly `yes` (in addition to [`CONFIRM_SUBMIT_ENV`]) to also submit
/// a second, independently-signed copy of the same order for the
/// byte-identical duplicate-submission question. Only meaningful once the
/// first submission's result already exists on disk.
pub const CONFIRM_DUPLICATE_ENV: &str = "POLYCOPY_CANARY_CONFIRM_DUPLICATE";

pub struct CanaryRunConfig {
    private_key: String,
    credential_source: CredentialSource,
    signature_type: ClobSignatureType,
    funder: Option<Address>,
    pub spec: CanaryOrderSpec,
    pub label: String,
}

impl CanaryRunConfig {
    pub fn from_env() -> Result<Self, CanaryRunConfigError> {
        let systemd_secrets = load_systemd_secret_credential()?;
        Self::from_lookup(|name| match &systemd_secrets {
            // A systemd invocation has an explicit credential directory. In
            // that mode signing/L2 material is accepted only from the
            // credential file, never from the process environment.
            Some(secrets) if secret_environment_name(name).is_some() => secrets.get(name).cloned(),
            _ => env::var(name).ok(),
        })
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, CanaryRunConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let private_key = required(&lookup, PRIVATE_KEY_ENV)?;
        let credential_source = credential_source(&lookup)?;

        let signature_type = match lookup(SIGNATURE_TYPE_ENV) {
            Some(raw) if !raw.trim().is_empty() => ClobSignatureType::from_str(&raw)?,
            _ => ClobSignatureType::Eoa,
        };

        let funder = match lookup(FUNDER_ENV) {
            Some(raw) if !raw.trim().is_empty() => Some(
                Address::from_str(raw.trim())
                    .map_err(|_| CanaryRunConfigError::InvalidFunderAddress)?,
            ),
            _ => None,
        };
        signature_type.validate_funder(funder)?;

        let token_id = required(&lookup, TOKEN_ID_ENV)?;
        let side = required(&lookup, SIDE_ENV)?
            .parse::<CanarySide>()
            .map_err(|_| CanaryRunConfigError::InvalidSide)?;
        let price = parse_decimal(&required(&lookup, PRICE_ENV)?, PRICE_ENV)?;
        let size = parse_decimal(&required(&lookup, SIZE_ENV)?, SIZE_ENV)?;
        let spec = CanaryOrderSpec::new(token_id, side, price, size)
            .map_err(CanaryRunConfigError::Spec)?;
        let label = required(&lookup, LABEL_ENV)?;

        Ok(Self {
            private_key,
            credential_source,
            signature_type,
            funder,
            spec,
            label,
        })
    }

    /// Confirms this run is authorized to submit the first live order.
    pub fn confirm_submit(&self) -> bool {
        std::env::var(CONFIRM_SUBMIT_ENV).as_deref() == Ok("yes")
    }

    /// Confirms this run is additionally authorized to submit the duplicate.
    pub fn confirm_duplicate(&self) -> bool {
        std::env::var(CONFIRM_DUPLICATE_ENV).as_deref() == Ok("yes")
    }

    /// Builds an authenticated CLOB client using supplied credentials or a
    /// strict derive-only L2 request. Never calls API-key creation.
    pub async fn authenticated_client(&self) -> Result<AuthenticatedClient, CanaryRunError> {
        let signer = LocalSigner::from_str(&self.private_key)
            .map_err(|_| CanaryRunError::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON));
        let unauthenticated = Client::new(CLOB_HOST, Config::default())
            .map_err(CanaryRunError::ClientInitialization)?;
        let credentials = match &self.credential_source {
            CredentialSource::Existing {
                api_key,
                api_secret,
                api_passphrase,
            } => {
                let api_key = api_key.parse().map_err(|_| CanaryRunError::InvalidApiKey)?;
                Credentials::new(api_key, api_secret.clone(), api_passphrase.clone())
            }
            CredentialSource::Derive { nonce } => unauthenticated
                .derive_api_key(&signer, *nonce)
                .await
                .map_err(CanaryRunError::CredentialDerivation)?,
        };
        let builder = unauthenticated
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(self.signature_type.as_sdk());
        let builder = match self.funder {
            Some(funder) => builder.funder(funder),
            None => builder,
        };
        builder
            .authenticate()
            .await
            .map_err(CanaryRunError::Authentication)
    }

    /// Builds the signer used both to authenticate and to sign orders.
    pub fn signer(&self) -> Result<impl Signer + Clone, CanaryRunError> {
        Ok(LocalSigner::from_str(&self.private_key)
            .map_err(|_| CanaryRunError::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON)))
    }
}

/// Builds the one `SignableOrder` for this attempt. Local field validation
/// plus one construction call against the authenticated client; no
/// order-writing call happens here (see the module doc for the exact
/// boundary).
pub async fn build_signable_order(
    client: &AuthenticatedClient,
    spec: &CanaryOrderSpec,
) -> Result<SignableOrder, CanaryRunError> {
    let token_id =
        U256::from_str(spec.token_id()).map_err(|_| CanaryRunError::InvalidCanaryTokenId)?;
    let side = match spec.side() {
        CanarySide::Buy => Side::Buy,
        CanarySide::Sell => Side::Sell,
    };

    client
        .limit_order()
        .token_id(token_id)
        .side(side)
        .price(spec.price())
        .size(spec.size())
        .order_type(OrderType::FAK)
        .build()
        .await
        .map_err(CanaryRunError::Building)
}

/// Signs `signable` twice from two independent clones. Both signatures are
/// expected to be byte-identical (EIP-712 signing over the same struct with
/// the same key is deterministic), which is what lets the duplicate-
/// submission question be tested without ever persisting a `SignedOrder`
/// across a process boundary (the SDK does not implement `Deserialize` for
/// it).
pub async fn sign_twice<S: Signer>(
    client: &AuthenticatedClient,
    signer: &S,
    signable: SignableOrder,
) -> Result<(SignedOrder, SignedOrder), CanaryRunError> {
    let first = client
        .sign(signer, signable.clone())
        .await
        .map_err(CanaryRunError::Signing)?;
    let second = client
        .sign(signer, signable)
        .await
        .map_err(CanaryRunError::Signing)?;
    Ok((first, second))
}

/// The one order-writing call in this project. Submits `order` exactly once;
/// callers must have already checked that no submission record exists yet
/// for this attempt (see `write_new_record` in `crate::canary`) before
/// calling this.
pub async fn submit(
    client: &AuthenticatedClient,
    order: SignedOrder,
) -> Result<PostOrderResponse, CanaryRunError> {
    client
        .post_order(order)
        .await
        .map_err(CanaryRunError::Submission)
}

/// Looks up a previously submitted order by its venue-assigned ID.
pub async fn lookup_by_id(
    client: &AuthenticatedClient,
    order_id: &str,
) -> Result<OpenOrderResponse, CanaryRunError> {
    client.order(order_id).await.map_err(CanaryRunError::Lookup)
}

/// Computes the order ID this project's own offline hash (see
/// `crate::venue::order_hash`) predicts for `payload`, by resolving the
/// token's neg-risk status and its matching exchange contract address. This
/// is a read-only call (one authenticated `neg_risk` GET) -- it never
/// writes anything to the venue, so it is safe to run on every dry run, not
/// only a confirmed live submission. Whether the result actually equals the
/// venue's real `order_id`/`taker_order_id` is exactly the open question
/// `docs/PHASE_0_5_CANARY_REPORT.md` still needs a live comparison to close.
pub async fn expected_order_id(
    client: &AuthenticatedClient,
    payload: &OrderPayload,
) -> Result<B256, CanaryRunError> {
    let token_id = match payload {
        OrderPayload::V1(p) => p.order.tokenId,
        OrderPayload::V2(p) => p.order.tokenId,
        _ => {
            return Err(CanaryRunError::OrderHash(
                OrderHashError::UnsupportedPayloadVersion,
            ))
        }
    };
    let neg_risk = client
        .neg_risk(token_id)
        .await
        .map_err(CanaryRunError::NegRiskQuery)?
        .neg_risk;
    let config = contract_config(POLYGON, neg_risk).ok_or(CanaryRunError::MissingContractConfig)?;
    let exchanges = ExchangeAddresses {
        v1: config.exchange,
        v2: config.exchange_v2,
    };

    order_hash::expected_order_id(payload, &exchanges, POLYGON).map_err(CanaryRunError::OrderHash)
}

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

    fn validate_funder(self, funder: Option<Address>) -> Result<(), CanaryRunConfigError> {
        match (self, funder) {
            (Self::Eoa, Some(_)) => Err(CanaryRunConfigError::UnexpectedFunder),
            (Self::Poly1271, None) => Err(CanaryRunConfigError::MissingFunder),
            (Self::Poly1271, Some(address)) if address.is_zero() => {
                Err(CanaryRunConfigError::InvalidFunderAddress)
            }
            _ => Ok(()),
        }
    }
}

impl FromStr for ClobSignatureType {
    type Err = CanaryRunConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eoa" => Ok(Self::Eoa),
            "proxy" => Ok(Self::Proxy),
            "gnosis_safe" => Ok(Self::GnosisSafe),
            "poly1271" => Ok(Self::Poly1271),
            _ => Err(CanaryRunConfigError::InvalidSignatureType),
        }
    }
}

fn credential_source<F>(lookup: &F) -> Result<CredentialSource, CanaryRunConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    let api_key = optional(lookup, API_KEY_ENV);
    let api_secret = optional(lookup, API_SECRET_ENV);
    let api_passphrase = optional(lookup, API_PASSPHRASE_ENV);
    let supplied_count = [
        api_key.is_some(),
        api_secret.is_some(),
        api_passphrase.is_some(),
    ]
    .into_iter()
    .filter(|supplied| *supplied)
    .count();
    let nonce = optional(lookup, API_NONCE_ENV)
        .map(|raw| {
            raw.parse::<u32>()
                .map_err(|_| CanaryRunConfigError::InvalidL2Nonce)
        })
        .transpose()?;

    match supplied_count {
        0 => Ok(CredentialSource::Derive { nonce }),
        3 if nonce.is_none() => Ok(CredentialSource::Existing {
            api_key: api_key.expect("three supplied values include API key"),
            api_secret: api_secret.expect("three supplied values include API secret"),
            api_passphrase: api_passphrase.expect("three supplied values include API passphrase"),
        }),
        3 => Err(CanaryRunConfigError::NonceWithExistingCredentials),
        _ => Err(CanaryRunConfigError::IncompleteL2Credentials),
    }
}

fn required<F>(lookup: &F, name: &'static str) -> Result<String, CanaryRunConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or(CanaryRunConfigError::MissingEnvironment { name })
}

fn optional<F>(lookup: &F, name: &str) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name).filter(|value| !value.trim().is_empty())
}

fn parse_decimal(raw: &str, name: &'static str) -> Result<Decimal, CanaryRunConfigError> {
    Decimal::from_str(raw.trim()).map_err(|_| CanaryRunConfigError::InvalidDecimal { name })
}

#[derive(Debug)]
pub enum CanaryRunConfigError {
    MissingEnvironment {
        name: &'static str,
    },
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
    InvalidFunderAddress,
    UnexpectedFunder,
    MissingFunder,
    InvalidSignatureType,
    InvalidDecimal {
        name: &'static str,
    },
    InvalidSide,
    IncompleteL2Credentials,
    InvalidL2Nonce,
    NonceWithExistingCredentials,
    Spec(crate::canary::CanarySpecError),
}

impl fmt::Display for CanaryRunConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment { name } => write!(formatter, "missing required {name}"),
            Self::EmptySystemdCredentialsDirectory => write!(
                formatter,
                "CREDENTIALS_DIRECTORY is set but empty; systemd secret credential cannot be read"
            ),
            Self::SystemdCredentialRead { path, source } => write!(
                formatter,
                "unable to read systemd secret credential at {}: {source}",
                path.display()
            ),
            Self::InvalidSystemdCredentialEntry { line } => write!(
                formatter,
                "systemd secret credential line {line} must use NAME=VALUE"
            ),
            Self::UnexpectedSystemdCredentialName { line, name } => write!(
                formatter,
                "systemd secret credential line {line} has unsupported name {name}"
            ),
            Self::DuplicateSystemdCredentialName { line, name } => write!(
                formatter,
                "systemd secret credential line {line} repeats {name}"
            ),
            Self::InvalidFunderAddress => write!(formatter, "invalid CLOB funder address"),
            Self::UnexpectedFunder => write!(formatter, "EOA mode must not set a funder"),
            Self::MissingFunder => write!(formatter, "Poly1271 mode requires a funder"),
            Self::InvalidSignatureType => write!(
                formatter,
                "CLOB signature type must be eoa, proxy, gnosis_safe, or poly1271"
            ),
            Self::InvalidDecimal { name } => write!(formatter, "invalid decimal value for {name}"),
            Self::InvalidSide => write!(formatter, "{SIDE_ENV} must be BUY or SELL"),
            Self::IncompleteL2Credentials => write!(
                formatter,
                "supply all three L2 credential variables or none to derive an existing credential"
            ),
            Self::InvalidL2Nonce => write!(formatter, "invalid L2 credential derivation nonce"),
            Self::NonceWithExistingCredentials => write!(
                formatter,
                "L2 credential nonce is only valid when deriving credentials"
            ),
            Self::Spec(source) => source.fmt(formatter),
        }
    }
}

impl Error for CanaryRunConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SystemdCredentialRead { source, .. } => Some(source),
            Self::Spec(source) => Some(source),
            _ => None,
        }
    }
}

type SystemdSecretValues = HashMap<&'static str, String>;

/// Reads the one systemd credential file when launched by the static canary
/// unit. Returning `None` retains the explicit local CLI workflow.
fn load_systemd_secret_credential() -> Result<Option<SystemdSecretValues>, CanaryRunConfigError> {
    let Some(directory) = env::var_os(SYSTEMD_CREDENTIALS_DIRECTORY_ENV) else {
        return Ok(None);
    };
    if directory.is_empty() {
        return Err(CanaryRunConfigError::EmptySystemdCredentialsDirectory);
    }

    let path = PathBuf::from(directory).join(SYSTEMD_SECRET_CREDENTIAL_NAME);
    let contents = fs::read_to_string(&path).map_err(|source| {
        CanaryRunConfigError::SystemdCredentialRead {
            path: path.clone(),
            source,
        }
    })?;
    parse_systemd_secret_credential(&contents).map(Some)
}

/// Parses only signing/L2 fields. Canary settings and both live-submit gates
/// must remain in the public EnvironmentFile and cannot be smuggled through
/// systemd credentials.
fn parse_systemd_secret_credential(
    contents: &str,
) -> Result<SystemdSecretValues, CanaryRunConfigError> {
    let mut values = SystemdSecretValues::new();

    for (index, raw_line) in contents.lines().enumerate() {
        let line = index + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let (raw_name, raw_value) = raw_line
            .split_once('=')
            .ok_or(CanaryRunConfigError::InvalidSystemdCredentialEntry { line })?;
        let raw_name = raw_name.trim();
        let name = secret_environment_name(raw_name).ok_or_else(|| {
            CanaryRunConfigError::UnexpectedSystemdCredentialName {
                line,
                name: raw_name.to_owned(),
            }
        })?;
        if values.insert(name, raw_value.to_owned()).is_some() {
            return Err(CanaryRunConfigError::DuplicateSystemdCredentialName { line, name });
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

#[derive(Debug)]
pub enum CanaryRunError {
    InvalidPrivateKey,
    InvalidApiKey,
    InvalidCanaryTokenId,
    ClientInitialization(SdkError),
    CredentialDerivation(SdkError),
    Authentication(SdkError),
    Building(SdkError),
    Signing(SdkError),
    Submission(SdkError),
    Lookup(SdkError),
    NegRiskQuery(SdkError),
    MissingContractConfig,
    OrderHash(OrderHashError),
}

impl fmt::Display for CanaryRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrivateKey => write!(formatter, "invalid CLOB signing key"),
            Self::InvalidApiKey => write!(formatter, "invalid CLOB L2 API key"),
            Self::InvalidCanaryTokenId => write!(formatter, "invalid canary outcome token ID"),
            Self::ClientInitialization(source) => {
                write!(formatter, "unable to initialize the CLOB client: {source}")
            }
            Self::CredentialDerivation(source) => write!(
                formatter,
                "unable to derive an existing CLOB L2 API credential; no credential was created: {source}"
            ),
            Self::Authentication(source) => write!(
                formatter,
                "unable to construct an authenticated CLOB client: {source}"
            ),
            Self::Building(source) => write!(formatter, "unable to build the canary order: {source}"),
            Self::Signing(source) => write!(formatter, "unable to sign the canary order: {source}"),
            Self::Submission(source) => {
                write!(formatter, "unable to submit the canary order: {source}")
            }
            Self::Lookup(source) => write!(formatter, "unable to look up the canary order: {source}"),
            Self::NegRiskQuery(source) => write!(
                formatter,
                "unable to resolve the canary token's neg-risk status: {source}"
            ),
            Self::MissingContractConfig => write!(
                formatter,
                "no exchange contract configuration for this chain/neg-risk combination"
            ),
            Self::OrderHash(source) => write!(formatter, "unable to compute the expected order ID: {source}"),
        }
    }
}

impl Error for CanaryRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPrivateKey
            | Self::InvalidApiKey
            | Self::InvalidCanaryTokenId
            | Self::MissingContractConfig => None,
            Self::ClientInitialization(source)
            | Self::CredentialDerivation(source)
            | Self::Authentication(source)
            | Self::Building(source)
            | Self::Signing(source)
            | Self::Submission(source)
            | Self::Lookup(source)
            | Self::NegRiskQuery(source) => Some(source),
            Self::OrderHash(source) => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_secret_credential_accepts_only_signing_and_l2_material() {
        let values = parse_systemd_secret_credential(
            "# canary settings do not belong here\n\
             POLYCOPY_CLOB_PRIVATE_KEY=0xtest\n\
             POLYCOPY_CLOB_L2_API_KEY=key\n\
             POLYCOPY_CLOB_L2_API_SECRET=secret=with=equals\n\
             POLYCOPY_CLOB_L2_API_PASSPHRASE=passphrase\n",
        )
        .expect("recognized secret fields are accepted");

        assert_eq!(
            values.get(PRIVATE_KEY_ENV).map(String::as_str),
            Some("0xtest")
        );
        assert_eq!(
            values.get(API_SECRET_ENV).map(String::as_str),
            Some("secret=with=equals")
        );
    }

    #[test]
    fn systemd_secret_credential_rejects_public_and_duplicate_fields() {
        assert!(matches!(
            parse_systemd_secret_credential("POLYCOPY_CANARY_CONFIRM_SUBMIT=yes\n"),
            Err(CanaryRunConfigError::UnexpectedSystemdCredentialName { .. })
        ));
        assert!(matches!(
            parse_systemd_secret_credential(
                "POLYCOPY_CLOB_PRIVATE_KEY=first\nPOLYCOPY_CLOB_PRIVATE_KEY=second\n"
            ),
            Err(CanaryRunConfigError::DuplicateSystemdCredentialName { .. })
        ));
        assert!(matches!(
            parse_systemd_secret_credential("POLYCOPY_CLOB_PRIVATE_KEY\n"),
            Err(CanaryRunConfigError::InvalidSystemdCredentialEntry { line: 1 })
        ));
    }
}
