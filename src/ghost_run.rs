//! Configuration and execution boundary for an authenticated, read-only GHOST run.
//!
//! The runner derives an existing L2 credential from the signing key when one
//! is not supplied. It deliberately never invokes the SDK path that can create
//! an API credential, because creation is outside Phase 0's read-only scope.

use std::{collections::HashMap, env, error::Error, fmt, fs, path::PathBuf, str::FromStr};

use polymarket_client_sdk_v2::{
    auth::{Credentials, LocalSigner, Signer as _},
    clob::{types::SignatureType, Client, Config},
    error::Error as SdkError,
    types::{Address, DateTime, Decimal},
    POLYGON,
};

use crate::{
    ExpectedTokenBalance, GhostSnapshot, GhostSnapshotError, GhostVerification, GhostVerifier,
    IntlClobReadAdapter, OutcomeTokenId,
};

const CLOB_HOST: &str = "https://clob.polymarket.com";

const PRIVATE_KEY_ENV: &str = "POLYCOPY_CLOB_PRIVATE_KEY";
const API_KEY_ENV: &str = "POLYCOPY_CLOB_L2_API_KEY";
const API_SECRET_ENV: &str = "POLYCOPY_CLOB_L2_API_SECRET";
const API_PASSPHRASE_ENV: &str = "POLYCOPY_CLOB_L2_API_PASSPHRASE";
const API_NONCE_ENV: &str = "POLYCOPY_CLOB_L2_NONCE";
const SIGNATURE_TYPE_ENV: &str = "POLYCOPY_CLOB_SIGNATURE_TYPE";
const FUNDER_ENV: &str = "POLYCOPY_CLOB_FUNDER";
const SNAPSHOT_AT_ENV: &str = "POLYCOPY_GHOST_SNAPSHOT_AT_UTC";
const COLLATERAL_ENV: &str = "POLYCOPY_GHOST_EXPECTED_COLLATERAL";
const TOKEN_BALANCES_ENV: &str = "POLYCOPY_GHOST_EXPECTED_TOKEN_BALANCES";
const SYSTEMD_CREDENTIALS_DIRECTORY_ENV: &str = "CREDENTIALS_DIRECTORY";
const SYSTEMD_SECRET_CREDENTIAL_NAME: &str = "polycopy-ghost-secrets";

/// All input necessary for one intentionally read-only GHOST verification.
///
/// The credentials and signer never implement `Debug`, are never written to
/// disk, and are never printed by the command-line surface.
pub struct GhostRunConfig {
    private_key: String,
    credential_source: CredentialSource,
    signature_type: GhostSignatureType,
    funder: Option<Address>,
    snapshot_at_utc: String,
    snapshot: GhostSnapshot,
}

impl GhostRunConfig {
    /// Loads and validates a run configuration without contacting Polymarket.
    pub fn from_env() -> Result<Self, GhostRunConfigError> {
        let systemd_secrets = load_systemd_secret_credential()?;
        Self::from_lookup(|name| match &systemd_secrets {
            // A systemd invocation has an explicit credential directory. In
            // that mode, signing/L2 material is accepted only from the
            // credential file, never from the process environment.
            Some(secrets) if secret_environment_name(name).is_some() => secrets.get(name).cloned(),
            _ => env::var(name).ok(),
        })
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, GhostRunConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let private_key = required(&lookup, PRIVATE_KEY_ENV)?;
        let credential_source = credential_source(&lookup)?;

        let signature_type = match lookup(SIGNATURE_TYPE_ENV) {
            Some(raw) if !raw.trim().is_empty() => GhostSignatureType::from_str(&raw)?,
            _ => GhostSignatureType::Eoa,
        };

        let funder = match lookup(FUNDER_ENV) {
            Some(raw) if !raw.trim().is_empty() => Some(
                Address::from_str(raw.trim())
                    .map_err(|_| GhostRunConfigError::InvalidFunderAddress)?,
            ),
            _ => None,
        };
        signature_type.validate_funder(funder)?;

        let snapshot_at_utc = required(&lookup, SNAPSHOT_AT_ENV)?;
        if !snapshot_at_utc.ends_with('Z')
            || DateTime::parse_from_rfc3339(&snapshot_at_utc).is_err()
        {
            return Err(GhostRunConfigError::InvalidSnapshotTimestamp);
        }

        let collateral = parse_decimal(&required(&lookup, COLLATERAL_ENV)?, COLLATERAL_ENV)?;
        let token_balances = parse_token_balances(&required(&lookup, TOKEN_BALANCES_ENV)?)?;
        let snapshot = GhostSnapshot::new(collateral, token_balances)?;

        Ok(Self {
            private_key,
            credential_source,
            signature_type,
            funder,
            snapshot_at_utc,
            snapshot,
        })
    }

    pub fn snapshot_at_utc(&self) -> &str {
        &self.snapshot_at_utc
    }

    /// Builds an authenticated client using supplied credentials or a strict
    /// derive-only L2 request. It never calls API-key creation.
    async fn strict_reader(&self) -> Result<IntlClobReadAdapter, GhostRunError> {
        let signer = LocalSigner::from_str(&self.private_key)
            .map_err(|_| GhostRunError::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON));
        let unauthenticated = Client::new(CLOB_HOST, Config::default())
            .map_err(GhostRunError::ClientInitialization)?;
        let credentials = match &self.credential_source {
            CredentialSource::Existing {
                api_key,
                api_secret,
                api_passphrase,
            } => {
                let api_key = api_key.parse().map_err(|_| GhostRunError::InvalidApiKey)?;
                Credentials::new(api_key, api_secret.clone(), api_passphrase.clone())
            }
            CredentialSource::Derive { nonce } => unauthenticated
                .derive_api_key(&signer, *nonce)
                .await
                .map_err(GhostRunError::CredentialDerivation)?,
        };
        let builder = unauthenticated
            .authentication_builder(&signer)
            .credentials(credentials)
            .signature_type(self.signature_type.as_sdk());
        let builder = match self.funder {
            Some(funder) => builder.funder(funder),
            None => builder,
        };
        let client = builder
            .authenticate()
            .await
            .map_err(GhostRunError::Authentication)?;

        Ok(IntlClobReadAdapter::new(client))
    }
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

/// Executes one authenticated GHOST check. The only venue calls are strict
/// balance reads made by `IntlClobReadAdapter`.
pub async fn run_ghost_verification(
    config: &GhostRunConfig,
) -> Result<GhostVerification, GhostRunError> {
    let reader = config.strict_reader().await?;
    Ok(GhostVerifier::new(reader).verify(&config.snapshot).await)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GhostSignatureType {
    Eoa,
    Proxy,
    GnosisSafe,
    Poly1271,
}

impl GhostSignatureType {
    fn as_sdk(self) -> SignatureType {
        match self {
            Self::Eoa => SignatureType::Eoa,
            Self::Proxy => SignatureType::Proxy,
            Self::GnosisSafe => SignatureType::GnosisSafe,
            Self::Poly1271 => SignatureType::Poly1271,
        }
    }

    fn validate_funder(self, funder: Option<Address>) -> Result<(), GhostRunConfigError> {
        match (self, funder) {
            (Self::Eoa, Some(_)) => Err(GhostRunConfigError::UnexpectedFunder),
            (Self::Poly1271, None) => Err(GhostRunConfigError::MissingFunder),
            (Self::Poly1271, Some(address)) if address.is_zero() => {
                Err(GhostRunConfigError::InvalidFunderAddress)
            }
            _ => Ok(()),
        }
    }
}

impl FromStr for GhostSignatureType {
    type Err = GhostRunConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "eoa" => Ok(Self::Eoa),
            "proxy" => Ok(Self::Proxy),
            "gnosis_safe" => Ok(Self::GnosisSafe),
            "poly1271" => Ok(Self::Poly1271),
            _ => Err(GhostRunConfigError::InvalidSignatureType),
        }
    }
}

#[derive(Debug)]
pub enum GhostRunConfigError {
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
    InvalidSnapshotTimestamp,
    InvalidDecimal {
        name: &'static str,
    },
    InvalidTokenBalanceEntry,
    EmptyTokenBalanceList,
    InvalidTokenId,
    IncompleteL2Credentials,
    InvalidL2Nonce,
    NonceWithExistingCredentials,
    Snapshot(GhostSnapshotError),
}

impl fmt::Display for GhostRunConfigError {
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
            Self::UnexpectedFunder => write!(formatter, "EOA GHOST mode must not set a funder"),
            Self::MissingFunder => write!(formatter, "Poly1271 GHOST mode requires a funder"),
            Self::InvalidSignatureType => write!(
                formatter,
                "CLOB signature type must be eoa, proxy, gnosis_safe, or poly1271"
            ),
            Self::InvalidSnapshotTimestamp => write!(
                formatter,
                "GHOST snapshot timestamp must be an RFC 3339 UTC timestamp ending in Z"
            ),
            Self::InvalidDecimal { name } => write!(formatter, "invalid decimal value for {name}"),
            Self::InvalidTokenBalanceEntry => write!(
                formatter,
                "each expected token balance must use decimal_token_id=decimal_balance"
            ),
            Self::EmptyTokenBalanceList => write!(
                formatter,
                "GHOST verification requires at least one expected outcome-token balance"
            ),
            Self::InvalidTokenId => write!(formatter, "invalid outcome token ID in GHOST snapshot"),
            Self::IncompleteL2Credentials => write!(
                formatter,
                "supply all three L2 credential variables or none to derive an existing credential"
            ),
            Self::InvalidL2Nonce => write!(formatter, "invalid L2 credential derivation nonce"),
            Self::NonceWithExistingCredentials => write!(
                formatter,
                "L2 credential nonce is only valid when deriving credentials"
            ),
            Self::Snapshot(source) => source.fmt(formatter),
        }
    }
}

impl Error for GhostRunConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SystemdCredentialRead { source, .. } => Some(source),
            Self::Snapshot(source) => Some(source),
            _ => None,
        }
    }
}

impl From<GhostSnapshotError> for GhostRunConfigError {
    fn from(source: GhostSnapshotError) -> Self {
        Self::Snapshot(source)
    }
}

#[derive(Debug)]
pub enum GhostRunError {
    InvalidPrivateKey,
    InvalidApiKey,
    ClientInitialization(SdkError),
    CredentialDerivation(SdkError),
    Authentication(SdkError),
}

impl fmt::Display for GhostRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrivateKey => write!(formatter, "invalid CLOB signing key"),
            Self::InvalidApiKey => write!(formatter, "invalid CLOB L2 API key"),
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
        }
    }
}

fn credential_source<F>(lookup: &F) -> Result<CredentialSource, GhostRunConfigError>
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
                .map_err(|_| GhostRunConfigError::InvalidL2Nonce)
        })
        .transpose()?;

    match supplied_count {
        0 => Ok(CredentialSource::Derive { nonce }),
        3 if nonce.is_none() => Ok(CredentialSource::Existing {
            api_key: api_key.expect("three supplied values include API key"),
            api_secret: api_secret.expect("three supplied values include API secret"),
            api_passphrase: api_passphrase.expect("three supplied values include API passphrase"),
        }),
        3 => Err(GhostRunConfigError::NonceWithExistingCredentials),
        _ => Err(GhostRunConfigError::IncompleteL2Credentials),
    }
}

impl Error for GhostRunError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPrivateKey | Self::InvalidApiKey => None,
            Self::ClientInitialization(source)
            | Self::CredentialDerivation(source)
            | Self::Authentication(source) => Some(source),
        }
    }
}

type SystemdSecretValues = HashMap<&'static str, String>;

/// Reads the one systemd credential file when this process was launched by the
/// GHOST unit. Returning `None` retains the explicit local CLI environment
/// workflow documented in `.env.example`.
fn load_systemd_secret_credential() -> Result<Option<SystemdSecretValues>, GhostRunConfigError> {
    let Some(directory) = env::var_os(SYSTEMD_CREDENTIALS_DIRECTORY_ENV) else {
        return Ok(None);
    };
    if directory.is_empty() {
        return Err(GhostRunConfigError::EmptySystemdCredentialsDirectory);
    }

    let path = PathBuf::from(directory).join(SYSTEMD_SECRET_CREDENTIAL_NAME);
    let contents =
        fs::read_to_string(&path).map_err(|source| GhostRunConfigError::SystemdCredentialRead {
            path: path.clone(),
            source,
        })?;
    parse_systemd_secret_credential(&contents).map(Some)
}

/// Parses only signing/L2 fields, never snapshot or runtime settings. This
/// prevents credentials from leaking through a systemd `EnvironmentFile`.
fn parse_systemd_secret_credential(
    contents: &str,
) -> Result<SystemdSecretValues, GhostRunConfigError> {
    let mut values = SystemdSecretValues::new();

    for (index, raw_line) in contents.lines().enumerate() {
        let line = index + 1;
        let trimmed = raw_line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let (raw_name, raw_value) = raw_line
            .split_once('=')
            .ok_or(GhostRunConfigError::InvalidSystemdCredentialEntry { line })?;
        let raw_name = raw_name.trim();
        let name = secret_environment_name(raw_name).ok_or_else(|| {
            GhostRunConfigError::UnexpectedSystemdCredentialName {
                line,
                name: raw_name.to_owned(),
            }
        })?;
        if values.insert(name, raw_value.to_owned()).is_some() {
            return Err(GhostRunConfigError::DuplicateSystemdCredentialName { line, name });
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

fn required<F>(lookup: &F, name: &'static str) -> Result<String, GhostRunConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or(GhostRunConfigError::MissingEnvironment { name })
}

fn optional<F>(lookup: &F, name: &str) -> Option<String>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name).filter(|value| !value.trim().is_empty())
}

fn parse_decimal(raw: &str, name: &'static str) -> Result<Decimal, GhostRunConfigError> {
    Decimal::from_str(raw.trim()).map_err(|_| GhostRunConfigError::InvalidDecimal { name })
}

fn parse_token_balances(raw: &str) -> Result<Vec<ExpectedTokenBalance>, GhostRunConfigError> {
    let mut balances = Vec::new();
    for entry in raw.split(',').filter(|entry| !entry.trim().is_empty()) {
        let (token_id, balance) = entry
            .split_once('=')
            .ok_or(GhostRunConfigError::InvalidTokenBalanceEntry)?;
        let token_id = OutcomeTokenId::from_str(token_id.trim())
            .map_err(|_| GhostRunConfigError::InvalidTokenId)?;
        let balance = parse_decimal(balance, TOKEN_BALANCES_ENV)?;
        balances.push(ExpectedTokenBalance::new(token_id, balance));
    }

    if balances.is_empty() {
        return Err(GhostRunConfigError::EmptyTokenBalanceList);
    }

    Ok(balances)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_environment() -> HashMap<&'static str, String> {
        HashMap::from([
            (PRIVATE_KEY_ENV, "not-checked-until-client-build".to_owned()),
            (SNAPSHOT_AT_ENV, "2026-08-30T00:00:00Z".to_owned()),
            (COLLATERAL_ENV, "25.5".to_owned()),
            (TOKEN_BALANCES_ENV, "123=1.5,456=0".to_owned()),
        ])
    }

    #[test]
    fn configuration_derives_an_existing_credential_by_default() {
        let environment = valid_environment();
        let config = GhostRunConfig::from_lookup(|name| environment.get(name).cloned())
            .expect("the complete GHOST configuration is valid without a network call");

        assert_eq!(config.snapshot_at_utc(), "2026-08-30T00:00:00Z");
        assert_eq!(config.snapshot.token_balances().len(), 2);
        assert!(matches!(
            config.credential_source,
            CredentialSource::Derive { nonce: None }
        ));
    }

    #[test]
    fn configuration_accepts_complete_existing_l2_credentials() {
        let mut environment = valid_environment();
        environment.insert(
            API_KEY_ENV,
            "00000000-0000-0000-0000-000000000000".to_owned(),
        );
        environment.insert(API_SECRET_ENV, "secret".to_owned());
        environment.insert(API_PASSPHRASE_ENV, "passphrase".to_owned());

        let config = GhostRunConfig::from_lookup(|name| environment.get(name).cloned())
            .expect("complete existing L2 credentials are accepted");

        assert!(matches!(
            config.credential_source,
            CredentialSource::Existing { .. }
        ));
    }

    #[test]
    fn configuration_rejects_partial_credentials_and_incompatible_nonce() {
        let mut environment = valid_environment();
        environment.insert(API_SECRET_ENV, "secret".to_owned());
        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::IncompleteL2Credentials)
        ));

        let mut environment = valid_environment();
        environment.insert(
            API_KEY_ENV,
            "00000000-0000-0000-0000-000000000000".to_owned(),
        );
        environment.insert(API_SECRET_ENV, "secret".to_owned());
        environment.insert(API_PASSPHRASE_ENV, "passphrase".to_owned());
        environment.insert(API_NONCE_ENV, "7".to_owned());
        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::NonceWithExistingCredentials)
        ));
    }

    #[test]
    fn configuration_rejects_an_empty_token_snapshot() {
        let mut environment = valid_environment();
        environment.insert(TOKEN_BALANCES_ENV, "".to_owned());
        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::MissingEnvironment {
                name: TOKEN_BALANCES_ENV
            })
        ));
    }

    #[test]
    fn configuration_rejects_non_utc_snapshots_and_invalid_wallet_modes() {
        let mut environment = valid_environment();
        environment.insert(SNAPSHOT_AT_ENV, "2026-08-30T08:00:00+08:00".to_owned());
        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::InvalidSnapshotTimestamp)
        ));

        environment.insert(SNAPSHOT_AT_ENV, "2026-08-30T00:00:00Z".to_owned());
        environment.insert(SIGNATURE_TYPE_ENV, "poly1271".to_owned());
        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::MissingFunder)
        ));
    }

    #[test]
    fn systemd_secret_credential_accepts_only_signing_and_l2_material() {
        let values = parse_systemd_secret_credential(
            "# no snapshot data belongs here\n\
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
    fn systemd_secret_credential_rejects_non_secret_or_ambiguous_entries() {
        assert!(matches!(
            parse_systemd_secret_credential("POLYCOPY_GHOST_EXPECTED_COLLATERAL=1\n"),
            Err(GhostRunConfigError::UnexpectedSystemdCredentialName { .. })
        ));
        assert!(matches!(
            parse_systemd_secret_credential(
                "POLYCOPY_CLOB_PRIVATE_KEY=first\nPOLYCOPY_CLOB_PRIVATE_KEY=second\n"
            ),
            Err(GhostRunConfigError::DuplicateSystemdCredentialName { .. })
        ));
        assert!(matches!(
            parse_systemd_secret_credential("POLYCOPY_CLOB_PRIVATE_KEY\n"),
            Err(GhostRunConfigError::InvalidSystemdCredentialEntry { line: 1 })
        ));
    }
}
