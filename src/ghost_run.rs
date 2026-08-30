//! Configuration and execution boundary for an authenticated, read-only GHOST run.
//!
//! The runner requires existing L2 credentials. It deliberately does not invoke
//! the SDK path that creates or derives API credentials, because credential
//! creation is an account mutation outside Phase 0's read-only scope.

use std::{error::Error, fmt, str::FromStr};

use polymarket_client_sdk_v2::{
    auth::{Credentials, LocalSigner, Signer as _},
    clob::{types::SignatureType, Client, Config},
    types::{Address, DateTime, Decimal},
    POLYGON,
};

use crate::{
    ExpectedTokenBalance, GhostSnapshot, GhostSnapshotError, GhostVerification, GhostVerifier,
    IntlClobReadAdapter, OutcomeTokenId,
};

const CLOB_HOST: &str = "https://clob-v2.polymarket.com";

const PRIVATE_KEY_ENV: &str = "POLYCOPY_CLOB_PRIVATE_KEY";
const API_KEY_ENV: &str = "POLYCOPY_CLOB_L2_API_KEY";
const API_SECRET_ENV: &str = "POLYCOPY_CLOB_L2_API_SECRET";
const API_PASSPHRASE_ENV: &str = "POLYCOPY_CLOB_L2_API_PASSPHRASE";
const SIGNATURE_TYPE_ENV: &str = "POLYCOPY_CLOB_SIGNATURE_TYPE";
const FUNDER_ENV: &str = "POLYCOPY_CLOB_FUNDER";
const SNAPSHOT_AT_ENV: &str = "POLYCOPY_GHOST_SNAPSHOT_AT_UTC";
const COLLATERAL_ENV: &str = "POLYCOPY_GHOST_EXPECTED_COLLATERAL";
const TOKEN_BALANCES_ENV: &str = "POLYCOPY_GHOST_EXPECTED_TOKEN_BALANCES";

/// All input necessary for one intentionally read-only GHOST verification.
///
/// The credentials and signer never implement `Debug`, are never written to
/// disk, and are never printed by the command-line surface.
pub struct GhostRunConfig {
    private_key: String,
    api_key: String,
    api_secret: String,
    api_passphrase: String,
    signature_type: GhostSignatureType,
    funder: Option<Address>,
    snapshot_at_utc: String,
    snapshot: GhostSnapshot,
}

impl GhostRunConfig {
    /// Loads and validates a run configuration without contacting Polymarket.
    pub fn from_env() -> Result<Self, GhostRunConfigError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup<F>(lookup: F) -> Result<Self, GhostRunConfigError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let private_key = required(&lookup, PRIVATE_KEY_ENV)?;
        let api_key = required(&lookup, API_KEY_ENV)?;
        let api_secret = required(&lookup, API_SECRET_ENV)?;
        let api_passphrase = required(&lookup, API_PASSPHRASE_ENV)?;

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
            api_key,
            api_secret,
            api_passphrase,
            signature_type,
            funder,
            snapshot_at_utc,
            snapshot,
        })
    }

    pub fn snapshot_at_utc(&self) -> &str {
        &self.snapshot_at_utc
    }

    /// Builds an authenticated client from supplied credentials without using
    /// the SDK's API-key creation or derivation flow.
    async fn strict_reader(&self) -> Result<IntlClobReadAdapter, GhostRunError> {
        let signer = LocalSigner::from_str(&self.private_key)
            .map_err(|_| GhostRunError::InvalidPrivateKey)?
            .with_chain_id(Some(POLYGON));
        let api_key = self
            .api_key
            .parse()
            .map_err(|_| GhostRunError::InvalidApiKey)?;
        let credentials = Credentials::new(
            api_key,
            self.api_secret.clone(),
            self.api_passphrase.clone(),
        );
        let builder = Client::new(CLOB_HOST, Config::default())
            .map_err(|_| GhostRunError::ClientInitialization)?
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
            .map_err(|_| GhostRunError::Authentication)?;

        Ok(IntlClobReadAdapter::new(client))
    }
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
    MissingEnvironment { name: &'static str },
    InvalidFunderAddress,
    UnexpectedFunder,
    MissingFunder,
    InvalidSignatureType,
    InvalidSnapshotTimestamp,
    InvalidDecimal { name: &'static str },
    InvalidTokenBalanceEntry,
    EmptyTokenBalanceList,
    InvalidTokenId,
    Snapshot(GhostSnapshotError),
}

impl fmt::Display for GhostRunConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingEnvironment { name } => write!(formatter, "missing required {name}"),
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
            Self::Snapshot(source) => source.fmt(formatter),
        }
    }
}

impl Error for GhostRunConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GhostRunError {
    InvalidPrivateKey,
    InvalidApiKey,
    ClientInitialization,
    Authentication,
}

impl fmt::Display for GhostRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPrivateKey => write!(formatter, "invalid CLOB signing key"),
            Self::InvalidApiKey => write!(formatter, "invalid CLOB L2 API key"),
            Self::ClientInitialization => write!(formatter, "unable to initialize the CLOB client"),
            Self::Authentication => write!(
                formatter,
                "unable to construct an authenticated CLOB client from existing L2 credentials"
            ),
        }
    }
}

impl Error for GhostRunError {}

fn required<F>(lookup: &F, name: &'static str) -> Result<String, GhostRunConfigError>
where
    F: Fn(&str) -> Option<String>,
{
    lookup(name)
        .filter(|value| !value.trim().is_empty())
        .ok_or(GhostRunConfigError::MissingEnvironment { name })
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
    use std::collections::HashMap;

    use super::*;

    fn valid_environment() -> HashMap<&'static str, String> {
        HashMap::from([
            (PRIVATE_KEY_ENV, "not-checked-until-client-build".to_owned()),
            (
                API_KEY_ENV,
                "00000000-0000-0000-0000-000000000000".to_owned(),
            ),
            (API_SECRET_ENV, "secret".to_owned()),
            (API_PASSPHRASE_ENV, "passphrase".to_owned()),
            (SNAPSHOT_AT_ENV, "2026-08-30T00:00:00Z".to_owned()),
            (COLLATERAL_ENV, "25.5".to_owned()),
            (TOKEN_BALANCES_ENV, "123=1.5,456=0".to_owned()),
        ])
    }

    #[test]
    fn configuration_accepts_existing_credentials_and_a_timestamped_snapshot() {
        let environment = valid_environment();
        let config = GhostRunConfig::from_lookup(|name| environment.get(name).cloned())
            .expect("the complete GHOST configuration is valid without a network call");

        assert_eq!(config.snapshot_at_utc(), "2026-08-30T00:00:00Z");
        assert_eq!(config.snapshot.token_balances().len(), 2);
    }

    #[test]
    fn configuration_requires_existing_l2_credentials_and_a_token_snapshot() {
        let mut environment = valid_environment();
        environment.remove(API_SECRET_ENV);

        assert!(matches!(
            GhostRunConfig::from_lookup(|name| environment.get(name).cloned()),
            Err(GhostRunConfigError::MissingEnvironment {
                name: API_SECRET_ENV
            })
        ));

        environment.insert(API_SECRET_ENV, "secret".to_owned());
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
}
