//! Computes the CLOB's expected order ID from an already-built signed order,
//! entirely offline once the verifying-contract address is known.
//!
//! Polymarket's on-chain CTF Exchange contract exposes a `public view
//! hashOrder(Order)` function that is exactly the standard EIP-712
//! typed-data digest of the order struct: `keccak256(0x1901 ||
//! domainSeparator || hashStruct(order))` (see the contract's
//! `Hashing.sol`). The official Rust SDK's own (private) `sign()` method
//! computes this exact same digest internally -- it is literally what gets
//! signed -- but never returns it to the caller. This module recomputes it
//! using the SDK's own public building blocks
//! (`polymarket_client_sdk_v2::contract_config` for the verifying-contract
//! address, and `alloy`'s `SolStruct::eip712_signing_hash` on the same
//! `OrderV1`/`OrderV2` struct the SDK signs), so a caller can have it before
//! ever sending the order, not only after reading a response.
//!
//! **Live-proven against the order-submission response's `orderID` field,
//! not yet against the trade-history endpoint's `taker_order_id` field.**
//! `docs/PHASE_0_5_CANARY_REPORT.md`'s Result 4 (2026-09-01) submitted a real
//! order that the venue rejected (a moved market price, unrelated to this
//! hash), and the venue's own `400` response echoed back an `orderID` that
//! was byte-identical to this module's precomputed value. That confirms the
//! formula against the field the venue assigns at submission time. It does
//! **not** yet confirm the narrower thing `reconcile.rs`'s
//! `recover_fak_taker_order_from_trades` actually depends on: that
//! `GET /data/trades`'s `taker_order_id` field, for an order that actually
//! matches, is the same value. A further live run that matches (not just
//! gets accepted-and-rejected) is still needed to close that gap.

use alloy::{dyn_abi::Eip712Domain, sol_types::SolStruct};
use polymarket_client_sdk_v2::{
    clob::types::{OrderPayload, SignatureType},
    types::{Address, ChainId, B256, U256},
};

/// `EIP712Domain.name` for every Polymarket CTF Exchange order, V1 or V2.
/// Reverse-engineered from the SDK's own private `sign()` method
/// (`polymarket_client_sdk_v2::clob::client`'s `ORDER_NAME` constant),
/// which is not itself exposed to a downstream crate. Kept as a single
/// named constant, not inlined, so it is easy to find if a future SDK
/// release changes it and this silently drifts out of sync.
const ORDER_DOMAIN_NAME: &str = "Polymarket CTF Exchange";
const ORDER_DOMAIN_VERSION_V1: &str = "1";
const ORDER_DOMAIN_VERSION_V2: &str = "2";

/// The verifying-contract addresses for one chain and one token's neg-risk
/// configuration, as returned by
/// [`polymarket_client_sdk_v2::contract_config`]. Each order payload
/// version signs against its own contract; V2 is optional because not
/// every chain/neg-risk combination has a V2 exchange deployed.
#[derive(Debug, Clone, Copy)]
pub struct ExchangeAddresses {
    pub v1: Address,
    pub v2: Option<Address>,
}

/// Computes the expected order ID for `payload`, given the exchange
/// addresses for its chain/neg-risk configuration.
///
/// Returns [`OrderHashError::UnsupportedSignatureType`] for a Poly1271
/// order: the deposit-wallet path signs a different, wrapped digest (see
/// the SDK's private `sign_poly1271_order`), which this function does not
/// implement -- consistent with Poly1271 remaining GHOST-only elsewhere in
/// this project until Phase 0.5 proves it.
pub fn expected_order_id(
    payload: &OrderPayload,
    exchanges: &ExchangeAddresses,
    chain_id: ChainId,
) -> Result<B256, OrderHashError> {
    match payload {
        OrderPayload::V1(p) => {
            if p.order.signatureType == SignatureType::Poly1271 as u8 {
                return Err(OrderHashError::UnsupportedSignatureType);
            }
            let domain = domain(ORDER_DOMAIN_VERSION_V1, chain_id, exchanges.v1);
            Ok(p.order.eip712_signing_hash(&domain))
        }
        OrderPayload::V2(p) => {
            if p.order.signatureType == SignatureType::Poly1271 as u8 {
                return Err(OrderHashError::UnsupportedSignatureType);
            }
            let exchange = exchanges.v2.ok_or(OrderHashError::MissingV2ExchangeAddress)?;
            let domain = domain(ORDER_DOMAIN_VERSION_V2, chain_id, exchange);
            Ok(p.order.eip712_signing_hash(&domain))
        }
        // `OrderPayload` is `#[non_exhaustive]`: a future SDK release could
        // add a V3 variant. Refusing outright is correct here -- guessing a
        // domain for an unrecognized payload shape would silently produce a
        // wrong hash instead of a visible error.
        _ => Err(OrderHashError::UnsupportedPayloadVersion),
    }
}

/// Formats a computed hash the same way the venue's JSON API formats an
/// order ID: a lowercase, `0x`-prefixed hex string.
pub fn format_order_id(hash: B256) -> String {
    format!("{hash:#x}")
}

fn domain(version: &'static str, chain_id: ChainId, exchange: Address) -> Eip712Domain {
    Eip712Domain {
        name: Some(ORDER_DOMAIN_NAME.into()),
        version: Some(version.into()),
        chain_id: Some(U256::from(chain_id)),
        verifying_contract: Some(exchange),
        salt: None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderHashError {
    UnsupportedSignatureType,
    MissingV2ExchangeAddress,
    UnsupportedPayloadVersion,
}

impl std::fmt::Display for OrderHashError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedSignatureType => write!(
                formatter,
                "order-ID computation does not support Poly1271 (deposit wallet) orders"
            ),
            Self::MissingV2ExchangeAddress => write!(
                formatter,
                "no V2 exchange contract is configured for this chain/neg-risk combination"
            ),
            Self::UnsupportedPayloadVersion => write!(
                formatter,
                "order-ID computation does not recognize this order payload version"
            ),
        }
    }
}

impl std::error::Error for OrderHashError {}

#[cfg(test)]
mod tests {
    use polymarket_client_sdk_v2::clob::types::{OrderPayload, OrderV1, OrderV2};

    use super::*;

    fn exchanges() -> ExchangeAddresses {
        ExchangeAddresses {
            v1: Address::repeat_byte(0x11),
            v2: Some(Address::repeat_byte(0x22)),
        }
    }

    // `OrderV1`/`OrderV2` are `#[non_exhaustive]`: even with `..Default::default()`,
    // a struct-literal expression is blocked outside the defining crate. The
    // sanctioned workaround is to build via `Default::default()` and then
    // assign each already-`pub` field individually.
    fn v2_order(signature_type: SignatureType) -> OrderV2 {
        let mut order = OrderV2::default();
        order.salt = U256::from(1u64);
        order.maker = Address::repeat_byte(0x33);
        order.signer = Address::repeat_byte(0x33);
        order.tokenId = U256::from(123_456u64);
        order.makerAmount = U256::from(1_000_000u64);
        order.takerAmount = U256::from(2_000_000u64);
        order.side = 0;
        order.signatureType = signature_type as u8;
        order.timestamp = U256::from(1_700_000_000u64);
        order.metadata = B256::ZERO;
        order.builder = B256::ZERO;
        order
    }

    fn v1_order(signature_type: SignatureType) -> OrderV1 {
        let mut order = OrderV1::default();
        order.salt = U256::from(1u64);
        order.maker = Address::repeat_byte(0x33);
        order.signer = Address::repeat_byte(0x33);
        order.taker = Address::ZERO;
        order.tokenId = U256::from(123_456u64);
        order.makerAmount = U256::from(1_000_000u64);
        order.takerAmount = U256::from(2_000_000u64);
        order.expiration = U256::ZERO;
        order.nonce = U256::ZERO;
        order.feeRateBps = U256::ZERO;
        order.side = 0;
        order.signatureType = signature_type as u8;
        order
    }

    #[test]
    fn the_same_v2_order_hashes_identically_on_repeated_calls() {
        let payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let first = expected_order_id(&payload, &exchanges(), 137).unwrap();
        let second = expected_order_id(&payload, &exchanges(), 137).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn changing_the_salt_changes_the_hash() {
        let base = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let mut changed_order = v2_order(SignatureType::GnosisSafe);
        changed_order.salt = U256::from(2u64);
        let changed = OrderPayload::new(changed_order, U256::from(9_999u64));

        let base_hash = expected_order_id(&base, &exchanges(), 137).unwrap();
        let changed_hash = expected_order_id(&changed, &exchanges(), 137).unwrap();
        assert_ne!(base_hash, changed_hash);
    }

    #[test]
    fn a_different_verifying_contract_changes_the_hash() {
        let payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let here = expected_order_id(&payload, &exchanges(), 137).unwrap();
        let other_exchanges = ExchangeAddresses {
            v1: Address::repeat_byte(0x11),
            v2: Some(Address::repeat_byte(0x99)),
        };
        let there = expected_order_id(&payload, &other_exchanges, 137).unwrap();
        assert_ne!(here, there);
    }

    #[test]
    fn a_different_chain_id_changes_the_hash() {
        let payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let polygon = expected_order_id(&payload, &exchanges(), 137).unwrap();
        let amoy = expected_order_id(&payload, &exchanges(), 80_002).unwrap();
        assert_ne!(polygon, amoy);
    }

    #[test]
    fn v1_and_v2_payloads_of_the_same_underlying_fields_still_hash_differently() {
        // Same salt/tokenId/amounts, but the V1 and V2 domains and struct
        // shapes differ (V1 carries taker/expiration/nonce/feeRateBps
        // in-struct; V2 carries timestamp/metadata/builder instead) -- they
        // must never collide.
        let v1_payload = OrderPayload::new_v1(v1_order(SignatureType::GnosisSafe));
        let v2_payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));

        let v1_hash = expected_order_id(&v1_payload, &exchanges(), 137).unwrap();
        let v2_hash = expected_order_id(&v2_payload, &exchanges(), 137).unwrap();
        assert_ne!(v1_hash, v2_hash);
    }

    #[test]
    fn a_poly1271_v2_order_is_rejected_not_silently_hashed_wrong() {
        let payload = OrderPayload::new(v2_order(SignatureType::Poly1271), U256::from(9_999u64));
        assert_eq!(
            expected_order_id(&payload, &exchanges(), 137),
            Err(OrderHashError::UnsupportedSignatureType)
        );
    }

    #[test]
    fn a_poly1271_v1_order_is_rejected_not_silently_hashed_wrong() {
        let payload = OrderPayload::new_v1(v1_order(SignatureType::Poly1271));
        assert_eq!(
            expected_order_id(&payload, &exchanges(), 137),
            Err(OrderHashError::UnsupportedSignatureType)
        );
    }

    #[test]
    fn a_v2_order_without_a_configured_v2_exchange_is_rejected() {
        let payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let no_v2 = ExchangeAddresses {
            v1: Address::repeat_byte(0x11),
            v2: None,
        };
        assert_eq!(
            expected_order_id(&payload, &no_v2, 137),
            Err(OrderHashError::MissingV2ExchangeAddress)
        );
    }

    #[test]
    fn format_order_id_is_a_lowercase_0x_prefixed_64_character_hex_string() {
        let payload = OrderPayload::new(v2_order(SignatureType::GnosisSafe), U256::from(9_999u64));
        let hash = expected_order_id(&payload, &exchanges(), 137).unwrap();
        let formatted = format_order_id(hash);

        assert!(formatted.starts_with("0x"));
        assert_eq!(formatted.len(), 66);
        assert!(formatted[2..].chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }
}
