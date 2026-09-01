//! Reconstruct a typed SDK `SignedOrder` from the JSON stored on an envelope.
//!
//! The SDK implements `Serialize` for `SignedOrder` but not `Deserialize`,
//! and the struct is `#[non_exhaustive]`. This module inverts the serialize
//! impl in `polymarket_client_sdk_v2` `clob/types/mod.rs` so a persisted
//! envelope can be handed back to `post_order` after a process restart.

use std::str::FromStr;

use alloy::primitives::{Address, Signature, B256, U256};
use polymarket_client_sdk_v2::{
    auth::ApiKey,
    clob::types::{OrderPayload, OrderSignature, OrderType, SignatureType, SignedOrder},
};
use serde::Deserialize;

/// Reconstructs a `SignedOrder` from the JSON produced by the SDK's
/// `Serialize` impl. V1 payloads are rejected: this engine only submits V2.
pub fn reconstruct_signed_order(json: &str) -> Result<SignedOrder, String> {
    let parsed: SignedOrderJson = serde_json::from_str(json)
        .map_err(|error| format!("cannot parse signed_order_json: {error}"))?;
    let signature_type = signature_type_from_u8(parsed.order.signature_type)?;
    let owner: ApiKey = parsed
        .owner
        .parse()
        .map_err(|_| format!("cannot parse owner ApiKey from JSON: {}", parsed.owner))?;

    if parsed.order.is_v2() {
        reconstruct_v2(parsed, signature_type, owner)
    } else {
        Err("V1 order reconstruction is not supported in the copy engine".to_owned())
    }
}

#[derive(Debug, Deserialize)]
struct SignedOrderJson {
    order: OrderBodyJson,
    #[serde(rename = "orderType")]
    order_type: OrderType,
    owner: String,
    #[serde(default, rename = "postOnly")]
    post_only: Option<bool>,
    #[serde(default, rename = "deferExec")]
    defer_exec: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct OrderBodyJson {
    /// SDK `ser_salt` writes this as a JSON number, not a string.
    salt: u64,
    maker: String,
    signer: String,
    #[serde(rename = "tokenId")]
    token_id: String,
    #[serde(rename = "makerAmount")]
    maker_amount: String,
    #[serde(rename = "takerAmount")]
    taker_amount: String,
    side: polymarket_client_sdk_v2::clob::types::Side,
    #[serde(rename = "signatureType")]
    signature_type: u8,
    signature: String,
    #[serde(default)]
    expiration: Option<String>,
    #[serde(default)]
    timestamp: Option<String>,
    #[serde(default)]
    metadata: Option<String>,
    #[serde(default)]
    builder: Option<String>,
}

impl OrderBodyJson {
    fn is_v2(&self) -> bool {
        self.timestamp.is_some()
    }
}

fn signature_type_from_u8(value: u8) -> Result<SignatureType, String> {
    match value {
        0 => Ok(SignatureType::Eoa),
        1 => Ok(SignatureType::Proxy),
        2 => Ok(SignatureType::GnosisSafe),
        3 => Ok(SignatureType::Poly1271),
        _ => Err(format!("unknown signatureType: {value}")),
    }
}

fn parse_ecdsa_signature(
    sig_str: &str,
    signature_type: SignatureType,
) -> Result<Signature, String> {
    if matches!(signature_type, SignatureType::Poly1271) {
        return Err("Poly1271 order reconstruction is not supported in the copy engine".to_owned());
    }
    Signature::from_str(sig_str).or_else(|_| {
        let hex = sig_str.strip_prefix("0x").unwrap_or(sig_str);
        Signature::from_str(hex)
            .map_err(|error| format!("cannot parse signature as ECDSA: {error}"))
    })
}

fn reconstruct_v2(
    parsed: SignedOrderJson,
    signature_type: SignatureType,
    owner: ApiKey,
) -> Result<SignedOrder, String> {
    let body = &parsed.order;
    let timestamp_str = body
        .timestamp
        .as_ref()
        .ok_or("V2 order missing timestamp")?;
    let metadata_str = body.metadata.as_ref().ok_or("V2 order missing metadata")?;
    let builder_str = body.builder.as_ref().ok_or("V2 order missing builder")?;
    let expiration_str = body
        .expiration
        .as_ref()
        .ok_or("V2 order missing expiration")?;

    let token_id =
        U256::from_str(&body.token_id).map_err(|error| format!("invalid tokenId: {error}"))?;
    let maker =
        Address::from_str(&body.maker).map_err(|error| format!("invalid maker: {error}"))?;
    let signer_addr =
        Address::from_str(&body.signer).map_err(|error| format!("invalid signer: {error}"))?;
    let maker_amount = U256::from_str(&body.maker_amount)
        .map_err(|error| format!("invalid makerAmount: {error}"))?;
    let taker_amount = U256::from_str(&body.taker_amount)
        .map_err(|error| format!("invalid takerAmount: {error}"))?;
    let timestamp_val =
        U256::from_str(timestamp_str).map_err(|error| format!("invalid timestamp: {error}"))?;
    let metadata_val =
        B256::from_str(metadata_str).map_err(|error| format!("invalid metadata: {error}"))?;
    let builder_val =
        B256::from_str(builder_str).map_err(|error| format!("invalid builder: {error}"))?;
    let expiration_val =
        U256::from_str(expiration_str).map_err(|error| format!("invalid expiration: {error}"))?;

    let ecdsa_sig = parse_ecdsa_signature(&body.signature, signature_type)?;
    let order_signature = OrderSignature::Ecdsa(ecdsa_sig);

    let mut order = polymarket_client_sdk_v2::clob::types::OrderV2::default();
    order.salt = U256::from(body.salt);
    order.maker = maker;
    order.signer = signer_addr;
    order.tokenId = token_id;
    order.makerAmount = maker_amount;
    order.takerAmount = taker_amount;
    order.side = body.side as u8;
    order.signatureType = parsed.order.signature_type;
    order.timestamp = timestamp_val;
    order.metadata = metadata_val;
    order.builder = builder_val;

    let payload = OrderPayload::new(order, expiration_val);
    let builder = SignedOrder::builder()
        .payload(payload)
        .signature(order_signature)
        .order_type(parsed.order_type)
        .owner(owner);
    match (parsed.post_only, parsed.defer_exec) {
        (Some(post_only), Some(defer_exec)) => {
            Ok(builder.post_only(post_only).defer_exec(defer_exec).build())
        }
        (Some(post_only), None) => Ok(builder.post_only(post_only).build()),
        (None, Some(defer_exec)) => Ok(builder.defer_exec(defer_exec).build()),
        (None, None) => Ok(builder.build()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_signed_order() -> SignedOrder {
        let mut order = polymarket_client_sdk_v2::clob::types::OrderV2::default();
        order.salt = U256::from(42u64);
        order.maker = Address::from_str("0x1111111111111111111111111111111111111111").unwrap();
        order.signer = Address::from_str("0x2222222222222222222222222222222222222222").unwrap();
        order.tokenId = U256::from_str("123456789").unwrap();
        order.makerAmount = U256::from(2_750_000u64);
        order.takerAmount = U256::from(5_000_000u64);
        order.side = 0;
        order.signatureType = 0;
        order.timestamp = U256::from(1_725_000_000u64);
        order.metadata = B256::ZERO;
        order.builder = B256::ZERO;

        let payload = OrderPayload::new(order, U256::from(0u64));
        let signature = Signature::from_str(concat!(
            "0x1111111111111111111111111111111111111111111111111111111111111111",
            "2222222222222222222222222222222222222222222222222222222222222222",
            "1b"
        ))
        .expect("dummy ECDSA signature must parse");
        let owner: ApiKey = "550e8400-e29b-41d4-a716-446655440000"
            .parse()
            .expect("uuid must parse");
        SignedOrder::builder()
            .payload(payload)
            .signature(OrderSignature::Ecdsa(signature))
            .order_type(OrderType::FAK)
            .owner(owner)
            .build()
    }

    #[test]
    fn reconstruct_round_trips_sdk_serialized_v2_json() {
        let original = sample_signed_order();
        let json = serde_json::to_string(&original).expect("SDK Serialize must succeed");
        assert!(
            json.contains("\"salt\":42"),
            "SDK writes salt as a JSON number: {json}"
        );
        let reconstructed = reconstruct_signed_order(&json).expect("reconstruct must succeed");
        let again = serde_json::to_string(&reconstructed).expect("re-serialize must succeed");
        let original_value: serde_json::Value = serde_json::from_str(&json).unwrap();
        let again_value: serde_json::Value = serde_json::from_str(&again).unwrap();
        assert_eq!(original_value, again_value);
    }

    #[test]
    fn reconstruct_rejects_v1_payloads() {
        let json = r#"{
            "order": {
                "salt": 1,
                "maker": "0x1111111111111111111111111111111111111111",
                "signer": "0x2222222222222222222222222222222222222222",
                "tokenId": "1",
                "makerAmount": "1",
                "takerAmount": "1",
                "side": "BUY",
                "signatureType": 0,
                "signature": "0x11"
            },
            "orderType": "FAK",
            "owner": "550e8400-e29b-41d4-a716-446655440000"
        }"#;
        let error = reconstruct_signed_order(json).expect_err("V1 must be rejected");
        assert!(error.contains("V1"));
    }
}
