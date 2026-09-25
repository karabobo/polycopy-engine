//! Derive the exact Polygon V2 verifying contract from a persisted wire envelope.
use std::str::FromStr;
use alloy::primitives::{Address, B256, U256};
use polymarket_client_sdk_v2::{clob::types::{OrderPayload, OrderV2}, POLYGON};
use crate::{copytrading::PreparedOrderEnvelope, venue::{gtd_chain_evidence::EXCHANGES, order_hash::{expected_order_id, ExchangeAddresses}}};

pub fn verify_v2_identity(envelope: &PreparedOrderEnvelope, funder: &str, neg_risk: bool) -> Result<String,String> {
    let fail = || "persisted signed V2 identity mismatch or unsupported format".to_owned();
    if envelope.order_type != "GTD" || envelope.side != "BUY" || !envelope.post_only {
        return Err(fail());
    }
    let signed: serde_json::Value = serde_json::from_str(&envelope.signed_order_json).map_err(|_|fail())?;
    let body = signed.get("order").ok_or_else(fail)?;
    if signed["orderType"] != "GTD" || signed["postOnly"] != true || body.get("timestamp").is_none()
        || body.get("metadata").is_none() || body.get("builder").is_none() || body.get("taker").is_some() {
        return Err(fail());
    }
    let field = |name: &str| body.get(name).and_then(serde_json::Value::as_str).ok_or_else(fail);
    let uint = |name: &str| U256::from_str(field(name)?).map_err(|_|fail());
    let mut order=OrderV2::default();
    order.salt=U256::from(body["salt"].as_u64().ok_or_else(fail)?);
    order.maker=Address::from_str(field("maker")?).map_err(|_|fail())?;
    order.signer=Address::from_str(field("signer")?).map_err(|_|fail())?;
    order.tokenId=uint("tokenId")?;
    order.makerAmount=uint("makerAmount")?;
    order.takerAmount=uint("takerAmount")?;
    order.timestamp=uint("timestamp")?;
    order.metadata=B256::from_str(field("metadata")?).map_err(|_|fail())?;
    order.builder=B256::from_str(field("builder")?).map_err(|_|fail())?;
    order.side=if body["side"]=="BUY" {0} else {return Err(fail())};
    order.signatureType=body["signatureType"].as_u64().ok_or_else(fail)?.try_into().map_err(|_|fail())?;
    if order.signatureType > 2 || order.tokenId.to_string()!=envelope.token_id
        || order.maker != Address::from_str(funder).map_err(|_|fail())?
        || body["expiration"].as_str()!=envelope.expires_at.as_deref().and_then(|v|chrono::DateTime::parse_from_rfc3339(v).ok()).map(|v|v.timestamp().to_string()).as_deref() {
        return Err(fail());
    }
    let config=polymarket_client_sdk_v2::contract_config(POLYGON,neg_risk).ok_or_else(fail)?;
    let contract=config.exchange_v2.ok_or_else(fail)?;
    let contract_text=format!("{contract:#x}");
    if !EXCHANGES.iter().any(|value|value.eq_ignore_ascii_case(&contract_text)) {return Err(fail());}
    let payload=OrderPayload::new(order,uint("expiration")?);
    let actual=expected_order_id(&payload,&ExchangeAddresses{v1:config.exchange,v2:config.exchange_v2},POLYGON)
        .map_err(|_|fail())?;
    if !format!("{actual:#x}").eq_ignore_ascii_case(&envelope.expected_taker_order_id) {return Err(fail());}
    Ok(contract_text)
}
