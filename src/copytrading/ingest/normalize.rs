//! Pure parsing of RTDS activity WebSocket messages into normalized trades.
//!
//! No networking, no database: this module only decides what one raw
//! message means. Protocol confirmed against a working reference
//! implementation (`PolymarketActivityWsService.kt` in this project's
//! predecessor, PolyHermes), not from official Polymarket documentation --
//! `docs.polymarket.com`'s WebSocket page does not mention this topic at
//! all, and the official Rust SDK (`polymarket_client_sdk_v2`) has no
//! equivalent channel. Endpoint: `wss://ws-live-data.polymarket.com`.
//! Subscribe with
//! `{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"},{"topic":"activity","type":"orders_matched"}]}`.
//! This is a global, unfiltered firehose of every trade on the platform,
//! not scoped to any address server-side; the caller filters client-side
//! (see `address_resolver`).
//!
//! There is no dedicated trade-ID field in the payload. The reference
//! implementation uses `transactionHash` as the de-facto trade identity and
//! de-duplicates on it (both "trades" and "orders_matched" can push the same
//! trade). This assumes one transaction hash corresponds to one trade; if a
//! single settlement transaction can ever contain more than one of a
//! leader's trades, this would under-count them. That assumption is
//! inherited from the reference implementation, not independently verified
//! here, and should be revisited if evidence of the collision appears.

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

/// One successfully parsed leader-agnostic trade from the activity firehose.
/// Whether it belongs to a watched leader is decided by the caller
/// (`address_resolver`), not here.
#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedTrade {
    /// Lowercased; the address that made the trade.
    pub trader_address: String,
    /// The outcome-token ID (`asset` in the raw payload). This is the exact
    /// CLOB token ID and must be used verbatim -- recomputing it from
    /// `condition_id` + `outcome_index` can silently disagree with what the
    /// CLOB itself uses for this token (a defect the reference
    /// implementation explicitly calls out avoiding).
    pub token_id: String,
    pub condition_id: String,
    pub outcome_index: i64,
    pub side: TradeSide,
    /// Decimal text, never parsed to a float: kept exactly as the venue
    /// represents it, whether the source JSON used a string or a number.
    pub size: String,
    pub price: String,
    pub occurred_at_utc: String,
    pub transaction_hash: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawActivityMessage {
    #[serde(default)]
    topic: String,
    #[serde(default, rename = "type")]
    message_type: String,
    #[serde(default)]
    payload: RawActivityPayload,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct RawActivityPayload {
    #[serde(default)]
    asset: String,
    #[serde(default, rename = "conditionId")]
    condition_id: String,
    #[serde(default, rename = "outcomeIndex")]
    outcome_index: Option<i64>,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    side: String,
    #[serde(default)]
    price: Option<Value>,
    #[serde(default)]
    size: Option<Value>,
    #[serde(default)]
    timestamp: Option<Value>,
    #[serde(default, rename = "transactionHash")]
    transaction_hash: Option<String>,
    #[serde(default)]
    trader: Option<RawTrader>,
    #[serde(default, rename = "proxyWallet")]
    proxy_wallet: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct RawTrader {
    address: Option<String>,
}

/// Parses one raw WebSocket text frame. Never panics on malformed input.
pub fn parse(raw: &str) -> ParseResult {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("pong") || trimmed.eq_ignore_ascii_case("ping") {
        return ParseResult::Skip;
    }

    let message: RawActivityMessage = match serde_json::from_str(trimmed) {
        Ok(message) => message,
        Err(_) => return ParseResult::Skip,
    };

    if message.topic != "activity" || (message.message_type != "trades" && message.message_type != "orders_matched") {
        return ParseResult::Skip;
    }

    let payload = message.payload;

    if payload.asset.is_empty() || payload.condition_id.is_empty() {
        return ParseResult::Rejected("missing asset or conditionId");
    }

    let Some(side) = parse_side(&payload.side) else {
        return ParseResult::Rejected("missing or invalid side");
    };

    let Some(price) = value_to_decimal_text(payload.price.as_ref()) else {
        return ParseResult::Rejected("missing or invalid price");
    };
    let Some(size) = value_to_decimal_text(payload.size.as_ref()) else {
        return ParseResult::Rejected("missing or invalid size");
    };

    let Some(transaction_hash) = payload.transaction_hash.filter(|hash| !hash.is_empty()) else {
        return ParseResult::Rejected("missing transactionHash");
    };

    let Some(trader_address) = payload
        .trader
        .and_then(|trader| trader.address)
        .or(payload.proxy_wallet)
        .filter(|address| !address.is_empty())
    else {
        return ParseResult::Rejected("missing trader address");
    };

    let outcome_index = match payload.outcome_index {
        Some(index) => index,
        None => match parse_outcome_index(payload.outcome.as_deref()) {
            Some(index) => index,
            None => return ParseResult::Rejected("missing or unrecognized outcome index"),
        },
    };

    let Some(occurred_at_utc) = payload.timestamp.as_ref().and_then(value_to_utc_rfc3339) else {
        return ParseResult::Rejected("missing or invalid timestamp");
    };

    ParseResult::Trade(NormalizedTrade {
        trader_address: trader_address.to_ascii_lowercase(),
        token_id: payload.asset,
        condition_id: payload.condition_id,
        outcome_index,
        side,
        size,
        price,
        occurred_at_utc,
        transaction_hash,
    })
}

#[derive(Debug, Clone, PartialEq)]
pub enum ParseResult {
    /// A trade this project can act on.
    Trade(NormalizedTrade),
    /// Valid JSON, recognized as an activity trade/orders_matched message,
    /// but missing a field required to act on it (e.g. no transaction
    /// hash, no trader address). Distinct from `Skip` so a caller can
    /// choose to log this rather than silently drop it.
    Rejected(&'static str),
    /// Not an activity trade message at all: a ping/pong keepalive or a
    /// message on a different topic/type. Not an error -- most messages on
    /// this firehose will be this.
    Skip,
}

fn parse_side(raw: &str) -> Option<TradeSide> {
    match raw.to_ascii_uppercase().as_str() {
        "BUY" => Some(TradeSide::Buy),
        "SELL" => Some(TradeSide::Sell),
        _ => None,
    }
}

fn parse_outcome_index(outcome: Option<&str>) -> Option<i64> {
    match outcome?.to_ascii_uppercase().as_str() {
        "YES" | "UP" | "TRUE" => Some(0),
        "NO" | "DOWN" | "FALSE" => Some(1),
        _ => None,
    }
}

/// `price`/`size` may arrive as either a JSON string or a JSON number.
/// Numbers are round-tripped through their JSON text (not through `f64`) to
/// avoid introducing binary-float rounding into a decimal quantity.
fn value_to_decimal_text(value: Option<&Value>) -> Option<String> {
    match value? {
        Value::String(text) if !text.is_empty() => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

/// `timestamp` may be a JSON string or number, in seconds or milliseconds
/// (disambiguated by magnitude, matching the reference implementation: a
/// value under 10^12 is treated as seconds).
fn value_to_utc_rfc3339(value: &Value) -> Option<String> {
    use chrono::SecondsFormat;

    let raw: i64 = match value {
        Value::Number(number) => number.as_i64()?,
        Value::String(text) => text.parse().ok()?,
        _ => return None,
    };
    let millis = if raw.unsigned_abs() < 1_000_000_000_000 {
        raw.checked_mul(1000)?
    } else {
        raw
    };

    chrono::DateTime::from_timestamp_millis(millis)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn trade_message(extra_payload_fields: &str) -> String {
        format!(
            r#"{{"topic":"activity","type":"trades","timestamp":1735689600,"connection_id":"c1","payload":{{"asset":"123456","conditionId":"0xcond","outcomeIndex":0,"side":"BUY","price":"0.55","size":"5","timestamp":1735689600,"transactionHash":"0xhash1","trader":{{"address":"0xABCDEF"}}{extra_payload_fields}}}}}"#
        )
    }

    #[test]
    fn parses_a_well_formed_trade_message() {
        let result = parse(&trade_message(""));
        let ParseResult::Trade(trade) = result else {
            panic!("expected a parsed trade, got {result:?}");
        };
        assert_eq!(trade.trader_address, "0xabcdef", "trader address must be lowercased");
        assert_eq!(trade.token_id, "123456");
        assert_eq!(trade.condition_id, "0xcond");
        assert_eq!(trade.outcome_index, 0);
        assert_eq!(trade.side, TradeSide::Buy);
        assert_eq!(trade.price, "0.55");
        assert_eq!(trade.size, "5");
        assert_eq!(trade.transaction_hash, "0xhash1");
    }

    #[test]
    fn accepts_orders_matched_the_same_as_trades() {
        let raw = trade_message("").replace("\"type\":\"trades\"", "\"type\":\"orders_matched\"");
        assert!(matches!(parse(&raw), ParseResult::Trade(_)));
    }

    #[test]
    fn skips_a_bare_ping_pong_keepalive() {
        assert_eq!(parse("ping"), ParseResult::Skip);
        assert_eq!(parse("PONG"), ParseResult::Skip);
        assert_eq!(parse("  pong  "), ParseResult::Skip);
    }

    #[test]
    fn skips_a_message_on_an_unrelated_topic_or_type() {
        assert_eq!(
            parse(r#"{"topic":"comments","type":"comment_created","payload":{}}"#),
            ParseResult::Skip
        );
        assert_eq!(
            parse(r#"{"topic":"activity","type":"something_else","payload":{}}"#),
            ParseResult::Skip
        );
    }

    #[test]
    fn skips_bytes_that_are_not_valid_json_at_all() {
        assert_eq!(parse("not json{{{"), ParseResult::Skip);
    }

    #[test]
    fn rejects_a_recognized_activity_message_missing_a_transaction_hash() {
        // Malformed, not "not applicable" -- the caller should be able to
        // tell these apart from ordinary firehose noise.
        let raw = r#"{"topic":"activity","type":"trades","payload":{"asset":"1","conditionId":"0xc","outcomeIndex":0,"side":"BUY","price":"0.5","size":"1","timestamp":1735689600,"trader":{"address":"0xabc"}}}"#;
        assert_eq!(parse(raw), ParseResult::Rejected("missing transactionHash"));
    }

    #[test]
    fn falls_back_to_proxy_wallet_when_trader_object_is_absent() {
        let raw = r#"{"topic":"activity","type":"trades","payload":{"asset":"1","conditionId":"0xc","outcomeIndex":0,"side":"SELL","price":"0.5","size":"1","timestamp":1735689600,"transactionHash":"0xh","proxyWallet":"0xFEDCBA"}}"#;
        let ParseResult::Trade(trade) = parse(raw) else {
            panic!("expected a parsed trade");
        };
        assert_eq!(trade.trader_address, "0xfedcba");
    }

    #[test]
    fn accepts_numeric_price_and_size_without_float_rounding() {
        let raw = r#"{"topic":"activity","type":"trades","payload":{"asset":"1","conditionId":"0xc","outcomeIndex":0,"side":"BUY","price":0.550000001,"size":5,"timestamp":1735689600,"transactionHash":"0xh","proxyWallet":"0xabc"}}"#;
        let ParseResult::Trade(trade) = parse(raw) else {
            panic!("expected a parsed trade");
        };
        assert_eq!(trade.price, "0.550000001");
        assert_eq!(trade.size, "5");
    }

    #[test]
    fn derives_outcome_index_from_the_outcome_string_when_the_index_field_is_absent() {
        let raw = r#"{"topic":"activity","type":"trades","payload":{"asset":"1","conditionId":"0xc","outcome":"No","side":"BUY","price":"0.5","size":"1","timestamp":1735689600,"transactionHash":"0xh","proxyWallet":"0xabc"}}"#;
        let ParseResult::Trade(trade) = parse(raw) else {
            panic!("expected a parsed trade");
        };
        assert_eq!(trade.outcome_index, 1);
    }

    #[test]
    fn treats_a_timestamp_under_1e12_as_seconds_and_converts_to_milliseconds() {
        let raw = trade_message("");
        let ParseResult::Trade(trade) = parse(&raw) else {
            panic!("expected a parsed trade");
        };
        // 1735689600 (seconds) == 2025-01-01T00:00:00Z.
        assert_eq!(trade.occurred_at_utc, "2025-01-01T00:00:00.000Z");
    }

    #[test]
    fn treats_a_timestamp_at_or_above_1e12_as_milliseconds_already() {
        let raw = trade_message("").replace("\"timestamp\":1735689600,\"transactionHash\"", "\"timestamp\":1735689600000,\"transactionHash\"");
        let ParseResult::Trade(trade) = parse(&raw) else {
            panic!("expected a parsed trade");
        };
        assert_eq!(trade.occurred_at_utc, "2025-01-01T00:00:00.000Z");
    }

    #[test]
    fn accepts_a_string_timestamp_the_same_as_a_numeric_one() {
        // Target the payload's timestamp specifically (the envelope also
        // carries an unrelated, ignored top-level timestamp field with the
        // same value) so this actually exercises string-timestamp parsing.
        let raw = trade_message("")
            .replace("\"timestamp\":1735689600,\"transactionHash\"", "\"timestamp\":\"1735689600\",\"transactionHash\"");
        let ParseResult::Trade(trade) = parse(&raw) else {
            panic!("expected a parsed trade");
        };
        assert_eq!(trade.occurred_at_utc, "2025-01-01T00:00:00.000Z");
    }
}
