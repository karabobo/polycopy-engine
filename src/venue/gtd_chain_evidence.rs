//! Strict, read-only Polygon GTD no-fill evidence. No RPC error is a zero result.
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

pub const EXCHANGES: [&str; 2] = [
    "0xe111180000d2663c0091e4f400237545b87b996b",
    "0xe2222d279d744050d28e00520010520000310f59",
];
const TOPIC: &str = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee";
const FINALITY: u64 = 128;
const MAX_BLOCKS: u64 = 5001;

#[cfg(test)]
#[path = "gtd_chain_evidence_tests.rs"]
mod tests;

fn invalid(message: impl Into<String>) -> String { message.into() }
fn hex_number(value: &Value) -> Result<u64, String> {
    let text = value.as_str().ok_or_else(|| invalid("RPC number missing"))?;
    u64::from_str_radix(text.strip_prefix("0x").ok_or("RPC number is not hex")?, 16)
        .map_err(|_| invalid("invalid RPC hex number"))
}
fn hash(value: &Value) -> Result<String, String> {
    let text = value.as_str().ok_or_else(|| invalid("missing block hash"))?;
    if text.len() != 66 || !text.starts_with("0x") || !text[2..].bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err(invalid("invalid block hash"));
    }
    Ok(text.to_ascii_lowercase())
}

#[derive(Debug)]
pub struct ZeroProof {
    order_hash: String,
    maker: String,
    token: String,
    detail: String,
}
impl ZeroProof {
    #[cfg(test)]
    pub(crate) fn fixture(order_hash: &str, maker: &str, token: &str) -> Self {
        Self { order_hash: order_hash.to_owned(), maker: maker.to_owned(),
            token: token.to_owned(), detail: "test-only complete two-node scan".to_owned() }
    }
    pub fn order_hash(&self) -> &str { &self.order_hash }
    pub fn maker(&self) -> &str { &self.maker }
    pub fn token(&self) -> &str { &self.token }
    pub fn detail(&self) -> &str { &self.detail }
}

pub struct ScanRequest<'a> {
    pub order_hash: &'a str,
    pub maker: &'a str,
    pub token: &'a str,
    pub start: u64,
    pub end: u64,
    pub submitted: DateTime<Utc>,
    pub settled: DateTime<Utc>,
}

type BoundarySnapshot = (Vec<(String, u64)>, Vec<Vec<String>>);

#[async_trait::async_trait]
trait RpcTransport: Send + Sync {
    async fn call(&self, url: &str, method: &str, params: Value) -> Result<Value, String>;
}
struct HttpTransport(reqwest::Client);
#[async_trait::async_trait]
impl RpcTransport for HttpTransport {
    async fn call(&self, url: &str, method: &str, params: Value) -> Result<Value, String> {
        let response = self.0.post(url).json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send().await.map_err(|_| invalid(format!("{method} transport failure")))?;
        if !response.status().is_success() { return Err(invalid(format!("{method} HTTP error"))); }
        response.json().await.map_err(|_| invalid(format!("{method} malformed JSON")))
    }
}

pub struct Rpc {
    transport: Arc<dyn RpcTransport>,
    endpoints: Vec<String>,
}
impl Rpc {
    pub fn new(endpoints: Vec<String>) -> Result<Self, String> {
        if endpoints.len() < 2 || endpoints.iter().any(|e| !e.starts_with("https://")) || {
            let unique = endpoints.iter().collect::<std::collections::HashSet<_>>();
            unique.len() != endpoints.len()
        } { return Err(invalid("at least two distinct HTTPS RPC endpoints required")); }
        let client = reqwest::Client::builder()
            .user_agent("polycopy-gtd-chain-evidence/1.0")
            .timeout(Duration::from_secs(20)).build()
            .map_err(|_| invalid("RPC client setup failed"))?;
        Ok(Self { transport: Arc::new(HttpTransport(client)), endpoints })
    }
    async fn call(&self, url: &str, method: &str, params: Value) -> Result<Value, String> {
        let value = self.transport.call(url, method, params).await?;
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") || value.get("id") != Some(&json!(1))
            || value.get("error").is_some_and(|v| !v.is_null()) || value.get("result").is_none_or(Value::is_null) {
            return Err(invalid(format!("{method} RPC error or missing result")));
        }
        Ok(value["result"].clone())
    }
    async fn block(&self, url: &str, number: u64) -> Result<(String,u64),String> {
        let block = self.call(url,"eth_getBlockByNumber",json!([format!("0x{number:x}"),false])).await?;
        if hex_number(&block["number"])? != number { return Err(invalid("block height mismatch")); }
        Ok((hash(&block["hash"])?,hex_number(&block["timestamp"])?))
    }
    /// Finds the first block whose timestamp is at or after the target time.
    /// Boundary hashes are independently compared later by `prove_zero`.
    pub async fn first_block_at(&self, timestamp: u64) -> Result<u64,String> {
        let url=&self.endpoints[0];
        let mut left=1;
        let head=hex_number(&self.call(url,"eth_blockNumber",json!([])).await?)?;
        let mut right=head.checked_sub(FINALITY)
            .filter(|height| *height >= 1)
            .ok_or_else(|| invalid("尚未达到最终确认: 链高度不足"))?;
        if self.block(url,right).await?.1 < timestamp {
            return Err(invalid("尚未达到最终确认: 最新已确认区块早于目标时间"));
        }
        while left<right {
            let mid=left+(right-left)/2;
            if self.block(url,mid).await?.1 < timestamp {left=mid+1;} else {right=mid;}
        }
        Ok(left)
    }
    /// `start` must be the first block that could contain a post-submission fill;
    /// `end` must contain all events through expiry plus settlement margin.
    pub async fn prove_zero(&self, request: ScanRequest<'_>) -> Result<ZeroProof,String> {
        let ScanRequest { order_hash, maker, token, start, end, submitted, settled } = request;
        if start == 0 || end < start || end-start >= MAX_BLOCKS || Utc::now() < settled
            || submitted.timestamp() < 0 || settled.timestamp() < submitted.timestamp()
            || order_hash.len()!=66 || !order_hash.starts_with("0x") || !order_hash[2..].bytes().all(|c|c.is_ascii_hexdigit())
            || maker.len()!=42 || !maker.starts_with("0x") || !maker[2..].bytes().all(|c|c.is_ascii_hexdigit())
            || token.parse::<alloy::primitives::U256>().is_err() {
            return Err(invalid("invalid order, maker, token, or bounded mature window"));
        }
        let mut canonical: Option<BoundarySnapshot> = None;
        for url in &self.endpoints {
            let head=hex_number(&self.call(url,"eth_blockNumber",json!([])).await?)?;
            if head < end.saturating_add(FINALITY) {return Err(invalid("insufficient finality"));}
            let blocks=vec![self.block(url,start-1).await?,self.block(url,start).await?,
                self.block(url,end).await?,self.block(url,end+1).await?];
            if blocks[0].1 >= submitted.timestamp() as u64
                || blocks[2].1 < settled.timestamp() as u64 {
                return Err(invalid("scan does not cover submission through settlement"));
            }
            let mut controls=vec![Vec::<String>::new();2];
            for first in (start..=end).step_by(100) {
                let last=(first+99).min(end);
                for (index,exchange) in EXCHANGES.iter().enumerate() {
                    for control in [false,true] {
                        let topics=if control { json!([TOPIC]) } else {json!([TOPIC,order_hash])};
                        let logs=self.call(url,"eth_getLogs",json!([{ "address":exchange,
                            "fromBlock":format!("0x{first:x}"),"toBlock":format!("0x{last:x}"),"topics":topics }])).await?;
                        let logs=logs.as_array().ok_or_else(||invalid("RPC logs not an array"))?;
                        for log in logs {
                            if log["removed"]==true || log["address"].as_str().is_none_or(|s|!s.eq_ignore_ascii_case(exchange))
                                || hex_number(&log["blockNumber"])? < first || hex_number(&log["blockNumber"])? > last
                                || log["topics"][0].as_str().is_none_or(|s|!s.eq_ignore_ascii_case(TOPIC)) {
                                return Err(invalid("malformed or out-of-window OrderFilled log"));
                            }
                            if control {
                                if log["topics"][1].as_str().is_some_and(|s|s.eq_ignore_ascii_case(order_hash)) {
                                    return Err(invalid("control stream contains target order absent from exact-hash query"));
                                }
                                // Comparing the entire canonical control stream (not just counts)
                                // catches disagreeing RPC nodes, missing logs and reorganizations.
                                let tx = hash(&log["transactionHash"])?;
                                let block_hash = hash(&log["blockHash"])?;
                                let index_in_block = hex_number(&log["logIndex"])?;
                                controls[index].push(format!("{block_hash}:{tx}:{index_in_block}:{}", log));
                            } else {
                                // Any exact-hash event, including an unexpected maker/token/side,
                                // is a conflict: never silently discard it as zero.
                                if log["topics"][1].as_str().is_none_or(|s|!s.eq_ignore_ascii_case(order_hash)) {
                                    return Err(invalid("exact-hash log topic mismatch"));
                                }
                                let topics=log["topics"].as_array().ok_or("malformed event topics")?;
                                if topics.len()!=4 || topics[2].as_str().is_none_or(|s|!s.eq_ignore_ascii_case(&format!("0x{:0>64}",&maker[2..]))) {
                                    return Err(invalid("exact-hash event maker mismatch; no-fill forbidden"));
                                }
                                let data=log["data"].as_str().ok_or("missing event data")?;
                                if data.len()!=450 || !data.starts_with("0x") || !data[2..].bytes().all(|c|c.is_ascii_hexdigit()) {
                                    return Err(invalid("malformed exact-hash event data; no-fill forbidden"));
                                }
                                let side=u64::from_str_radix(&data[2..66],16).map_err(|_|invalid("invalid event side"))?;
                                let event_token=alloy::primitives::U256::from_str_radix(&data[66..130],16)
                                    .map_err(|_|invalid("invalid event token"))?;
                                if side!=0 || event_token.to_string()!=token {
                                    return Err(invalid("exact-hash event side/token mismatch; no-fill forbidden"));
                                }
                                return Err(invalid("exact-hash OrderFilled exists; no-fill forbidden"));
                            }
                        }
                    }
                }
            }
            if controls.iter().any(Vec::is_empty) {return Err(invalid("control query found no OrderFilled on a scanned exchange"));}
            for stream in &mut controls {stream.sort();}
            let snapshot=(blocks,controls);
            if canonical.as_ref().is_some_and(|prior| prior!=&snapshot) {
                return Err(invalid("RPC nodes disagree on boundary blocks or control evidence"));
            }
            canonical=Some(snapshot);
        }
        Ok(ZeroProof { order_hash: order_hash.to_owned(), maker: maker.to_owned(), token: token.to_owned(), detail: format!("two-node exact-hash zero; range={start}..={end}; boundary hashes={:?}",canonical.unwrap().0.iter().map(|b|&b.0).collect::<Vec<_>>()) })
    }
}
