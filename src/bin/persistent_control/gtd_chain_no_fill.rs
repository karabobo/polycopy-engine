//! One locked, evidence-backed operator action. Never accepts a caller-supplied zero verdict.
use std::str::FromStr;
use chrono::{DateTime, Utc, Duration};
use polycopy_engine::{
    copytrading::{PersistentError, PreparedOrderEnvelope, resolve_chain_proven_gtd_no_fill},
    venue::{gtd_chain_evidence::{Rpc, ScanRequest}, gtd_signed_identity::verify_v2_identity,
        intl_clob::OutcomeTokenId, intl_clob_exec::IntlClobCopyAdapter,
        trade_history_recovery::TradeHistoryWindow},
};
use sqlx::SqlitePool;

fn refuse(message: impl Into<String>) -> PersistentError { PersistentError::Config(message.into()) }
fn parse_time(raw: &str) -> Result<DateTime<Utc>, PersistentError> {
    DateTime::parse_from_rfc3339(raw).map(|v|v.with_timezone(&Utc))
        .map_err(|_|refuse("missing or invalid persisted GTD time"))
}

pub async fn run(pool: &SqlitePool, account_id: i64, attempt_id: i64) -> Result<(), PersistentError> {
    type AttemptIdentity = (String,String,String,String,String,String,String,String,String);
    let row: Option<AttemptIdentity> = sqlx::query_as(
        "SELECT ci.status, oa.status, ci.token_id, a.funder_address, oa.envelope_json, \
         oa.venue_order_id, oa.submission_started_at, oa.accounted_filled_qty, ci.side \
         FROM order_attempts oa JOIN copy_intents ci ON ci.id=oa.intent_id \
         JOIN accounts a ON a.id=ci.account_id WHERE oa.id=? AND ci.account_id=?")
        .bind(attempt_id).bind(account_id).fetch_optional(pool).await
        .map_err(|e|PersistentError::Database(e.to_string()))?;
    let (intent_status,attempt_status,token,funder,raw,order_id,submitted_raw,accounted,side)=
        row.ok_or(PersistentError::UnresolvedRecovery)?;
    let envelope: PreparedOrderEnvelope=serde_json::from_str(&raw).map_err(|_|PersistentError::UnresolvedRecovery)?;
    if intent_status!="needs_reconcile" || attempt_status!="uncertain" || side!="BUY"
        || envelope.token_id!=token || envelope.order_type!="GTD" || !envelope.post_only
        || envelope.expected_taker_order_id!=order_id
        || accounted.parse::<rust_decimal::Decimal>().ok()!=Some(rust_decimal::Decimal::ZERO) {
        return Err(PersistentError::UnresolvedRecovery);
    }
    let submitted=parse_time(&submitted_raw)?;
    let expires_raw=envelope.expires_at.as_deref().ok_or_else(||refuse("GTD has no signed expiry"))?;
    let expires=parse_time(expires_raw)?;
    let settled=expires.checked_add_signed(Duration::minutes(30)).ok_or_else(||refuse("invalid expiry margin"))?;
    if submitted>expires || Utc::now()<settled {return Err(refuse("GTD expiry plus settlement margin has not elapsed"));}
    let adapter=IntlClobCopyAdapter::from_env().await.map_err(|_|refuse("authenticated CLOB setup failed"))?;
    let token_id=OutcomeTokenId::from_str(&token).map_err(|_|refuse("invalid persisted token"))?;
    let neg_risk=adapter.client().neg_risk(polymarket_client_sdk_v2::types::U256::from_str(&token)
        .map_err(|_|refuse("invalid token for neg-risk query"))?).await
        .map_err(|_|refuse("neg-risk lookup failed"))?.neg_risk;
    let signed_exchange=verify_v2_identity(&envelope,&funder,neg_risk).map_err(refuse)?;
    // The signed exchange must be one of the contracts scanned below; verify
    // this BEFORE using the absence of logs as a no-fill result.
    let endpoints=std::env::var("POLYCOPY_GTD_CHAIN_RPCS")
        .map_err(|_|refuse("set POLYCOPY_GTD_CHAIN_RPCS to two independent HTTPS endpoints"))?
        .split(',').map(str::trim).map(str::to_owned).collect();
    let rpc=Rpc::new(endpoints).map_err(refuse)?;
    let start_time=submitted.checked_sub_signed(Duration::seconds(1))
        .ok_or_else(||refuse("invalid submission window"))?;
    if start_time.timestamp()<0 {return Err(refuse("submission window precedes chain genesis"));}
    let first=rpc.first_block_at(start_time.timestamp() as u64).await.map_err(refuse)?;
    let end=rpc.first_block_at(settled.timestamp() as u64).await.map_err(refuse)?;
    // A full interval is bounded to limit RPC load and ambiguous pagination.
    let proof=rpc.prove_zero(ScanRequest {
        order_hash:&order_id, maker:&funder,token:&token,start:first,end,
        submitted,settled,
    }).await.map_err(refuse)?;
    let window=TradeHistoryWindow::new(start_time,settled+Duration::seconds(1))
        .map_err(|_|refuse("invalid authenticated CLOB window"))?;
    // Do not use a matcher that skips unknown roles/statuses or incompatible
    // prices to establish absence. Any exact hash in the complete unfiltered
    // authenticated stream conflicts with a no-fill decision.
    let trades=adapter.read_adapter().trades_between_unfiltered(&token_id,window.after(),window.before())
        .await.map_err(|_|refuse("authenticated CLOB history unavailable or incomplete"))?;
    if trades.iter().any(|trade|trade.taker_order_id.eq_ignore_ascii_case(&order_id)
        || trade.maker_orders.iter().any(|fill|fill.order_id.eq_ignore_ascii_case(&order_id))) {
        return Err(refuse("authenticated CLOB contains exact order hash; no-fill forbidden"));
    }
    // The proof is private to the RPC module; only its completed scan can
    // construct one. Recheck persisted identity and status under BEGIN IMMEDIATE.
    let case_id=resolve_chain_proven_gtd_no_fill(pool,account_id,attempt_id,&submitted_raw,expires_raw,&proof).await?;
    println!("GTD chain-proven no-fill: account_id={account_id} attempt_id={attempt_id} case_id={case_id} signed_exchange={signed_exchange}; reservation and linked cases closed atomically; fuse NOT cleared");
    Ok(())
}
