//! Live Activity WS supervisor plus independent REST audit backfill.
//!
//! The Activity WS is the only realtime execution trigger. REST preserves a
//! durable, high-water-marked audit trail but is deliberately not allowed to
//! stop a healthy WS execution loop when the public Data API rate-limits it.

use std::{error::Error, sync::Arc, time::Duration};

use sqlx::SqlitePool;

use super::{activity_ws, backfill_leader, AddressResolver, WsExecutionGate};

const DATA_API_HOST: &str = "https://data-api.polymarket.com";
const INITIAL_AUDIT_RETRY_DELAY: Duration = Duration::from_secs(60);
const MAX_AUDIT_RETRY_DELAY: Duration = Duration::from_secs(15 * 60);
const AUDIT_INTER_LEADER_DELAY: Duration = Duration::from_secs(1);

/// Handles for the independent realtime and audit tasks. Dropping this value
/// aborts both tasks; only `realtime_connected` is an execution gate.
pub struct IngestSupervisor {
    realtime: tokio::task::JoinHandle<()>,
    audit: tokio::task::JoinHandle<()>,
    execution_gate: WsExecutionGate,
}

impl IngestSupervisor {
    pub fn realtime_connected(&self) -> bool {
        !self.realtime.is_finished() && self.execution_gate.is_connected()
    }

    pub fn realtime_finished(&self) -> bool {
        self.realtime.is_finished()
    }
}

impl Drop for IngestSupervisor {
    fn drop(&mut self) {
        self.realtime.abort();
        self.audit.abort();
    }
}

/// Starts the realtime WS and its independent REST audit worker.
///
/// Only a disconnected/finished WS pauses execution. REST errors are logged
/// and retried with bounded exponential backoff; they never make a healthy WS
/// unavailable. The resolver is refreshed before each audit pass.
pub fn spawn_supervised_ingest(
    pool: SqlitePool,
    resolver: Arc<AddressResolver>,
    backfill_every: Duration,
) -> Result<IngestSupervisor, Box<dyn Error>> {
    let backfill_client = polymarket_client_sdk_v2::data::Client::new(DATA_API_HOST)?;
    let execution_gate = WsExecutionGate::default();
    let ws_pool = pool.clone();
    let ws_resolver = resolver.clone();
    let ws_gate = execution_gate.clone();
    let realtime = tokio::spawn(async move {
        activity_ws::run_with_execution_gate(ws_pool, ws_resolver.as_ref(), ws_gate).await;
    });
    let audit = tokio::spawn(async move {
        run_audit_backfill(pool, resolver, backfill_client, backfill_every).await;
    });
    Ok(IngestSupervisor {
        realtime,
        audit,
        execution_gate,
    })
}

async fn run_audit_backfill(
    pool: SqlitePool,
    resolver: Arc<AddressResolver>,
    client: polymarket_client_sdk_v2::data::Client,
    backfill_every: Duration,
) -> ! {
    let mut retry_delay = INITIAL_AUDIT_RETRY_DELAY;
    loop {
        let result = match resolver.reload_from_db(&pool).await {
            Ok(()) => backfill_enabled_leaders(&pool, resolver.as_ref(), &client)
                .await
                .map_err(|error| error.to_string()),
            Err(error) => Err(error.to_string()),
        };

        match result {
            Ok(0) => {
                retry_delay = INITIAL_AUDIT_RETRY_DELAY;
                tokio::time::sleep(backfill_every).await;
            }
            Ok(failures) => {
                log_rest_audit_event(
                    "pass_degraded",
                    None,
                    &format!("{failures} leader backfill failure(s)"),
                    Some(retry_delay),
                );
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(MAX_AUDIT_RETRY_DELAY);
            }
            Err(detail) => {
                log_rest_audit_event("worker_failure", None, &detail, Some(retry_delay));
                tokio::time::sleep(retry_delay).await;
                retry_delay = (retry_delay * 2).min(MAX_AUDIT_RETRY_DELAY);
            }
        }
    }
}

const REST_AUDIT_EVENT_PREFIX: &str = "REST_AUDIT_EVENT: ";

fn log_rest_audit_event(
    kind: &str,
    leader_id: Option<i64>,
    detail: &str,
    next_retry: Option<Duration>,
) {
    eprintln!(
        "{REST_AUDIT_EVENT_PREFIX}{}",
        serde_json::json!({
            "kind": kind,
            "leader_id": leader_id,
            "detail": detail,
            "next_retry_ms": next_retry.map(|value| value.as_millis() as u64),
        })
    );
}

async fn backfill_enabled_leaders(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    client: &polymarket_client_sdk_v2::data::Client,
) -> Result<usize, Box<dyn Error>> {
    let aliases: Vec<(i64, String)> = sqlx::query_as(
        "SELECT lwa.leader_id, lwa.address FROM leader_wallet_aliases lwa \
         JOIN leader_config lc ON lc.id = lwa.leader_id \
         WHERE lwa.enabled = 1 AND lc.enabled = 1",
    )
    .fetch_all(pool)
    .await?;
    let mut failures = 0;
    let alias_count = aliases.len();
    for (index, (leader_id, address)) in aliases.into_iter().enumerate() {
        match backfill_leader(pool, resolver, client, leader_id, &address).await {
            Ok(summary) => eprintln!(
                "backfill leader {leader_id}: fetched={} ingested={} rejected={}",
                summary.fetched, summary.ingested, summary.rejected
            ),
            Err(error) => {
                failures += 1;
                log_rest_audit_event("leader_failure", Some(leader_id), &error.to_string(), None);
            }
        }
        // Audit has no realtime deadline. Deliberately space public Data API
        // requests so a large watched set does not turn one audit round into
        // a burst that starves its own next rounds via 429 responses.
        if index + 1 < alias_count {
            tokio::time::sleep(AUDIT_INTER_LEADER_DELAY).await;
        }
    }
    Ok(failures)
}
