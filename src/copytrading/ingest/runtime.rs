//! Shared fail-stop supervisor for the activity WebSocket and REST backfill.
//!
//! Both bounded `copy_run` and persistent execution require the same event
//! delivery contract: a WS or REST-backfill failure makes canonical leader
//! event delivery uncertain, so the supervisor terminates and the runner stops
//! before planning another order. Keeping this in ingest prevents the two
//! runner implementations from drifting.

use std::{error::Error, sync::Arc, time::Duration};

use sqlx::SqlitePool;

use super::{activity_ws, backfill_leader, AddressResolver};

const DATA_API_HOST: &str = "https://data-api.polymarket.com";

/// Starts the coupled real-time WS and REST-backfill supervisor.
///
/// A clean return from the spawned task means one source stopped or failed;
/// callers must treat that as an event-delivery failure and safe-stop before
/// executing further work. The resolver is refreshed before each backfill so
/// leader-address changes become visible to both sources together.
pub fn spawn_supervised_ingest(
    pool: SqlitePool,
    resolver: Arc<AddressResolver>,
    backfill_every: Duration,
) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
    let backfill_client = polymarket_client_sdk_v2::data::Client::new(DATA_API_HOST)?;
    Ok(tokio::spawn(async move {
        let ws_pool = pool.clone();
        let ws_resolver = resolver.clone();
        let websocket = activity_ws::run(ws_pool, ws_resolver.as_ref());
        tokio::pin!(websocket);
        let backfill = async {
            loop {
                resolver
                    .reload_from_db(&pool)
                    .await
                    .map_err(|error| error.to_string())?;
                backfill_enabled_leaders(&pool, resolver.as_ref(), &backfill_client)
                    .await
                    .map_err(|error| error.to_string())?;
                tokio::time::sleep(backfill_every).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        };
        tokio::pin!(backfill);

        // Event delivery is a financial-correctness prerequisite: on either
        // source stopping, end the supervisor. Runner health checks observe
        // that completion and safe-stop before the next plan/submit cycle.
        tokio::select! {
            result = &mut websocket => eprintln!("activity websocket supervisor stopped: {result:?}"),
            result = &mut backfill => eprintln!("activity REST backfill supervisor stopped: {result:?}"),
        }
    }))
}

async fn backfill_enabled_leaders(
    pool: &SqlitePool,
    resolver: &AddressResolver,
    client: &polymarket_client_sdk_v2::data::Client,
) -> Result<(), Box<dyn Error>> {
    let aliases: Vec<(i64, String)> = sqlx::query_as(
        "SELECT leader_id, address FROM leader_wallet_aliases WHERE enabled = 1",
    )
    .fetch_all(pool)
    .await?;
    for (leader_id, address) in aliases {
        let summary = backfill_leader(pool, resolver, client, leader_id, &address).await?;
        eprintln!(
            "backfill leader {leader_id}: fetched={} ingested={} rejected={}",
            summary.fetched, summary.ingested, summary.rejected
        );
    }
    Ok(())
}
