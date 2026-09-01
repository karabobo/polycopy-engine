//! Phase 6 live copy-execution daemon.
//!
//! Wires ingestion (optional), planning, and the recovery-matrix walker into
//! one process. Venue writes require `POLYCOPY_ENGINE_EXECUTE=yes`.

#[cfg(feature = "execute")]
mod live {
    use std::{collections::BTreeSet, env, error::Error, time::Duration};

    use chrono::Utc;
    use polycopy_engine::copytrading::ingest::AddressResolver;
    use polycopy_engine::{
        copytrading::{
            orchestrate::{execute_one_intent, list_runnable_intents, live_execute_enabled},
            plan::{plan_next_batch, verify_schedule_compatible_with_pending_work},
        },
        venue::intl_clob_exec::IntlClobCopyAdapter,
        EngineLock,
    };
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;

    const EXECUTE_GUARD_ENV: &str = "POLYCOPY_ENGINE_EXECUTE";
    const DB_PATH_ENV: &str = "POLYCOPY_DB_PATH";
    const ACCOUNT_ID_ENV: &str = "POLYCOPY_ACCOUNT_ID";
    const TICK_SECONDS_ENV: &str = "POLYCOPY_TICK_SECONDS";
    const ALLOWED_LEADERS_ENV: &str = "POLYCOPY_ALLOWED_LEADER_IDS";
    const MAX_ATTEMPTS_ENV: &str = "POLYCOPY_MAX_ATTEMPTS_PER_RUN";
    const MAX_ORDER_NOTIONAL_ENV: &str = "POLYCOPY_MAX_ORDER_NOTIONAL";
    const MAX_RUNTIME_ENV: &str = "POLYCOPY_MAX_RUNTIME_SECONDS";
    const BACKFILL_SECONDS_ENV: &str = "POLYCOPY_BACKFILL_EVERY_SECONDS";
    const DATA_API_HOST: &str = "https://data-api.polymarket.com";

    #[derive(Debug, Clone)]
    struct RuntimeLimits {
        allowed_leader_ids: BTreeSet<i64>,
        max_attempts_per_run: u64,
        max_order_notional: Decimal,
        max_runtime: Duration,
        backfill_every: Duration,
    }

    impl RuntimeLimits {
        fn from_env() -> Result<Self, Box<dyn Error>> {
            let allowed_leader_ids = parse_allowed_leaders(
                &env::var(ALLOWED_LEADERS_ENV)
                    .map_err(|_| format!("missing {ALLOWED_LEADERS_ENV}"))?,
            )?;
            if allowed_leader_ids.len() != 1 {
                return Err(format!(
                    "{ALLOWED_LEADERS_ENV} must contain exactly one leader id for the bounded live gate"
                )
                .into());
            }

            let max_attempts_per_run = parse_one(MAX_ATTEMPTS_ENV)?;
            let max_order_notional = parse_micro_notional(MAX_ORDER_NOTIONAL_ENV)?;
            let max_runtime = Duration::from_secs(parse_positive_u64(MAX_RUNTIME_ENV)?);
            let backfill_every = Duration::from_secs(
                env::var(BACKFILL_SECONDS_ENV)
                    .ok()
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or(60),
            );

            Ok(Self {
                allowed_leader_ids,
                max_attempts_per_run,
                max_order_notional,
                max_runtime,
                backfill_every,
            })
        }
    }

    pub async fn run() -> Result<(), Box<dyn Error>> {
        if !live_execute_enabled(env::var(EXECUTE_GUARD_ENV).ok().as_deref()) {
            return Err(format!(
                "{EXECUTE_GUARD_ENV} must be set to the exact value yes before copy_run will submit"
            )
            .into());
        }

        let db_path = env::var(DB_PATH_ENV).map_err(|_| format!("missing {DB_PATH_ENV}"))?;
        let account_id: i64 = env::var(ACCOUNT_ID_ENV)
            .map_err(|_| format!("missing {ACCOUNT_ID_ENV}"))?
            .parse()
            .map_err(|_| format!("invalid {ACCOUNT_ID_ENV}"))?;
        let tick = env::var(TICK_SECONDS_ENV)
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(5);
        let limits = RuntimeLimits::from_env()?;

        let _lock = EngineLock::acquire_for_database(&db_path)?;
        let pool = polycopy_engine::copytrading::open_and_migrate(&db_path).await?;
        verify_schedule_compatible_with_pending_work(&pool).await?;
        verify_single_allowed_leader_scope(&pool, &limits.allowed_leader_ids).await?;
        let adapter = IntlClobCopyAdapter::from_env().await?;

        #[cfg(feature = "ingest")]
        let ingest_guard = {
            use std::sync::Arc;

            let resolver = Arc::new(AddressResolver::new());
            resolver.reload_from_db(&pool).await?;
            Some(spawn_supervised_ingest(
                pool.clone(),
                resolver,
                limits.backfill_every,
            )?)
        };
        #[cfg(not(feature = "ingest"))]
        let ingest_guard: Option<tokio::task::JoinHandle<()>> = None;

        let started = tokio::time::Instant::now();
        let mut attempts_used = 0u64;
        loop {
            if attempts_used >= limits.max_attempts_per_run {
                eprintln!("max attempt gate reached: {attempts_used}");
                return Ok(());
            }
            if started.elapsed() >= limits.max_runtime {
                eprintln!(
                    "max runtime gate reached: {}s",
                    limits.max_runtime.as_secs()
                );
                return Ok(());
            }
            if let Some(handle) = &ingest_guard {
                if handle.is_finished() {
                    return Err("activity ingestion supervisor stopped".into());
                }
            }

            plan_next_batch(&pool, account_id).await?;
            let intent_ids = runnable_intents_for_allowed_leaders(
                &pool,
                account_id,
                &limits.allowed_leader_ids,
                limits.max_order_notional,
                limits.max_attempts_per_run - attempts_used,
            )
            .await?;

            for intent_id in intent_ids {
                match execute_one_intent(
                    &pool,
                    adapter.read_adapter(),
                    &adapter,
                    &adapter,
                    adapter.read_adapter(),
                    intent_id,
                    Utc::now(),
                )
                .await
                {
                    Ok(outcome) => eprintln!("intent {intent_id}: {outcome:?}"),
                    Err(error) => eprintln!("intent {intent_id}: {error}"),
                }
                attempts_used += 1;
                if attempts_used >= limits.max_attempts_per_run {
                    break;
                }
            }

            tokio::time::sleep(Duration::from_secs(tick)).await;
        }
    }

    fn parse_allowed_leaders(raw: &str) -> Result<BTreeSet<i64>, Box<dyn Error>> {
        let mut ids = BTreeSet::new();
        for part in raw.split(',') {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let id: i64 = trimmed
                .parse()
                .map_err(|_| format!("invalid leader id in {ALLOWED_LEADERS_ENV}: {trimmed}"))?;
            if id <= 0 {
                return Err(format!("leader ids in {ALLOWED_LEADERS_ENV} must be positive").into());
            }
            ids.insert(id);
        }
        if ids.is_empty() {
            return Err(format!("{ALLOWED_LEADERS_ENV} must contain one leader id").into());
        }
        Ok(ids)
    }

    fn parse_positive_u64(name: &'static str) -> Result<u64, Box<dyn Error>> {
        let value: u64 = env::var(name)
            .map_err(|_| format!("missing {name}"))?
            .parse()
            .map_err(|_| format!("invalid {name}"))?;
        if value == 0 {
            return Err(format!("{name} must be greater than zero").into());
        }
        Ok(value)
    }

    /// The first seven-day progression is deliberately one attempted order at
    /// a time. Increasing concurrency or a per-run budget needs a separate
    /// production review, not a relaxed environment value.
    fn parse_one(name: &'static str) -> Result<u64, Box<dyn Error>> {
        parse_one_value(parse_positive_u64(name)?)
    }

    fn parse_one_value(value: u64) -> Result<u64, Box<dyn Error>> {
        if value == 1 {
            Ok(value)
        } else {
            Err(
                "POLYCOPY_MAX_ATTEMPTS_PER_RUN must be exactly 1 during bounded live progression"
                    .into(),
            )
        }
    }

    /// A process-level maximum is an independent ceiling on the immutable
    /// policy snapshot. Five USDC is intentionally a canary-sized hard cap,
    /// not a configurable production allocation.
    fn parse_micro_notional(name: &'static str) -> Result<Decimal, Box<dyn Error>> {
        let value: Decimal = env::var(name)
            .map_err(|_| format!("missing {name}"))?
            .parse()
            .map_err(|_| format!("invalid {name}"))?;
        validate_micro_notional(value).map_err(|_| {
            format!("{name} must be greater than zero and no more than 5 USDC during bounded live progression").into()
        })
    }

    fn validate_micro_notional(value: Decimal) -> Result<Decimal, ()> {
        if value > Decimal::ZERO && value <= Decimal::new(5, 0) {
            Ok(value)
        } else {
            Err(())
        }
    }

    async fn verify_single_allowed_leader_scope(
        pool: &SqlitePool,
        allowed_leader_ids: &BTreeSet<i64>,
    ) -> Result<(), Box<dyn Error>> {
        let enabled: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM leader_config WHERE enabled = 1 ORDER BY id")
                .fetch_all(pool)
                .await?;
        let enabled_set: BTreeSet<i64> = enabled.into_iter().collect();
        if enabled_set != *allowed_leader_ids {
            return Err(format!(
                "enabled leaders {:?} do not exactly match {ALLOWED_LEADERS_ENV} {:?}",
                enabled_set, allowed_leader_ids
            )
            .into());
        }
        Ok(())
    }

    async fn runnable_intents_for_allowed_leaders(
        pool: &SqlitePool,
        account_id: i64,
        allowed_leader_ids: &BTreeSet<i64>,
        max_order_notional: Decimal,
        remaining_attempts: u64,
    ) -> Result<Vec<i64>, Box<dyn Error>> {
        let mut ids = Vec::new();
        for intent_id in list_runnable_intents(pool, account_id).await? {
            let (leader_id, snapshot_json): (i64, String) = sqlx::query_as(
                "SELECT leader_id, config_snapshot_json FROM copy_intents WHERE id = ?",
            )
            .bind(intent_id)
            .fetch_one(pool)
            .await?;
            if allowed_leader_ids.contains(&leader_id) {
                let snapshot: polycopy_engine::copytrading::PolicySnapshot =
                    serde_json::from_str(&snapshot_json)
                        .map_err(|_| "runnable intent has invalid immutable policy snapshot")?;
                let snapshot_max: Decimal = snapshot
                    .max_order_notional
                    .parse()
                    .map_err(|_| "runnable intent has invalid immutable max_order_notional")?;
                if snapshot_max > max_order_notional {
                    return Err(format!(
                        "intent {intent_id} policy max_order_notional {snapshot_max} exceeds {MAX_ORDER_NOTIONAL_ENV} {max_order_notional}"
                    )
                    .into());
                }
                ids.push(intent_id);
                if ids.len() as u64 >= remaining_attempts {
                    break;
                }
            }
        }
        Ok(ids)
    }

    #[cfg(feature = "ingest")]
    fn spawn_supervised_ingest(
        pool: SqlitePool,
        resolver: std::sync::Arc<AddressResolver>,
        backfill_every: Duration,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
        use polycopy_engine::copytrading::ingest::{activity_ws, backfill_leader};

        let backfill_client = polymarket_client_sdk_v2::data::Client::new(DATA_API_HOST)?;
        let supervisor = tokio::spawn(async move {
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

            // A realtime feed or a REST catch-up failure makes event delivery
            // uncertain. End the supervisor so the executor notices and exits
            // before it can plan or submit another order.
            tokio::select! {
                result = &mut websocket => eprintln!("activity websocket supervisor stopped: {result:?}"),
                result = &mut backfill => eprintln!("activity REST backfill supervisor stopped: {result:?}"),
            }
        });

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

        Ok(supervisor)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn allowed_leaders_must_be_single_positive_id() {
            assert_eq!(
                parse_allowed_leaders(" 7 ").expect("one id"),
                BTreeSet::from([7])
            );
            assert!(parse_allowed_leaders("").is_err());
            assert!(parse_allowed_leaders("1,2").is_ok());
            assert!(parse_allowed_leaders("0").is_err());
            assert!(parse_allowed_leaders("abc").is_err());
        }

        #[test]
        fn bounded_progression_accepts_only_one_attempt_and_a_micro_cap() {
            assert!(parse_one_value(1).is_ok());
            assert!(parse_one_value(2).is_err());
            assert!(validate_micro_notional(Decimal::new(5, 0)).is_ok());
            assert!(validate_micro_notional(Decimal::new(501, 2)).is_err());
        }
    }
}

#[cfg(feature = "execute")]
#[tokio::main]
async fn main() {
    if let Err(error) = live::run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("copy_run requires --features execute (or --features copy_run)");
    std::process::exit(2);
}
