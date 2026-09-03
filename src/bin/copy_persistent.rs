//! Persistent live copy-execution runner.
//!
//! This is intentionally separate from bounded `copy_run`. It enforces an
//! account-level fuse and a durable rolling budget before each submit.

#[cfg(feature = "copy_run")]
mod live {
    use std::{collections::BTreeSet, env, error::Error, fmt, time::Duration};

    use chrono::Utc;
    use polycopy_engine::{
        copytrading::{
            assert_persistent_startup_clear, ensure_fuse_clear,
            ingest::{activity_ws, backfill_leader, AddressResolver},
            orchestrate::{
                execute_one_intent_with_marker, list_runnable_intents, OrchestrateError,
                OrchestrateOutcome,
            },
            pause_persistent_fuse, verify_schedule_compatible_with_pending_work, PersistentError,
            PersistentRuntimeConfig, PersistentSubmitMarker, EXIT_CONFIG, EXIT_LOCK_COLLISION,
        },
        venue::intl_clob_exec::IntlClobCopyAdapter,
        EngineLock, EngineLockError,
    };
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;

    const DB_PATH_ENV: &str = "POLYCOPY_DB_PATH";
    const DATA_API_HOST: &str = "https://data-api.polymarket.com";

    pub async fn run() -> Result<(), RunnerError> {
        let db_path = env::var(DB_PATH_ENV).map_err(|_| {
            RunnerError::Persistent(PersistentError::Config(
                "missing POLYCOPY_DB_PATH".to_owned(),
            ))
        })?;
        let config = PersistentRuntimeConfig::from_env().map_err(RunnerError::Persistent)?;

        let _lock = match EngineLock::acquire_for_database(&db_path) {
            Ok(lock) => lock,
            Err(EngineLockError::AlreadyHeld { .. }) => return Err(RunnerError::LockCollision),
            Err(error) => return Err(RunnerError::Other(error.to_string())),
        };
        let pool = polycopy_engine::copytrading::open_and_migrate(&db_path)
            .await
            .map_err(|error| RunnerError::Other(error.to_string()))?;

        polycopy_engine::copytrading::persistent::verify_config(&pool, &config)
            .await
            .map_err(RunnerError::Persistent)?;
        verify_schedule_compatible_with_pending_work(&pool)
            .await
            .map_err(|error| RunnerError::Other(error.to_string()))?;
        verify_single_allowed_leader_scope(&pool, &config.allowed_leader_ids)
            .await
            .map_err(RunnerError::Persistent)?;
        ensure_fuse_clear(&pool, config.account_id)
            .await
            .map_err(RunnerError::Persistent)?;
        assert_persistent_startup_clear(&pool, config.account_id)
            .await
            .map_err(RunnerError::Persistent)?;

        let adapter = IntlClobCopyAdapter::from_env()
            .await
            .map_err(|error| RunnerError::Other(error.to_string()))?;
        let resolver = std::sync::Arc::new(AddressResolver::new());
        resolver
            .reload_from_db(&pool)
            .await
            .map_err(|error| RunnerError::Other(error.to_string()))?;
        let ingest_guard = spawn_supervised_ingest(pool.clone(), resolver, config.backfill_every)
            .map_err(|error| RunnerError::Other(error.to_string()))?;
        let marker = PersistentSubmitMarker { config: &config };

        loop {
            if ingest_guard.is_finished() {
                pause_persistent_fuse(
                    &pool,
                    config.account_id,
                    "activity ingestion supervisor stopped",
                    "copy_persistent",
                )
                .await
                .map_err(RunnerError::Persistent)?;
                return Err(RunnerError::Persistent(PersistentError::FuseOpen));
            }
            ensure_fuse_clear(&pool, config.account_id)
                .await
                .map_err(RunnerError::Persistent)?;
            polycopy_engine::copytrading::plan_next_batch(&pool, config.account_id)
                .await
                .map_err(|error| RunnerError::Other(error.to_string()))?;
            let intent_ids = runnable_intents_for_allowed_leaders(
                &pool,
                config.account_id,
                &config.allowed_leader_ids,
                config.max_order_notional,
            )
            .await
            .map_err(RunnerError::Persistent)?;

            for intent_id in intent_ids {
                let outcome = execute_one_intent_with_marker(
                    &pool,
                    adapter.read_adapter(),
                    &adapter,
                    &adapter,
                    adapter.read_adapter(),
                    &marker,
                    intent_id,
                    Utc::now(),
                )
                .await;
                match outcome {
                    Ok(OrchestrateOutcome::Filled { filled_qty }) => {
                        eprintln!("intent {intent_id}: filled_qty={filled_qty}");
                    }
                    Ok(OrchestrateOutcome::Rejected) => {
                        eprintln!("intent {intent_id}: rejected");
                    }
                    Ok(OrchestrateOutcome::Expired | OrchestrateOutcome::NotClaimed) => {
                        eprintln!("intent {intent_id}: non-submitted outcome");
                    }
                    Ok(OrchestrateOutcome::Blocked(reason))
                    | Ok(OrchestrateOutcome::NeedsReconcile(reason)) => {
                        open_runtime_fuse(&pool, config.account_id, reason).await?;
                        return Err(RunnerError::Persistent(PersistentError::FuseOpen));
                    }
                    Ok(OrchestrateOutcome::Uncertain) => {
                        open_runtime_fuse(&pool, config.account_id, "uncertain submission").await?;
                        return Err(RunnerError::Persistent(PersistentError::FuseOpen));
                    }
                    Err(OrchestrateError::Persistent(PersistentError::DecisionExpired)) => {
                        eprintln!("intent {intent_id}: expired before submission");
                    }
                    Err(OrchestrateError::Persistent(error)) => {
                        if !matches!(error, PersistentError::BudgetExceeded { .. }) {
                            open_runtime_fuse(&pool, config.account_id, &error.to_string()).await?;
                        }
                        return Err(RunnerError::Persistent(error));
                    }
                    Err(error) => {
                        open_runtime_fuse(&pool, config.account_id, &error.to_string()).await?;
                        return Err(RunnerError::Persistent(PersistentError::FuseOpen));
                    }
                }
            }

            tokio::time::sleep(config.tick).await;
        }
    }

    async fn open_runtime_fuse(
        pool: &SqlitePool,
        account_id: i64,
        reason: &str,
    ) -> Result<(), RunnerError> {
        pause_persistent_fuse(pool, account_id, reason, "copy_persistent")
            .await
            .map_err(RunnerError::Persistent)
    }

    async fn verify_single_allowed_leader_scope(
        pool: &SqlitePool,
        allowed_leader_ids: &BTreeSet<i64>,
    ) -> Result<(), PersistentError> {
        let enabled: Vec<i64> =
            sqlx::query_scalar("SELECT id FROM leader_config WHERE enabled = 1 ORDER BY id")
                .fetch_all(pool)
                .await
                .map_err(|error| PersistentError::Database(error.to_string()))?;
        let enabled_set: BTreeSet<i64> = enabled.into_iter().collect();
        if enabled_set != *allowed_leader_ids {
            return Err(PersistentError::ConfigMismatch);
        }
        Ok(())
    }

    async fn runnable_intents_for_allowed_leaders(
        pool: &SqlitePool,
        account_id: i64,
        allowed_leader_ids: &BTreeSet<i64>,
        max_order_notional: Decimal,
    ) -> Result<Vec<i64>, PersistentError> {
        let mut ids = Vec::new();
        for intent_id in list_runnable_intents(pool, account_id)
            .await
            .map_err(|error| PersistentError::Database(error.to_string()))?
        {
            let (leader_id, snapshot_json): (i64, String) = sqlx::query_as(
                "SELECT leader_id, config_snapshot_json FROM copy_intents WHERE id = ?",
            )
            .bind(intent_id)
            .fetch_one(pool)
            .await
            .map_err(|error| PersistentError::Database(error.to_string()))?;
            if allowed_leader_ids.contains(&leader_id) {
                let snapshot: polycopy_engine::copytrading::PolicySnapshot =
                    serde_json::from_str(&snapshot_json)
                        .map_err(|_| PersistentError::MalformedBudgetState)?;
                let snapshot_max: Decimal = snapshot
                    .max_order_notional
                    .parse()
                    .map_err(|_| PersistentError::MalformedBudgetState)?;
                if snapshot_max > max_order_notional {
                    return Err(PersistentError::ConfigMismatch);
                }
                ids.push(intent_id);
                break;
            }
        }
        Ok(ids)
    }

    fn spawn_supervised_ingest(
        pool: SqlitePool,
        resolver: std::sync::Arc<AddressResolver>,
        backfill_every: Duration,
    ) -> Result<tokio::task::JoinHandle<()>, Box<dyn Error>> {
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

            tokio::select! {
                result = &mut websocket => eprintln!("activity websocket supervisor stopped: {result:?}"),
                result = &mut backfill => eprintln!("activity REST backfill supervisor stopped: {result:?}"),
            }
        });
        Ok(supervisor)
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

    #[derive(Debug)]
    pub enum RunnerError {
        LockCollision,
        Persistent(PersistentError),
        Other(String),
    }

    impl RunnerError {
        pub fn exit_code(&self) -> i32 {
            match self {
                Self::LockCollision => EXIT_LOCK_COLLISION,
                Self::Persistent(error) => error.exit_code(),
                Self::Other(_) => EXIT_CONFIG,
            }
        }
    }

    impl fmt::Display for RunnerError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::LockCollision => {
                    write!(formatter, "copy-engine process lock is already held")
                }
                Self::Persistent(error) => write!(formatter, "{error}"),
                Self::Other(error) => write!(formatter, "{error}"),
            }
        }
    }
}

#[cfg(feature = "copy_run")]
#[tokio::main]
async fn main() {
    if let Err(error) = live::run().await {
        eprintln!("{error}");
        std::process::exit(error.exit_code());
    }
}

#[cfg(not(feature = "copy_run"))]
fn main() {
    eprintln!("copy_persistent requires --features copy_run");
    std::process::exit(2);
}
