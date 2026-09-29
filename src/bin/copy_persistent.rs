//! Persistent live copy-execution runner.
//!
//! This is intentionally separate from bounded `copy_run`. It enforces an
//! account-level fuse and a durable rolling budget before each submit.

#[cfg(feature = "copy_run")]
mod live {
    use std::{collections::BTreeSet, env, fmt, future::Future};

    use chrono::Utc;
    use polycopy_engine::copytrading::db::{is_sqlite_busy, BUSY_RETRY_DELAYS};
    use polycopy_engine::{
        copytrading::{
            assert_persistent_startup_clear, ensure_fuse_clear, execute_one_intent_with_marker,
            gtd_poll_requires_fuse, ingest::{spawn_supervised_ingest, AddressResolver},
            list_runnable_intents_by_phase, poll_accepted_gtd_intent, pause_persistent_fuse,
            verify_schedule_compatible_with_pending_work, OrchestrateError, OrchestrateOutcome,
            PersistentError, PersistentRuntimeConfig, PersistentSubmitMarker, EXIT_CONFIG,
            EXIT_LOCK_COLLISION,
        },
        venue::{intl_clob::TickCollateralCache, intl_clob_exec::IntlClobCopyAdapter},
        EngineLock, EngineLockError,
    };
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;

    const DB_PATH_ENV: &str = "POLYCOPY_DB_PATH";

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
            if ingest_guard.realtime_finished() {
                pause_persistent_fuse(
                    &pool,
                    config.account_id,
                    "activity websocket supervisor stopped",
                    "copy_persistent",
                )
                .await
                .map_err(RunnerError::Persistent)?;
                return Err(RunnerError::Persistent(PersistentError::FuseOpen));
            }
            // Do not plan or submit during a WS reconnect gap. REST audit
            // health is intentionally not an execution gate: backfill events
            // are ledger-only and cannot create executable intents.
            if !ingest_guard.realtime_connected() {
                eprintln!("activity websocket unavailable; execution paused until reconnect");
                tokio::time::sleep(config.tick).await;
                continue;
            }
            if let Err(error) = retry_local_busy("ensure_fuse_clear", || {
                ensure_fuse_clear(&pool, config.account_id)
            })
            .await
            {
                if is_sqlite_busy(&error.to_string()) {
                    log_busy_pause("ensure_fuse_clear", &error.to_string());
                    tokio::time::sleep(config.tick).await;
                    continue;
                }
                return Err(RunnerError::Persistent(error));
            }
            if let Err(error) = retry_local_busy("plan_next_batch", || {
                polycopy_engine::copytrading::plan_next_batch(&pool, config.account_id)
            })
            .await
            {
                if is_sqlite_busy(&error.to_string()) {
                    log_busy_pause("plan_next_batch", &error.to_string());
                    tokio::time::sleep(config.tick).await;
                    continue;
                }
                return Err(RunnerError::Other(error.to_string()));
            }
            let (poll_ids, work_ids) = match retry_local_busy("list_runnable_intents", || {
                runnable_intents_for_allowed_leaders(
                    &pool,
                    config.account_id,
                    &config.allowed_leader_ids,
                    config.max_order_notional,
                )
            })
            .await
            {
                Ok(ids) => ids,
                Err(error) if is_sqlite_busy(&error.to_string()) => {
                    log_busy_pause("list_runnable_intents", &error.to_string());
                    tokio::time::sleep(config.tick).await;
                    continue;
                }
                Err(error) => return Err(RunnerError::Persistent(error)),
            };

            // One cache per tick: every intent in this batch sees the same
            // collateral, so the two account-level reads happen once here
            // rather than once per intent. Rebuilt on the next tick so the
            // figure never outlives the batch it was taken for.
            let collateral = TickCollateralCache::new(adapter.read_adapter());
            for (is_poll, intent_id) in poll_ids.into_iter().map(|id| (true, id))
                .chain(work_ids.into_iter().map(|id| (false, id)))
            {
                let outcome = if is_poll {
                    poll_accepted_gtd_intent(&pool, &adapter, intent_id, Utc::now()).await
                } else {
                    execute_one_intent_with_marker(
                        &pool, &collateral, &adapter, &adapter, adapter.read_adapter(),
                        &marker, intent_id, Utc::now(),
                    ).await
                };
                if is_poll && outcome.as_ref().is_ok_and(gtd_poll_requires_fuse) {
                    let reason = format!("GTD poll intent {intent_id} requires reconciliation: {outcome:?}");
                    open_runtime_fuse(&pool, config.account_id, &reason).await?;
                    return Err(RunnerError::Persistent(PersistentError::FuseOpen));
                }
                match outcome {
                    Ok(OrchestrateOutcome::Filled { filled_qty }) => {
                        eprintln!("intent {intent_id}: filled_qty={filled_qty}");
                    }
                    Ok(OrchestrateOutcome::Resting) => {
                        eprintln!("intent {intent_id}: post-only GTD remains on book");
                    }
                    Ok(OrchestrateOutcome::GtdLookupRetry { detail, remaining }) => {
                        eprintln!(
                            "intent {intent_id}: GTD order lookup failed: {detail}; retrying read, expiry plus settlement margin in {} seconds; no resubmission",
                            remaining.num_seconds()
                        );
                    }
                    Ok(OrchestrateOutcome::Rejected) => {
                        // Pull the persisted reason so the line tells the
                        // operator which limit tripped. The token `rejected`
                        // is kept verbatim for the ops panel's important-only
                        // filter (`ops/services.rs` `is_important_log`).
                        let reason: Option<String> = sqlx::query_scalar(
                            "SELECT rejection_reason FROM copy_intents WHERE id = ?",
                        )
                        .bind(intent_id)
                        .fetch_one(&pool)
                        .await
                        .unwrap_or(None);
                        match reason {
                            Some(detail) if !detail.is_empty() => {
                                eprintln!("intent {intent_id}: rejected: {detail}");
                            }
                            _ => {
                                eprintln!("intent {intent_id}: rejected");
                            }
                        }
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
                    Err(OrchestrateError::Persistent(PersistentError::DecisionExpired)) if !is_poll => {
                        eprintln!("intent {intent_id}: expired before submission");
                    }
                    Err(OrchestrateError::Persistent(error)) => {
                        if is_poll {
                            open_runtime_fuse(&pool, config.account_id, &error.to_string()).await?;
                            return Err(RunnerError::Persistent(PersistentError::FuseOpen));
                        }
                        // A budget refusal is a limit doing its job: the
                        // ledger is consistent and nothing needs an operator,
                        // so it stops this run without also latching the fuse
                        // that would block the next one. Three variants
                        // belong in the no-fuse list:
                        //   * `BudgetExceeded` -- legacy variant covering
                        //     `reconfigure_config`'s start-up refusal and
                        //     the `amount <= 0` branch. The marker now maps
                        //     the per-order ceiling (`requested > max_order
                        //     _notional`) to `OrderNotionalExceeded`, so
                        //     this arm is no longer hit on the live submit
                        //     path. Kept here for symmetry / for callers
                        //     outside `submit_prepared`.
                        //   * `LeaderBudgetExhausted` -- one Leader spent
                        //     its own rolling window.
                        //   * `AccountBudgetExhausted` -- the account
                        //     rolling window is full.
                        // `OrderNotionalExceeded` is deliberately absent:
                        // it is intercepted by `submit_prepared` long before
                        // this arm, so the runner would only see it if the
                        // intercept were bypassed. If that ever happens,
                        // latching the fuse is the right backstop because
                        // the marker's mapping is a single, well-understood
                        // line that cannot quietly regress. Listing only the
                        // account variant here used to let a per-Leader
                        // refusal latch the fuse on its way out, so
                        // recovering from it needed a manual resume on top
                        // of a restart; that gap is what motivated
                        // extending this list.
                        if !matches!(
                            error,
                            PersistentError::BudgetExceeded { .. }
                                | PersistentError::LeaderBudgetExhausted { .. }
                                | PersistentError::AccountBudgetExhausted { .. }
                        ) {
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

    async fn retry_local_busy<T, E, F, Fut>(operation: &str, mut work: F) -> Result<T, E>
    where
        E: std::fmt::Display,
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        for (attempt, delay) in BUSY_RETRY_DELAYS.iter().enumerate() {
            match work().await {
                Err(error) if is_sqlite_busy(&error.to_string()) => {
                    eprintln!(
                        "DB_BUSY: component=runner operation={operation} retry={} delay_ms={}",
                        attempt + 1,
                        delay.as_millis()
                    );
                    tokio::time::sleep(*delay).await;
                }
                result => return result,
            }
        }
        work().await
    }

    fn log_busy_pause(operation: &str, error: &str) {
        eprintln!("DB_BUSY: component=runner operation={operation} action=pause detail={error}");
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
    ) -> Result<(Vec<i64>, Vec<i64>), PersistentError> {
        let mut polls = Vec::new();
        let mut work = Vec::new();
        let (poll_ids, work_ids) = list_runnable_intents_by_phase(pool, account_id)
            .await
            .map_err(|error| PersistentError::Database(error.to_string()))?;
        for (is_poll, intent_id) in poll_ids.into_iter().map(|id| (true, id))
            .chain(work_ids.into_iter().map(|id| (false, id)))
        {
            if !is_poll && !work.is_empty() {
                break;
            }
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
                if is_poll {
                    polls.push(intent_id);
                } else {
                    work.push(intent_id);
                }
            }
        }
        Ok((polls, work))
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
