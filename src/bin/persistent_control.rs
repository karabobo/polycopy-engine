//! Operator control for persistent copy execution.
//!
//! This tool writes only local persistent configuration/fuse state.  Its
//! `reconcile-uncertain` command additionally performs a strict read-only
//! trade-history lookup; it has no order-submission path.

#[cfg(feature = "execute")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use polycopy_engine::copytrading::{
        cancel_overdue_pre_submit_intent, init_persistent_config,
        inspect_uncertain_attempt_for_operator, open_and_migrate, pause_persistent_fuse,
        persistent_fuse_status, reconfigure_persistent_config, release_definitive_rejection,
        resolve_no_virtual_lot_sell_case, resolve_operator_confirmed_no_fill,
        resolve_pre_submit_balance_case, resume_persistent_fuse, OperatorUncertainLookup,
        PersistentRuntimeConfig,
    };
    use polycopy_engine::{
        engine_lock::EngineLock, venue::intl_clob::StrictAccountBalanceReader,
        venue::intl_clob_exec::IntlClobCopyAdapter,
    };

    let result: Result<(), polycopy_engine::copytrading::PersistentError> = async {
        let db_path = std::env::var("POLYCOPY_DB_PATH").map_err(|_| {
            polycopy_engine::copytrading::PersistentError::Config(
                "missing POLYCOPY_DB_PATH".to_owned(),
            )
        })?;
        let command = std::env::args().nth(1).ok_or_else(|| {
            polycopy_engine::copytrading::PersistentError::Config(
                "usage: persistent_control init-config|reconfigure|status|pause|resume [reason]|cancel-overdue-pre-submit <intent-id>|release-definitive-rejection <attempt-id>|resolve-no-virtual-lot-sell <intent-id>|reconcile-uncertain <attempt-id> [--confirm-no-fill <reason>]|reconcile-preflight"
                    .to_owned(),
            )
        })?;
        let pool = open_and_migrate(&db_path)
            .await
            .map_err(|error| {
                polycopy_engine::copytrading::PersistentError::Database(error.to_string())
            })?;
        match command.as_str() {
            "init-config" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot initialize persistent config while an engine owns the database: {error}"
                    ))
                })?;
                let config = PersistentRuntimeConfig::from_env()?;
                init_persistent_config(&pool, &config).await?;
                println!(
                    "persistent config initialized: account_id={} allowed_leaders={} max_order={} account_24h_turnover_circuit_breaker={}",
                    config.account_id,
                    config.allowed_leaders_text(),
                    config.max_order_notional,
                    config.rolling_budget
                );
            }
            "reconfigure" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot reconfigure persistent mode while an engine owns the database: {error}"
                    ))
                })?;
                let config = PersistentRuntimeConfig::from_env()?;
                reconfigure_persistent_config(&pool, &config).await?;
                println!(
                    "persistent config reconfigured: account_id={} allowed_leaders={} max_order={} account_24h_turnover_circuit_breaker={}",
                    config.account_id,
                    config.allowed_leaders_text(),
                    config.max_order_notional,
                    config.rolling_budget
                );
            }
            "status" => {
                let account_id = account_id_from_env()?;
                match persistent_fuse_status(&pool, account_id).await? {
                    Some((paused_at, reason, actor)) => {
                        println!(
                            "persistent status: paused account_id={account_id} paused_at={paused_at} actor={actor} reason={reason}"
                        );
                    }
                    None => println!("persistent status: running-allowed account_id={account_id}"),
                }
            }
            "pause" => {
                let account_id = account_id_from_env()?;
                let reason = required_reason()?;
                pause_persistent_fuse(&pool, account_id, &reason, "persistent_control").await?;
                println!("persistent fuse paused: account_id={account_id} reason={reason}");
            }
            "resume" => {
                let account_id = account_id_from_env()?;
                let reason = required_reason()?;
                resume_persistent_fuse(&pool, account_id, &reason).await?;
                println!("persistent fuse resumed: account_id={account_id} reason={reason}");
            }
            "cancel-overdue-pre-submit" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot cancel an overdue intent while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let intent_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "cancel-overdue-pre-submit requires an intent id".to_owned(),
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "invalid intent id".to_owned(),
                        )
                    })?;
                cancel_overdue_pre_submit_intent(&pool, account_id, intent_id)
                    .await
                    .map_err(|error| {
                        polycopy_engine::copytrading::PersistentError::Config(format!(
                            "refusing overdue pre-submit cancellation: {error}"
                        ))
                    })?;
                println!(
                    "overdue pre-submit intent cancelled locally: account_id={account_id} intent_id={intent_id}"
                );
            }
            "release-definitive-rejection" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot release a rejected reservation while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let attempt_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "release-definitive-rejection requires an attempt id".to_owned(),
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "invalid attempt id".to_owned(),
                        )
                    })?;
                release_definitive_rejection(&pool, account_id, attempt_id).await?;
                println!(
                    "definitive rejection reservation released: account_id={account_id} attempt_id={attempt_id}"
                );
            }
            "resolve-no-virtual-lot-sell" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot resolve a sell case while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let intent_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "resolve-no-virtual-lot-sell requires an intent id".to_owned(),
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "invalid intent id".to_owned(),
                        )
                    })?;
                let case_id = resolve_no_virtual_lot_sell_case(&pool, account_id, intent_id).await?;
                println!(
                    "no-virtual-lot sell case resolved locally: account_id={account_id} intent_id={intent_id} case_id={case_id}"
                );
            }
            "reconcile-uncertain" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot reconcile an uncertain attempt while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let (attempt_id, confirmation) = reconcile_uncertain_arguments()?;
                let adapter = IntlClobCopyAdapter::from_env().await.map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "strict trade-history lookup could not authenticate: {error}"
                    ))
                })?;
                let lookup = inspect_uncertain_attempt_for_operator(
                    &pool,
                    adapter.read_adapter(),
                    account_id,
                    attempt_id,
                    chrono::Utc::now(),
                )
                .await
                .map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "uncertain attempt remains unresolved; strict lookup did not prove no-fill: {error}"
                    ))
                })?;
                match (lookup, confirmation) {
                    (OperatorUncertainLookup::NotFound, None) => println!(
                        "uncertain attempt inspected: account_id={account_id} attempt_id={attempt_id} exact prepared envelope was not found in fresh authenticated trade history; no local state changed. To record a human no-fill decision after reviewing this result, rerun: persistent_control reconcile-uncertain {attempt_id} --confirm-no-fill <reason>"
                    ),
                    (OperatorUncertainLookup::NotFound, Some(reason)) => {
                        let case_id = resolve_operator_confirmed_no_fill(
                            &pool,
                            account_id,
                            attempt_id,
                            &reason,
                        )
                        .await?;
                        println!(
                            "uncertain attempt resolved as operator-confirmed no-fill: account_id={account_id} attempt_id={attempt_id} case_id={case_id}; the reservation was released with an auditable operator-no-fill state. Run persistent_control resume <reason> separately after reviewing all remaining recovery state."
                        );
                    }
                    (OperatorUncertainLookup::Recovered { order_id }, _) => {
                        return Err(polycopy_engine::copytrading::PersistentError::Config(format!(
                            "strict history recovered venue order id {}; refusing no-fill resolution; reconcile its receipt instead",
                            order_id.0
                        )));
                    }
                }
            }
            "reconcile-preflight" => {
                let config = PersistentRuntimeConfig::from_env()?;
                polycopy_engine::copytrading::persistent::verify_config(&pool, &config).await?;
                let adapter = IntlClobCopyAdapter::from_env().await.map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "strict collateral preflight could not authenticate: {error}"
                    ))
                })?;
                let balance = adapter
                    .read_adapter()
                    .collateral_balance_strict()
                    .await
                    .map_err(|error| {
                        polycopy_engine::copytrading::PersistentError::Config(format!(
                            "strict collateral balance preflight failed: {error}"
                        ))
                    })?;
                let allowance = adapter
                    .read_adapter()
                    .collateral_allowance_strict()
                    .await
                    .map_err(|error| {
                        polycopy_engine::copytrading::PersistentError::Config(format!(
                            "strict collateral allowance preflight failed: {error}"
                        ))
                    })?;
                let usable = balance.min(allowance);
                if usable < config.max_order_notional {
                    return Err(polycopy_engine::copytrading::PersistentError::Config(format!(
                        "strict preflight usable collateral {usable} is below the configured per-order cap {}",
                        config.max_order_notional
                    )));
                }
                let case_id = resolve_pre_submit_balance_case(&pool, config.account_id).await?;
                println!(
                    "pre-submit reconciliation resolved: case_id={case_id} usable_collateral={usable}"
                );
            }
            _ => {
                return Err(polycopy_engine::copytrading::PersistentError::Config(
                    "usage: persistent_control init-config|reconfigure|status|pause|resume [reason]|cancel-overdue-pre-submit <intent-id>|release-definitive-rejection <attempt-id>|resolve-no-virtual-lot-sell <intent-id>|reconcile-uncertain <attempt-id> [--confirm-no-fill <reason>]|reconcile-preflight"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }
    .await;

    if let Err(error) = result {
        eprintln!("{error}");
        process::exit(error.exit_code());
    }
}

#[cfg(feature = "execute")]
fn account_id_from_env() -> Result<i64, polycopy_engine::copytrading::PersistentError> {
    std::env::var("POLYCOPY_PERSISTENT_ACCOUNT_ID")
        .map_err(|_| {
            polycopy_engine::copytrading::PersistentError::Config(
                "missing POLYCOPY_PERSISTENT_ACCOUNT_ID".to_owned(),
            )
        })?
        .parse()
        .map_err(|_| {
            polycopy_engine::copytrading::PersistentError::Config(
                "invalid POLYCOPY_PERSISTENT_ACCOUNT_ID".to_owned(),
            )
        })
}

#[cfg(feature = "execute")]
fn required_reason() -> Result<String, polycopy_engine::copytrading::PersistentError> {
    let reason = std::env::args().skip(2).collect::<Vec<_>>().join(" ");
    if reason.trim().is_empty() {
        Err(polycopy_engine::copytrading::PersistentError::Config(
            "pause/resume requires an explicit reason".to_owned(),
        ))
    } else {
        Ok(reason)
    }
}

#[cfg(feature = "execute")]
fn reconcile_uncertain_arguments(
) -> Result<(i64, Option<String>), polycopy_engine::copytrading::PersistentError> {
    let args = std::env::args().skip(2).collect::<Vec<_>>();
    let attempt_id = args
        .first()
        .ok_or_else(|| {
            polycopy_engine::copytrading::PersistentError::Config(
                "reconcile-uncertain requires an attempt id".to_owned(),
            )
        })?
        .parse()
        .map_err(|_| {
            polycopy_engine::copytrading::PersistentError::Config("invalid attempt id".to_owned())
        })?;
    match args.get(1).map(String::as_str) {
        None => Ok((attempt_id, None)),
        Some("--confirm-no-fill") => {
            let reason = args[2..].join(" ");
            if reason.trim().is_empty() {
                return Err(polycopy_engine::copytrading::PersistentError::Config(
                    "--confirm-no-fill requires an explicit operator reason".to_owned(),
                ));
            }
            Ok((attempt_id, Some(reason)))
        }
        Some(_) => Err(polycopy_engine::copytrading::PersistentError::Config(
            "reconcile-uncertain accepts only --confirm-no-fill <reason> after the attempt id"
                .to_owned(),
        )),
    }
}

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("persistent_control requires the execute feature");
    std::process::exit(2);
}
