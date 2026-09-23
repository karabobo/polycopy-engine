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
        inspect_uncertain_attempt_for_operator, mark_attempt_gtd_lookup_failed, open_and_migrate,
        open_reconciliation_case, pause_persistent_fuse, persistent_fuse_status,
        reconfigure_persistent_config, release_definitive_rejection,
        resolve_exhausted_fak_no_match,
        resolve_exhausted_maker_only_crossing,
        resolve_no_virtual_lot_sell_case,
        resolve_operator_confirmed_no_fill,
        resolve_pre_submit_balance_case, restore_reservation_and_finalize_recovered_fill,
        resume_persistent_fuse, OperatorUncertainLookup,
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
                "usage: persistent_control init-config|reconfigure|status|pause|resume [reason]|cancel-overdue-pre-submit <intent-id>|release-definitive-rejection <attempt-id>|resolve-exhausted-fak-no-match <intent-id>|resolve-exhausted-maker-only-crossing <intent-id>|resolve-no-virtual-lot-sell <intent-id>|reconcile-uncertain <attempt-id> [--confirm-no-fill <reason>]|reconcile-fill <attempt-id>|reconcile-preflight|mark-attempt-gtd-uncertain <attempt-id>"
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
            "resolve-exhausted-fak-no-match" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot resolve exhausted FAK retries while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let intent_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| polycopy_engine::copytrading::PersistentError::Config(
                        "resolve-exhausted-fak-no-match requires an intent id".to_owned(),
                    ))?
                    .parse()
                    .map_err(|_| polycopy_engine::copytrading::PersistentError::Config(
                        "invalid intent id".to_owned(),
                    ))?;
                let case_id = resolve_exhausted_fak_no_match(&pool, account_id, intent_id).await?;
                println!(
                    "exhausted FAK no-match retries resolved: account_id={account_id} intent_id={intent_id} case_id={case_id}; no order was submitted or replayed. Review status and use persistent_control resume <reason> separately."
                );
            }
            "resolve-exhausted-maker-only-crossing" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot resolve exhausted maker-only crossing retries while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let intent_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| polycopy_engine::copytrading::PersistentError::Config(
                        "resolve-exhausted-maker-only-crossing requires an intent id".to_owned(),
                    ))?
                    .parse()
                    .map_err(|_| polycopy_engine::copytrading::PersistentError::Config(
                        "invalid intent id".to_owned(),
                    ))?;
                let case_id = resolve_exhausted_maker_only_crossing(&pool, account_id, intent_id).await?;
                println!(
                    "exhausted maker-only crossing retries resolved: account_id={account_id} intent_id={intent_id} case_id={case_id}; no order was submitted or replayed. Review status and use persistent_control resume <reason> separately."
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
                    (OperatorUncertainLookup::NotFound, confirmation) => {
                        let intent_id: i64 = sqlx::query_scalar(
                            "SELECT oa.intent_id FROM order_attempts oa \
                             JOIN copy_intents ci ON ci.id = oa.intent_id \
                             WHERE oa.id = ? AND ci.account_id = ? AND oa.status = 'uncertain'",
                        )
                        .bind(attempt_id)
                        .bind(account_id)
                        .fetch_optional(&pool)
                        .await
                        .map_err(|error| polycopy_engine::copytrading::PersistentError::Database(error.to_string()))?
                        .ok_or(polycopy_engine::copytrading::PersistentError::UnresolvedRecovery)?;
                        open_reconciliation_case(
                            &pool,
                            intent_id,
                            Some(attempt_id),
                            "unknown_submission",
                            "operator strict trade-history lookup contained no exact prepared-order identifier",
                        )
                        .await
                        .map_err(|error| polycopy_engine::copytrading::PersistentError::Config(format!(
                            "could not record no-match reconciliation state: {error}"
                        )))?;
                        if let Some(reason) = confirmation {
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
                        } else {
                            println!(
                                "uncertain attempt inspected: account_id={account_id} attempt_id={attempt_id} exact prepared envelope was not found in fresh authenticated trade history; an unknown-submission reconciliation case was opened and the intent remains blocked. To record a human no-fill decision after reviewing this result, rerun: persistent_control reconcile-uncertain {attempt_id} --confirm-no-fill <reason>"
                            );
                        }
                    }
                    (
                        OperatorUncertainLookup::Recovered {
                            order_id,
                            filled_qty,
                            ..
                        },
                        _,
                    ) => {
                        return Err(polycopy_engine::copytrading::PersistentError::Config(format!(
                            "strict history recovered venue order id {} filled_qty={filled_qty}; refusing no-fill resolution; run persistent_control reconcile-fill {attempt_id}",
                            order_id.0
                        )));
                    }
                }
            }
            "reconcile-fill" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot reconcile a recovered fill while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let attempt_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "reconcile-fill requires an attempt id".to_owned(),
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "invalid attempt id".to_owned(),
                        )
                    })?;
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
                        "uncertain attempt remains unresolved; strict lookup did not recover a fill: {error}"
                    ))
                })?;
                match lookup {
                    OperatorUncertainLookup::NotFound => {
                        return Err(polycopy_engine::copytrading::PersistentError::Config(
                            "strict history did not recover this envelope; do not invent a fill"
                                .to_owned(),
                        ));
                    }
                    OperatorUncertainLookup::Recovered {
                        order_id,
                        filled_qty,
                        maker_notional_usdc,
                    } => {
                        eprintln!(
                            "strict recovered fill: attempt_id={attempt_id} venue_order_id={} filled_qty={filled_qty} maker_notional_usdc={maker_notional_usdc}; accounting atomically",
                            order_id.0
                        );
                        let case_id = restore_reservation_and_finalize_recovered_fill(
                            &pool,
                            account_id,
                            attempt_id,
                            filled_qty,
                            maker_notional_usdc,
                        )
                        .await?;
                        println!(
                            "recovered fill accounted: account_id={account_id} attempt_id={attempt_id} venue_order_id={} filled_qty={filled_qty} maker_notional_usdc={maker_notional_usdc} case_id={case_id}; run persistent_control resume <reason> after reviewing remaining recovery state",
                            order_id.0
                        );
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
            // Operator command for `docs/poll-resting-gtd-404-permanently-blocks-startup.md`
            // Part 2: transitions one stuck `'accepted'` GTD attempt (the
            // canonical example is intent 723's attempt 537, which
            // `poll_resting_gtd` rejected via unhandled `Err` on every
            // startup before the orchestrator-side fix could run) to
            // `'uncertain'` so `walk_existing_attempt` no longer routes
            // it through `poll_resting_gtd`. GTD maker reconciliation
            // still needs a dedicated workflow; `reconcile-uncertain`
            // is FAK/taker-only and must NOT be used for this attempt.
            //
            // Strictly narrower than the orchestrator helper:
            //   * require `account_id` match (the existing convention
            //     for every account-scoped command above; refusing
            //     cross-account writes is a defense-in-depth check
            //     against the operator working on the wrong database
            //     row);
            //   * require the attempt's envelope be GTD (a non-GTD
            //     `'accepted'` attempt was already covered by the
            //     strict-lookup machinery before this fix; if
            //     `mark_attempt_gtd_lookup_failed` somehow fires on a
            //     FAK, refuse);
            //   * require `status = 'accepted'` (the only startup-
            //     deadlocking state this command exists to resolve; on
            //     any other status the orchestrator machinery already
            //     owns the next step).
            //
            // This command only blocks replay and records the unresolved
            // attempt. It does not prove a fill or no-fill; do not resume
            // until GTD maker trade history is reconciled separately.
            "mark-attempt-gtd-uncertain" => {
                let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "cannot mark an attempt uncertain while an engine owns the database: {error}"
                    ))
                })?;
                let account_id = account_id_from_env()?;
                let attempt_id = std::env::args()
                    .nth(2)
                    .ok_or_else(|| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "mark-attempt-gtd-uncertain requires an attempt id".to_owned(),
                        )
                    })?
                    .parse()
                    .map_err(|_| {
                        polycopy_engine::copytrading::PersistentError::Config(
                            "invalid attempt id".to_owned(),
                        )
                    })?;
                // Account-bound lookup: refuse if the attempt either
                // does not exist or belongs to a different account.
                // The same predicate is used by `inspect_uncertain_
                // attempt_for_operator` (the FAK-side counterpart);
                // staying symmetrical keeps the audit on the operator
                // command surface bounded by what the orchestrator
                // itself enforces.
                let lookup: Option<(i64, String, String)> = sqlx::query_as(
                    "SELECT oa.intent_id, oa.status, \
                            json_extract(oa.envelope_json, '$.order_type') \
                     FROM order_attempts oa \
                     JOIN copy_intents ci ON ci.id = oa.intent_id \
                     WHERE oa.id = ? AND ci.account_id = ?",
                )
                .bind(attempt_id)
                .bind(account_id)
                .fetch_optional(&pool)
                .await
                .map_err(|error| {
                    polycopy_engine::copytrading::PersistentError::Database(error.to_string())
                })?;
                let (intent_id, attempt_status, order_type) = lookup.ok_or_else(|| {
                    polycopy_engine::copytrading::PersistentError::Config(format!(
                        "attempt {attempt_id} not found for account {account_id} \
                         (wrong account or unknown attempt)"
                    ))
                })?;
                if order_type != "GTD" {
                    return Err(polycopy_engine::copytrading::PersistentError::Config(format!(
                        "mark-attempt-gtd-uncertain only applies to GTD attempts; \
                         attempt {attempt_id} has order_type={order_type:?} \
                         (the orchestrator already has recovery machinery for non-GTD `'accepted'` attempts)"
                    )));
                }
                if attempt_status != "accepted" {
                    return Err(polycopy_engine::copytrading::PersistentError::Config(format!(
                        "mark-attempt-gtd-uncertain only applies to `'accepted'` GTD attempts; \
                         attempt {attempt_id} has status={attempt_status:?} \
                         (re-routing through the recovery matrix is the orchestrator's job, not this command's)"
                    )));
                }
                let detail = "operator manually marked GTD attempt uncertain after \
                     poll_resting_gtd's live-order lookup went cold \
                     (see docs/poll-resting-gtd-404-permanently-blocks-startup.md)"
                    .to_owned();
                let transitioned =
                    mark_attempt_gtd_lookup_failed(&pool, intent_id, attempt_id, &detail)
                        .await
                        .map_err(|error| {
                            polycopy_engine::copytrading::PersistentError::Database(
                                error.to_string(),
                            )
                        })?;
                if !transitioned {
                    return Err(polycopy_engine::copytrading::PersistentError::Config(
                        "mark-attempt-gtd-uncertain: attempt row was no longer in \
                         `'accepted'` state when the update ran; another runner tick \
                         or operator command already moved it. Re-run reconcile-uncertain \
                         to inspect the current status."
                            .to_owned(),
                    ));
                }
                println!(
                    "GTD attempt marked uncertain: account_id={account_id} \
                     attempt_id={attempt_id} intent_id={intent_id}; \
                     strict_query_failure case opened atomically. \
                     GTD maker fills require separate reconciliation; \
                     do not run the FAK-only reconcile-uncertain command \
                     or resume the service yet."
                );
            }
            _ => {
                return Err(polycopy_engine::copytrading::PersistentError::Config(
                    "usage: persistent_control init-config|reconfigure|status|pause|resume [reason]|cancel-overdue-pre-submit <intent-id>|release-definitive-rejection <attempt-id>|resolve-exhausted-fak-no-match <intent-id>|resolve-exhausted-maker-only-crossing <intent-id>|resolve-no-virtual-lot-sell <intent-id>|reconcile-uncertain <attempt-id> [--confirm-no-fill <reason>]|reconcile-fill <attempt-id>|reconcile-preflight|mark-attempt-gtd-uncertain <attempt-id>"
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
