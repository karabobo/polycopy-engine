//! Applies (creates or reconciles) a database's account/leader/policy
//! configuration from one JSON file. Replaces the old separate copy_setup
//! and copy_policy_setup one-shot tools: this same command handles
//! first-time setup and later adjustments, because the underlying
//! reconciliation function (`copytrading::apply_trading_config`) already
//! refuses the one class of change that would be unsafe to fold into an
//! in-place update -- see that module's own doc comment.
//!
//! It authenticates only to derive the existing CLOB credential's signing
//! address; it never creates an API key and never prepares, signs,
//! submits, cancels, or changes a venue order.
//!
//! Acquires the same `EngineLock` `copy_run`/`copy_persistent` hold while
//! live, so this refuses to run (rather than write concurrently) if the
//! engine was not actually stopped first.
//!
//! ```text
//! Usage:
//!   POLYCOPY_DB_PATH=...                    (required)
//!   POLYCOPY_SETUP_CONFIG=...               (required) path to the trading-config JSON file
//!   POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING=1  (optional, default 1) the ceiling every
//!                                            leader's max_order_notional must not exceed;
//!                                            raise this explicitly, never silently, to
//!                                            configure a larger per-order risk cap. This
//!                                            governs only what may be configured --
//!                                            copy_run's own independent, hardcoded runtime
//!                                            ceiling on what may actually be submitted is a
//!                                            separate gate this does not touch.
//! ```

#[cfg(feature = "execute")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use polycopy_engine::{
        copytrading::{apply_trading_config, open_and_migrate, ChangeKind, ConfigApplyOptions},
        engine_lock::EngineLock,
        venue::intl_clob_exec::IntlClobCopyAdapter,
    };
    use rust_decimal::Decimal;

    let result: Result<String, String> = async {
        let db_path = required("POLYCOPY_DB_PATH")?;
        let config_path = required("POLYCOPY_SETUP_CONFIG")?;
        let ceiling: Decimal = match std::env::var("POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING") {
            Ok(raw) if !raw.trim().is_empty() => raw
                .parse()
                .map_err(|_| "POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING must be a decimal".to_owned())?,
            _ => Decimal::ONE,
        };
        if ceiling <= Decimal::ZERO {
            return Err("POLYCOPY_CONFIG_MAX_NOTIONAL_CEILING must be greater than 0".to_owned());
        }

        let raw_json = std::fs::read_to_string(&config_path)
            .map_err(|error| format!("unable to read {config_path}: {error}"))?;
        let config = serde_json::from_str(&raw_json)
            .map_err(|error| format!("invalid trading config JSON: {error}"))?;

        let _lock = EngineLock::acquire_for_database(&db_path).map_err(|error| {
            format!(
                "unable to lock database -- is copy_run or copy_persistent still running? stop \
                 it first: {error}"
            )
        })?;
        let pool = open_and_migrate(&db_path)
            .await
            .map_err(|error| error.to_string())?;

        // Derive-only: proves the configured signer can represent the
        // account, never creates or writes anything to the venue.
        let adapter = IntlClobCopyAdapter::from_env()
            .await
            .map_err(|error| format!("derive-only authentication failed: {error}"))?;
        let signing_address = format!("{:#x}", adapter.signer().address());

        let summary = apply_trading_config(
            &pool,
            &config,
            &signing_address,
            &ConfigApplyOptions {
                max_notional_ceiling: ceiling,
            },
        )
        .await
        .map_err(|error| error.to_string())?;

        println!(
            "CONFIG_APPLIED: {}",
            serde_json::to_string(&summary).unwrap_or_default()
        );

        let mut lines = vec![format!(
            "account {} ({:?})",
            summary.account_id, summary.account_change
        )];
        for leader in &summary.leaders {
            lines.push(format!(
                "leader {} \"{}\" ({:?}) -- aliases +{}/-{}, policy_changed={}",
                leader.leader_id,
                leader.label,
                leader.change,
                leader.aliases_added,
                leader.aliases_disabled,
                leader.policy_changed
            ));
        }
        let any_change = summary.account_change != ChangeKind::Unchanged
            || summary
                .leaders
                .iter()
                .any(|leader| leader.change != ChangeKind::Unchanged);
        lines.push(if any_change {
            "config applied".to_owned()
        } else {
            "config applied: no changes (already matches the database)".to_owned()
        });
        Ok(lines.join("\n"))
    }
    .await;

    match result {
        Ok(summary) => println!("{summary}"),
        Err(error) => {
            eprintln!("copy_config_apply failed: {error}");
            process::exit(3);
        }
    }
}

#[cfg(feature = "execute")]
fn required(name: &'static str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("copy_config_apply requires the execute feature");
    std::process::exit(2);
}
