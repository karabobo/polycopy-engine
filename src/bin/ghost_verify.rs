//! Authenticated, read-only Phase 0 GHOST verification command.

#[cfg(feature = "intl_clob")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use polycopy_engine::ghost_run::{run_ghost_verification, GhostRunConfig};

    let result: Result<(), String> = async {
        let config = GhostRunConfig::from_env().map_err(|error| error.to_string())?;
        let report = run_ghost_verification(&config)
            .await
            .map_err(|error| error.to_string())?;

        println!("GHOST snapshot timestamp: {}", config.snapshot_at_utc());
        println!("collateral: {}", status_name(report.collateral()));
        for (index, token_balance) in report.token_balances().iter().enumerate() {
            println!(
                "outcome token #{}: {}",
                index + 1,
                status_name(token_balance.result())
            );
        }

        if report.is_clean() {
            println!("GHOST verification is clean; this does not authorize automated trading.");
            Ok(())
        } else {
            Err("GHOST verification is unclean; reconciliation is required".to_owned())
        }
    }
    .await;

    if let Err(error) = result {
        eprintln!("GHOST verification failed: {error}");
        std::process::exit(3);
    }
}

#[cfg(feature = "intl_clob")]
fn status_name<E>(result: &polycopy_engine::BalanceVerification<E>) -> &'static str {
    match result {
        polycopy_engine::BalanceVerification::Match { .. } => "match",
        polycopy_engine::BalanceVerification::Mismatch { .. } => "mismatch",
        polycopy_engine::BalanceVerification::QueryFailed { .. } => "query_failed",
    }
}

#[cfg(not(feature = "intl_clob"))]
fn main() {
    eprintln!("ghost_verify requires the intl_clob feature");
    std::process::exit(2);
}
