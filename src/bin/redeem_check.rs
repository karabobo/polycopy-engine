//! One-shot, no-order-capable check for redeemable positions across every
//! configured account. Polymarket's own positions API reports
//! `redeemable: true` once a market has resolved (see
//! `copytrading::redemption`'s module doc); this binary only polls that
//! endpoint and reads the local ledger -- it never signs anything, and
//! cannot place an order or claim a redemption itself. Meant to run on a
//! schedule (e.g. a systemd timer) alongside `copy_run`, not as a
//! long-lived process.
//!
//! ```text
//! Usage:
//!   POLYCOPY_DB_PATH=...      (required) existing, already-configured database
//!   POLYCOPY_ACCOUNT_ID=...   (optional) check only this account; default:
//!                             every row in `accounts`
//! ```
//!
//! Emits one `REDEMPTION_EVENT:`-prefixed JSON line per redeemable
//! position found, plus a human-readable summary.
//!
//! Exit code 0: ran cleanly, nothing redeemable found. Exit code 1: ran
//! cleanly but found one or more redeemable positions -- this is the
//! "there is something to go claim" signal, not a tool failure; wire a
//! systemd timer's `OnFailure=` (or equivalent) to it if you want an
//! alert on exactly this. Exit code 3: could not run at all (missing/bad
//! env var, bad database, unknown/misconfigured account, or the positions
//! API call itself failed) -- mirrors `ingest_observe`'s env-var-driven
//! binaries, which use the same two-bucket scheme rather than a separate
//! usage-error code.

#[cfg(feature = "redeem_detect")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use polycopy_engine::copytrading::{
        detect_redeemable_positions, open_read_only, RedeemablePosition, REDEMPTION_EVENT_PREFIX,
    };
    use polymarket_client_sdk_v2::data::Client;

    // rustls 0.23+ does not pick a default crypto backend on its own;
    // without installing one, the positions API's TLS connect can panic or
    // hang. Mirrors activity_ws.rs's own call for the same reason.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let result: Result<Vec<RedeemablePosition>, String> = async {
        let db_path = required("POLYCOPY_DB_PATH")?;
        let account_filter: Option<i64> = match std::env::var("POLYCOPY_ACCOUNT_ID") {
            Ok(raw) if !raw.trim().is_empty() => Some(
                raw.parse()
                    .map_err(|_| "POLYCOPY_ACCOUNT_ID must be an integer".to_owned())?,
            ),
            _ => None,
        };

        let pool = open_read_only(&db_path)
            .await
            .map_err(|error| error.to_string())?;
        let client =
            Client::new("https://data-api.polymarket.com").map_err(|error| error.to_string())?;

        let account_ids: Vec<i64> = match account_filter {
            Some(id) => vec![id],
            None => sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(&pool)
                .await
                .map_err(|error| error.to_string())?,
        };
        if account_ids.is_empty() {
            return Err(
                "no accounts configured in this database -- run copy_setup first".to_owned(),
            );
        }

        let mut found = Vec::new();
        for account_id in account_ids {
            let positions = detect_redeemable_positions(&pool, &client, account_id)
                .await
                .map_err(|error| format!("account {account_id}: {error}"))?;
            found.extend(positions);
        }
        Ok(found)
    }
    .await;

    match result {
        Ok(found) => {
            for position in &found {
                println!(
                    "{REDEMPTION_EVENT_PREFIX}{}",
                    serde_json::to_string(position).unwrap_or_default()
                );
                println!(
                    "  account {}: \"{}\" outcome \"{}\" size {} (value {}) -- tracked in local lots: {}",
                    position.account_id,
                    position.title,
                    position.outcome,
                    position.size,
                    position.current_value,
                    position.tracked_in_local_lots
                );
            }
            if found.is_empty() {
                println!("no redeemable positions found");
                process::exit(0);
            }
            println!(
                "{} redeemable position(s) found -- nothing was claimed, this tool cannot submit \
                 that transaction; redeem manually",
                found.len()
            );
            process::exit(1);
        }
        Err(error) => {
            eprintln!("redeem_check failed: {error}");
            process::exit(3);
        }
    }
}

#[cfg(feature = "redeem_detect")]
fn required(name: &'static str) -> Result<String, String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

#[cfg(not(feature = "redeem_detect"))]
fn main() {
    eprintln!("redeem_check requires the redeem_detect feature");
    std::process::exit(2);
}
