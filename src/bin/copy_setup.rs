//! One-time safe initialization for the bounded test-copy progression.
//!
//! It authenticates only to derive the existing CLOB credential and signer
//! identity, then writes local configuration rows. It never creates an API
//! key and never prepares, signs, submits, cancels, or changes a venue order.

#[cfg(feature = "execute")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use chrono::{SecondsFormat, Utc};
    use polycopy_engine::{
        copytrading::open_and_migrate, initialize_fresh_test_copy_setup,
        venue::intl_clob_exec::IntlClobCopyAdapter, InitialCopySetup,
    };

    let result: Result<(), String> = async {
        let db_path = required("POLYCOPY_DB_PATH")?;
        let leader_address = required("POLYCOPY_SETUP_LEADER_ADDRESS")?;
        let funder_address = required("POLYCOPY_CLOB_FUNDER")?;
        let signature_type = required("POLYCOPY_CLOB_SIGNATURE_TYPE")?;
        let max_order_notional = required("POLYCOPY_MAX_ORDER_NOTIONAL")?;
        let activation_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        // This derive-only authentication validates that the protected current
        // test credential can represent the configured Safe. It has no order
        // write path; setup needs the resulting public signing address only.
        let adapter = IntlClobCopyAdapter::from_env()
            .await
            .map_err(|error| format!("derive-only authentication failed: {error}"))?;
        let signing_address = format!("{:#x}", adapter.signer().address());
        let pool = open_and_migrate(db_path)
            .await
            .map_err(|error| error.to_string())?;
        let setup = InitialCopySetup {
            account_label: "test-safe-copy".to_owned(),
            signing_address,
            signature_type,
            funder_address,
            leader_label: "test-leader-1".to_owned(),
            leader_address,
            activation_at,
            max_order_notional,
        };
        let result = initialize_fresh_test_copy_setup(&pool, &setup)
            .await
            .map_err(|error| error.to_string())?;
        println!(
            "test copy setup complete: account_id={} leader_id={} activation_at={}",
            result.account_id, result.leader_id, result.activation_at
        );
        Ok(())
    }
    .await;

    if let Err(error) = result {
        eprintln!("copy setup failed: {error}");
        process::exit(3);
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
    eprintln!("copy_setup requires the execute feature");
    std::process::exit(2);
}
