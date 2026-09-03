//! Applies the short high-frequency test policy before execution work exists.
//!
//! This command only changes the local policy transactionally. It has no
//! credential input, venue client, signing, or order-submission surface.

#[cfg(feature = "execute")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use polycopy_engine::{configure_fresh_high_frequency_policy, copytrading::open_and_migrate};

    let result: Result<(), String> = async {
        let db_path =
            std::env::var("POLYCOPY_DB_PATH").map_err(|_| "missing POLYCOPY_DB_PATH".to_owned())?;
        let pool = open_and_migrate(db_path)
            .await
            .map_err(|error| error.to_string())?;
        configure_fresh_high_frequency_policy(&pool)
            .await
            .map_err(|error| error.to_string())?;
        println!("high-frequency test policy complete: signal_age=3s decision_window=3s");
        Ok(())
    }
    .await;
    if let Err(error) = result {
        eprintln!("high-frequency policy setup failed: {error}");
        process::exit(3);
    }
}

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("copy_policy_setup requires the execute feature");
    std::process::exit(2);
}
