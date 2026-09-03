//! Operator control for persistent copy execution.
//!
//! This tool writes only local persistent configuration/fuse state. It has no
//! venue adapter and no order-submission path.

#[cfg(feature = "execute")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::process;

    use polycopy_engine::copytrading::{
        init_persistent_config, open_and_migrate, pause_persistent_fuse, persistent_fuse_status,
        resume_persistent_fuse, PersistentRuntimeConfig,
    };

    let result: Result<(), polycopy_engine::copytrading::PersistentError> = async {
        let db_path = std::env::var("POLYCOPY_DB_PATH").map_err(|_| {
            polycopy_engine::copytrading::PersistentError::Config(
                "missing POLYCOPY_DB_PATH".to_owned(),
            )
        })?;
        let command = std::env::args().nth(1).ok_or_else(|| {
            polycopy_engine::copytrading::PersistentError::Config(
                "usage: persistent_control init-config|status|pause|resume [reason]".to_owned(),
            )
        })?;
        let pool = open_and_migrate(db_path)
            .await
            .map_err(|error| {
                polycopy_engine::copytrading::PersistentError::Database(error.to_string())
            })?;
        match command.as_str() {
            "init-config" => {
                let config = PersistentRuntimeConfig::from_env()?;
                init_persistent_config(&pool, &config).await?;
                println!(
                    "persistent config initialized: account_id={} allowed_leaders={} max_order={} rolling_24h={}",
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
            _ => {
                return Err(polycopy_engine::copytrading::PersistentError::Config(
                    "usage: persistent_control init-config|status|pause|resume [reason]"
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

#[cfg(not(feature = "execute"))]
fn main() {
    eprintln!("persistent_control requires the execute feature");
    std::process::exit(2);
}
