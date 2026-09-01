//! Phase 0.5 CLOB submission-safety canary probe.
//!
//! **Dry run by default.** This command always builds and signs the one
//! configured canary order, but it only calls the venue's order-writing
//! endpoint if the operator explicitly sets `POLYCOPY_CANARY_CONFIRM_SUBMIT=yes`
//! in the process environment. Nothing about the tool itself, and nothing an
//! AI assistant does, can set that variable on your behalf — you set it
//! yourself, in your own shell, only when you intend to place the order.
//!
//! Configuration is read entirely from the environment; see
//! `docs/PHASE_0_5_CANARY_REPORT.md` and the credential variables already
//! documented in `README.md` for Phase 0 (`POLYCOPY_CLOB_*`). Additional
//! canary-specific variables:
//!
//! ```text
//! POLYCOPY_CANARY_LABEL=<short stable label, used for canary-artifacts/<label>/>
//! POLYCOPY_CANARY_TOKEN_ID=<decimal outcome token ID>
//! POLYCOPY_CANARY_SIDE=BUY|SELL
//! POLYCOPY_CANARY_PRICE=<decimal, strictly between 0 and 1>
//! POLYCOPY_CANARY_SIZE=<decimal, positive>
//! POLYCOPY_CANARY_CONFIRM_SUBMIT=yes      # only this exact value submits the first order
//! POLYCOPY_CANARY_CONFIRM_DUPLICATE=yes   # only this exact value also submits the duplicate
//! POLYCOPY_CANARY_VERIFY_TRADE_LOOKUP=yes # read-only exact taker_order_id check; never submits
//! POLYCOPY_CANARY_EXPECTED_ORDER_ID=[persisted expected order ID for lookup mode]
//! POLYCOPY_CANARY_ARTIFACTS_DIR=/var/lib/polycopy-engine/canary-artifacts
//! ```
//!
//! The order is always Fill-And-Kill: this project has no cancel-order
//! client, so a resting GTC canary could be filled later with nothing able to
//! close it. Choose `POLYCOPY_CANARY_PRICE` away from the current market and
//! `POLYCOPY_CANARY_SIZE` at the venue's minimum to keep an accidental match
//! astronomically unlikely.

#[cfg(feature = "intl_clob")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::path::PathBuf;

    use polycopy_engine::canary::{write_new_record, CanaryLookupRecord, CanarySubmissionRecord};
    use polycopy_engine::canary_run::{
        build_signable_order, expected_order_id, lookup_by_id, sign_twice, submit, CanaryRunConfig,
    };
    use polycopy_engine::venue::intl_clob::{IntlClobReadAdapter, OutcomeTokenId};
    use polycopy_engine::venue::order_hash::format_order_id;
    use polymarket_client_sdk_v2::{clob::types::request::OrdersRequest, types::U256};
    use std::str::FromStr as _;

    let result: Result<(), String> = async {
        let config = CanaryRunConfig::from_env().map_err(|error| error.to_string())?;
        let artifacts_root = std::env::var_os("POLYCOPY_CANARY_ARTIFACTS_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("canary-artifacts"));
        let artifacts_dir = artifacts_root.join(&config.label);
        let spec_path = artifacts_dir.join("spec.json");

        println!("== Phase 0.5 canary probe: {} ==", config.label);
        println!(
            "token_id={} side={:?} price={} size={} order_type=FAK",
            config.spec.token_id(),
            config.spec.side(),
            config.spec.price(),
            config.spec.size()
        );

        let client = config
            .authenticated_client()
            .await
            .map_err(|error| error.to_string())?;

        // This branch constructs no signer and no order. It is intentionally
        // usable after a response-loss canary to prove whether the only
        // documented read-only recovery key can find that exact FAK.
        if std::env::var("POLYCOPY_CANARY_VERIFY_TRADE_LOOKUP").as_deref() == Ok("yes") {
            let expected = std::env::var("POLYCOPY_CANARY_EXPECTED_ORDER_ID")
                .map_err(|_| "missing POLYCOPY_CANARY_EXPECTED_ORDER_ID for read-only trade lookup")?;
            if expected.trim().is_empty() {
                return Err("POLYCOPY_CANARY_EXPECTED_ORDER_ID must not be empty".into());
            }
            let token = OutcomeTokenId::from_str(config.spec.token_id())
                .map_err(|error| error.to_string())?;
            let reader = IntlClobReadAdapter::new(client.clone());
            let now = chrono::Utc::now();
            let record = match reader
                .trades_for_token_between(&token, now - chrono::Duration::hours(24), now)
                .await
            {
                Ok(trades) => {
                    let matches: Vec<_> = trades
                        .iter()
                        .filter(|trade| trade.taker_order_id == expected)
                        .collect();
                    match matches.as_slice() {
                        [trade] => CanaryLookupRecord::found(
                            &config.label,
                            &now_utc(),
                            "trade_history_taker_order_id",
                            trade.taker_order_id.clone(),
                            format!("{:?}", trade.status),
                            trade.size.to_string(),
                        ),
                        [] => CanaryLookupRecord::not_found(
                            &config.label,
                            &now_utc(),
                            "trade_history_taker_order_id",
                        ),
                        _ => CanaryLookupRecord::query_failed(
                            &config.label,
                            &now_utc(),
                            "trade_history_taker_order_id",
                            format!("ambiguous: {} exact taker_order_id matches", matches.len()),
                        ),
                    }
                }
                Err(error) => CanaryLookupRecord::query_failed(
                    &config.label,
                    &now_utc(),
                    "trade_history_taker_order_id",
                    error,
                ),
            };
            let lookup_path = artifacts_dir.join("trade_history_taker_order_id.json");
            write_new_record(
                &lookup_path,
                &serde_json::to_string_pretty(&record)
                    .map_err(|error| format!("unable to serialize trade lookup: {error}"))?,
            )
            .map_err(|error| error.to_string())?;
            println!(
                "READ-ONLY trade-history lookup persisted: {} found={}",
                lookup_path.display(),
                record.found_order_id.as_deref() == Some(expected.as_str())
            );
            return Ok(());
        }

        let signer = config.signer().map_err(|error| error.to_string())?;
        let signable = build_signable_order(&client, &config.spec)
            .await
            .map_err(|error| error.to_string())?;

        let prepared_at = now_utc();
        let record = config.spec.to_record(&config.label, &prepared_at);
        let record_json = serde_json::to_string_pretty(&record)
            .map_err(|error| format!("unable to serialize the canary spec record: {error}"))?;
        write_new_record(&spec_path, &record_json).map_err(|error| error.to_string())?;
        println!("persisted spec: {}", spec_path.display());

        let (first, second) = sign_twice(&client, &signer, signable)
            .await
            .map_err(|error| error.to_string())?;
        let signatures_match = serialized_order(&first) == serialized_order(&second);
        println!(
            "two independent signatures over the same built order are byte-identical: {signatures_match}"
        );

        // Read-only (one `neg_risk` GET): safe to compute on every run,
        // dry or live. A failure here is not fatal to the probe -- it just
        // means this run cannot contribute evidence to the still-open
        // `docs/PHASE_0_5_CANARY_REPORT.md` question of whether this value
        // equals the venue's real order_id.
        let expected_id = match expected_order_id(&client, &first.payload).await {
            Ok(hash) => {
                let formatted = format_order_id(hash);
                println!("expected_order_id (this project's offline prediction): {formatted}");
                Some(formatted)
            }
            Err(error) => {
                println!("expected_order_id: FAILED to compute: {error}");
                None
            }
        };

        if !config.confirm_submit() {
            println!();
            println!("DRY RUN: no order was submitted to Polymarket.");
            println!(
                "Everything above this line ran against the live venue except the order-writing \
                 call itself (client construction, authentication, and order building/signing all \
                 succeeded)."
            );
            println!(
                "To place this exact canary order yourself, re-run this command with \
                 POLYCOPY_CANARY_CONFIRM_SUBMIT=yes set in your own shell."
            );
            return Ok(());
        }

        let submission_path = artifacts_dir.join("submission_1.json");
        let submitted_at = now_utc();
        let response = submit(&client, first)
            .await
            .map_err(|error| error.to_string())?;
        let submission_record = CanarySubmissionRecord {
            label: config.label.clone(),
            submitted_at_utc: submitted_at,
            order_id: response.order_id.clone(),
            status: format!("{:?}", response.status),
            success: response.success,
            making_amount: response.making_amount.to_string(),
            taking_amount: response.taking_amount.to_string(),
            transaction_hash_count: response.transaction_hashes.len(),
            trade_id_count: response.trade_ids.len(),
            expected_order_id: expected_id.clone(),
        };
        let submission_json = serde_json::to_string_pretty(&submission_record)
            .map_err(|error| format!("unable to serialize the submission record: {error}"))?;
        write_new_record(&submission_path, &submission_json).map_err(|error| error.to_string())?;
        println!(
            "SUBMITTED order_id={} status={:?} success={}",
            response.order_id, response.status, response.success
        );
        match &expected_id {
            Some(expected) if expected == &response.order_id => println!(
                "expected_order_id MATCHES the venue's real order_id -- live proof for \
                 docs/PHASE_0_5_CANARY_REPORT.md."
            ),
            Some(expected) => println!(
                "expected_order_id ({expected}) DOES NOT MATCH the venue's real order_id \
                 ({}) -- this project's order-ID hypothesis is disproven for this run.",
                response.order_id
            ),
            None => println!(
                "expected_order_id was not computed this run; no comparison available."
            ),
        }

        // A lookup failure is itself a Phase 0.5 finding (e.g. the venue may
        // not surface a fully matched order via this endpoint), not a reason
        // to abort the remaining checks — record it and continue.
        let lookup_by_id_path = artifacts_dir.join("lookup_by_id.json");
        let by_id_record = match lookup_by_id(&client, &response.order_id).await {
            Ok(looked_up) => {
                println!("lookup by order_id: found={}", looked_up.id == response.order_id);
                CanaryLookupRecord::found(
                    &config.label,
                    &now_utc(),
                    "order_id",
                    looked_up.id.clone(),
                    format!("{:?}", looked_up.status),
                    looked_up.size_matched.to_string(),
                )
            }
            Err(error) => {
                println!("lookup by order_id: FAILED: {error}");
                CanaryLookupRecord::query_failed(&config.label, &now_utc(), "order_id", error)
            }
        };
        let by_id_json = serde_json::to_string_pretty(&by_id_record)
            .map_err(|error| format!("unable to serialize the lookup record: {error}"))?;
        write_new_record(&lookup_by_id_path, &by_id_json).map_err(|error| error.to_string())?;

        let lookup_by_fields_path = artifacts_dir.join("lookup_by_fields.json");
        let by_fields_record = match U256::from_str(config.spec.token_id()) {
            Err(_) => CanaryLookupRecord::query_failed(
                &config.label,
                &now_utc(),
                "asset_id_field_match",
                "canary token ID became invalid after submission",
            ),
            Ok(token_id) => {
                let filter = OrdersRequest::builder().asset_id(token_id).build();
                match client.orders(&filter, None).await {
                    Ok(page) => {
                        let blind_match =
                            page.data.iter().find(|order| order.id == response.order_id);
                        println!(
                            "blind field-based lookup among {} order(s) for this token: found={}",
                            page.data.len(),
                            blind_match.is_some()
                        );
                        match blind_match {
                            Some(order) => CanaryLookupRecord::found(
                                &config.label,
                                &now_utc(),
                                "asset_id_field_match",
                                order.id.clone(),
                                format!("{:?}", order.status),
                                order.size_matched.to_string(),
                            ),
                            None => CanaryLookupRecord::not_found(
                                &config.label,
                                &now_utc(),
                                "asset_id_field_match",
                            ),
                        }
                    }
                    Err(error) => {
                        println!("blind field-based lookup: FAILED: {error}");
                        CanaryLookupRecord::query_failed(
                            &config.label,
                            &now_utc(),
                            "asset_id_field_match",
                            error,
                        )
                    }
                }
            }
        };
        let by_fields_json = serde_json::to_string_pretty(&by_fields_record)
            .map_err(|error| format!("unable to serialize the lookup record: {error}"))?;
        write_new_record(&lookup_by_fields_path, &by_fields_json)
            .map_err(|error| error.to_string())?;

        if !config.confirm_duplicate() {
            println!();
            println!(
                "Duplicate-submission question not tested this run \
                 (POLYCOPY_CANARY_CONFIRM_DUPLICATE was not set to yes)."
            );
            return Ok(());
        }

        let duplicate_path = artifacts_dir.join("submission_2.json");
        let duplicate_submitted_at = now_utc();
        let duplicate_response = submit(&client, second)
            .await
            .map_err(|error| error.to_string())?;
        let duplicate_record = CanarySubmissionRecord {
            label: config.label.clone(),
            submitted_at_utc: duplicate_submitted_at,
            order_id: duplicate_response.order_id.clone(),
            status: format!("{:?}", duplicate_response.status),
            success: duplicate_response.success,
            making_amount: duplicate_response.making_amount.to_string(),
            taking_amount: duplicate_response.taking_amount.to_string(),
            transaction_hash_count: duplicate_response.transaction_hashes.len(),
            trade_id_count: duplicate_response.trade_ids.len(),
            // `second` shares `first`'s exact payload (only the signature
            // differs between two independent `sign()` calls), so the same
            // offline prediction applies to both submissions.
            expected_order_id: expected_id.clone(),
        };
        let duplicate_json = serde_json::to_string_pretty(&duplicate_record)
            .map_err(|error| format!("unable to serialize the duplicate submission record: {error}"))?;
        write_new_record(&duplicate_path, &duplicate_json).map_err(|error| error.to_string())?;
        println!(
            "DUPLICATE SUBMISSION order_id={} status={:?} success={} (compare against the first \
             order_id={} to classify: same ID = idempotent, different ID = independently \
             executable, error = deterministic rejection)",
            duplicate_response.order_id,
            duplicate_response.status,
            duplicate_response.success,
            response.order_id
        );

        Ok(())
    }
    .await;

    if let Err(error) = result {
        eprintln!("canary probe failed: {error}");
        if looks_like_a_platform_outage(&error) {
            eprintln!(
                "This looks like a platform-wide issue, not something specific to this account \
                 or market (confirmed live on 2026-08-31: the identical rejection happened on \
                 two unrelated markets during a real Polymarket CLOB outage). Before retrying, \
                 check https://status.polymarket.com for an open incident, and wait until it is \
                 marked resolved -- retrying during its cancel-only window will just fail again."
            );
        }
        std::process::exit(3);
    }
}

/// A conservative heuristic, not a venue-documented error code: matches the
/// exact rejection text observed live on 2026-08-31 during a confirmed
/// platform-wide CLOB outage (see docs/PHASE_0_5_CANARY_REPORT.md, Result 2).
/// A false negative here just means a plainer error message; it never
/// changes retry behavior, since this project does not auto-retry regardless.
#[cfg(feature = "intl_clob")]
fn looks_like_a_platform_outage(error: &str) -> bool {
    error.to_ascii_lowercase().contains("trading is disabled")
}

#[cfg(feature = "intl_clob")]
fn serialized_order(order: &polymarket_client_sdk_v2::clob::types::SignedOrder) -> String {
    // Route the "are these two signed orders identical" check through the
    // same JSON encoding the venue receives, so "byte identical" means what
    // it says (salt and payload are identical by construction; signature is
    // the only field that could differ between two independent `sign` calls).
    serde_json::to_string(order).unwrap_or_default()
}

#[cfg(feature = "intl_clob")]
fn now_utc() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::SecondsFormat;

    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must be after the Unix epoch")
        .as_secs();
    polymarket_client_sdk_v2::types::DateTime::from_timestamp(secs as i64, 0)
        .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Secs, true))
        .unwrap_or_default()
}

#[cfg(not(feature = "intl_clob"))]
fn main() {
    eprintln!("canary_probe requires the intl_clob feature");
    std::process::exit(2);
}

#[cfg(all(test, feature = "intl_clob"))]
mod tests {
    use super::looks_like_a_platform_outage;

    #[test]
    fn recognizes_the_exact_rejection_seen_live_during_the_2026_08_31_outage() {
        let error = r#"unable to submit the canary order: Status: error(503 Service Unavailable) making POST call to /order with {"error":"trading is disabled"}"#;
        assert!(looks_like_a_platform_outage(error));
    }

    #[test]
    fn is_case_insensitive() {
        assert!(looks_like_a_platform_outage("TRADING IS DISABLED"));
    }

    #[test]
    fn does_not_flag_unrelated_errors() {
        assert!(!looks_like_a_platform_outage("invalid CLOB signing key"));
        assert!(!looks_like_a_platform_outage(
            "missing required POLYCOPY_CANARY_TOKEN_ID"
        ));
    }
}
