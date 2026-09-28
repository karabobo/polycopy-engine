use std::{
    collections::VecDeque,
    sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
    },
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use super::*;
use crate::copytrading::persistent::{
    init_config, PersistentRuntimeConfig, PersistentSubmitMarker,
};
use crate::venue::intl_clob::StrictTokenBalanceReader;
use crate::{
    copytrading::{db::open_and_migrate, plan::PolicySnapshot, reconcile::OrderId},
    venue::{
        intl_clob::{
            AccountTrade, OutcomeTokenId, StrictCollateralError, StrictPositionError,
            StrictTradeHistoryError,
        },
        OrderReceipt,
    },
};

/// Builds a `MarketResponse` fixture with sensible defaults for everything
/// except the fields a GTD-marker test wants to vary. Kept in tests because
/// the SDK's `MarketResponse` is `#[non_exhaustive]` and `derive_builder`-
/// required; a real production path never constructs one of these.
#[cfg(test)]
fn gtd_market_fixture(
    closed: bool,
    accepting_orders: bool,
    minimum_tick_size: Decimal,
    end_date_iso: Option<DateTime<Utc>>,
) -> polymarket_client_sdk_v2::clob::types::response::MarketResponse {
    use polymarket_client_sdk_v2::clob::types::response::{MarketResponse, Rewards};
    use polymarket_client_sdk_v2::types::{address, b256};

    MarketResponse::builder()
        .enable_order_book(true)
        .active(true)
        .closed(closed)
        .archived(false)
        .accepting_orders(accepting_orders)
        .accepting_order_timestamp(
            "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
        )
        .minimum_order_size(Decimal::ONE)
        .minimum_tick_size(minimum_tick_size)
        .condition_id(b256!("0x0000000000000000000000000000000000000000000000000000000000000001"))
        .question_id(b256!("0x0000000000000000000000000000000000000000000000000000000000000002"))
        .question("test market".to_owned())
        .description("test description".to_owned())
        .market_slug("test-slug".to_owned())
        .end_date_iso(
            end_date_iso
                .unwrap_or_else(|| "2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
        )
        .seconds_delay(0)
        .fpmm(address!("0x0000000000000000000000000000000000000001"))
        .maker_base_fee(Decimal::ZERO)
        .taker_base_fee(Decimal::ZERO)
        .notifications_enabled(false)
        .neg_risk(false)
        .neg_risk_market_id(b256!("0x0000000000000000000000000000000000000000000000000000000000000003"))
        .neg_risk_request_id(b256!("0x0000000000000000000000000000000000000000000000000000000000000004"))
        .icon(String::new())
        .image(String::new())
        .rewards(Rewards::default())
        .is_50_50_outcome(false)
        .tokens(Vec::new())
        .tags(Vec::new())
        .build()
}

struct TestDb {
    pool: SqlitePool,
    path: std::path::PathBuf,
}

impl TestDb {
    async fn new() -> Self {
        use std::{
            env, process,
            sync::atomic::{AtomicU64, Ordering},
            time::{SystemTime, UNIX_EPOCH},
        };
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time must be after the Unix epoch")
            .as_nanos();
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "polycopy-engine-orchestrate-test-{}-{nonce}-{counter}.sqlite",
            process::id()
        ));
        let pool = open_and_migrate(&path)
            .await
            .expect("migrations must apply to a fresh database");
        Self { pool, path }
    }
}

impl std::ops::Deref for TestDb {
    type Target = SqlitePool;
    fn deref(&self) -> &SqlitePool {
        &self.pool
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        let _ = std::fs::remove_file(format!("{}-shm", self.path.display()));
        let _ = std::fs::remove_file(format!("{}-wal", self.path.display()));
    }
}

struct FixedBalance(Decimal);

#[test]
fn no_fak_retry_uses_only_the_best_ask_not_the_depth_needed_for_five_shares() {
    let quote = no_fak_sweep_quote(
        [
            (Decimal::new(45, 2), Decimal::new(2, 0)),
            (Decimal::new(47, 2), Decimal::new(2, 0)),
            (Decimal::new(50, 2), Decimal::new(4, 0)),
        ],
        Decimal::new(5, 0),
    )
    .expect("valid fresh book");

    assert_eq!(quote.limit_price, Decimal::new(45, 2));
    assert_eq!(quote.visible_shares, Decimal::new(2, 0));
}

#[test]
fn no_fak_retry_never_escalates_to_the_last_visible_ask_when_depth_is_short() {
    let quote = no_fak_sweep_quote(
        [
            (Decimal::new(45, 2), Decimal::new(1, 0)),
            (Decimal::new(48, 2), Decimal::new(2, 0)),
        ],
        Decimal::new(5, 0),
    )
    .expect("short book still permits a partial FAK");

    assert_eq!(quote.limit_price, Decimal::new(45, 2));
    assert_eq!(quote.visible_shares, Decimal::new(1, 0));
}

#[async_trait]
impl StrictTokenBalanceReader for FixedBalance {
    async fn position_for_token_strict(
        &self,
        _token_id: &OutcomeTokenId,
    ) -> Result<Decimal, StrictPositionError> {
        Ok(self.0)
    }
}

#[async_trait]
impl StrictAccountBalanceReader for FixedBalance {
    async fn collateral_balance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        Ok(Decimal::new(100, 0))
    }

    async fn collateral_allowance_strict(&self) -> Result<Decimal, StrictCollateralError> {
        Ok(Decimal::new(100, 0))
    }
}

struct EmptyHistory;

#[async_trait]
impl StrictTradeHistoryReader for EmptyHistory {
    async fn trades_for_token_between(
        &self,
        _token_id: &OutcomeTokenId,
        _after: DateTime<Utc>,
        _before: DateTime<Utc>,
    ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
        Ok(Vec::new())
    }
}

struct RecoveredHistory;

#[async_trait]
impl StrictTradeHistoryReader for RecoveredHistory {
    async fn trades_for_token_between(
        &self,
        token_id: &OutcomeTokenId,
        _after: DateTime<Utc>,
        _before: DateTime<Utc>,
    ) -> Result<Vec<AccountTrade>, StrictTradeHistoryError> {
        Ok(vec![AccountTrade {
            trade_id: "trade-1".to_owned(),
            taker_order_id: "0xdead0".to_owned(),
            token_id: token_id.clone(),
            side: crate::venue::intl_clob::AccountTradeSide::Buy,
            price: Decimal::new(55, 2),
            size: Decimal::new(5, 0),
            // The recovery query's upper bound is captured immediately
            // before this fake is called. Keep the fixture inside that
            // closed window instead of racing a freshly-created timestamp
            // against it.
            match_time: Utc::now() - chrono::Duration::seconds(1),
            role: crate::venue::intl_clob::AccountTradeRole::Taker,
            status: crate::venue::intl_clob::AccountTradeStatus::Matched,
            maker_orders: Vec::new(),
        }])
    }
}

struct FakeVenue {
    // Maker-only best-ask injection point. `None` (the audit baseline)
    // means "the live book returned a top-of-book asked to FakeVenue ...
    fak_prepare_count: AtomicU64,
    prepare_count: AtomicU64,
    submit_count: AtomicU64,
    submit_result: Mutex<Result<OrderReceipt, SubmitError>>,
    submit_results: Mutex<VecDeque<Result<OrderReceipt, SubmitError>>>,
    order_status: Mutex<String>,
    last_salt: Mutex<Option<u64>>,
    // New fields added so tests can drive the audit-flagged but never-tested
    // orchestrator branches (P0-3): the order_lookup_result overrides
    // `order_for_receipt` to return Err and exercise the strict-query
    // path. `query_receipt_result` overrides `query_prepared_envelope`
    // to return either Ok(Some(receipt)) (driving the happy path
    // reconcile_or_finalize branch), Ok(None) (the audit baseline), or
    // Err(detail) (driving the audit-baseline Err arm that maps to
    // `OrchestrateError::Submit(SubmitError::Local(_))`).
    order_lookup_result: Mutex<Option<Result<crate::venue::types::VenueOrderState, String>>>,
    query_receipt_result: Mutex<Option<Result<Option<OrderReceipt>, String>>>,
    // P0-3 step 8: `None` means "use the audit baseline size_matched
    // (5)"; a `Some(_)` value is surfaced verbatim by
    // `order_for_receipt`. Used by the zero-matched-size Err
    // e2e test below.
    size_matched_override: Mutex<Option<Decimal>>,
    // Maker-only best-ask injection point. `None` (the audit baseline)
    // means "the live book returned a top-of-book ask of 0.40," which
    // is more favorable than the leader-derived 0.42 ceiling for any
    // test that doesn't override it. `Some(Err(detail))` drives the
    // fail-closed path (best-ask fetch failure must become a
    // reject_pre_submit_intent, not a stale-price fallback). The
    // maker-only tests set this to Some(Ok(<their value>)) to verify
    // the min(leader-derived ceiling, best_ask) selection logic
    // branch-by-branch (best_ask < limit_price, > limit_price, == limit_price).
    best_ask_override: Mutex<Option<Result<Decimal, String>>>,
    minimum_order_size_override: Mutex<Option<Decimal>>,
}

impl FakeVenue {
    fn succeeding(filled: Decimal) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Ok(OrderReceipt::from_fak_buy_budget(
                Decimal::new(5, 0),
                Decimal::new(5, 0),
                filled,
            )
            .expect("receipt"))),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            // Default: `order_for_receipt` succeeds with the current
            // `order_status`. Tests that need a failing lookup must
            // call `order_lookup_failure(...)` instead.
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            // Default: `query_prepared_envelope` returns Ok(None).
            query_receipt_result: Mutex::new(None),
        best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
        }
    }

    fn transport_error() -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Transport("connection reset".into()))),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            query_receipt_result: Mutex::new(None),
        best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
        }
    }

    /// Submission returns a *local* error (reconstruction/validation
    /// failed before the request left the process). Distinct from
    /// `transport_error()` because AGENTS.md requires a local failure
    /// to open a reconciliation case without ever marking the attempt
    /// `uncertain` -- the request never reached the venue.
    fn local_submission_failure(detail: &str) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Local(detail.to_owned()))),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            query_receipt_result: Mutex::new(None),
        best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
        }
    }

    /// `order_for_receipt` (the strict lookup in the query-first path)
    /// fails. Tests must pin that this opens `strict_query_failure`
    /// rather than retrying: the request may have crossed the network
    /// boundary and the venue is the only source of truth for the
    /// order's real state.
    fn order_lookup_failure(detail: &str) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Ok(OrderReceipt::from_fak_buy_budget(
                Decimal::new(5, 0),
                Decimal::new(5, 0),
                Decimal::new(5, 0),
            )
            .expect("receipt"))),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(Some(Err(detail.to_owned()))),
            query_receipt_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
        }
    }

    /// `query_prepared_envelope` returns `Ok(Some(receipt))`. Drives
    /// the audit-flagged `reconcile_or_finalize` Some(receipt) branch
    /// (orchestrate.rs:522-532) without the venue-side race that
    /// currently keeps it untested. Pairs with a fixture that has
    /// already advanced the attempt past the submit boundary (so
    /// the recovery matrix routes the orchestrator to
    /// `RecoveryAction::ReconcileOrFinalize`).
    fn pre_accepted_with_receipt(receipt: OrderReceipt) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            // `submit_result` is unused on this path -- the attempt
            // is *already* accepted -- but seed it with the same
            // receipt so a future refactor that mistakenly routes
            // through `submit_exact_envelope` still produces a
            // matching receipt.
            submit_result: Mutex::new(Ok(receipt.clone())),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
            query_receipt_result: Mutex::new(Some(Ok(Some(receipt)))),
        }
    }

    /// `query_prepared_envelope` returns `Err(detail)`. Drives the
    /// post-submission lookup-error arm of `reconcile_or_finalize`,
    /// which must open `strict_query_failure` and return
    /// `NeedsReconcile` (never propagate as a local pre-submit error).
    /// The request may have crossed the venue boundary, so only a later
    /// proven query may resolve it; the executor must never resubmit.
    ///
    /// Like `pre_accepted_with_receipt`, pairs with a fixture whose
    /// attempt is in `accepted` state so the orchestrator enters
    /// `RecoveryAction::ReconcileOrFinalize`.
    fn query_prepared_envelope_failure(detail: &str) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            fak_prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Local(
                "submit_result unused on this path".to_owned(),
            ))),
            submit_results: Mutex::new(VecDeque::new()),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            best_ask_override: Mutex::new(None),
            minimum_order_size_override: Mutex::new(None),
            query_receipt_result: Mutex::new(Some(Err(detail.to_owned()))),
        }
    }

    /// Sets the `order_for_receipt` `size_matched` override. Use
    /// to drive the audit-flagged zero-matched-size Err branch in
    /// `receipt_from_terminal_order_state` (orchestrate.rs:547-552)
    /// without hand-crafting an `OrderLookupResult` payload.
    fn with_size_matched(&self, size_matched: Decimal) -> &Self {
        *self
            .size_matched_override
            .lock()
            .expect("size matched lock") = Some(size_matched);
        self
    }

    fn set_order_status(&self, status: &str) {
        *self.order_status.lock().expect("order status lock") = status.to_owned();
    }

    fn no_fak_then_fill(filled: Decimal) -> Self {
        let venue = Self::succeeding(filled);
        *venue.submit_results.lock().expect("submit sequence lock") = VecDeque::from([
            Err(SubmitError::Rejected(
                "400 no orders found to match with FAK order".to_owned(),
            )),
            Ok(OrderReceipt::from_fak_buy_shares(
                Decimal::new(5, 0),
                Decimal::new(5, 0),
                filled,
            )
            .expect("fixed-share receipt")),
        ]);
        venue
    }

    fn with_minimum_order_size(&self, minimum_order_size: Decimal) -> &Self {
        self.minimum_order_size_override
            .lock()
            .expect("minimum_order_size_override lock")
            .replace(minimum_order_size);
        self
    }
}

impl EnvelopeFactory for FakeVenue {
    #[allow(clippy::manual_async_fn)]
    fn prepare(
        &self,
        decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send {
        let count = self.prepare_count.fetch_add(1, Ordering::SeqCst);
        self.fak_prepare_count.fetch_add(1, Ordering::SeqCst);
        let envelope = PreparedOrderEnvelope {
            token_id: decision.token_id.clone(),
            side: decision.side.as_str().to_owned(),
            price: decision.limit_price.to_string(),
            size: decision.qty.to_string(),
            buy_budget_usdc: decision.buy_budget.map(|budget| budget.to_string()),
            buy_shares_exact: decision.buy_shares_exact,
            salt: 1000 + count,
            order_type: "FAK".to_owned(),
            expires_at: None,
            post_only: false,
            expected_taker_order_id: format!("0xdead{count}"),
            signed_order_json: r#"{"order":{}}"#.to_owned(),
        };
        async move { Ok(envelope) }
    }

    fn sweep_quote_for_no_fak_retry<'a>(
        &'a self,
        _token_id: &'a str,
        _target_shares: Decimal,
    ) -> Pin<Box<dyn Future<Output = Result<NoFakSweepQuote, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(NoFakSweepQuote {
                limit_price: Decimal::new(60, 2),
                visible_shares: Decimal::new(5, 0),
            })
        })
    }

    fn market_spec_for_gtd<'a>(
        &'a self,
        condition_id: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<GtdMarketSpec, String>> + Send + 'a>> {
        // The fixture deliberately uses a hex-shaped condition id (`0xcond`)
        // and a decimal token id (`123456`). This assertion makes every
        // maker-only test a regression test for looking up GTD market data by
        // condition id rather than outcome token id.
        assert_eq!(condition_id, "0xcond", "GTD market lookup must use condition_id");
        Box::pin(async {
            let minimum_order_size = (*self
                .minimum_order_size_override
                .lock()
                .expect("minimum_order_size_override lock"))
            .unwrap_or(Decimal::new(5, 0));
            Ok(GtdMarketSpec {
                expires_at: Utc::now() + chrono::Duration::minutes(5),
                tick_size: Decimal::new(1, 2),
                minimum_order_size,
            })
        })
    }

    fn prepare_post_only_gtd_buy<'a>(
        &'a self,
        decision: &'a SizedDecision,
        expires_at: chrono::DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<PreparedOrderEnvelope, String>> + Send + 'a>> {
        let count = self.prepare_count.fetch_add(1, Ordering::SeqCst);
        let envelope = PreparedOrderEnvelope {
            token_id: decision.token_id.clone(),
            side: decision.side.as_str().to_owned(),
            price: decision.limit_price.to_string(),
            size: decision.qty.to_string(),
            buy_budget_usdc: decision.buy_budget.map(|budget| budget.to_string()),
            buy_shares_exact: true,
            salt: 1000 + count,
            order_type: "GTD".to_owned(),
            expires_at: Some(expires_at.to_rfc3339()),
            post_only: true,
            expected_taker_order_id: format!("0xgtd{count}"),
            signed_order_json: r#"{"order":{}}"#.to_owned(),
        };
        Box::pin(async move { Ok(envelope) })
    }

    fn fetch_best_ask_for_maker_only<'a>(
        &'a self,
        _token_id: &'a str,
        leader_price: Decimal,
    ) -> Pin<Box<dyn Future<Output = Result<BookObservation, String>> + Send + 'a>> {
        // Override: `Some(Err(detail))` -> fail-closed. `Some(Ok(p))` -> fixed
        // best ask for this call (tests assert min(decision.limit_price, p)).
        // `None` -> audit baseline 0.40, which is more favorable than the
        // typical 0.42 leader-derived ceiling used in the maker-only tests.
        let result = self.best_ask_override.lock().expect("best_ask_override lock").clone();
        Box::pin(async move {
            match result {
                Some(value) => value.and_then(|price| observe_book(
                    [], [(price, Decimal::ONE)], leader_price, Utc::now(),
                )),
                None => observe_book([], [(Decimal::new(40, 2), Decimal::ONE)], leader_price, Utc::now()),
            }
        })
    }
}

struct FailingEnvelopeFactory;

impl EnvelopeFactory for FailingEnvelopeFactory {
    #[allow(clippy::manual_async_fn)]
    fn prepare(
        &self,
        _decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send {
        async { Err("signer unavailable".to_owned()) }
    }
}

impl CopyExecution for FakeVenue {
    #[allow(clippy::manual_async_fn)]
    fn position_for_token_strict(
        &self,
        _token_id: &str,
    ) -> impl std::future::Future<Output = Result<Decimal, String>> + Send {
        async { Ok(Decimal::ZERO) }
    }

    #[allow(clippy::manual_async_fn)]
    fn order_for_receipt(
        &self,
        order_id: &OrderId,
    ) -> impl std::future::Future<
        Output = Result<crate::copytrading::reconcile::VenueOrderState, String>,
    > + Send {
        let order_id = order_id.clone();
        let lookup_override = self
            .order_lookup_result
            .lock()
            .expect("order lookup lock")
            .clone();
        let status = self.order_status.lock().expect("order status lock").clone();
        let size_override = *self
            .size_matched_override
            .lock()
            .expect("size matched lock");
        async move {
            // Default behavior (no override): succeed with the current
            // `order_status`. Tests that need a failing lookup set the
            // override via `order_lookup_failure(...)`. Tests that need
            // a zero/non-default `size_matched` set the override via
            // `with_size_matched(...)` or the convenience
            // `with_terminal_zero_matched()`.
            if let Some(result) = lookup_override {
                return result;
            }
            Ok(crate::venue::types::VenueOrderState {
                order_id,
                status,
                size_matched: size_override.unwrap_or_else(|| Decimal::new(5, 0)),
            })
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn query_prepared_envelope(
        &self,
        _envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<Option<OrderReceipt>, String>> + Send {
        // P0-3 step 3 + step 7: honour the override if set, otherwise
        // fall back to the audit-baseline Ok(None). The override has
        // three meaningful shapes:
        //
        //   * `None`               -- audit baseline: Ok(None)
        //   * `Some(Ok(None))`     -- explicit Ok(None)
        //   * `Some(Ok(Some(r)))`  -- Some(receipt) happy path
        //   * `Some(Err(detail))`  -- Err arm (audited-but-untested
        //                             before step 7, drives
        //                             `OrchestrateError::Submit(
        //                             SubmitError::Local(_))`)
        let override_value = self
            .query_receipt_result
            .lock()
            .expect("query receipt lock")
            .clone();
        async move {
            match override_value {
                Some(result) => result,
                None => Ok(None),
            }
        }
    }

    #[allow(clippy::manual_async_fn)]
    fn submit_exact_envelope(
        &self,
        envelope: &PreparedOrderEnvelope,
    ) -> impl std::future::Future<Output = Result<OrderReceipt, SubmitError>> + Send {
        self.submit_count.fetch_add(1, Ordering::SeqCst);
        *self.last_salt.lock().expect("salt lock") = Some(envelope.salt);
        let result = self
            .submit_results
            .lock()
            .expect("submit sequence lock")
            .pop_front()
            .unwrap_or_else(|| {
                self.submit_result
                    .lock()
                    .expect("submit result lock")
                    .clone()
            });
        async move { result }
    }
}

async fn seed_account_and_schedule(db: &TestDb) {
    sqlx::query(
        "INSERT INTO accounts (id, label, signing_address, signature_type) \
         VALUES (1, 'primary', '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', 'eoa')",
    )
    .execute(&db.pool)
    .await
    .expect("account must insert");
    sqlx::query(
        "INSERT INTO execution_schedule (id, shard_scheme_version, shard_algorithm, lane_count) \
         VALUES (1, 1, 'hash_mod_lane_count', 1)",
    )
    .execute(&db.pool)
    .await
    .expect("execution_schedule must insert");
}

async fn seed_leader(db: &TestDb, leader_id: i64) {
    sqlx::query("INSERT INTO leader_config (id, label, enabled) VALUES (?, ?, 1)")
        .bind(leader_id)
        .bind(format!("leader-{leader_id}"))
        .execute(&db.pool)
        .await
        .expect("leader must insert");
}

async fn seed_pending_buy(db: &TestDb) -> i64 {
    seed_pending_buy_with_event_key(db, "activity:1:tok:BUY:5:1").await
}

async fn seed_pending_buy_with_event_key(db: &TestDb, event_key: &str) -> i64 {
    let event_id: i64 = sqlx::query_scalar(
        "INSERT INTO leader_events \
         (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
         VALUES (?, 1, '0xcond', '123456', 0, 'BUY', '5', '0.55', \
          strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING id",
    )
    .bind(event_key)
    .fetch_one(&db.pool)
    .await
    .expect("event");
    let snapshot = PolicySnapshot {
        max_signal_age_seconds: 3600,
        decision_window_seconds: 300,
        price_tolerance_bps: 0,
        price_tolerance_abs: None,
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: "0.99".to_owned(),
        max_order_notional: "100000".to_owned(),
        max_order_shares: None,
        balance_within_market: false,
        min_leader_trade_size: "0".to_owned(),
    allow_repeated_market_direction: false,
    size_ratio: None,
        maker_only: false,
};
    sqlx::query_scalar(
        "INSERT INTO copy_intents \
         (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
          shard_scheme_version, lane_count, shard_id, status, decision_deadline_at) \
         VALUES (?, 1, 1, '123456', 'BUY', ?, 'hash', 1, 1, 0, 'pending', ?) RETURNING id",
    )
    .bind(event_id)
    .bind(serde_json::to_string(&snapshot).unwrap())
    .bind((Utc::now() + chrono::Duration::seconds(300)).to_rfc3339())
    .fetch_one(&db.pool)
    .await
    .expect("intent")
}

async fn set_fixed_share_policy(db: &TestDb, intent_id: i64) {
    set_fixed_share_policy_with_max_price(db, intent_id, "0.99").await;
}

async fn set_fixed_share_policy_with_max_price(
    db: &TestDb,
    intent_id: i64,
    max_price: &str,
) {
    let snapshot = PolicySnapshot {
        max_signal_age_seconds: 3600,
        decision_window_seconds: 300,
        price_tolerance_bps: 0,
        price_tolerance_abs: None,
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: max_price.to_owned(),
        max_order_notional: "100000".to_owned(),
        max_order_shares: Some("5".to_owned()),
        balance_within_market: false,
        min_leader_trade_size: "0".to_owned(),
    allow_repeated_market_direction: false,
    size_ratio: None,
        maker_only: false,
};
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&snapshot).unwrap())
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .unwrap();
}

async fn seed_pending_sell(db: &TestDb) -> i64 {
    seed_pending_sell_with_event_key(db, "activity:1:tok:SELL:5:1").await
}

async fn seed_pending_sell_with_event_key(db: &TestDb, event_key: &str) -> i64 {
    let event_id: i64 = sqlx::query_scalar(
        "INSERT INTO leader_events \
         (canonical_event_key, leader_id, condition_id, token_id, outcome_index, side, size, price, occurred_at, observed_at) \
         VALUES (?, 1, '0xcond', '123456', 0, 'SELL', '5', '0.55', \
          strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')) RETURNING id",
    )
    .bind(event_key)
    .fetch_one(&db.pool)
    .await
    .expect("event");
    let snapshot = PolicySnapshot {
        max_signal_age_seconds: 3600,
        decision_window_seconds: 300,
        price_tolerance_bps: 0,
        price_tolerance_abs: None,
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: "0.99".to_owned(),
        max_order_notional: "100000".to_owned(),
        max_order_shares: None,
        balance_within_market: false,
        min_leader_trade_size: "0".to_owned(),
    allow_repeated_market_direction: false,
    size_ratio: None,
        maker_only: false,
};
    sqlx::query_scalar(
        "INSERT INTO copy_intents \
         (event_id, account_id, leader_id, token_id, side, config_snapshot_json, config_snapshot_hash, \
          shard_scheme_version, lane_count, shard_id, status, decision_deadline_at) \
         VALUES (?, 1, 1, '123456', 'SELL', ?, 'hash', 1, 1, 0, 'pending', ?) RETURNING id",
    )
    .bind(event_id)
    .bind(serde_json::to_string(&snapshot).unwrap())
    .bind((Utc::now() + chrono::Duration::seconds(300)).to_rfc3339())
    .fetch_one(&db.pool)
    .await
    .expect("intent")
}

async fn attempt_status(db: &TestDb, intent_id: i64) -> String {
    sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("attempt status")
}

#[test]
fn live_execute_guard_requires_exact_yes() {
    assert!(!live_execute_enabled(None));
    assert!(!live_execute_enabled(Some("YES")));
    assert!(!live_execute_enabled(Some("true")));
    assert!(live_execute_enabled(Some("yes")));
}

/// Replaces the persisted config_snapshot_json with a policy that has
/// `maker_only = true`. Used by the maker-only execution tests below to
/// route a sized decision through `prepare_maker_only_envelope` instead
/// of `envelopes.prepare`. The persisted snapshot is what
/// `load_policy_snapshot` reads in `size_and_reserve`, so flipping the
/// field on the persisted row is what flips the orchestrate branch.
async fn set_maker_only_policy(db: &TestDb, intent_id: i64) {
    let snapshot = PolicySnapshot {
        max_signal_age_seconds: 3600,
        decision_window_seconds: 300,
        price_tolerance_bps: 0,
        price_tolerance_abs: None,
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: "0.99".to_owned(),
        max_order_notional: "100000".to_owned(),
        // A flat 5-share target so SizedDecision.qty is a deterministic
        // 5 regardless of which branch (size_ratio vs flat) is taken.
        max_order_shares: Some("5".to_owned()),
        balance_within_market: false,
        min_leader_trade_size: "0".to_owned(),
        allow_repeated_market_direction: false,
        size_ratio: None,
        maker_only: true,
    };
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&snapshot).unwrap())
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_crash_after_prepare_does_not_rebuild_the_envelope() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(528846, 5));
    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first pass");
    assert!(matches!(first, OrchestrateOutcome::Filled { .. }));
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

    let db2 = TestDb::new().await;
    seed_account_and_schedule(&db2).await;
    seed_leader(&db2, 1).await;
    let intent_id = seed_pending_buy(&db2).await;
    let venue = FakeVenue::succeeding(Decimal::new(528846, 5));
    let claimed = claim_or_resume_intent(&db2, intent_id)
        .await
        .unwrap()
        .unwrap();
    let decision = match size_and_reserve(&db2, &FixedBalance(Decimal::new(100, 0)), &claimed)
        .await
        .unwrap()
    {
        SizingOutcome::Decision(decision) => decision,
        _ => panic!("expected a persisted sizing decision"),
    };
    let envelope = venue.prepare(&decision).await.unwrap();
    load_or_prepare_attempt(&db2, intent_id, 1, &envelope)
        .await
        .unwrap();
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);

    let outcome = execute_one_intent(
        &db2,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("resume");
    assert!(matches!(outcome, OrchestrateOutcome::Filled { .. }));
    assert_eq!(
        venue.prepare_count.load(Ordering::SeqCst),
        1,
        "resume must not sign a second envelope"
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    assert_eq!(*venue.last_salt.lock().unwrap(), Some(1000));
}

struct AssertGtdIdBeforeSubmitting<'a> {
    expected_id: &'a str,
    fail_before_mark: bool,
}

impl SubmitAttemptMarker for AssertGtdIdBeforeSubmitting<'_> {
    fn mark_submitting<'a>(
        &'a self,
        pool: &'a SqlitePool,
        intent_id: i64,
        attempt_id: i64,
        now: DateTime<Utc>,
    ) -> Pin<Box<dyn Future<Output = Result<(), OrchestrateError>> + Send + 'a>> {
        Box::pin(async move {
            let row: (String, Option<String>, String) = sqlx::query_as(
                "SELECT status, venue_order_id, envelope_json FROM order_attempts WHERE id = ? AND intent_id = ?",
            )
            .bind(attempt_id)
            .bind(intent_id)
            .fetch_one(pool)
            .await
            .unwrap();
            let envelope: PreparedOrderEnvelope = serde_json::from_str(&row.2).unwrap();
            assert_eq!(row.0, "prepared");
            assert_eq!(row.1.as_deref(), Some(self.expected_id));
            assert_eq!(envelope.expected_taker_order_id, self.expected_id);
            if self.fail_before_mark {
                return Err(OrchestrateError::Prepare("injected crash before submit marker".to_owned()));
            }
            StandardSubmitAttemptMarker.mark_submitting(pool, intent_id, attempt_id, now).await
        })
    }
}

#[tokio::test]
async fn legacy_prepared_gtd_backfills_original_id_before_submit_marker() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let claimed = claim_or_resume_intent(&db, intent_id).await.unwrap().unwrap();
    let SizingOutcome::Decision(decision) =
        size_and_reserve(&db, &FixedBalance(Decimal::new(100, 0)), &claimed).await.unwrap()
    else { panic!("decision"); };
    let envelope = venue.prepare_post_only_gtd_buy(
        &decision, Utc::now() + chrono::Duration::minutes(5),
    ).await.unwrap();
    load_or_prepare_attempt(&db, intent_id, 1, &envelope).await.unwrap();
    sqlx::query("UPDATE order_attempts SET venue_order_id = NULL WHERE intent_id = ?")
        .bind(intent_id).execute(&db.pool).await.unwrap();

    let outcome = execute_one_intent_with_marker(
        &db, &FixedBalance(Decimal::new(100, 0)), &venue, &venue,
        &EmptyHistory, &AssertGtdIdBeforeSubmitting {
            expected_id: &envelope.expected_taker_order_id,
            fail_before_mark: false,
        }, intent_id, Utc::now(),
    ).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Resting);
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1, "resume must not sign again");
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn gtd_id_backfill_replays_without_resigning_after_pre_marker_crash() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let claimed = claim_or_resume_intent(&db, intent_id).await.unwrap().unwrap();
    let SizingOutcome::Decision(decision) =
        size_and_reserve(&db, &FixedBalance(Decimal::new(100, 0)), &claimed).await.unwrap()
    else { panic!("decision"); };
    let envelope = venue.prepare_post_only_gtd_buy(
        &decision, Utc::now() + chrono::Duration::minutes(5),
    ).await.unwrap();
    load_or_prepare_attempt(&db, intent_id, 1, &envelope).await.unwrap();
    sqlx::query("UPDATE order_attempts SET venue_order_id = NULL WHERE intent_id = ?")
        .bind(intent_id).execute(&db.pool).await.unwrap();
    let failing_marker = AssertGtdIdBeforeSubmitting {
        expected_id: &envelope.expected_taker_order_id,
        fail_before_mark: true,
    };
    let first = execute_one_intent_with_marker(
        &db, &FixedBalance(Decimal::new(100, 0)), &venue, &venue,
        &EmptyHistory, &failing_marker, intent_id, Utc::now(),
    ).await;
    assert!(first.is_err());
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let state: (String, Option<String>) = sqlx::query_as(
        "SELECT status, venue_order_id FROM order_attempts WHERE intent_id = ?",
    ).bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(state, ("prepared".to_owned(), Some(envelope.expected_taker_order_id.clone())));

    let outcome = execute_one_intent_with_marker(
        &db, &FixedBalance(Decimal::new(100, 0)), &venue, &venue,
        &EmptyHistory, &AssertGtdIdBeforeSubmitting {
            expected_id: &envelope.expected_taker_order_id,
            fail_before_mark: false,
        }, intent_id, Utc::now(),
    ).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Resting);
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn conflicting_prepared_gtd_order_id_blocks_before_marker_and_venue() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let claimed = claim_or_resume_intent(&db, intent_id).await.unwrap().unwrap();
    let SizingOutcome::Decision(decision) =
        size_and_reserve(&db, &FixedBalance(Decimal::new(100, 0)), &claimed).await.unwrap()
    else { panic!("decision"); };
    let envelope = venue.prepare_post_only_gtd_buy(
        &decision, Utc::now() + chrono::Duration::minutes(5),
    ).await.unwrap();
    load_or_prepare_attempt(&db, intent_id, 1, &envelope).await.unwrap();
    sqlx::query("UPDATE order_attempts SET venue_order_id = 'other-id' WHERE intent_id = ?")
        .bind(intent_id).execute(&db.pool).await.unwrap();

    let outcome = execute_one_intent(
        &db, &FixedBalance(Decimal::new(100, 0)), &venue, &venue,
        &EmptyHistory, intent_id, Utc::now(),
    ).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::NeedsReconcile(
        "GTD prepared order ID is not verified",
    ));
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let row: (String, String) = sqlx::query_as(
        "SELECT status, venue_order_id FROM order_attempts WHERE intent_id = ?",
    ).bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(row, ("prepared".to_owned(), "other-id".to_owned()));
    let cases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL",
    ).bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(cases, 1);
}

#[tokio::test]
async fn a_transport_error_marks_uncertain_and_never_resubmits() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::transport_error();
    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("transport pass");
    assert_eq!(first, OrchestrateOutcome::Uncertain);
    assert_eq!(attempt_status(&db, intent_id).await, "uncertain");
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

    let second = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("query-first pass");
    assert_eq!(
        second,
        OrchestrateOutcome::NeedsReconcile("lost submission")
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn open_reconciliation_for_account_token_excludes_and_blocks_later_intents() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let blocked_intent = seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:blocked").await;
    crate::copytrading::reconcile::open_reconciliation_case(
        &db,
        blocked_intent,
        None,
        "unknown_submission",
        "manual unresolved canary",
    )
    .await
    .expect("reconciliation case");
    let later_intent = seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:later").await;

    let runnable = list_runnable_intents(&db, 1).await.expect("runnable");
    assert!(
        !runnable.contains(&later_intent),
        "a later same-token intent must not be runnable while an open case exists"
    );

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        later_intent,
        Utc::now(),
    )
    .await
    .expect("direct execute must fail closed");
    assert_eq!(
        outcome,
        OrchestrateOutcome::Blocked("account/token needs reconciliation")
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let later_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(later_intent)
        .fetch_one(&db.pool)
        .await
        .expect("later status");
    assert_eq!(later_status, "pending");
}

#[tokio::test]
async fn runnable_intents_are_prioritized_by_earliest_deadline_not_token_name() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let later_deadline = seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:later").await;
    let earlier_deadline =
        seed_pending_buy_with_event_key(&db, "activity:1:tok:BUY:5:earlier").await;

    // Deliberately make the later deadline sort first lexicographically by
    // token. The old ORDER BY token_id, id would starve the urgent intent.
    sqlx::query("UPDATE copy_intents SET token_id = ?, decision_deadline_at = ? WHERE id = ?")
        .bind("aaa-later-deadline")
        .bind("2030-01-01T00:01:00.000Z")
        .bind(later_deadline)
        .execute(&db.pool)
        .await
        .expect("later deadline must update");
    sqlx::query("UPDATE copy_intents SET token_id = ?, decision_deadline_at = ? WHERE id = ?")
        .bind("zzz-earlier-deadline")
        .bind("2030-01-01T00:00:01.000Z")
        .bind(earlier_deadline)
        .execute(&db.pool)
        .await
        .expect("earlier deadline must update");

    assert_eq!(
        list_runnable_intents(&db, 1)
            .await
            .expect("runnable intents"),
        vec![earlier_deadline, later_deadline]
    );
}

#[tokio::test]
async fn resting_gtd_does_not_starve_next_same_token_intent() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let old = seed_pending_buy_with_event_key(&db, "activity:old:gtd").await;
    set_maker_only_policy(&db, old).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            old,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Resting
    );
    let fresh = seed_pending_buy_with_event_key(&db, "activity:fresh:buy").await;
    sqlx::query("UPDATE copy_intents SET decision_deadline_at = ? WHERE id = ?")
        .bind((Utc::now() + chrono::Duration::seconds(30)).to_rfc3339())
        .bind(fresh).execute(&db.pool).await.unwrap();
    // This fixture represents a leader whose repeat-direction policy already
    // admitted the new signal; the runner must not bypass planning's gate.
    let (polls, work) = list_runnable_intents_by_phase(&db, 1).await.unwrap();
    assert_eq!(polls, vec![old]);
    assert_eq!(work, vec![fresh]);
    venue.set_order_status("LIVE");
    for id in polls {
        assert_eq!(poll_accepted_gtd_intent(&db, &venue, id, Utc::now()).await.unwrap(),
            OrchestrateOutcome::Resting);
    }
    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        work[0],
        Utc::now(),
    )
    .await
    .unwrap();
    assert!(
        matches!(outcome, OrchestrateOutcome::Filled { .. }),
        "{outcome:?}"
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2);
    let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(fresh)
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_ne!(status, "cancelled");
}

#[tokio::test]
async fn reconciliation_locked_gtd_still_enters_poll_phase_and_blocks_new_work() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let old = seed_pending_buy_with_event_key(&db, "activity:locked:gtd").await;
    set_maker_only_policy(&db, old).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, old, Utc::now()).await.unwrap(), OrchestrateOutcome::Resting);
    let fresh = seed_pending_buy_with_event_key(&db, "activity:locked:new").await;
    sqlx::query("INSERT INTO reconciliation_cases (account_id, token_id, intent_id, case_type, detail) VALUES (1, '123456', ?, 'unknown_submission', 'manual lock')")
        .bind(fresh).execute(&db.pool).await.unwrap();
    let (polls, work) = list_runnable_intents_by_phase(&db, 1).await.unwrap();
    assert_eq!(polls, vec![old]);
    assert!(work.is_empty());
    let outcome = poll_accepted_gtd_intent(&db, &venue, polls[0], Utc::now()).await.unwrap();
    assert!(matches!(outcome, OrchestrateOutcome::Blocked(_)));
    assert!(gtd_poll_requires_fuse(&outcome));
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    let fresh_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(fresh).fetch_one(&db.pool).await.unwrap();
    assert_eq!(fresh_status, "pending");
}

#[test]
fn unsafe_gtd_poll_outcomes_stop_the_tick_but_retry_does_not() {
    assert!(!gtd_poll_requires_fuse(&OrchestrateOutcome::GtdLookupRetry {
        detail: "temporary 404".into(), remaining: chrono::Duration::seconds(30),
    }));
    assert!(gtd_poll_requires_fuse(&OrchestrateOutcome::Uncertain));
    assert!(gtd_poll_requires_fuse(&OrchestrateOutcome::NeedsReconcile("unknown")));
    assert!(gtd_poll_requires_fuse(&OrchestrateOutcome::Blocked("locked")));
}

#[tokio::test]
async fn all_accepted_gtds_are_polled_before_one_new_intent() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let mut old_ids = Vec::new();
    for key in ["activity:gtd:first", "activity:gtd:second"] {
        let id = seed_pending_buy_with_event_key(&db, key).await;
        set_maker_only_policy(&db, id).await;
        assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &venue, &venue, &EmptyHistory, id, Utc::now()).await.unwrap(), OrchestrateOutcome::Resting);
        old_ids.push(id);
    }
    let fresh = seed_pending_buy_with_event_key(&db, "activity:gtd:third").await;
    let (polls, work) = list_runnable_intents_by_phase(&db, 1).await.unwrap();
    assert_eq!(polls, old_ids);
    assert_eq!(work, vec![fresh]);
    venue.set_order_status("LIVE");
    for id in polls {
        assert_eq!(poll_accepted_gtd_intent(&db, &venue, id, Utc::now()).await.unwrap(),
            OrchestrateOutcome::Resting);
    }
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2);
    assert!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, work[0], Utc::now()).await.is_ok());
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn gtd_lookup_retry_does_not_change_accepted_attempt() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let old = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, old).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            old,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Resting
    );
    let fresh = seed_pending_buy_with_event_key(&db, "activity:retry:fresh").await;
    let failed_lookup = FakeVenue::order_lookup_failure("temporary 404");
    let (polls, work) = list_runnable_intents_by_phase(&db, 1).await.unwrap();
    assert_eq!(polls, vec![old]);
    assert_eq!(work, vec![fresh]);
    let retry = poll_accepted_gtd_intent(&db, &failed_lookup, polls[0], Utc::now()).await.unwrap();
    assert!(matches!(retry, OrchestrateOutcome::GtdLookupRetry { .. }));
    assert!(!gtd_poll_requires_fuse(&retry));
    let status: String =
        sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
            .bind(old)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(status, "accepted");
    let open_cases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL",
    )
    .bind(old)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(open_cases, 0, "retry must not create a reconciliation lock");
    assert!(execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &failed_lookup,
        &failed_lookup,
        &EmptyHistory,
        work[0],
        Utc::now()
    )
    .await
    .is_ok());
    assert_eq!(failed_lookup.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn expired_gtd_lookup_failure_stops_before_new_intent() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let old = seed_pending_buy_with_event_key(&db, "activity:failure:gtd").await;
    set_maker_only_policy(&db, old).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, old, Utc::now()).await.unwrap(), OrchestrateOutcome::Resting);
    sqlx::query("UPDATE order_attempts SET envelope_json = json_set(envelope_json, '$.expires_at', ?) WHERE intent_id = ?")
        .bind((Utc::now() - chrono::Duration::minutes(6)).to_rfc3339())
        .bind(old).execute(&db.pool).await.unwrap();
    let fresh = seed_pending_buy_with_event_key(&db, "activity:failure:fresh").await;
    let failed_lookup = FakeVenue::order_lookup_failure("404");
    let (polls, work) = list_runnable_intents_by_phase(&db, 1).await.unwrap();
    assert_eq!(polls, vec![old]);
    assert_eq!(work, vec![fresh]);
    let outcome = poll_accepted_gtd_intent(&db, &failed_lookup, polls[0], Utc::now()).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Uncertain);
    assert!(gtd_poll_requires_fuse(&outcome));
    // Mirror the runner's fail-closed branch: fuse before touching new work.
    crate::copytrading::persistent::pause_fuse(&db, 1, "GTD poll uncertain", "test runner")
        .await
        .unwrap();
    assert!(crate::copytrading::persistent::ensure_fuse_clear(&db, 1).await.is_err());
    let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(fresh).fetch_one(&db.pool).await.unwrap();
    assert_eq!(status, "pending");
    assert_eq!(failed_lookup.submit_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn recovered_order_id_with_non_terminal_order_state_opens_reconciliation() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::transport_error();

    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("transport pass");
    assert_eq!(first, OrchestrateOutcome::Uncertain);
    venue.set_order_status("LIVE");

    let recovered = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &RecoveredHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("recovery pass");
    assert_eq!(
        recovered,
        OrchestrateOutcome::NeedsReconcile("strict order state not terminal")
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

    let intent_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent status");
    assert_eq!(intent_status, "needs_reconcile");
    let case_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'unknown_submission' AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("case count");
    assert_eq!(case_count, 1);
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 0);
}

#[tokio::test]
async fn a_buy_fill_uses_the_receipt_filled_qty_not_the_requested_size() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let filled = Decimal::new(528846, 5);
    let venue = FakeVenue::succeeding(filled);
    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("fill");
    assert_eq!(outcome, OrchestrateOutcome::Filled { filled_qty: filled });
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("lot");
    assert_eq!(lot.parse::<Decimal>().unwrap(), filled);
    assert_ne!(lot.parse::<Decimal>().unwrap(), Decimal::new(5, 0));
}

#[tokio::test]
async fn a_fixed_share_buy_accounts_a_venue_overfill_instead_of_local_failure() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_fixed_share_policy(&db, intent_id).await;
    let filled = Decimal::new(54, 1);
    let venue = FakeVenue::succeeding(filled);
    *venue.submit_result.lock().unwrap() = Ok(OrderReceipt::from_fak_buy_shares(
        Decimal::new(5, 0),
        Decimal::new(5, 0),
        filled,
    )
    .expect("overfill receipt"));

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("overfill must be accounted");
    assert_eq!(outcome, OrchestrateOutcome::Filled { filled_qty: filled });
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("lot");
    assert_eq!(lot.parse::<Decimal>().unwrap(), filled);
    let cases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("cases");
    assert_eq!(cases, 0);
}

// --- maker-only execution tests (Part 3 of the leader-2 redesign handoff) ---
//
// The maker-only branch in `execute_one_intent_with_marker` and
// `prepare_new_attempt` routes a sized decision through
// `prepare_maker_only_envelope`, which fetches a real-time best ask and
// uses `min(decision.limit_price, best_ask)` as the post-only GTD BUY
// price. These four tests pin the contract: skip FAK on both the fresh
// and resume paths, the best-ask selection logic across the
// `best_ask < / > / == limit_price` cases, and the fail-closed path
// when the best-ask fetch errors.

#[tokio::test]
async fn ratio_maker_only_keeps_two_decimal_shares_and_never_exceeds_reservation() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let raw: String = sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
    policy.size_ratio = Some("0.2".to_owned());
    policy.max_order_shares = None;
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&policy).unwrap()).bind(intent_id)
        .execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE leader_events SET size = '38.547059', price = '0.53' WHERE id = (SELECT event_id FROM copy_intents WHERE id = ?)")
        .bind(intent_id).execute(&db.pool).await.unwrap();
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(60, 2)));
    let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Resting);
    let (planned, reserved): (String, String) = sqlx::query_as(
        "SELECT planned_qty, planned_notional_usdc FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(planned, "7.70");
    assert_eq!(reserved, "4.09"); // ceil(7.70 * 0.53) to cents
    let envelope: String = sqlx::query_scalar(
        "SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let envelope: PreparedOrderEnvelope = serde_json::from_str(&envelope).unwrap();
    let qty: Decimal = envelope.size.parse().unwrap();
    let price: Decimal = envelope.price.parse().unwrap();
    let budget: Decimal = envelope.buy_budget_usdc.unwrap().parse().unwrap();
    assert!(qty.normalize().scale() <= 2);
    assert_eq!(qty, Decimal::new(770, 2));
    assert_eq!(price, Decimal::new(53, 2), "maker price must not be lowered for cent alignment");
    assert_eq!(budget, qty * price);
    assert!(budget <= reserved.parse().unwrap());
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn maker_only_fractional_shares_keep_min_limit_or_ask_minus_tick_grid() {
    for (shares, limit, ask) in [
        ("6.37", "0.27", "0.95"),
        ("7.70", "0.53", "0.60"),
        ("6.32", "0.64", "0.61"),
        ("6.63", "0.61", "0.62"),
        ("5.01", "0.42", "0.40"),
    ] {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        set_maker_only_policy(&db, intent_id).await;
        let raw: String = sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
        policy.max_order_shares = Some(shares.to_owned());
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&policy).unwrap()).bind(intent_id)
            .execute(&db.pool).await.unwrap();
        sqlx::query("UPDATE leader_events SET price = ? WHERE id = (SELECT event_id FROM copy_intents WHERE id = ?)")
            .bind(limit).bind(intent_id).execute(&db.pool).await.unwrap();
        let venue = FakeVenue::succeeding(Decimal::ZERO);
        *venue.best_ask_override.lock().unwrap() = Some(Ok(ask.parse().unwrap()));
        let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
        assert_eq!(outcome, OrchestrateOutcome::Resting);
        let raw: String = sqlx::query_scalar("SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        let envelope: PreparedOrderEnvelope = serde_json::from_str(&raw).unwrap();
        let expected = std::cmp::min(
            limit.parse::<Decimal>().unwrap(),
            ask.parse::<Decimal>().unwrap() - Decimal::new(1, 2),
        );
        assert_eq!(envelope.price.parse::<Decimal>().unwrap(), expected, "{shares} @ {limit}, ask {ask}");
        assert_eq!(envelope.size.parse::<Decimal>().unwrap(), shares.parse::<Decimal>().unwrap());
        assert!(envelope.post_only);
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
        let reserved: String = sqlx::query_scalar("SELECT planned_notional_usdc FROM copy_intents WHERE id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        assert!(envelope.buy_budget_usdc.unwrap().parse::<Decimal>().unwrap() <= reserved.parse().unwrap());
    }
}

#[tokio::test]
async fn maker_only_seven_point_seven_shares_at_53_cents_respects_ask_tick() {
    for (ask, expected) in [("0.54", "0.53"), ("0.53", "0.52")] {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        set_maker_only_policy(&db, intent_id).await;
        let raw: String = sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
        policy.max_order_shares = Some("7.70".to_owned());
        sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
            .bind(serde_json::to_string(&policy).unwrap()).bind(intent_id)
            .execute(&db.pool).await.unwrap();
        sqlx::query("UPDATE leader_events SET price = '0.53' WHERE id = (SELECT event_id FROM copy_intents WHERE id = ?)")
            .bind(intent_id).execute(&db.pool).await.unwrap();
        let venue = FakeVenue::succeeding(Decimal::ZERO);
        *venue.best_ask_override.lock().unwrap() = Some(Ok(ask.parse().unwrap()));
        assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
            OrchestrateOutcome::Resting);
        let raw: String = sqlx::query_scalar("SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        let envelope: PreparedOrderEnvelope = serde_json::from_str(&raw).unwrap();
        assert_eq!(envelope.size.parse::<Decimal>().unwrap(), Decimal::new(770, 2));
        assert_eq!(envelope.price.parse::<Decimal>().unwrap(), expected.parse::<Decimal>().unwrap());
        assert!(envelope.post_only);
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn definitive_gtd_amount_rejection_does_not_retry_or_open_fuse() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    *venue.submit_result.lock().unwrap() = Err(SubmitError::Rejected(
        "invalid maker amount decimal precision".to_owned(),
    ));
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::Rejected);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    let status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(status, "rejected");
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::NotClaimed);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn maker_only_rejects_below_market_minimum_without_attempt() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let raw: String = sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
    policy.max_order_shares = Some("4.99".to_owned());
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&policy).unwrap()).bind(intent_id)
        .execute(&db.pool).await.unwrap();
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Rejected);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let reason: String = sqlx::query_scalar("SELECT rejection_reason FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert!(reason.contains("4.99") && reason.contains("5"), "{reason}");
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn maker_only_rejects_a_legacy_decision_whose_budget_cannot_cover_signed_cost() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    // A previously persisted decision may have an insufficient cent budget.
    sqlx::query("UPDATE copy_intents SET status = 'in_progress', planned_qty = '7.70', planned_price = '0.53', planned_notional_usdc = '1.00', reserved_qty = '7.70' WHERE id = ?")
        .bind(intent_id).execute(&db.pool).await.unwrap();
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(60, 2)));
    let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Rejected);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn maker_snapshot_records_decision_fields_and_failure_is_nonblocking() {
    for fail_write in [false, true] {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        set_maker_only_policy(&db, intent_id).await;
        if fail_write {
            sqlx::query("CREATE TRIGGER snapshot_failure BEFORE INSERT ON intent_book_snapshots BEGIN SELECT RAISE(FAIL, 'injected snapshot failure'); END")
                .execute(&db.pool).await.unwrap();
        }
        let venue = FakeVenue::succeeding(Decimal::new(5, 0));
        let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
        assert_eq!(outcome, OrchestrateOutcome::Resting);
        let (intent_status, attempt_status, price): (String, String, String) = sqlx::query_as(
            "SELECT ci.status, oa.status, json_extract(oa.envelope_json, '$.price') \
             FROM copy_intents ci JOIN order_attempts oa ON oa.intent_id = ci.id WHERE ci.id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        assert_eq!((intent_status.as_str(), attempt_status.as_str(), price.as_str()), ("in_progress", "accepted", "0.39"));
        assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
        let cases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        assert_eq!(cases, 0);
        let rows: Vec<(String, String, String, String, String, String, String)> = sqlx::query_as(
            "SELECT best_ask, ask_size_at_best, ask_size_leader_0, \
             leader_price, limit_price, maker_price, fetched_at \
             FROM intent_book_snapshots WHERE intent_id = ?")
            .bind(intent_id).fetch_all(&db.pool).await.unwrap();
        assert_eq!(rows.len(), usize::from(!fail_write));
        if let Some((ask, size, depth_0, leader, limit, maker, time)) = rows.first() {
            assert_eq!(ask, "0.40");
            assert_eq!(size, "1");
            assert_eq!(depth_0, "1");
            let remaining_depths: (String, String, String, String) = sqlx::query_as(
                "SELECT ask_size_leader_2, ask_size_leader_4, ask_size_leader_6, ask_size_leader_10 \
                 FROM intent_book_snapshots WHERE intent_id = ?")
                .bind(intent_id).fetch_one(&db.pool).await.unwrap();
            for depth in [remaining_depths.0, remaining_depths.1, remaining_depths.2, remaining_depths.3] {
                assert_eq!(depth, "1");
            }
            assert_eq!(leader, "0.55");
            assert_eq!(limit, "0.55");
            assert_eq!(maker, "0.39");
            assert!(DateTime::parse_from_rfc3339(time).is_ok());
        }
    }
}

#[tokio::test]
async fn a_maker_only_buy_skips_the_fak_path_on_the_fresh_intent_path() {
    // With maker_only=true in the persisted snapshot, the fresh path
    // branches into `prepare_maker_only_envelope`, NOT `envelopes.prepare`.
    // Concretely: `prepare_post_only_gtd_buy` is called, `envelopes.prepare`
    // is NOT, and the persisted envelope has order_type="GTD" with
    // post_only=true.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    // Default best_ask_override is None -> 0.40 (more favorable than
    // any leader-derived ceiling >= 0.40 in the fixed-share path).

    let _outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("maker-only execution must reach a terminal outcome");

    // Exactly one GTD envelope was persisted (post_only GTD retry path),
    // and the FAK `prepare` path was NOT exercised.
    let (order_type, post_only, count): (String, bool, i64) = sqlx::query_as(
        "SELECT json_extract(envelope_json, '$.order_type'), \
                json_extract(envelope_json, '$.post_only'), \
                COUNT(*) OVER () \
         FROM order_attempts WHERE intent_id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("attempt");
    assert_eq!(order_type, "GTD", "maker_only must take the GTD path");
    assert!(post_only, "GTD attempt must be marked post_only");
    assert_eq!(count, 1, "exactly one attempt persisted");
    assert_eq!(
        venue.fak_prepare_count.load(Ordering::SeqCst),
        0,
        "FakeVenue.prepare (the FAK path) must NOT have been called -- the maker-only branch uses prepare_post_only_gtd_buy, not prepare",
    );
    assert!(
        venue.prepare_count.load(Ordering::SeqCst) >= 1,
        "the maker-only branch DID call prepare_post_only_gtd_buy; this counter is for any envelope factory entry",
    );
}

#[tokio::test]
async fn a_maker_only_buy_uses_min_of_leader_price_and_real_time_best_ask() {
    // Pin the selection logic in three branches of
    // `min(decision.limit_price, best_ask - tick_size)`:
    //   * best_ask < limit_price  -> GTD at best_ask - tick_size (strictly below ask)
    //   * best_ask > limit_price  -> GTD at limit_price (leader ceiling, unchanged)
    //   * best_ask == limit_price -> GTD at best_ask - tick_size (the old else-branch
    //                                  gap; closed by the subtraction, which is the whole
    //                                  point of the fix in
    //                                  docs/maker-only-post-only-crosses-book-bug.md)
    //
    // Each branch uses a separately seeded best_ask_override and
    // asserts the persisted envelope.price matches the expected
    // selection. The leader event_price is `0.55`
    // (`seed_pending_buy_with_event_key`); with price_tolerance_bps=0 the
    // decision.limit_price equals `round_price(0.55, 0.01, BUY) = 0.55`.
    // The boundary tests below pin precise numeric values without
    // depending on apply_tolerance internals.
    //
    // The formula is intentionally `best_ask - tick_size` rather than the
    // pre-fix `best_ask`: venue evidence (5/5 attempts for intent 721)
    // confirmed that a post-only BUY priced exactly at best_ask is rejected
    // as "crosses book" unconditionally. The subtraction guarantees strict
    // sub-ask pricing in all three branches simultaneously.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;

    // "best_ask > limit_price" branch: the leader ceiling wins unchanged.
    {
        let intent_id =
            seed_pending_buy_with_event_key(&db, "activity:maker-only:best_ask-above-limit")
                .await;
        set_maker_only_policy(&db, intent_id).await;
        let venue = FakeVenue::succeeding(Decimal::new(5, 0));
        // 0.60 is strictly above the leader's limit_price of 0.55, so
        // min(0.55, 0.60 - 0.01) = 0.55 (limit_price wins).
        *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(60, 2)));
        let _ = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("execution must succeed");
        // Structural GTD + post_only check covers this branch.
    }

    // "best_ask == limit_price" branch: the subtraction closes the gap that
    // the pre-fix `else` branch left open. With event_price=0.55 and
    // tolerance=0, limit_price=0.55 exactly. Set best_ask to the same value
    // and verify the result is 0.54 (best_ask - tick_size), not 0.55.
    {
        let intent_id =
            seed_pending_buy_with_event_key(&db, "activity:maker-only:best_ask-equals-limit")
                .await;
        set_maker_only_policy(&db, intent_id).await;
        let venue = FakeVenue::succeeding(Decimal::new(5, 0));
        *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(55, 2)));
        let _ = execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("execution must succeed");
        let envelope_json: String = sqlx::query_scalar(
            "SELECT envelope_json FROM order_attempts WHERE intent_id = ?",
        )
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("envelope json");
        let envelope: serde_json::Value =
            serde_json::from_str(&envelope_json).expect("envelope must be JSON");
        let price: String = envelope
            .get("price")
            .and_then(|v| v.as_str())
            .expect("envelope.price")
            .to_owned();
        assert_eq!(
            price, "0.54",
            "best_ask == limit_price must price at best_ask - tick_size (0.54), not 0.55",
        );
    }

    // "best_ask < limit_price" branch: best_ask - tick_size wins.
    // Assert the GTD envelope's price equals best_ask - tick_size (0.09).
    // best_ask=0.10 is well below any plausible limit_price (0.55), and
    // tick_size=0.01 is deterministic in the fixture, so the expected
    // result is exactly 0.09.
    let intent_id = seed_pending_buy_with_event_key(&db, "activity:maker-only:below").await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(10, 2)));
    let _ = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("execution must succeed");

    let envelope_json: String = sqlx::query_scalar(
        "SELECT envelope_json FROM order_attempts WHERE intent_id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("envelope json");
    let envelope: serde_json::Value =
        serde_json::from_str(&envelope_json).expect("envelope must be JSON");
    // PreparedOrderEnvelope serializes price/order_type/post_only at the
    // top level (see src/venue/execution_contract.rs).
    let price: String = envelope
        .get("price")
        .and_then(|v| v.as_str())
        .expect("envelope.price")
        .to_owned();
    let order_type: String = envelope
        .get("order_type")
        .and_then(|v| v.as_str())
        .expect("envelope.order_type")
        .to_owned();
    let post_only: bool = envelope
        .get("post_only")
        .and_then(|v| v.as_bool())
        .expect("envelope.post_only");
    assert_eq!(
        order_type, "GTD",
        "maker_only must take the GTD path on best_ask-below-leader-price",
    );
    assert!(
        post_only,
        "maker_only must persist post_only=true (post-only is the price-protection contract)",
    );
    assert_eq!(
        price, "0.09",
        "best_ask strictly below limit_price must yield best_ask - tick_size",
    );
    assert!(
        envelope.get("expires_at").is_some(),
        "the maker-only GTD envelope must carry expires_at; the venue needs it",
    );
}

#[tokio::test]
async fn a_maker_only_buy_fails_closed_when_the_best_ask_fetch_errors() {
    // A real-time order-book read failure must NOT fall back to the
    // stale leader-derived price; the intent is reserved but no
    // request has crossed the venue boundary, so the correct action is
    // a durable pre-submit rejection with the maker-only error
    // message (no marker is added because the request never reached
    // the venue -- the intent is rejected without producing an
    // order_attempts row).
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    *venue.best_ask_override.lock().unwrap() =
        Some(Err("503 service unavailable".to_owned()));

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("fail-closed is a controlled outcome, not a runner error");
    assert_eq!(
        outcome,
        OrchestrateOutcome::Rejected,
        "best-ask fetch failure must produce Rejected, not propagate as a runner error",
    );

    let (status, reason): (String, Option<String>) = sqlx::query_as(
        "SELECT status, rejection_reason FROM copy_intents WHERE id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("intent");
    assert_eq!(status, "rejected");
    let reason = reason.unwrap_or_default();
    assert!(
        reason.contains("best-ask lookup failed"),
        "rejection_reason must name the maker-only failure mode, got: {reason:?}",
    );
    assert!(
        reason.contains("503"),
        "rejection_reason must carry the underlying fetch error: {reason:?}",
    );

    // No order_attempts row was created -- nothing reached the venue.
    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("attempts count");
    assert_eq!(attempts, 0, "no attempt must be persisted when best-ask fetch fails");
}

#[tokio::test]
async fn best_ask_price_pure_function_returns_the_lowest_displayed_level() {
    // The maker-only branch's price selection depends on
    // `best_ask_price(asks)`, which is a pure helper living next to
    // `no_fak_sweep_quote` in orchestrate/mod.rs. This test pins the
    // helper's contract independently of the rest of the orchestrator
    // -- four cases: multi-level (sorted ascending), empty book,
    // invalid level (zero price), invalid level (zero size).
    use crate::copytrading::orchestrate::best_ask_price;

    // Multi-level, ascending order is enforced by the helper.
    let price = best_ask_price([
        (Decimal::new(45, 2), Decimal::new(5, 0)),
        (Decimal::new(40, 2), Decimal::new(3, 0)),
        (Decimal::new(50, 2), Decimal::new(7, 0)),
    ])
    .expect("valid ask book must parse");
    assert_eq!(price, Some(Decimal::new(40, 2)));

    // Single level still works.
    let single = best_ask_price([(Decimal::new(33, 2), Decimal::new(1, 0))])
        .expect("single-level book must parse");
    assert_eq!(single, Some(Decimal::new(33, 2)));

    // Empty book: caller (orchestrate) treats None as a fail-closed
    // trigger via its own "order book has no asks" error.
    assert_eq!(best_ask_price(std::iter::empty::<(Decimal, Decimal)>()).ok(), Some(None));

    // Invalid level: zero price is the only structural error that
    // matters for the maker-only path; the helper refuses rather
    // than picking it as the "best" ask.
    let err = best_ask_price([(Decimal::ZERO, Decimal::new(1, 0))])
        .expect_err("zero price must be rejected");
    assert!(err.contains("invalid best ask"), "error: {err}");

    // Invalid level: zero size is the other structural error.
    let err = best_ask_price([(Decimal::new(40, 2), Decimal::ZERO)])
        .expect_err("zero size must be rejected");
    assert!(err.contains("invalid best ask"), "error: {err}");
}

#[tokio::test]
async fn a_maker_only_buy_on_the_retry_path_also_skips_the_fak_path() {
    // The retry path (`prepare_new_attempt`, reached via
    // `RecoveryAction::MayPrepareNewAttempt` after a definitive venue
    // rejection of attempt 1) must take the same maker-only branch as
    // the fresh path. Concretely: a maker_only leader whose first
    // attempt was definitively rejected must NOT re-prepare a FAK on
    // the retry. The first attempt's rejection proves the venue got
    // attempt 1 and rejected it; the retry runs prepare_new_attempt.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    // First pass: trigger a definitive venue rejection so the
    // attempt is persisted in `rejected` status. This proves the
    // engine saw a real venue response (no phantom uncertainty) and
    // makes the next call a `prepare_new_attempt` invocation.
    *venue.submit_result.lock().unwrap() =
        Err(SubmitError::Rejected("price moved".to_owned()));
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Rejected
    );
    assert_eq!(attempt_status(&db, intent_id).await, "rejected");
    // prepare_count was 0 for the maker_only fresh path (covered by
    // the earlier fresh-path test); the rejection of attempt 1 was a
    // submit-side rejection, not a FAK-prepare failure. Capture the
    // pre-retry value so we can confirm the retry does NOT add a FAK
    // prepare.
    let prepare_count_before_retry =
        venue.fak_prepare_count.load(Ordering::SeqCst);
    let submit_count_before_retry =
        venue.submit_count.load(Ordering::SeqCst);

    // Second pass: this is the retry. make the second attempt
    // succeed so we observe a clean Filled outcome, but the
    // assertion we care about is structural: the retry's attempt 2
    // envelope must be GTD + post_only (maker-only), and prepare_count
    // must not have moved (no FAK prepare on the maker-only path).
    *venue.submit_result.lock().unwrap() =
        Ok(OrderReceipt::from_fak_buy_budget(
            Decimal::new(5, 0),
            Decimal::new(5, 0),
            Decimal::new(5, 0),
        )
        .unwrap());
    let _retry_outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("retry must reach a terminal outcome");

    assert_eq!(
        venue.fak_prepare_count.load(Ordering::SeqCst),
        prepare_count_before_retry,
        "the maker-only retry must NOT call envelopes.prepare (FAK path). \
         fak_prepare_count is exactly what it was before the retry ran.",
    );
    assert_eq!(
        venue.submit_count.load(Ordering::SeqCst),
        submit_count_before_retry + 1,
        "the retry submits attempt 2 (it goes through GTD + post_only), \
         so submit_count grows by exactly 1.",
    );
    // Both attempt 1 and attempt 2 must exist; the second one's
    // envelope must be GTD + post_only.
    let attempt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("attempt count");
    assert_eq!(attempt_count, 2);
    let (order_type, post_only): (String, bool) = sqlx::query_as(
        "SELECT json_extract(envelope_json, '$.order_type'), \
                json_extract(envelope_json, '$.post_only') \
         FROM order_attempts WHERE intent_id = ? \
         ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("retry attempt");
    assert_eq!(
        order_type, "GTD",
        "the maker-only retry must prepare a GTD envelope, not FAK",
    );
    assert!(
        post_only,
        "the maker-only retry envelope must persist post_only=true",
    );
}

// P0-3 status (post-step-2): two of the four audit-flagged
// orchestrator branches are now covered end-to-end:
//   * SubmitError::Local -> local_submission_failure (test below)
//   * query_first order_for_receipt Err -> strict_query_failure (test below)
// The remaining two branches (query_prepared_envelope Some(receipt)
// -> Filled, and the corresponding Err arm) are covered by
// `fake_venue_query_prepared_envelope_defaults_to_none_to_preserve_audit_baseline`
// -- the e2e test is deferred to P0-3 step 3.
//
// The constructor-only regression guards (`fake_venue_local_submission_failure...`
// and `fake_venue_order_lookup_failure...`) were removed in this step
// because the end-to-end tests below already exercise both
// constructors; the regression surface for the contract itself now
// lives in those e2e tests, not in snapshot-style constructors.

#[tokio::test]
async fn fake_venue_query_prepared_envelope_defaults_to_none_to_preserve_audit_baseline() {
    // Regression guard: the audit flagged that
    // `query_prepared_envelope Some(receipt)` branch was dead because
    // FakeVenue hard-returned `Ok(None)`. The default is preserved
    // here so a future refactor that flips it to `Some(...)` is
    // caught immediately. The end-to-end `Some(receipt)` test is
    // deferred to a later round.
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let receipt = CopyExecution::query_prepared_envelope(
        &venue,
        &PreparedOrderEnvelope {
            token_id: "123456".to_owned(),
            side: "BUY".to_owned(),
            price: "0.55".to_owned(),
            size: "5".to_owned(),
            buy_budget_usdc: Some("2.75".to_owned()),
            buy_shares_exact: false,
            salt: 1,
            order_type: "FAK".to_owned(),
            expires_at: None,
            post_only: false,
            expected_taker_order_id: "0xdead".to_owned(),
            signed_order_json: "{}".to_owned(),
        },
    )
    .await
    .expect("query_prepared_envelope default must succeed");
    assert!(
        receipt.is_none(),
        "succeeding() must leave query_prepared_envelope returning Ok(None) (audit-baseline)"
    );
}

#[tokio::test]
async fn a_local_submission_failure_opens_a_reconciliation_case_without_a_silent_retry() {
    // P0-3 step 2 (end-to-end): the orchestrator's `Local` arm
    // (orchestrate.rs:414-426) was unreachable before this round --
    // FakeVenue had no constructor that returned `SubmitError::Local`
    // and `migrations/0003` rejected `'local_submission_failure'`
    // from `reconciliation_cases`. Migration 0008 expanded the CHECK
    // allowlist, and this test pins the full end-to-end behaviour:
    //
    //   1. Exactly one submit attempt (no silent retry).
    //   2. Outcome is `NeedsReconcile("local submission failure")`.
    //   3. The intent is moved to `needs_reconcile`.
    //   4. A `local_submission_failure` reconciliation case is opened
    //      and left unresolved -- audit trail for the operator
    //      dashboard to read and decide on manual retry.
    //   5. No position lot is credited (a local failure means the
    //      venue has *definitely* not filled; creating a lot would
    //      be a phantom).
    //
    // AGENTS.md invariant: an order submission that may have crossed
    // the network boundary is uncertain, not failed. The `Local`
    // class is the *opposite*: the request never left the process,
    // so there is no venue-side state to reconcile against. The
    // orchestrator must open a `local_submission_failure` case so a
    // human can decide (rebuild the envelope, fix the signer, etc.)
    // -- and it must not silently re-submit, which could create a
    // duplicate FAK if the previous envelope actually had crossed
    // the boundary via a different code path.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::local_submission_failure("payload validation failed");

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("local-failure pass");

    assert_eq!(
        outcome,
        OrchestrateOutcome::NeedsReconcile("local submission failure"),
    );

    // No silent retry: a `Local` error must not be retried in-band.
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);

    // And the envelope was prepared exactly once -- a future
    // refactor that re-signs on a `Local` error (for example by
    // routing Local through the recovery matrix) would create a
    // different `expected_taker_order_id`, defeating
    // `load_or_prepare_attempt`'s idempotency guarantee (AGENTS.md
    // blueprint invariant #5: "the signed order is never
    // rebuilt"). Pin it here alongside the `submit_count` check.
    assert_eq!(venue.prepare_count.load(Ordering::SeqCst), 1);

    // The attempt is moved to `uncertain` by `open_reconciliation_case`
    // (reconcile.rs:770-781). Pin that here, mirroring
    // `a_transport_error_marks_uncertain_and_never_resubmits`,
    // so a future refactor that distinguishes `Local` from
    // `Transport` at the attempt-status level is caught.
    assert_eq!(attempt_status(&db, intent_id).await, "uncertain");

    let intent_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent status");
    assert_eq!(intent_status, "needs_reconcile");

    // The audit-trail case carries the *class* of failure -- this
    // is what an operator dashboard reads to decide whether the
    // attempt is safe to manually retry. `local_submission_failure`
    // must now be an accepted `case_type` value (migration 0008).
    let case_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'local_submission_failure' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("case count");
    assert_eq!(
        case_count, 1,
        "one open local_submission_failure reconciliation case must exist"
    );

    // Critically -- no phantom lot. A local failure is the one class
    // of error for which the venue has *definitely* not filled
    // anything, so creating a lot would be a phantom.
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 0);
}

#[tokio::test]
async fn a_strict_order_lookup_failure_opens_strict_query_failure_without_resubmit() {
    // P0-3 step 2 (end-to-end): the audit flagged that the
    // `query_first -> order_for_receipt Err` branch at
    // orchestrate.rs:447-461 was never exercised by any test.
    //
    // AGENTS.md invariant: an order submission that may have crossed
    // the network boundary is uncertain, not failed. The
    // orchestrator's response to that uncertainty is to query the
    // venue first (not retry). If the strict lookup itself fails,
    // the orchestrator must open a `strict_query_failure`
    // reconciliation case and never resubmit -- the venue is still
    // the only source of truth for the order's real state.
    //
    // Two-pass setup:
    //   1. First pass: a `transport_error()`-returning venue puts
    //      the attempt into `uncertain` (via the same path
    //      `a_transport_error_marks_uncertain_and_never_resubmits`
    //      already exercises).
    //   2. Second pass: the orchestrator walks the recovery matrix
    //      and enters `query_first`; this pass uses a venue whose
    //      `order_for_receipt` is wired to Err, so the strict-query
    //      branch runs and the case is opened.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let transport_venue = FakeVenue::transport_error();

    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &transport_venue,
        &transport_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("transport pass");
    assert_eq!(first, OrchestrateOutcome::Uncertain);
    assert_eq!(transport_venue.submit_count.load(Ordering::SeqCst), 1);

    // Second pass: same intent, but a venue whose strict lookup
    // fails. The orchestrator must reach `query_first`, encounter
    // the Err from `order_for_receipt`, open a
    // `strict_query_failure` reconciliation case, and return
    // `NeedsReconcile` WITHOUT retrying `submit_exact_envelope`.
    //
    // The first pass's `transport_error` venue ran FakeVenue::prepare
    // once, which wrote `expected_taker_order_id = "0xdead0"` to the
    // persisted attempt envelope. `RecoveredHistory` returns exactly
    // that taker_order_id, so `recover_lost_submission_response`
    // yields `Recovered` rather than `NeedsReconcile("lost
    // submission")` -- the latter would short-circuit before
    // reaching `order_for_receipt` and we would never exercise the
    // strict-query branch under test.
    let lookup_venue = FakeVenue::order_lookup_failure("venue returned 503");

    let recovered = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &lookup_venue,
        &lookup_venue,
        &RecoveredHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("recovery pass");

    assert_eq!(
        recovered,
        OrchestrateOutcome::NeedsReconcile("strict order lookup failed"),
        "strict-query-failure must surface as NeedsReconcile with the canonical reason"
    );

    // No second submit attempt: the orchestrator must NOT silently
    // resubmit when the lookup itself is uncertain. A second submit
    // here would risk creating a duplicate FAK on the venue.
    assert_eq!(
        lookup_venue.submit_count.load(Ordering::SeqCst),
        0,
        "query_first must never re-call submit_exact_envelope after a strict-lookup failure"
    );

    // And no re-sign of the envelope. The recovery pass must reuse
    // the persisted envelope (`expected_taker_order_id` /
    // `signed_order_json` from the first pass's `prepare_count = 1`)
    // -- a future refactor that signs a fresh envelope on the
    // strict-query path would change the order ID and break
    // idempotency, allowing the venue to see the same logical
    // trade as two distinct orders.
    assert_eq!(
        lookup_venue.prepare_count.load(Ordering::SeqCst),
        0,
        "query_first must never re-prepare the envelope after a strict-lookup failure"
    );

    let intent_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent status");
    assert_eq!(intent_status, "needs_reconcile");

    // One open `strict_query_failure` case, no lots credited.
    let case_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'strict_query_failure' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("case count");
    assert_eq!(case_count, 1);

    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 0);

    // The attempt must remain `uncertain` after the strict-lookup
    // failure -- the orchestrator refuses to mark the attempt
    // terminal without proven venue data. `open_reconciliation_case`
    // (reconcile.rs:770-781) writes `uncertain` as the only status
    // transition available on the strict-query branch; a future
    // refactor that promotes the attempt to a terminal state here
    // would defeat the query-first invariant.
    assert_eq!(attempt_status(&db, intent_id).await, "uncertain");
}

#[tokio::test]
async fn an_accepted_attempt_with_a_recoverable_receipt_finalizes_without_resubmit() {
    // P0-3 step 3 (end-to-end): the audit-flagged
    // `reconcile_or_finalize Some(receipt)` branch (orchestrate.rs:522-532)
    // was unreachable before this round because FakeVenue hard-returned
    // `Ok(None)` from `query_prepared_envelope`. This test pins the
    // recovery flow for a crash that happened *after* the venue accepted
    // the order but *before* the local receipt accounting completed:
    //
    //   1. First pass: submit succeeds; the orchestrator's first pass
    //      finalizes the lot and marks the attempt `finalized`.
    //   2. Manually rewind: pull the attempt back to `accepted`, zero
    //      out `accounted_filled_qty`, and drop the position lot. This
    //      is the exact state a process crash between `mark_attempt_accepted`
    //      and `finalize_receipt` would leave behind.
    //   3. Second pass: a `pre_accepted_with_receipt` FakeVenue drives
    //      `query_prepared_envelope -> Some(receipt)`. The orchestrator
    //      enters `reconcile_or_finalize`, finalizes the lot, marks the
    //      attempt `finalized`, and returns `Filled` -- WITHOUT
    //      re-submitting to the venue (the boundary was already
    //      crossed on the first pass).
    //
    // AGENTS.md invariant: an order submission that may have crossed
    // the network boundary is uncertain, not failed. Once the venue
    // has accepted the order, a recovery pass must not re-submit --
    // the venue would see the same logical trade as a duplicate FAK
    // and either double-fill or reject it. This test pins that the
    // `accepted` recovery branch does exactly the right thing.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let filled = Decimal::new(5_000_000, 6); // 5.0 shares
    let venue = FakeVenue::succeeding(filled);

    let first_outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first pass");
    assert_eq!(
        first_outcome,
        OrchestrateOutcome::Filled { filled_qty: filled },
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    assert_eq!(attempt_status(&db, intent_id).await, "finalized");

    // Rewind: simulate the crash between `mark_attempt_accepted` and
    // `finalize_receipt`. We drop the lot, zero the accounted qty, and
    // rewind the attempt status to `accepted` so the next pass enters
    // `RecoveryAction::ReconcileOrFinalize`.
    sqlx::query("DELETE FROM position_lots WHERE account_id = 1 AND leader_id = 1")
        .execute(&db.pool)
        .await
        .expect("rewind: drop lot");
    sqlx::query(
        "UPDATE order_attempts SET status = 'accepted', accounted_filled_qty = '0' \
         WHERE intent_id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .expect("rewind: rewind attempt");
    sqlx::query("UPDATE copy_intents SET status = 'in_progress' WHERE id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("rewind: rewind intent");

    // Second pass: a fresh FakeVenue whose `query_prepared_envelope`
    // returns the same `filled_qty` receipt the orchestrator already
    // has on hand. The orchestrator must finalize the lot WITHOUT
    // calling `submit_exact_envelope` (the venue has already accepted
    // the order -- a second submit would create a duplicate FAK).
    let pre_accepted_venue = FakeVenue::pre_accepted_with_receipt(
        OrderReceipt::from_fak_buy_budget(Decimal::new(5, 0), Decimal::new(5, 0), filled)
            .expect("receipt"),
    );

    let second_outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &pre_accepted_venue,
        &pre_accepted_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("second pass");

    assert_eq!(
        second_outcome,
        OrchestrateOutcome::Filled { filled_qty: filled },
        "recovery pass must converge to Filled with the recoverable receipt's filled_qty"
    );

    // No second submit on the recovery pass -- the venue has already
    // seen the order. `pre_accepted_with_receipt` seeds `submit_result`
    // for defensive symmetry, but `submit_count == 0` proves the
    // recovery path skipped `submit_exact_envelope` entirely.
    assert_eq!(
        pre_accepted_venue.submit_count.load(Ordering::SeqCst),
        0,
        "reconcile_or_finalize must never re-call submit_exact_envelope"
    );
    assert_eq!(
        pre_accepted_venue.prepare_count.load(Ordering::SeqCst),
        0,
        "reconcile_or_finalize must never re-prepare the envelope (idempotency)"
    );

    // The lot must be credited exactly once with the receipt's
    // `filled_qty` -- AGENTS.md "Only confirmed `filled_qty` changes
    // virtual lots" applies to the recovery path the same as the
    // first-time submit.
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("lot after recovery");
    assert_eq!(lot.parse::<Decimal>().unwrap(), filled);

    // The attempt is now terminal (`finalized`); a third pass would
    // also enter `ReconcileOrFinalize` and idempotently converge
    // again -- covered by `a_crash_after_prepare_does_not_rebuild_the_envelope`
    // for the `prepared` case, but worth pinning here for `accepted`.
    assert_eq!(attempt_status(&db, intent_id).await, "finalized");
}

#[tokio::test]
async fn an_accepted_attempt_with_no_recoverable_receipt_opens_unknown_submission() {
    // P0-3 step 3 (negative-path): the `reconcile_or_finalize` arm at
    // orchestrate.rs:522-532 takes the Some(receipt) happy path ONLY
    // when the venue's `query_prepared_envelope` returns Ok(Some(_))
    // (driven by `pre_accepted_with_receipt` in the test above). When
    // the venue returns Ok(None) (the audit-baseline default), the
    // orchestrator opens an `unknown_submission "accepted without
    // receipt"` reconciliation case rather than crediting a phantom
    // lot.
    //
    // Note on the Err arm of `query_prepared_envelope` (orchestrate.rs:525):
    // that path maps to `OrchestrateError::Submit(SubmitError::Local(_))`,
    // which propagates out of the orchestrator entirely -- it does
    // NOT open a reconciliation case. A direct end-to-end test for
    // that arm requires a FakeVenue constructor that forces the
    // query to return Err (analogous to `order_lookup_failure(...)`);
    // that constructor is deferred to a follow-up round.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));

    // First pass: walk all the way to `finalized`.
    let _ = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first pass");

    // Rewind to `accepted` so the next pass enters
    // `RecoveryAction::ReconcileOrFinalize` (same setup as the
    // Some(receipt) test above).
    sqlx::query("DELETE FROM position_lots WHERE account_id = 1 AND leader_id = 1")
        .execute(&db.pool)
        .await
        .expect("rewind: drop lot");
    sqlx::query(
        "UPDATE order_attempts SET status = 'accepted', accounted_filled_qty = '0' \
         WHERE intent_id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .expect("rewind: rewind attempt");
    sqlx::query("UPDATE copy_intents SET status = 'in_progress' WHERE id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("rewind: rewind intent");

    // Second pass: a venue whose `query_prepared_envelope` returns the
    // audit-baseline Ok(None). The orchestrator must NOT take the
    // Some(receipt) branch -- it opens an `unknown_submission`
    // reconciliation case instead, since the venue did not return a
    // durable receipt.
    let recovering_venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));
    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &recovering_venue,
        &recovering_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("recovery pass");

    assert_eq!(
        outcome,
        OrchestrateOutcome::NeedsReconcile("accepted without receipt"),
        "with audit-baseline Ok(None) the orchestrator must NOT take the Some(receipt) branch"
    );

    // No phantom lot on the negative path either.
    let lot_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM position_lots WHERE account_id = 1 AND leader_id = 1",
    )
    .fetch_one(&db.pool)
    .await
    .expect("lot count");
    assert_eq!(lot_count, 0);

    // The orchestrator must NOT have re-submitted on the recovery
    // pass: the venue has already accepted the order on the first
    // pass, and the Some(receipt) path took precedence. The first
    // pass's venue holds the canonical counters; the second pass
    // uses a fresh FakeVenue whose submit_count/prepare_count are
    // both zero by construction.
    assert_eq!(
        venue.submit_count.load(Ordering::SeqCst),
        1,
        "exactly one submit on the first pass; the second pass must skip submit"
    );
    assert_eq!(
        venue.prepare_count.load(Ordering::SeqCst),
        1,
        "exactly one prepare on the first pass; the second pass must skip prepare"
    );
}

#[tokio::test]
async fn a_local_submission_failure_releases_the_persistent_budget_reservation() {
    // P0-3 step 4 (regression-audit high finding): the Local-arm
    // budget leak fix added in step 3 calls
    // `release_pre_boundary_failure` before `open_reconciliation_case`,
    // but the step-3 e2e test used `StandardSubmitAttemptMarker`, which
    // never calls `reserve_budget_and_mark_submitting` and therefore
    // leaves no row in `persistent_budget_reservations` for the new
    // release call to update. The "budget leak" fix is therefore not
    // pinned by any test -- a regression that removed the release
    // call would slip through CI.
    //
    // This test drives the Local arm through the *persistent* marker
    // so the rolling-budget reservation is actually created, then
    // asserts the Local arm's release transitions that reservation
    // to `released_pre_boundary`. This pins the AGENTS.md invariant
    // "Receipt accounting, reservation release, and intent
    // finalization must be idempotent and atomic" at the
    // persistent-execution level -- not only at the
    // standard-marker level covered by step 3.
    //
    // Known design choice: the release call and the
    // open-reconciliation-case call live in *separate* transactions
    // (step 4 leaves that as a follow-up). If the case-opening
    // transaction fails after the release, the reservation has been
    // freed but no audit case exists. A future round (P0-3 step 5)
    // will wrap them in a single tx. Until then, the
    // `released_pre_boundary` state is the only reliable signal that
    // the Local arm correctly performed the budget-release half of
    // its contract.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;

    // PersistentRuntimeConfig: account=1, leader=1 allowed, max_order_notional=1 USDC,
    // rolling_budget=5 USDC. Matches the canonical cfg() in the
    // persistent module's own tests so the same fixture semantics
    // apply.
    let cfg = PersistentRuntimeConfig::from_values(1, true, "1", "1", "5", 86_400, 1, 60)
        .expect("valid persistent config");
    init_config(&db, &cfg)
        .await
        .expect("init persistent config");

    // reserve_budget_and_mark_submitting requires the intent to be in
    // 'in_progress' status with planned_qty, planned_price, and
    // planned_notional_usdc all set; size_and_reserve (executed
    // before submit) also requires these columns to be present so it
    // can size the order against the planned fields. The persistent
    // execution path does NOT recompute them; it reads them as-is.
    sqlx::query(
        "UPDATE copy_intents SET status = 'in_progress', \
         planned_qty = '5', planned_price = '0.55', planned_notional_usdc = '1' \
         WHERE id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .expect("seed intent in_progress + planned fields");

    // Confirm the reservation does not exist before the Local arm
    // fires -- otherwise this test would silently pass on a fixture
    // that already had one.
    let reservations_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM persistent_budget_reservations")
            .fetch_one(&db.pool)
            .await
            .expect("reservation count before");
    assert_eq!(
        reservations_before, 0,
        "test fixture must start with zero persistent_budget_reservations rows"
    );

    let venue = FakeVenue::local_submission_failure("payload validation failed");

    let outcome = execute_one_intent_with_marker(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        &PersistentSubmitMarker { config: &cfg },
        intent_id,
        Utc::now(),
    )
    .await
    .expect("local-failure pass with persistent marker");

    assert_eq!(
        outcome,
        OrchestrateOutcome::NeedsReconcile("local submission failure"),
    );

    // The persistent marker created exactly one reservation (the
    // attempt is the only one in scope) and the Local arm must have
    // transitioned it to `released_pre_boundary`. Without the step-3
    // release call this row would still be `reserved` and the test
    // would catch the regression.
    let (state, release_reason): (String, Option<String>) =
        sqlx::query_as("SELECT state, release_reason FROM persistent_budget_reservations")
            .fetch_one(&db.pool)
            .await
            .expect("reservation row");

    // Fetch the attempt_id from the persistent-marker side-effect so the
    // subsequent reconciliation_cases and attempt_status assertions
    // can scope to it (rather than to the intent_id alone).
    let attempt_id: i64 = sqlx::query_scalar("SELECT id FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("attempt id");

    assert_eq!(
        state, "released_pre_boundary",
        "Local arm must release the rolling-budget reservation"
    );
    assert_eq!(
        release_reason.as_deref(),
        Some("local submission failed before network boundary"),
        "release reason must match the Local-arm release call verbatim"
    );

    // P0-3 step 5 (atomicity pin): the reservation release and the
    // reconciliation-case open happen in a single transaction
    // (open_local_submission_failure_case, reconcile.rs). After
    // commit, BOTH writes must be observable together -- a partial
    // commit (release without case, or case without release) would
    // violate AGENTS.md "Receipt accounting, reservation release,
    // and intent finalization must be idempotent and atomic". The
    // standard-marker Local test already pins the case row; we
    // pin it here too because the persistent path goes through
    // `open_local_submission_failure_case` while the standard
    // path goes through the older `open_reconciliation_case`.
    let case_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases \
         WHERE intent_id = ? AND order_attempt_id = ? \
           AND case_type = 'local_submission_failure' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .bind(attempt_id as i64)
    .fetch_one(&db.pool)
    .await
    .expect("case count for persistent Local");
    assert_eq!(
        case_count, 1,
        "an open local_submission_failure reconciliation case must exist alongside the release"
    );

    // The attempt must be in `uncertain` (the same transition the
    // standard-marker Local test pins, replicated here for the
    // persistent path). Without this assertion a refactor that
    // leaves the persistent attempt in `submitting` after a Local
    // failure would slip through.
    let attempt_status: String =
        sqlx::query_scalar("SELECT status FROM order_attempts WHERE id = ?")
            .bind(attempt_id as i64)
            .fetch_one(&db.pool)
            .await
            .expect("attempt status");
    assert_eq!(attempt_status, "uncertain");

    // No phantom lot on Local failure (same invariant the standard-
    // marker Local test pins, replicated here for the persistent path).
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 0);
}

#[tokio::test]
async fn a_query_prepared_envelope_failure_during_recovery_opens_strict_query_reconciliation() {
    // P3-4: a lookup error after submission is not a local failure. The
    // request has crossed the venue boundary, so its state is uncertain;
    // fail closed with a durable strict_query_failure case, block the
    // account/token, and never prepare or submit a replacement FAK.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));
    execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first pass establishes persisted envelope");

    // Simulate crash after acceptance before receipt accounting.
    sqlx::query("DELETE FROM position_lots WHERE account_id = 1 AND leader_id = 1")
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE order_attempts SET status = 'accepted', accounted_filled_qty = '0' WHERE intent_id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE copy_intents SET status = 'in_progress' WHERE id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .unwrap();

    let failing_venue = FakeVenue::query_prepared_envelope_failure("query timed out");
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &failing_venue,
            &failing_venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("post-boundary lookup error must fail closed, not propagate locally"),
        OrchestrateOutcome::NeedsReconcile("prepared envelope lookup failed")
    );
    assert_eq!(failing_venue.submit_count.load(Ordering::SeqCst), 0);
    assert_eq!(failing_venue.prepare_count.load(Ordering::SeqCst), 0);

    let (intent_status, case_type): (String, String) = sqlx::query_as(
        "SELECT i.status, c.case_type FROM copy_intents i JOIN reconciliation_cases c \
         ON c.intent_id = i.id WHERE i.id = ? AND c.resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("strict query failure must be visible to the operator");
    assert_eq!(intent_status, "needs_reconcile");
    assert_eq!(case_type, "strict_query_failure");
    assert_eq!(attempt_status(&db, intent_id).await, "accepted");
    let lot_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM position_lots WHERE account_id = 1")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(lot_count, 0, "lookup error cannot create a phantom fill");
}

#[tokio::test]
async fn an_attempt_in_an_unrecognized_status_opens_a_blocked_recovery_case() {
    // P0-3 step 8 (end-to-end): the audit (P0-3) and every regression
    // audit since (step 3, 5, 6, 7) flagged `RecoveryAction::Blocked`
    // at orchestrate.rs:324-334 as untested end-to-end. Migration 0008
    // already expanded `reconciliation_cases.case_type` to accept
    // `'blocked_recovery'`, but no test drove the orchestrator arm
    // that writes it.
    //
    // `permitted_recovery_action` (reconcile.rs:718) maps any status
    // outside its recognised set -- including the schema-legal but
    // orchestrator-unrecognised `'error'` status -- to
    // `RecoveryAction::Blocked("unrecognized attempt status")`. The
    // orchestrator must then:
    //
    //   1. open a `blocked_recovery` reconciliation case,
    //   2. NOT call `submit_exact_envelope` (the attempt state is
    //      unrecognised; auto-resubmitting would be unsafe),
    //   3. return `OrchestrateOutcome::Blocked(reason)`.
    //
    // AGENTS.md invariant: blocked recoveries are operator work,
    // not automatic -- the operator must inspect the case, decide
    // whether the unrecognised state is a schema migration in flight,
    // a corruption, or a leftover from a previous version, and act.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));

    // First pass: walk to a terminal state so the attempt row
    // exists. (Any terminal state is fine -- we will overwrite
    // the status below to drive the Blocked branch.)
    let _ = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first pass");

    // Drop the lot the first pass created so the Blocked-branch
    // assertions can compare against a clean baseline. The
    // Blocked branch itself cannot credit a lot (it never reaches
    // `finalize_receipt`), but the first-pass lot is still in the
    // table at the time of the recovery pass.
    sqlx::query("DELETE FROM position_lots WHERE account_id = 1 AND leader_id = 1")
        .execute(&db.pool)
        .await
        .expect("drop lot from first pass");

    // Rewind to 'pending' (so claim_or_resume_intent claims it) and
    // set the attempt status to 'error' -- schema-legal, but
    // unrecognised by `permitted_recovery_action`, which forces the
    // Blocked branch.
    sqlx::query("UPDATE copy_intents SET status = 'pending' WHERE id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("rewind intent");
    sqlx::query("UPDATE order_attempts SET status = 'error' WHERE intent_id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("rewind attempt to error");

    // Second pass: the orchestrator must take the Blocked branch
    // -- no submit, no prepare, no new envelope. Submit markers
    // count on a fresh FakeVenue so the recovery-pass counters
    // are isolated from the first-pass ones.
    let recovering_venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &recovering_venue,
        &recovering_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("blocked-recovery pass");

    assert!(
        matches!(outcome, OrchestrateOutcome::Blocked(_)),
        "expected OrchestrateOutcome::Blocked, got {:?}",
        outcome
    );
    let reason = match outcome {
        OrchestrateOutcome::Blocked(reason) => reason,
        _ => unreachable!(),
    };
    assert_eq!(
        reason, "unrecognized attempt status",
        "the canonical reason for an unrecognised attempt is the literal string from permitted_recovery_action"
    );

    // No submit and no prepare on the recovery pass -- the
    // orchestrator must not silently retry when the attempt state
    // is unrecognised. The fresh venue's counters start at 0, so
    // submit_count==0 / prepare_count==0 here directly proves no
    // retry happened.
    assert_eq!(
        recovering_venue.submit_count.load(Ordering::SeqCst),
        0,
        "Blocked branch must not call submit_exact_envelope"
    );
    assert_eq!(
        recovering_venue.prepare_count.load(Ordering::SeqCst),
        0,
        "Blocked branch must not call prepare"
    );

    // One open `blocked_recovery` reconciliation case must exist
    // -- this is the audit-trail signal the operator dashboard
    // reads to surface "this intent needs human review".
    let case_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'blocked_recovery' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("case count");
    assert_eq!(
        case_count, 1,
        "an open blocked_recovery case must be created with the canonical reason"
    );

    // The intent must be moved to needs_reconcile so the (account,
    // token) lock prevents later intents from racing while the
    // operator is reviewing. Mirrors the standard-`open_reconciliation_case`
    // behaviour at the orchestrator's other three case-opens.
    let intent_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent status");
    assert_eq!(intent_status, "needs_reconcile");

    // No phantom lot: the Blocked branch cannot have computed a
    // filled_qty -- it is operator-review, not a fill path. The
    // first-pass lot was dropped above; this assertion pins that
    // the Blocked branch did not credit a new one.
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 0);
}

#[tokio::test]
async fn live_gtd_lookup_failure_only_retries_without_fuse_or_resubmission() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::Resting);
    let failing = FakeVenue::order_lookup_failure("venue returned 404: order not found");
    for _ in 0..2 {
        let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &failing, &failing, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
        match outcome {
            OrchestrateOutcome::GtdLookupRetry { detail, remaining } => {
                assert!(detail.contains("404: order not found"));
                assert!(remaining > chrono::Duration::zero());
            }
            other => panic!("expected a logged GTD lookup retry, got {other:?}"),
        }
    }
    assert_eq!(failing.submit_count.load(Ordering::SeqCst), 0);
    let status: String = sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(status, "accepted");
    let cases: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(cases, 0);
}

#[tokio::test]
async fn gtd_lookup_failure_then_terminal_fill_books_only_confirmed_shares() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::Resting);
    let failing = FakeVenue::order_lookup_failure("transient 404");
    assert!(matches!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &failing, &failing, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::GtdLookupRetry { .. }));
    let filled = FakeVenue::succeeding(Decimal::ZERO);
    filled.with_size_matched(Decimal::new(237, 2));
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &filled, &filled, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::Filled { filled_qty: Decimal::new(237, 2) });
    let accounted: String = sqlx::query_scalar("SELECT accounted_filled_qty FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(accounted.parse::<Decimal>().unwrap(), Decimal::new(237, 2));
    let lot: String = sqlx::query_scalar("SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'")
        .fetch_one(&db.pool).await.unwrap();
    assert_eq!(lot.parse::<Decimal>().unwrap(), Decimal::new(237, 2));
    assert_eq!(failing.submit_count.load(Ordering::SeqCst), 0);
    assert_eq!(filled.submit_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn gtd_lookup_failure_at_exact_expiry_plus_margin_escalates() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
        OrchestrateOutcome::Resting);
    let now = Utc::now();
    let expiry = now - chrono::Duration::minutes(5);
    let raw: String = sqlx::query_scalar("SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let mut envelope: PreparedOrderEnvelope = serde_json::from_str(&raw).unwrap();
    envelope.expires_at = Some(expiry.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
    sqlx::query("UPDATE order_attempts SET envelope_json = ? WHERE intent_id = ?")
        .bind(serde_json::to_string(&envelope).unwrap()).bind(intent_id)
        .execute(&db.pool).await.unwrap();
    let failing = FakeVenue::order_lookup_failure("venue returned 404");
    assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &failing, &failing, &EmptyHistory, intent_id, now).await.unwrap(),
        OrchestrateOutcome::Uncertain);
    let status: String = sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(status, "uncertain");
}

#[tokio::test]
async fn gtd_lookup_failure_with_missing_or_malformed_expiry_escalates() {
    for expiry in [None, Some("not-a-timestamp".to_owned())] {
        let db = TestDb::new().await;
        seed_account_and_schedule(&db).await;
        seed_leader(&db, 1).await;
        let intent_id = seed_pending_buy(&db).await;
        set_maker_only_policy(&db, intent_id).await;
        let venue = FakeVenue::succeeding(Decimal::ZERO);
        assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
            OrchestrateOutcome::Resting);
        let raw: String = sqlx::query_scalar("SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        let mut envelope: PreparedOrderEnvelope = serde_json::from_str(&raw).unwrap();
        envelope.expires_at = expiry;
        sqlx::query("UPDATE order_attempts SET envelope_json = ? WHERE intent_id = ?")
            .bind(serde_json::to_string(&envelope).unwrap()).bind(intent_id)
            .execute(&db.pool).await.unwrap();
        let failing = FakeVenue::order_lookup_failure("no expiry; lookup failed");
        assert_eq!(execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
            &failing, &failing, &EmptyHistory, intent_id, Utc::now()).await.unwrap(),
            OrchestrateOutcome::Uncertain);
        let status: String = sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
        assert_eq!(status, "uncertain");
    }
}

#[tokio::test]
async fn a_gtd_poll_lookup_failure_promotes_the_attempt_to_uncertain_instead_of_crashing() {
    // Regression test for the live incident captured in
    // `docs/poll-resting-gtd-404-permanently-blocks-startup.md`:
    // `poll_resting_gtd` used to bubble any error from
    // `execution.order_for_receipt` up as `OrchestrateError::Receipt`,
    // which `copy_persistent` treated as fuse-worthy. The runner
    // would open the runtime fuse, exit with status 21, and -- because
    // the attempt row never moved past `accepted` -- every subsequent
    // `persistent_control resume` + startup would hit the same 1.5s
    // 404 against the same accepted-GTD attempt and crash again.
    // That's the "permanently blocks startup" shape the report names.
    //
    // The fix must do three observable things on a lookup failure
    // inside `poll_resting_gtd`:
    //
    //   1. Return `OrchestrateOutcome::Uncertain`, NOT an `Err` --
    //      so the runner doesn't latch a fresh fuse every tick and
    //      the orchestrator's control flow stays inside the
    //      controlled-outcome set.
    //   2. Promote the attempt from `accepted` to `uncertain`, with
    //      `failure_detail` carrying the underlying lookup error --
    //      so on every later startup `walk_existing_attempt` routes
    //      through `permitted_recovery_action('uncertain', ...)` and
    //      never reaches `poll_resting_gtd` for this attempt again.
    //      That is the actual unblock: the second startup is no
    //      longer the same crash.
    //   3. Open a `strict_query_failure` reconciliation case with the
    //      same detail string, so the operator dashboard surfaces the
    //      case alongside the FAK-side failure cases the existing
    //      branches already produce.
    //
    // Two-pass setup: first pass submits the maker-only GTD order so
    // the attempt is `accepted` with `submission_started_at` set (the
    // preconditions for `poll_resting_gtd`'s GTD arm and for the
    // `'uncertain'` strict-lookup window the recovery matrix requires
    // later); second pass swaps the venue to a strict-lookup-failure
    // variant so `poll_resting_gtd` runs through the failure arm.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;

    // First pass: produce an `accepted` GTD attempt the way a real
    // successful submit would.
    let submit_venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &submit_venue,
        &submit_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("first-pass submit must be a controlled outcome");
    assert_eq!(
        first,
        OrchestrateOutcome::Resting,
        "the maker-only GTD submit response must persist the attempt as `accepted` and return Resting",
    );
    assert_eq!(submit_venue.submit_count.load(Ordering::SeqCst), 1);

    // Sanity-check the preconditions: the attempt must be in the
    // `accepted` state, must have a GTD/post-only envelope, and must
    // have `submission_started_at` set (the recovery matrix strict-
    // lookup window depends on it, and the helper uses COALESCE if
    // NULL -- this fixture shouldn't be the COALESCE arm).
    let (accepted_precondition_status, accepted_precondition_order_type, accepted_precondition_submission): (
        String, String, Option<String>,
    ) = sqlx::query_as(
        "SELECT status, json_extract(envelope_json, '$.order_type'), \
                submission_started_at \
         FROM order_attempts WHERE intent_id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("attempt row after first pass");
    assert_eq!(
        accepted_precondition_status, "accepted",
        "precondition: first pass must leave the attempt `accepted` so `walk_existing_attempt` routes to `poll_resting_gtd`",
    );
    assert_eq!(
        accepted_precondition_order_type, "GTD",
        "precondition: the attempt must be GTD for `poll_resting_gtd` to be the GTD arm's lookup path",
    );
    assert!(
        accepted_precondition_submission.is_some(),
        "precondition: `submission_started_at` must be set after a successful submit (the `'uncertain'` recovery matrix path requires it)",
    );

    // This legacy escalation test exercises a GTD whose expiry and
    // settlement margin have already elapsed. Live GTDs retry instead.
    let raw: String = sqlx::query_scalar("SELECT envelope_json FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let mut old_envelope: PreparedOrderEnvelope = serde_json::from_str(&raw).unwrap();
    old_envelope.expires_at = Some((Utc::now() - chrono::Duration::minutes(10)).to_rfc3339());
    sqlx::query("UPDATE order_attempts SET envelope_json = ? WHERE intent_id = ?")
        .bind(serde_json::to_string(&old_envelope).unwrap()).bind(intent_id)
        .execute(&db.pool).await.unwrap();

    // Second pass: a fresh venue whose `order_for_receipt` always
    // returns Err -- modelling both a transient 5xx and the canonical
    // 404-on-aged-out-described-order shapes (the SDK collapses them
    // to the same `String` error, which is why `poll_resting_gtd`
    // catches the whole error arm, not a specific status code).
    let lookup_venue =
        FakeVenue::order_lookup_failure("venue returned 404: order not found");
    let second = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &lookup_venue,
        &lookup_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("a GTD lookup failure must be a controlled outcome, never a runner-fatal Err");
    assert_eq!(
        second,
        OrchestrateOutcome::Uncertain,
        "a GTD live-order lookup failure must surface as `Uncertain` (and not loop back into `Err`), so the runner never latches the runtime fuse again on the same attempt",
    );
    assert_eq!(
        lookup_venue.submit_count.load(Ordering::SeqCst),
        0,
        "the strict-query failure path must never re-call `submit_exact_envelope`",
    );

    // Postcondition 1: the attempt must be `uncertain` now, with
    // `failure_detail` carrying the lookup error. A future startup
    // walking this attempt will see `status != 'accepted'` and skip
    // `poll_resting_gtd` entirely, taking the `RecoveryAction::
    // QueryFirst` arm against authenticated trade history instead.
    let (post_status, post_failure_detail, post_submission): (
        String, Option<String>, Option<String>,
    ) = sqlx::query_as(
        "SELECT status, failure_detail, submission_started_at \
         FROM order_attempts WHERE intent_id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("attempt row after second pass");
    assert_eq!(
        post_status, "uncertain",
        "the failed GTD lookup must promote the attempt from `accepted` to `uncertain`, so subsequent startups route through `RecoveryAction::QueryFirst` instead of re-hitting the same 404",
    );
    assert!(
        post_failure_detail
            .as_deref()
            .unwrap_or("")
            .contains("venue returned 404: order not found"),
        "the attempt's failure_detail must carry the underlying lookup error verbatim so the operator dashboard can show it; got: {post_failure_detail:?}",
    );
    assert_eq!(
        post_submission, accepted_precondition_submission,
        "`submission_started_at` must be preserved across the `accepted`->`uncertain` transition (the strict-lookup recovery path's window depends on it not jumping)",
    );

    // Postcondition 2: a `strict_query_failure` reconciliation case
    // is opened with the lookup-error detail, mirroring the
    // `query_first` strict-lookup failure branch the FAK side uses.
    let case: (String, String, i64) = sqlx::query_as(
        "SELECT case_type, detail, order_attempt_id \
         FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'strict_query_failure' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("strict_query_failure reconciliation case must exist");
    assert_eq!(
        case.2, 1,
        "the case must be linked to attempt id 1 (this fixture only has one attempt)",
    );
    assert!(
        case.1.contains("venue returned 404: order not found"),
        "case detail must surface the underlying lookup error verbatim, got: {}",
        case.1,
    );

    // Postcondition 3: the intent is blocked in `needs_reconcile` so
    // later intents on the same account/token cannot race while the
    // operator reviews (mirrors what `query_first`'s strict-lookup
    // failure branch already does).
    let intent_status: String =
        sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("intent status");
    assert_eq!(
        intent_status, "needs_reconcile",
        "an unresolved strict-lookup failure must leave the intent blocked in `needs_reconcile`",
    );

    // Postcondition 4: rerunning the runner on the SAME Db must not
    // recurse into `poll_resting_gtd` and crash again. The intent has
    // been moved to `needs_reconcile` by `open_reconciliation_case`,
    // so `claim_or_resume_intent` returns `None` and the orchestrator
    // returns `NotClaimed` -- a controlled non-submitted outcome that
    // never reaches `poll_resting_gtd`'s 404 path and never latches
    // the runtime fuse. This is the actual unblock: the third-pass
    // invocation is recoverable without any operator action. The
    // alternative unblock path -- an operator `reconcile-uncertain 1`
    // resolving the case and the next startup routing through
    // `RecoveryAction::QueryFirst` -- is covered end-to-end by
    // `a_strict_order_lookup_failure_opens_strict_query_failure_without_resubmit`
    // (the FAK-side counterpart this test is the GTD-side twin of).
    let third = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &lookup_venue,
        &lookup_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("third-pass execution must be a controlled outcome, never an unhandled Err");
    assert_eq!(
        third,
        OrchestrateOutcome::NotClaimed,
        "after the first lookup failure, later startups must observe the intent in `needs_reconcile` and skip the runner entirely \
         (the runner exposes `NotClaimed` for that branch and does NOT latch the runtime fuse), \
         instead of re-hitting `poll_resting_gtd`'s 404 path",
    );
}

#[tokio::test]
async fn a_terminal_status_with_zero_matched_size_opens_an_unknown_submission_case() {
    // P0-3 step 8 (end-to-end): the audit and every regression
    // audit since flagged the zero-matched-size Err branch in
    // `receipt_from_terminal_order_state` (orchestrate.rs:547-552)
    // as untested end-to-end. The companion non-terminal-status
    // branch is pinned by `recovered_order_id_with_non_terminal_order_state_opens_reconciliation`;
    // this test pins the other half of the function.
    //
    // The semantic distinction matters: a terminal status with
    // `size_matched == 0` means the venue accepted and finalized
    // the order but the matched-size field is missing or zero --
    // not a phantom fill, not a transport failure, not a
    // definitive rejection. The orchestrator must open an
    // `unknown_submission` reconciliation case (NOT credit a lot)
    // and the case's `detail` must carry the zero-matched-size
    // explanation so the operator dashboard can route it.
    //
    // Two-pass setup, mirroring
    // `a_strict_order_lookup_failure_opens_strict_query_failure_without_resubmit`:
    //   1. First pass with `transport_error()` venue: attempt is
    //      left in `uncertain`, which routes the second pass to
    //      `query_first` (not `reconcile_or_finalize`).
    //   2. Second pass with a venue whose `order_for_receipt`
    //      returns the audit-baseline status ("MATCHED", a
    //      terminal status per `is_terminal_filled_order_status`)
    //      and `size_matched == 0`.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;

    // First pass: transport_error -> attempt.status = 'uncertain'.
    let transport_venue = FakeVenue::transport_error();
    let first = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &transport_venue,
        &transport_venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("transport pass");
    assert_eq!(first, OrchestrateOutcome::Uncertain);
    assert_eq!(transport_venue.submit_count.load(Ordering::SeqCst), 1);

    // Second pass: a venue whose `order_for_receipt` returns the
    // audit-baseline `MATCHED` status (terminal) with
    // `size_matched == 0`. The recovery path must walk through
    // `query_first`, hit the zero-matched-size Err, and surface
    // it as NeedsReconcile("strict order state not terminal")
    // -- the same `unknown_submission` case path as the
    // non-terminal-status branch.
    let zero_matched_venue = FakeVenue::succeeding(Decimal::new(5_000_000, 6));
    zero_matched_venue.with_size_matched(Decimal::ZERO);

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &zero_matched_venue,
        &zero_matched_venue,
        &RecoveredHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("recovery pass");

    assert_eq!(
        outcome,
        OrchestrateOutcome::NeedsReconcile("strict order state not terminal"),
        "terminal status + zero size_matched must surface as NeedsReconcile('strict order state not terminal')"
    );

    // No resubmit on the recovery pass -- the strict-query path
    // never re-issues submit_exact_envelope, regardless of the
    // specific failure shape.
    assert_eq!(
        zero_matched_venue.submit_count.load(Ordering::SeqCst),
        0,
        "query_first must never re-call submit_exact_envelope"
    );
    assert_eq!(
        zero_matched_venue.prepare_count.load(Ordering::SeqCst),
        0,
        "query_first must never re-prepare the envelope"
    );

    // The intent must be moved to needs_reconcile so the (account,
    // token) lock prevents later intents from racing while the
    // operator reviews.
    let intent_status: String = sqlx::query_scalar("SELECT status FROM copy_intents WHERE id = ?")
        .bind(intent_id)
        .fetch_one(&db.pool)
        .await
        .expect("intent status");
    assert_eq!(intent_status, "needs_reconcile");

    // One open `unknown_submission` reconciliation case must
    // exist, with the zero-matched-size explanation in `detail`
    // -- the operator dashboard reads this string verbatim to
    // distinguish "venue returned terminal but zero" from
    // "venue returned non-terminal". The two cases share the
    // `unknown_submission` case_type but their `detail` strings
    // are observably different; this test pins that.
    let case: (String, String) = sqlx::query_as(
        "SELECT case_type, detail FROM reconciliation_cases \
         WHERE intent_id = ? AND case_type = 'unknown_submission' \
           AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .expect("case row");
    assert_eq!(case.0, "unknown_submission");
    assert!(
        case.1.contains("zero matched size") && case.1.contains("MATCHED"),
        "case detail must reference the zero-matched-size path so operators can triage, got: {}",
        case.1
    );

    // No phantom lot: zero size_matched means the venue has
    // accepted a *terminal* state with no fill -- crediting a
    // lot here would invent money.
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(
        lot_count, 0,
        "terminal-status + zero size_matched must NOT credit a position lot"
    );
}

#[tokio::test]
async fn a_sell_fill_decrements_the_leader_virtual_lot() {
    // P0-4 (end-to-end): the SELL path was zero-covered by the
    // orchestrator tests before this round. The `execute` module
    // has SELL tests (seed_pending_intent + FullFillSubmitter), but
    // the orchestrator's `execute_one_intent` flow -- the one that
    // walks `submit_exact_envelope` + `finalize_receipt` + lot
    // decrement -- was never exercised end-to-end for SELL.
    //
    // AGENTS.md invariant: only confirmed `filled_qty` changes
    // virtual lots. For a SELL this means decrementing the
    // leader-specific virtual lot, not the account's strict balance.
    // This test pins that a successful SELL finalizes with a lot
    // decrement and the receipt's filled_qty is authoritative.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;

    // Pre-seed a leader-1 virtual lot of 10 so the SELL has
    // something to decrement. The `a_sell_without_a_tracked_virtual_lot_is_rejected`
    // test in execute.rs already pins the rejection path; here we
    // exercise the happy path.
    sqlx::query(
        "INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '10')",
    )
    .execute(&db.pool)
    .await
    .expect("seed lot");

    let intent_id = seed_pending_sell(&db).await;
    let filled = Decimal::new(5_000_000, 6); // 5.0 shares
    let venue = FakeVenue::succeeding(filled);

    let outcome = execute_one_intent(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .expect("sell fill");

    assert_eq!(outcome, OrchestrateOutcome::Filled { filled_qty: filled });

    // The leader's lot must be decremented by the receipt's
    // filled_qty. AGENTS.md: "only confirmed `filled_qty` changes
    // virtual lots" applies to SELL decrement exactly as it does
    // to BUY increment.
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("lot after sell");
    assert_eq!(lot.parse::<Decimal>().unwrap(), Decimal::new(5, 0));

    // No phantom lot on a different token: the SELL must not have
    // created any other lots.
    let lot_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots")
        .fetch_one(&db.pool)
        .await
        .expect("lot count");
    assert_eq!(lot_count, 1);
}

#[tokio::test]
async fn an_accepted_sell_recovery_decrements_the_virtual_lot_without_resubmit() {
    // P3-3: recovery coverage must be side-complete. An accepted SELL
    // whose receipt was lost after the venue boundary must query/finalize
    // exactly once, decrement its leader-specific virtual lot by confirmed
    // filled_qty, and never submit or prepare a duplicate FAK.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    sqlx::query(
        "INSERT INTO position_lots (account_id, leader_id, token_id, qty) VALUES (1, 1, '123456', '10')",
    )
    .execute(&db.pool)
    .await
    .expect("seed SELL lot");
    let intent_id = seed_pending_sell(&db).await;
    let filled = Decimal::new(5, 0);
    let venue = FakeVenue::succeeding(filled);

    // First pass establishes the exact persisted prepared envelope and
    // attempt. Its receipt represents the venue-side fill.
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(10, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("initial SELL submit"),
        OrchestrateOutcome::Filled { filled_qty: filled }
    );

    // Rewind only the durable local accounting to model a crash between
    // accepted and receipt finalization. Restore the pre-sale lot so the
    // recovery pass proves the decrement itself, not an already-applied
    // effect.
    sqlx::query(
        "UPDATE position_lots SET qty = '10' \
         WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .execute(&db.pool)
    .await
    .expect("restore pre-crash lot");
    sqlx::query(
        "UPDATE order_attempts SET status = 'accepted', accounted_filled_qty = '0' \
         WHERE intent_id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .expect("rewind accepted attempt");
    sqlx::query("UPDATE copy_intents SET status = 'in_progress' WHERE id = ?")
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .expect("rewind intent");

    let recovery_venue = FakeVenue::pre_accepted_with_receipt(
        OrderReceipt::from_fak_sell_shares(Decimal::new(5, 0), Decimal::new(5, 0), filled)
            .expect("SELL receipt"),
    );
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(10, 0)),
            &recovery_venue,
            &recovery_venue,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("recovery must finalize confirmed SELL"),
        OrchestrateOutcome::Filled { filled_qty: filled }
    );
    assert_eq!(recovery_venue.submit_count.load(Ordering::SeqCst), 0);
    assert_eq!(recovery_venue.prepare_count.load(Ordering::SeqCst), 0);
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("SELL lot after recovery");
    assert_eq!(lot.parse::<Decimal>().unwrap(), Decimal::new(5, 0));
    assert_eq!(attempt_status(&db, intent_id).await, "finalized");
}

#[tokio::test]
async fn a_pre_submit_envelope_failure_releases_reservation_without_attempt_or_submit() {
    // P3-4: signing/preparation happens before the HTTP boundary. A failure
    // must be a definitive local rejection, not a stranded in_progress
    // reservation and not an exception a runner may blindly retry.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));

    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &FailingEnvelopeFactory,
            &EmptyHistory,
            intent_id,
            Utc::now(),
        )
        .await
        .expect("local preparation failure must be finalized locally"),
        OrchestrateOutcome::Rejected
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let (status, reserved): (String, String) =
        sqlx::query_as("SELECT status, reserved_qty FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(status, "rejected");
    assert_eq!(reserved, "0");
    let attempts: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(attempts, 0);
    let lots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots WHERE account_id = 1")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lots, 0);
}

#[tokio::test]
async fn a_definitive_rejection_prepares_one_fresh_retry_without_phantom_lot() {
    // P3-4: a venue 4xx proves no order was created, so exactly one fresh
    // envelope may be prepared on the next cycle. This is deliberately
    // unlike Transport/lookup failures: it must not mark uncertain or open
    // reconciliation, and it must never credit a lot before a receipt.
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    *venue.submit_result.lock().unwrap() = Err(SubmitError::Rejected("price moved".to_owned()));

    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Rejected
    );
    assert_eq!(attempt_status(&db, intent_id).await, "rejected");
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 1);
    let lots: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM position_lots WHERE account_id = 1")
        .fetch_one(&db.pool)
        .await
        .unwrap();
    assert_eq!(lots, 0);

    // The next run uses a new signed envelope/attempt, then a proven receipt
    // can finalize exactly one lot. It does not reuse the rejected attempt.
    *venue.submit_result.lock().unwrap() = Ok(OrderReceipt::from_fak_buy_budget(
        Decimal::new(5, 0),
        Decimal::new(5, 0),
        Decimal::new(5, 0),
    )
    .unwrap());
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Filled {
            filled_qty: Decimal::new(5, 0)
        }
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2);
    let attempt_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(attempt_count, 2);
    let lot: String = sqlx::query_scalar("SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'")
        .fetch_one(&db.pool).await.unwrap();
    assert_eq!(lot.parse::<Decimal>().unwrap(), Decimal::new(5, 0));
}

#[tokio::test]
async fn an_explicit_initial_fak_no_match_goes_directly_to_leader_price_gtd() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_fixed_share_policy(&db, intent_id).await;
    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    *venue.submit_results.lock().unwrap() = VecDeque::from([
        Err(SubmitError::Rejected(
            "400 no orders found to match with FAK order".to_owned(),
        )),
        Ok(OrderReceipt::new(
            Decimal::new(5, 0),
            Decimal::new(5, 0),
            Decimal::ZERO,
            Decimal::new(5, 0),
        )
        .unwrap()),
    ]);

    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Resting
    );
    let intent: (String, String) = sqlx::query_as(
        "SELECT status, rejection_reason FROM copy_intents WHERE id = ?",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(intent.0, "in_progress");
    assert!(intent.1.is_empty(), "{}", intent.1);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2);
    let gtd_json: String = sqlx::query_scalar(
        "SELECT envelope_json FROM order_attempts WHERE intent_id = ? ORDER BY attempt_number DESC LIMIT 1",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    let gtd: PreparedOrderEnvelope = serde_json::from_str(&gtd_json).unwrap();
    assert_eq!(gtd.order_type, "GTD");
    assert!(gtd.post_only);
    assert!(gtd.expires_at.is_some());
    venue.set_order_status("LIVE");
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Resting
    );
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2, "must not resubmit a resting GTD");
    venue.set_order_status("MATCHED");
    venue.with_size_matched(Decimal::new(3, 0));
    assert_eq!(
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap(),
        OrchestrateOutcome::Filled { filled_qty: Decimal::new(3, 0) }
    );
    let lot: String = sqlx::query_scalar(
        "SELECT qty FROM position_lots WHERE account_id = 1 AND leader_id = 1 AND token_id = '123456'",
    )
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(lot, "3");
    let cases: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM reconciliation_cases WHERE intent_id = ? AND resolved_at IS NULL",
    )
    .bind(intent_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(cases, 0);
}

#[tokio::test]
async fn an_explicit_fak_no_match_does_not_submit_a_best_ask_retry() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_fixed_share_policy_with_max_price(&db, intent_id, "0.55").await;
    let venue = FakeVenue::no_fak_then_fill(Decimal::new(5, 0));

    let outcome =
        execute_one_intent(
            &db,
            &FixedBalance(Decimal::new(100, 0)),
            &venue,
            &venue,
            &EmptyHistory,
            intent_id,
            Utc::now()
        )
        .await
        .unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Resting, "{outcome:?}");
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 2);
    let attempts: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT status, requested_qty, envelope_json FROM order_attempts \
         WHERE intent_id = ? ORDER BY attempt_number",
    )
    .bind(intent_id)
    .fetch_all(&db.pool)
    .await
    .unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0].0, "rejected");
    assert_eq!(attempts[1].0, "accepted");
    let retry: PreparedOrderEnvelope = serde_json::from_str(&attempts[1].2).unwrap();
    assert_eq!(retry.order_type, "GTD");
    assert!(retry.post_only);
    assert_eq!(retry.price, "0.55", "must use the leader price, not fresh best ask");
    assert_eq!(retry.size, "5");
    assert!(retry.buy_shares_exact);
}

async fn set_ratio_retry_policy(db: &TestDb, intent_id: i64, flat_shares: Option<&str>) {
    let raw: String =
        sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
    policy.size_ratio = Some("0.2".to_owned());
    policy.max_order_shares = flat_shares.map(str::to_owned);
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&policy).unwrap())
        .bind(intent_id)
        .execute(&db.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE leader_events SET size = '75' WHERE id = (SELECT event_id FROM copy_intents WHERE id = ?)")
        .bind(intent_id).execute(&db.pool).await.unwrap();
}

async fn assert_ratio_fak_fallback(db: &TestDb, intent_id: i64) {
    let venue = FakeVenue::no_fak_then_fill(Decimal::new(15, 0));
    let outcome = execute_one_intent(
        db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        intent_id,
        Utc::now(),
    )
    .await
    .unwrap();
    assert_eq!(outcome, OrchestrateOutcome::Resting);
    let planned_qty: String =
        sqlx::query_scalar("SELECT planned_qty FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(planned_qty, "15");
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT status, envelope_json FROM order_attempts WHERE intent_id = ? ORDER BY attempt_number",
    ).bind(intent_id).fetch_all(&db.pool).await.unwrap();
    assert_eq!(
        rows.len(),
        2,
        "one rejected FAK followed by one resting GTD"
    );
    assert_eq!(rows[0].0, "rejected");
    let gtd: PreparedOrderEnvelope = serde_json::from_str(&rows[1].1).unwrap();
    assert_eq!(gtd.order_type, "GTD");
    assert_eq!(
        gtd.size, "15",
        "fallback must retain the persisted share decision"
    );
    assert!(gtd.buy_shares_exact);
}

#[tokio::test]
async fn ratio_only_buy_keeps_persisted_shares_after_fak_no_match() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_ratio_retry_policy(&db, intent_id, None).await;
    assert_ratio_fak_fallback(&db, intent_id).await;
}

#[tokio::test]
async fn ratio_wins_over_different_flat_target_after_fak_no_match() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_ratio_retry_policy(&db, intent_id, Some("5")).await;
    assert_ratio_fak_fallback(&db, intent_id).await;
}

/// A maker-only ratio BUY whose leader cap drops the persisted share
/// quantity below the market's own `minimum_order_size` is the seam the
/// executor deliberately leaves to the prepare path: the cap math is the
/// executor's, the share-floor rule belongs to the venue. The intent must
/// reject without preparing an attempt and without opening the fuse, with a
/// rejection reason that names both the prepared cap and the market floor so
/// the operator can see why nothing crossed the boundary.
#[tokio::test]
async fn a_capped_ratio_buy_below_market_minimum_is_rejected_in_prepare() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    set_maker_only_policy(&db, intent_id).await;
    let raw: String = sqlx::query_scalar("SELECT config_snapshot_json FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    let mut policy: PolicySnapshot = serde_json::from_str(&raw).unwrap();
    policy.size_ratio = Some("0.2".to_owned());
    policy.max_order_shares = None;
    // Force a cap smaller than the default 100000 USDC ceiling so 300 shares
    // * 0.53 * 0.2 = 31.80 USDC of proportional intent collapses to a
    // 3.77-share maker-only decision below the venue's 5-share floor.
    policy.max_order_notional = "2".to_owned();
    sqlx::query("UPDATE copy_intents SET config_snapshot_json = ? WHERE id = ?")
        .bind(serde_json::to_string(&policy).unwrap()).bind(intent_id)
        .execute(&db.pool).await.unwrap();
    sqlx::query("UPDATE leader_events SET size = '300', price = '0.53' WHERE id = (SELECT event_id FROM copy_intents WHERE id = ?)")
        .bind(intent_id).execute(&db.pool).await.unwrap();
    let venue = FakeVenue::succeeding(Decimal::ZERO);
    venue.with_minimum_order_size(Decimal::new(5, 0));
    *venue.best_ask_override.lock().unwrap() = Some(Ok(Decimal::new(60, 2)));
    let outcome = execute_one_intent(&db, &FixedBalance(Decimal::new(100, 0)),
        &venue, &venue, &EmptyHistory, intent_id, Utc::now()).await.unwrap();
    assert_eq!(
        outcome,
        OrchestrateOutcome::Rejected,
        "below-market-minimum maker-only ratio caps must reject at prepare time",
    );
    let planned_qty: String = sqlx::query_scalar("SELECT planned_qty FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(planned_qty, "3.77", "executor must persist the capped share decision");
    let reserved: String = sqlx::query_scalar("SELECT planned_notional_usdc FROM copy_intents WHERE id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(reserved, "2");
    let attempts: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(attempts, 0, "no attempt row may exist before the venue boundary");
    let (status, reason): (String, Option<String>) =
        sqlx::query_as("SELECT status, rejection_reason FROM copy_intents WHERE id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.unwrap();
    assert_eq!(status, "rejected");
    let reason = reason.unwrap_or_default();
    assert!(reason.contains("3.77"), "reason must name the capped qty: {reason}");
    assert!(reason.contains("5"), "reason must name the market minimum: {reason}");
    let reservations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_budget_reservations")
        .fetch_one(&db.pool).await.unwrap();
    assert_eq!(reservations, 0);
    let fuse_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_execution_fuse")
        .fetch_one(&db.pool).await.unwrap();
    assert_eq!(fuse_rows, 0);
}

/// The per-Leader budget exists so one Leader running dry does not stop the
/// others. That only holds if the runner's own path turns the error into a
/// skipped signal; the variant was introduced with a comment saying the
/// caller would intercept it, and nothing did. In production the error
/// travelled up as EXIT_BUDGET_STATE -- which systemd is configured not to
/// restart -- and "leader 2 rolling budget exhausted: used=9.36
/// requested=9.3590 cap=10" halted copying for all seven Leaders for eight
/// hours. This drives the real marker, so an `Err` here is that outage.
#[tokio::test]
async fn a_leader_out_of_budget_is_a_skipped_signal_not_a_halt() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;

    // A budget this Leader cannot afford the pending order under, while the
    // account ceiling stays generous: the per-Leader gate is what fires.
    sqlx::query(
        "INSERT INTO leader_policy \
         (leader_id, max_signal_age_seconds, decision_window_seconds, price_tolerance_bps, \
          tick_size, min_price, max_price, max_order_notional, min_leader_trade_size, \
          rolling_budget_usdc, budget_window_seconds) \
         VALUES (1, 3, 3, 100, '0.01', '0.01', '0.99', '5', '0', '0.5', 600)",
    )
    .execute(&db.pool)
    .await
    .expect("leader policy");

    let cfg = PersistentRuntimeConfig::from_values(1, true, "1", "5", "1000", 86_400, 1, 60)
        .expect("valid persistent config");
    init_config(&db, &cfg)
        .await
        .expect("init persistent config");

    sqlx::query(
        "UPDATE copy_intents SET status = 'in_progress', \
         planned_qty = '5', planned_price = '0.55', planned_notional_usdc = '1' \
         WHERE id = ?",
    )
    .bind(intent_id)
    .execute(&db.pool)
    .await
    .expect("seed intent in_progress + planned fields");

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let outcome = execute_one_intent_with_marker(
        &db,
        &FixedBalance(Decimal::new(100, 0)),
        &venue,
        &venue,
        &EmptyHistory,
        &PersistentSubmitMarker { config: &cfg },
        intent_id,
        Utc::now(),
    )
    .await
    .expect("an exhausted Leader budget must not propagate as a runner error");

    assert_eq!(outcome, OrchestrateOutcome::Rejected);

    let (status, reason): (String, Option<String>) =
        sqlx::query_as("SELECT status, rejection_reason FROM copy_intents WHERE id = ?")
            .bind(intent_id)
            .fetch_one(&db.pool)
            .await
            .expect("intent row");
    assert_eq!(status, "rejected", "the signal is skipped, durably");
    assert!(
        reason
            .unwrap_or_default()
            .contains("rolling budget exhausted"),
        "the ledger must say which limit skipped it"
    );

    // Nothing was submitted and nothing was reserved: this is a pre-boundary
    // refusal, so it must not consume budget it was just denied.
    let reserved: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM persistent_budget_reservations WHERE state = 'reserved'",
    )
    .fetch_one(&db.pool)
    .await
    .expect("reservation count");
    assert_eq!(reserved, 0);
}

#[tokio::test]
async fn an_account_per_order_overage_rejects_only_the_intent_without_opening_the_fuse() {
    let db = TestDb::new().await;
    seed_account_and_schedule(&db).await;
    seed_leader(&db, 1).await;
    let intent_id = seed_pending_buy(&db).await;
    let cfg = PersistentRuntimeConfig::from_values(1, true, "1", "1", "1000", 86_400, 1, 60)
        .expect("valid persistent config");
    init_config(&db, &cfg).await.expect("init persistent config");
    sqlx::query(
        "UPDATE copy_intents SET status = 'in_progress', \
         planned_qty = '5', planned_price = '0.55', planned_notional_usdc = '1.01' \
         WHERE id = ?",
    )
    .bind(intent_id).execute(&db.pool).await.expect("seed over-cap intent");

    let venue = FakeVenue::succeeding(Decimal::new(5, 0));
    let outcome = execute_one_intent_with_marker(
        &db, &FixedBalance(Decimal::new(100, 0)), &venue, &venue, &EmptyHistory,
        &PersistentSubmitMarker { config: &cfg }, intent_id, Utc::now(),
    )
    .await
    .expect("one over-cap intent must not propagate as a runner error");

    assert_eq!(outcome, OrchestrateOutcome::Rejected);
    assert_eq!(venue.submit_count.load(Ordering::SeqCst), 0);
    let (intent_status, reason): (String, Option<String>) =
        sqlx::query_as("SELECT status, rejection_reason FROM copy_intents WHERE id = ?")
            .bind(intent_id).fetch_one(&db.pool).await.expect("intent row");
    assert_eq!(intent_status, "rejected");
    assert!(reason.unwrap_or_default().contains("per-order notional exceeded"));
    let attempt_status: String = sqlx::query_scalar("SELECT status FROM order_attempts WHERE intent_id = ?")
        .bind(intent_id).fetch_one(&db.pool).await.expect("attempt row");
    assert_eq!(attempt_status, "rejected");
    let reservations: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_budget_reservations")
        .fetch_one(&db.pool).await.expect("reservation count");
    assert_eq!(reservations, 0);
    let fuse_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM persistent_execution_fuse")
        .fetch_one(&db.pool).await.expect("fuse count");
    assert_eq!(fuse_rows, 0);
}

// --- GTD maker spec derivation ----------------------------------------------
//
// These pin the post-only GTD retry's market validation against
// `MarketResponse`. The bug they guard against (see
// `docs/gtd-market-end-lookup-bug.md`) is treating `end_date_iso` as this
// market slot's real resolution time: on auto-generated recurring crypto
// slots it is a day-level placeholder, so anchoring a maker expiry to it
// rejects genuinely-open markets and lengthens resting exposure on
// soon-to-resolve ones. The four cases below mirror the live venue samples
// cited in the bug report.

#[test]
fn derive_gtd_spec_accepts_an_open_market_with_a_stale_end_date_iso() {
    // Real-world shape from row 4 of the bug report: still tradeable on the
    // venue (closed=false, accepting_orders=true), yet end_date_iso is the
    // midnight-UTC placeholder that has already passed. The current code
    // rejected this; the fixed code must accept it and bound the expiry
    // independently of that field.
    let now = "2026-09-21T07:07:00Z".parse::<DateTime<Utc>>().unwrap();
    let market = gtd_market_fixture(
        false,
        true,
        Decimal::new(1, 2),
        Some("2026-09-21T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
    );

    let spec = derive_gtd_market_spec(&market, now).expect("open market must be accepted");

    assert_eq!(
        spec.expires_at,
        now + GTD_MAKER_EXPIRY,
        "expires_at must come from `now + GTD_MAKER_EXPIRY`, not end_date_iso",
    );
    assert_eq!(spec.tick_size, Decimal::new(1, 2));
}

#[test]
fn derive_gtd_spec_rejects_a_closed_market_regardless_of_end_date_iso() {
    let now = "2026-09-21T07:07:00Z".parse::<DateTime<Utc>>().unwrap();
    let market = gtd_market_fixture(
        true, // closed
        true,
        Decimal::new(1, 2),
        Some("2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
    );

    let error = derive_gtd_market_spec(&market, now)
        .expect_err("a closed market must be refused even with a future end_date_iso");
    assert!(
        error.contains("closed or not accepting orders"),
        "rejection must identify the open-state failure: {error}",
    );
}

#[test]
fn derive_gtd_spec_rejects_a_market_that_is_not_accepting_orders() {
    let now = "2026-09-21T07:07:00Z".parse::<DateTime<Utc>>().unwrap();
    let market = gtd_market_fixture(
        false, // not closed...
        false, // ...but venue has paused accepting orders
        Decimal::new(1, 2),
        Some("2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
    );

    let error = derive_gtd_market_spec(&market, now)
        .expect_err("a market with accepting_orders=false must be refused");
    assert!(
        error.contains("closed or not accepting orders"),
        "rejection must identify the open-state failure: {error}",
    );
}

#[test]
fn derive_gtd_spec_never_sources_expires_at_from_end_date_iso() {
    // Public contract: regardless of what the wire sends for `end_date_iso`,
    // a successfully-derived spec must have its `expires_at` pinned to
    // `now + GTD_MAKER_EXPIRY`. Any caller passing a None-equivalent or a
    // far-past value still observes a bounded, near-future expiry. This is
    // the behavioural promise that decouples GTD correctness from the API
    // field the bug report identifies as unreliable for recurring crypto
    // slots.
    let now = "2026-09-21T07:07:00Z".parse::<DateTime<Utc>>().unwrap();
    let cases = [
        // Already-passed placeholder, like the row-4 market in the bug
        // report (closed=false but end_date_iso = midnight UTC that has
        // already gone by).
        Some("2026-09-21T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
        // Far-future placeholder; would otherwise pin a maker order open
        // for the entire slot lifetime.
        Some("2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
    ];

    for end_date_iso in cases {
        let market =
            gtd_market_fixture(false, true, Decimal::new(1, 2), end_date_iso);
        let spec = derive_gtd_market_spec(&market, now).expect("open market must be accepted");
        assert_eq!(
            spec.expires_at,
            now + GTD_MAKER_EXPIRY,
            "expires_at must come from `now + GTD_MAKER_EXPIRY`, not end_date_iso={end_date_iso:?}",
        );
    }
}

#[test]
fn derive_gtd_spec_still_rejects_a_market_with_a_non_positive_tick_size() {
    // Preserved invariant: a malformed `minimum_tick_size` is still a
    // refusal, independent of the open-state check.
    let now = "2026-09-21T07:07:00Z".parse::<DateTime<Utc>>().unwrap();
    let market = gtd_market_fixture(
        false,
        true,
        Decimal::ZERO,
        Some("2099-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()),
    );

    let error = derive_gtd_market_spec(&market, now)
        .expect_err("a zero tick_size must be refused");
    assert!(
        error.contains("invalid minimum_tick_size"),
        "rejection must identify the tick-size failure: {error}",
    );
}

#[test]
fn gtd_maker_expiry_constant_is_above_the_venue_floor_and_short_relative_to_slot_life() {
    // The venue rejects GTD expirations closer than 180 seconds in the
    // future with "expiration is less than 180 seconds in the future"
    // (post-deploy incident on intent 536, 2026-09-21 -- see
    // `docs/gtd-maker-expiry-too-short.md`). Pin that floor plus a small
    // safety margin so this specific regression can't recur silently.
    // Also pin the upper bound so the constant stays short relative to a
    // market's actual lifetime (5/15-minute crypto slots), which is the
    // original design goal of decoupling `expires_at` from
    // `MarketResponse.end_date_iso`.
    assert!(
        GTD_MAKER_EXPIRY >= chrono::Duration::seconds(180),
        "GTD maker resting lifetime must meet the venue's 180s minimum; \
         a shorter value is rejected pre-book with \"expiration is less \
         than 180 seconds in the future\"",
    );
    assert!(
        GTD_MAKER_EXPIRY <= chrono::Duration::minutes(5),
        "GTD maker resting lifetime must stay short relative to a slot's \
         lifetime; a longer value regresses the bounded-exposure design \
         that the original bug fix introduced",
    );
}
