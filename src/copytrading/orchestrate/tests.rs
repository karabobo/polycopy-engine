use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

use async_trait::async_trait;
use chrono::Utc;
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
        }])
    }
}

struct FakeVenue {
    prepare_count: AtomicU64,
    submit_count: AtomicU64,
    submit_result: Mutex<Result<OrderReceipt, SubmitError>>,
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
}

impl FakeVenue {
    fn succeeding(filled: Decimal) -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Ok(OrderReceipt::from_fak_buy_budget(
                Decimal::new(5, 0),
                Decimal::new(5, 0),
                filled,
            )
            .expect("receipt"))),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            // Default: `order_for_receipt` succeeds with the current
            // `order_status`. Tests that need a failing lookup must
            // call `order_lookup_failure(...)` instead.
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            // Default: `query_prepared_envelope` returns Ok(None).
            query_receipt_result: Mutex::new(None),
        }
    }

    fn transport_error() -> Self {
        Self {
            prepare_count: AtomicU64::new(0),
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Transport("connection reset".into()))),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            query_receipt_result: Mutex::new(None),
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
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Local(detail.to_owned()))),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
            query_receipt_result: Mutex::new(None),
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
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Ok(OrderReceipt::from_fak_buy_budget(
                Decimal::new(5, 0),
                Decimal::new(5, 0),
                Decimal::new(5, 0),
            )
            .expect("receipt"))),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(Some(Err(detail.to_owned()))),
            query_receipt_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
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
            submit_count: AtomicU64::new(0),
            // `submit_result` is unused on this path -- the attempt
            // is *already* accepted -- but seed it with the same
            // receipt so a future refactor that mistakenly routes
            // through `submit_exact_envelope` still produces a
            // matching receipt.
            submit_result: Mutex::new(Ok(receipt.clone())),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
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
            submit_count: AtomicU64::new(0),
            submit_result: Mutex::new(Err(SubmitError::Local(
                "submit_result unused on this path".to_owned(),
            ))),
            order_status: Mutex::new("MATCHED".to_owned()),
            last_salt: Mutex::new(None),
            order_lookup_result: Mutex::new(None),
            size_matched_override: Mutex::new(None),
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
}

impl EnvelopeFactory for FakeVenue {
    #[allow(clippy::manual_async_fn)]
    fn prepare(
        &self,
        decision: &SizedDecision,
    ) -> impl std::future::Future<Output = Result<PreparedOrderEnvelope, String>> + Send {
        let count = self.prepare_count.fetch_add(1, Ordering::SeqCst);
        let envelope = PreparedOrderEnvelope {
            token_id: decision.token_id.clone(),
            side: decision.side.as_str().to_owned(),
            price: decision.limit_price.to_string(),
            size: decision.qty.to_string(),
            salt: 1000 + count,
            order_type: "FAK".to_owned(),
            expected_taker_order_id: format!("0xdead{count}"),
            signed_order_json: r#"{"order":{}}"#.to_owned(),
        };
        async move { Ok(envelope) }
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
            .submit_result
            .lock()
            .expect("submit result lock")
            .clone();
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
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: "0.99".to_owned(),
        max_order_notional: "100000".to_owned(),
        min_leader_trade_size: "0".to_owned(),
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
        tick_size: "0.01".to_owned(),
        min_price: "0.01".to_owned(),
        max_price: "0.99".to_owned(),
        max_order_notional: "100000".to_owned(),
        min_leader_trade_size: "0".to_owned(),
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
            salt: 1,
            order_type: "FAK".to_owned(),
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
