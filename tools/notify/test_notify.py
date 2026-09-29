"""Tests for the Feishu notifier. Run: python3 -m unittest tools/notify/test_notify.py"""

import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import notify  # noqa: E402


class FakeSender:
    def __init__(self):
        self.sent = []

    def send(self, card, description, dedup=""):
        self.sent.append((card["header"]["title"]["content"], description, card))
        return True

    def titles(self):
        return [title for title, _, _ in self.sent]


def iso(seconds):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(seconds)) + ".000Z"


def budget_db(cap="600", window=86400):
    db = sqlite3.connect(":memory:")
    db.execute(
        "CREATE TABLE persistent_execution_config (id INTEGER PRIMARY KEY, account_id INTEGER,"
        " rolling_budget_usdc TEXT, budget_window_seconds INTEGER)"
    )
    db.execute(
        "CREATE TABLE persistent_budget_reservations (id INTEGER PRIMARY KEY, account_id INTEGER,"
        " amount_usdc TEXT, reserved_at TEXT, state TEXT)"
    )
    db.execute("INSERT INTO persistent_execution_config VALUES (1, 1, ?, ?)", (cap, window))
    return db


def reserve(db, amount, at, state="reserved", account=1):
    db.execute(
        "INSERT INTO persistent_budget_reservations (account_id, amount_usdc, reserved_at, state)"
        " VALUES (?, ?, ?, ?)",
        (account, str(amount), iso(at), state),
    )


def intent(**overrides):
    base = {
        "id": 1, "status": "in_progress", "reason": "", "planned_price": "0.5",
        "leader_price": "0.48", "leader_id": 2, "side": "BUY", "limit_price": "0.50",
        "budget": "5.0", "size": "10", "order_type": "GTD", "expires_at": "2026-09-29T05:20:20Z",
        "filled_qty": None, "attempt_status": "accepted",
    }
    base.update(overrides)
    return base


class ReasonText(unittest.TestCase):
    def test_engine_budget_wording_is_translated(self):
        self.assertTrue(
            notify._reason("leader 2 rolling budget exhausted: used=9.36 requested=9.3590 cap=10")
            .startswith("该 leader 的滚动预算已用尽")
        )
        account = notify._reason("account rolling budget exhausted: used=595 requested=8 cap=600")
        self.assertTrue(account.startswith("账户滚动额度已用满"))
        self.assertIn("used=595", account)
        self.assertTrue(
            notify._reason("account per-order notional exceeded: requested=12 cap=10").startswith("超过账户单笔上限")
        )
        self.assertEqual(
            notify._reason("computed buy notional is below the CLOB minimum of 1 USDC"),
            "金额低于交易所 1 USDC 下单门槛",
        )
        self.assertEqual(notify._reason("something new"), "something new")

    def test_only_the_account_rolling_refusal_is_suppressed(self):
        self.assertTrue(notify.is_account_budget_refusal("account rolling budget exhausted: used=1"))
        self.assertFalse(notify.is_account_budget_refusal("leader 2 rolling budget exhausted: used=1"))


class AccountBudget(unittest.TestCase):
    def test_counts_reserved_rows_inside_the_window_like_the_engine(self):
        db = budget_db()
        now = time.time()
        reserve(db, 100, now - 3600)
        reserve(db, 50.5, now - 60)
        reserve(db, 400, now - 90000)  # outside the 24 h window
        reserve(db, 70, now - 60, state="released_pre_boundary")
        reserve(db, 80, now - 60, account=2)
        budget = notify.account_budget(db, now)
        self.assertAlmostEqual(budget["used"], 150.5)
        self.assertEqual(budget["cap"], 600.0)
        self.assertEqual(budget["oldest"], iso(now - 3600))

    def test_missing_runtime_row_means_no_budget(self):
        db = budget_db()
        db.execute("DELETE FROM persistent_execution_config")
        self.assertIsNone(notify.account_budget(db))


class Kinds(unittest.TestCase):
    def test_each_order_path_gets_its_own_card(self):
        self.assertEqual(notify.outcome_kind(intent()), "resting")
        self.assertEqual(notify.outcome_kind(intent(attempt_status="submitting")), "pending")
        self.assertEqual(notify.outcome_kind(intent(status="completed", attempt_status="finalized")), "maker_filled")
        self.assertEqual(
            notify.outcome_kind(intent(status="completed", order_type="FAK", attempt_status="finalized")),
            "fak_filled",
        )
        self.assertEqual(
            notify.outcome_kind(intent(status="cancelled", reason=notify.GTD_EXPIRED_REASON)), "maker_expired"
        )
        self.assertEqual(
            notify.outcome_kind(intent(status="rejected", order_type="FAK", reason="venue found no")), "not_copied"
        )

    def test_partial_fill_is_settled_only_when_its_attempt_is_final(self):
        self.assertFalse(notify.is_settled(intent(status="partially_filled", attempt_status="accepted")))
        self.assertTrue(notify.is_settled(intent(status="partially_filled", attempt_status="finalized")))
        self.assertFalse(notify.is_settled(intent(status="in_progress")))

    def test_maker_fill_spends_filled_shares_at_its_price(self):
        card = notify.maker_filled_card(
            intent(status="completed", filled_qty="4", size="10", limit_price="0.50"), "leader2-ratio-maker", ""
        )
        self.assertEqual(card["header"]["title"]["content"], "挂单部分成交")
        text = json.dumps(card, ensure_ascii=False)
        self.assertIn("2.00 USDC", text)
        self.assertIn("leader2-ratio-maker", text)
        # A fee taken in shares is not a partial fill.
        full = notify.maker_filled_card(intent(status="completed", filled_qty="9.9912", size="10"), "x", "")
        self.assertEqual(full["header"]["title"]["content"], "挂单成交")


class Flow(unittest.TestCase):
    def setUp(self):
        self.rows = []
        self._intent_rows, self._identity = notify.intent_rows, notify.leader_identity
        notify.intent_rows = lambda db, after, watching: [r for r in self.rows if r["id"] > after or r["id"] in watching]
        notify.leader_identity = lambda db, leader_id: ("leader2-fixed-5-shares", "0xabcd…1234")

    def tearDown(self):
        notify.intent_rows, notify.leader_identity = self._intent_rows, self._identity

    def test_resting_is_announced_once_then_the_fill(self):
        sender, state = FakeSender(), {"intent_cursor": 0, "watching": []}
        names = {"leader2-fixed-5-shares": "leader2-ratio-maker"}
        self.rows = [intent(id=7)]
        notify.process_intents(None, state, sender, names)
        notify.process_intents(None, state, sender, names)
        self.assertEqual(sender.titles(), ["开始挂单(maker,等对手成交)"])
        self.assertIn("leader2-ratio-maker", json.dumps(sender.sent[0][2], ensure_ascii=False))
        self.rows = [intent(id=7, status="completed", attempt_status="finalized", filled_qty="10")]
        notify.process_intents(None, state, sender, names)
        self.assertEqual(sender.titles()[-1], "挂单成交")
        self.assertEqual(state["watching"], [])
        self.assertEqual(state["rested"], [])

    def test_account_budget_refusals_become_one_warning_and_a_count(self):
        db = budget_db(cap="100")
        now = time.time()
        reserve(db, 95, now - 3600)
        sender, state = FakeSender(), {"intent_cursor": 0, "watching": []}
        refusal = "account rolling budget exhausted: used=95 requested=8 cap=100"
        self.rows = [intent(id=i, status="rejected", order_type="GTD", reason=refusal) for i in (1, 2, 3)]
        notify.process_intents(db, state, sender, {})
        self.assertEqual(sender.titles(), ["账户额度即将用满"])
        self.assertIn("95.00 / 100 USDC(95%)", json.dumps(sender.sent[0][2], ensure_ascii=False))
        self.assertEqual(state["budget_skipped"], 3)
        # The window rolls: usage falls below the clear mark.
        db.execute("DELETE FROM persistent_budget_reservations")
        self.assertTrue(notify.check_budget(db, state, sender))
        self.assertEqual(sender.titles()[-1], "账户额度已恢复")
        self.assertIn("3 个", json.dumps(sender.sent[-1][2], ensure_ascii=False))

    def test_budget_warning_has_hysteresis(self):
        db = budget_db(cap="100")
        now = time.time()
        reserve(db, 91, now - 60)
        sender, state = FakeSender(), {}
        self.assertTrue(notify.check_budget(db, state, sender))
        db.execute("UPDATE persistent_budget_reservations SET amount_usdc = '85'")
        self.assertFalse(notify.check_budget(db, state, sender), "85% is between the marks")
        self.assertEqual(sender.titles(), ["账户额度即将用满"])


class Backfill(unittest.TestCase):
    def setUp(self):
        self._events, self._identity = notify.new_leader_events, notify.leader_identity
        notify.leader_identity = lambda db, leader_id: ("leader2-fixed-5-shares", "")

    def tearDown(self):
        notify.new_leader_events, notify.leader_identity = self._events, self._identity

    def event(self, event_id, realtime):
        return {
            "id": event_id, "leader_id": 2, "side": "BUY", "size": "10", "price": "0.5",
            "outcome_index": 0, "occurred_at": "2026-09-29T05:17:00.000Z", "realtime": realtime,
            "title": "BTC up", "outcome": "Up", "event_slug": "btc",
        }

    def test_many_backfilled_trades_become_one_card_and_live_ones_stay_single(self):
        events = [self.event(i, realtime=False) for i in range(1, 6)] + [self.event(6, realtime=True)]
        notify.new_leader_events = lambda db, after: events
        sender, state = FakeSender(), {"event_cursor": 0}
        notify.process_events(None, state, sender, {})
        self.assertEqual(
            sorted(sender.titles()),
            sorted(["买入 · Leader 出手(实时)", "Leader 成交 · REST 回填 5 笔(不跟单)"]),
        )
        self.assertEqual(state["event_cursor"], 6)

    def test_a_few_backfilled_trades_are_marked_individually(self):
        notify.new_leader_events = lambda db, after: [self.event(1, realtime=False)]
        sender = FakeSender()
        notify.process_events(None, {"event_cursor": 0}, sender, {})
        self.assertEqual(sender.titles(), ["买入 · Leader 出手(REST 回填,不跟单)"])


class DisplayNames(unittest.TestCase):
    def test_reads_display_names_and_tolerates_a_missing_file(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump({"leaders": [{"label": "leader2-fixed-5-shares", "display_name": "leader2-ratio-maker"},
                                   {"label": "leader-fixed-5-shares"}]}, handle)
        try:
            self.assertEqual(notify.display_names(handle.name), {"leader2-fixed-5-shares": "leader2-ratio-maker"})
        finally:
            os.unlink(handle.name)
        self.assertEqual(notify.display_names("/nonexistent/trading-config.json"), {})


if __name__ == "__main__":
    unittest.main()
