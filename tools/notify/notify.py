#!/usr/bin/env python3
"""Push leader trades and copy outcomes to Feishu, as they happen.

Runs beside the engine, never inside it. The engine's job is to copy trades;
a call that hangs, a malformed card, or a Feishu outage must not be able to
stall the executor or take the process down. This reads the engine's
SQLite database read-only, keeps its own cursor in its own directory, and
writes nothing the engine will ever read. Killing it costs notifications and
nothing else.

That separation is not theoretical caution. This engine has twice been
stopped by something that had no business stopping it -- a per-leader budget
error that halted every leader for eight hours, and collateral reads that
serialized the executor -- so a cosmetic feature does not get to share the
trading process.

The loop polls rather than subscribes because a leader trade lands in the
database about a second after it happens; a two-second poll is already
inside the noise, and polling has no reconnect state to get wrong.
"""

from __future__ import annotations

import calendar
import json
import os
import re
import sqlite3
import sys
import time
import subprocess
import urllib.error
import urllib.request
from typing import Any

# --- configuration -------------------------------------------------------

DB_PATH = os.environ.get("POLYCOPY_DB_PATH", "/var/lib/polycopy-engine/polycopy.sqlite")
STATE_DIR = os.environ.get("NOTIFY_STATE_DIR", "/var/lib/polycopy-engine-notify")
POLL_SECONDS = float(os.environ.get("NOTIFY_POLL_SECONDS", "2"))
ENGINE_UNIT = os.environ.get("NOTIFY_ENGINE_UNIT", "polycopy-engine-persistent")

#: Feishu's per-app quota is well above anything this can generate, but a bug
#: that notifies in a loop would burn it and get the app throttled for
#: everything else it does. The bucket is the backstop for that, not for
#: normal traffic.
MAX_PER_MINUTE = int(os.environ.get("NOTIFY_MAX_PER_MINUTE", "20"))

STATE_PATH = os.path.join(STATE_DIR, "cursor.json")

POLYMARKET_EVENT_URL = "https://polymarket.com/event/"


def log(message: str) -> None:
    """One line per notable action, to the journal. Never a credential."""

    print(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} {message}", flush=True)


# --- state ---------------------------------------------------------------


def load_state() -> dict[str, Any]:
    try:
        with open(STATE_PATH, encoding="utf-8") as handle:
            state = json.load(handle)
    except (OSError, ValueError):
        return {}
    if not isinstance(state, dict):
        return {}
    return state


def save_state(state: dict[str, Any]) -> None:
    """Atomic, so a kill mid-write cannot leave a cursor that replays or skips."""

    os.makedirs(STATE_DIR, exist_ok=True)
    temp = STATE_PATH + ".partial"
    with open(temp, "w", encoding="utf-8") as handle:
        json.dump(state, handle)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temp, STATE_PATH)


# --- database ------------------------------------------------------------


def connect() -> sqlite3.Connection:
    """Read-only by URI, so a coding mistake cannot write to the ledger."""

    return sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True, timeout=5)


def max_ids(db: sqlite3.Connection) -> tuple[int, int]:
    events = db.execute("SELECT COALESCE(MAX(id), 0) FROM leader_events").fetchone()[0]
    intents = db.execute("SELECT COALESCE(MAX(id), 0) FROM copy_intents").fetchone()[0]
    return events, intents


def leader_identity(db: sqlite3.Connection, leader_id: int) -> tuple[str, str]:
    """Label plus a shortened address. The full 42 characters are noise on a
    phone; the first and last few are what a person actually recognizes."""

    row = db.execute(
        "SELECT l.label, ("
        "  SELECT a.address FROM leader_wallet_aliases a"
        "  WHERE a.leader_id = l.id AND a.enabled = 1 ORDER BY a.id LIMIT 1"
        ") FROM leader_config l WHERE l.id = ?",
        (leader_id,),
    ).fetchone()
    if row is None:
        return (f"leader{leader_id}", "")
    label = row[0] or f"leader{leader_id}"
    address = row[1] or ""
    short = f"{address[:6]}…{address[-4:]}" if len(address) >= 12 else address
    return label, short


def new_leader_events(db: sqlite3.Connection, after_id: int) -> list[dict[str, Any]]:
    """Every new fill by a watched leader, with the market name recovered from
    the raw observation. `leader_events` stores only a condition id; the human
    readable title exists solely in the payload the venue sent."""

    rows = db.execute(
        "SELECT e.id, e.leader_id, e.side, e.size, e.price, e.outcome_index,"
        "       e.occurred_at, e.realtime_observed,"
        "       (SELECT o.payload FROM leader_event_observations o"
        "        WHERE o.leader_event_id = e.id ORDER BY o.id LIMIT 1)"
        " FROM leader_events e WHERE e.id > ? ORDER BY e.id",
        (after_id,),
    ).fetchall()

    events = []
    for row in rows:
        payload = {}
        if row[8]:
            try:
                payload = json.loads(row[8]).get("payload") or {}
            except (ValueError, AttributeError):
                payload = {}
        events.append(
            {
                "id": row[0],
                "leader_id": row[1],
                "side": row[2],
                "size": row[3],
                "price": row[4],
                "outcome_index": row[5],
                "occurred_at": row[6],
                "realtime": bool(row[7]),
                "title": payload.get("title") or "",
                "outcome": payload.get("outcome") or "",
                "event_slug": payload.get("eventSlug") or "",
            }
        )
    return events


def intent_rows(db: sqlite3.Connection, after_id: int, watching: list[int]) -> list[dict]:
    """New intents plus the ones already seen that had not settled yet.

    An intent's row appears before its outcome is known, so a cursor on id
    alone would report every copy as "pending" forever. Ids still in flight
    are re-read each pass until they reach a terminal status.
    """

    ids = ",".join(str(int(i)) for i in watching) if watching else None
    clause = "i.id > ?" + (f" OR i.id IN ({ids})" if ids else "")
    # The latest attempt decides the kind of order (FAK or resting GTD): a
    # post-only order refused for crossing is retried as a new attempt, and
    # the first attempt of an intent may be a FAK that fell back to a maker.
    rows = db.execute(
        "SELECT i.id, i.event_id, i.status, i.rejection_reason, i.planned_price,"
        "       e.price, e.leader_id, e.side,"
        "       (SELECT a.envelope_json FROM order_attempts a"
        "        WHERE a.intent_id = i.id ORDER BY a.id DESC LIMIT 1),"
        "       (SELECT a.accounted_filled_qty FROM order_attempts a"
        "        WHERE a.intent_id = i.id AND a.accounted_filled_qty > 0"
        "        ORDER BY a.id DESC LIMIT 1),"
        "       (SELECT a.status FROM order_attempts a"
        "        WHERE a.intent_id = i.id ORDER BY a.id DESC LIMIT 1)"
        f" FROM copy_intents i JOIN leader_events e ON e.id = i.event_id"
        f" WHERE {clause} ORDER BY i.id",
        (after_id,),
    ).fetchall()

    intents = []
    for row in rows:
        envelope = {}
        if row[8]:
            try:
                envelope = json.loads(row[8])
            except ValueError:
                envelope = {}
        intents.append(
            {
                "id": row[0],
                "status": row[2],
                "reason": row[3] or "",
                "planned_price": row[4],
                "leader_price": row[5],
                "leader_id": row[6],
                "side": row[7],
                "limit_price": envelope.get("price"),
                "budget": envelope.get("buy_budget_usdc"),
                "size": envelope.get("size"),
                "order_type": envelope.get("order_type") or "",
                "expires_at": envelope.get("expires_at"),
                "filled_qty": row[9],
                "attempt_status": row[10] or "",
            }
        )
    return intents


def account_budget(db: sqlite3.Connection, now: float | None = None) -> dict[str, Any] | None:
    """The account's rolling budget as the engine counts it.

    Mirrors `persistent::rolling_reserved_total`: every order reserves its
    notional when it is submitted and keeps it for the whole window, filled
    or not. Only a definitive venue rejection or an operator-confirmed no
    fill releases it. The cutoff is formatted exactly like the engine's
    (RFC 3339, milliseconds, `Z`) because the comparison is on the text.
    """

    row = db.execute(
        "SELECT account_id, rolling_budget_usdc, budget_window_seconds"
        " FROM persistent_execution_config WHERE id = 1"
    ).fetchone()
    if row is None:
        return None
    try:
        account_id, cap, window = int(row[0]), float(row[1]), int(row[2])
    except (TypeError, ValueError):
        return None
    now = time.time() if now is None else now
    start = now - window
    cutoff = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(start)) + f".{int(start % 1 * 1000):03d}Z"
    used, oldest = db.execute(
        "SELECT COALESCE(SUM(CAST(amount_usdc AS REAL)), 0), MIN(reserved_at)"
        " FROM persistent_budget_reservations"
        " WHERE account_id = ? AND state = 'reserved' AND reserved_at >= ?",
        (account_id, cutoff),
    ).fetchone()
    return {"used": float(used), "cap": cap, "window": window, "oldest": oldest}


# --- Feishu --------------------------------------------------------------


def _credentials() -> dict[str, str]:
    """Read app credentials, preferring systemd's credential store.

    A value in the environment is readable from /proc for the life of the
    process; LoadCredential keeps it in a file only this unit can open. The
    app secret is a signing credential for the whole app, so it gets the same
    handling as the engine's own key, not less.
    """

    values: dict[str, str] = {}
    wanted = ("FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_CHAT_ID")

    directory = os.environ.get("CREDENTIALS_DIRECTORY", "")
    if directory:
        try:
            with open(os.path.join(directory, "feishu"), encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, value = line.split("=", 1)
                    key = key.strip()
                    if key in wanted:
                        values[key] = value.strip().strip("\"'")
        except OSError:
            pass

    for key in wanted:
        from_env = os.environ.get(key, "").strip()
        if from_env and key not in values:
            values[key] = from_env
    return values


TOKEN_URL = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
MESSAGE_URL = "https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type=chat_id"
CHATS_URL = "https://open.feishu.cn/open-apis/im/v1/chats?page_size=100"


def _post_json(url: str, body: dict, token: str = "") -> dict:
    """One JSON round trip, with every failure turned into a dict rather than
    an exception. Nothing in this file is allowed to die because Feishu had a
    bad minute."""

    headers = {"Content-Type": "application/json; charset=utf-8"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST"
    )
    return _read(request)


def _read(request: urllib.request.Request) -> dict:
    """Always return the body, including on a 4xx.

    Feishu answers a rejection with a JSON body naming the code and the
    reason; urllib raises on the status, and discarding the exception's body
    would turn every API-level rejection into an opaque "400 Bad Request".
    """

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as error:
        try:
            return json.loads(error.read().decode("utf-8", "replace"))
        except (ValueError, OSError):
            return {"code": -1, "msg": f"HTTP {error.code} with no readable body"}
    except (urllib.error.URLError, OSError, TimeoutError, ValueError) as error:
        return {"code": -1, "msg": f"{type(error).__name__}: {error}"}


def _get_json(url: str, token: str) -> dict:
    request = urllib.request.Request(
        url, headers={"Authorization": f"Bearer {token}"}, method="GET"
    )
    return _read(request)


class Sender:
    """Posts cards as a self-built Feishu app, and refuses to let the API
    become a liability.

    Every failure mode is contained: timeouts are bounded, an error is logged
    and dropped rather than retried forever, and the rate bucket stops a
    notification loop from burning the app's quota.

    Two details of this API are easy to get wrong and are pinned here. The
    card goes in `content` as a JSON-encoded *string*, not an object, and
    without the `{"msg_type", "card"}` envelope a webhook would need. And the
    tenant token expires -- two hours at most -- so it is cached with a margin
    rather than fetched per message or fetched once and trusted forever.
    """

    #: Refresh this long before the stated expiry. Feishu issues a fresh token
    #: when under thirty minutes remain; asking earlier than that costs one
    #: round trip and removes any chance of sending with a token that expires
    #: between the check and the request.
    TOKEN_MARGIN_SECONDS = 600

    def __init__(self, app_id: str, app_secret: str, chat_id: str) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.chat_id = chat_id
        self._token = ""
        self._token_expires_at = 0.0
        self._minute = 0
        self._sent_this_minute = 0
        self._suppressed = 0

    @property
    def configured(self) -> bool:
        return bool(self.app_id and self.app_secret and self.chat_id)

    def token(self) -> str:
        """Cached tenant token. Never logged: it is a bearer credential for
        everything the app can do, not just this notifier."""

        if self._token and time.time() < self._token_expires_at:
            return self._token
        answer = _post_json(
            TOKEN_URL, {"app_id": self.app_id, "app_secret": self.app_secret}
        )
        if answer.get("code") != 0 or not answer.get("tenant_access_token"):
            log(f"token request failed: code={answer.get('code')} {answer.get('msg')}")
            self._token = ""
            return ""
        self._token = answer["tenant_access_token"]
        expire = float(answer.get("expire") or 7200)
        self._token_expires_at = time.time() + max(expire - self.TOKEN_MARGIN_SECONDS, 60)
        return self._token

    def _allowed(self) -> bool:
        minute = int(time.time() // 60)
        if minute != self._minute:
            if self._suppressed:
                log(f"rate limit: {self._suppressed} message(s) suppressed last minute")
                self._suppressed = 0
            self._minute = minute
            self._sent_this_minute = 0
        if self._sent_this_minute >= MAX_PER_MINUTE:
            self._suppressed += 1
            return False
        self._sent_this_minute += 1
        return True

    def send(self, card: dict[str, Any], description: str, dedup: str = "") -> bool:
        if not self.configured:
            log(f"not configured; would have sent: {description}")
            return False
        if not self._allowed():
            return False
        token = self.token()
        if not token:
            return False

        body: dict[str, Any] = {
            "receive_id": self.chat_id,
            "msg_type": "interactive",
            # A JSON string, per the API: an object here is rejected.
            "content": json.dumps(card, ensure_ascii=False),
        }
        if dedup:
            # Feishu drops a repeat of the same uuid, so a cursor that replays
            # after a crash cannot post the same fill twice.
            body["uuid"] = dedup[:50]

        answer = _post_json(MESSAGE_URL, body, token)
        if answer.get("code") != 0:
            log(f"send failed ({description}): code={answer.get('code')} {answer.get('msg')}")
            # An expired or revoked token is worth one immediate retry; any
            # other failure is dropped rather than hammered.
            if answer.get("code") in (99991663, 99991664, 99991661):
                self._token = ""
                token = self.token()
                if token:
                    answer = _post_json(MESSAGE_URL, body, token)
                    return answer.get("code") == 0
            return False
        return True

    def list_chats(self) -> list[tuple[str, str]]:
        """Groups this app can see, for finding the chat_id by its name."""

        token = self.token()
        if not token:
            return []
        answer = _get_json(CHATS_URL, token)
        if answer.get("code") != 0:
            log(f"chat list failed: code={answer.get('code')} {answer.get('msg')}")
            return []
        items = (answer.get("data") or {}).get("items") or []
        return [(item.get("chat_id", ""), item.get("name", "")) for item in items]


# --- cards ---------------------------------------------------------------


#: The engine writes rejection reasons in English, for the log and the
#: ledger. On a phone they are read at a glance, so the ones that actually
#: recur are translated. An unrecognized reason passes through verbatim --
#: swallowing it would hide exactly the failure nobody has seen before.
REASON_TEXT = {
    "venue found no matching liquidity for FAK order":
        "盘口无对手盘(FAK 立即成交失败)",
    "computed buy notional is below the CLOB minimum of 1 USDC":
        "金额低于交易所 1 USDC 下单门槛",
    "signal age exceeds policy":
        "信号过期(超过 max_signal_age_seconds)",
    "leader sell arrived before this account's own fill was visible on the venue":
        "本账户的买入尚未在场馆可见,leader 卖出先到",
}


#: Reasons whose text starts with a variable part (the leader id), matched by
#: pattern. The engine's own wording is `PersistentError`'s Display, stored
#: verbatim as the rejection reason: "leader 2 rolling budget exhausted: ...".
REASON_PATTERNS = [
    (re.compile(r"^leader \d+ rolling budget exhausted:?"),
     "该 leader 的滚动预算已用尽,跳过这个信号"),
    (re.compile(r"^account rolling budget exhausted:?"),
     "账户滚动额度已用满,跳过这个信号(额度释放后自动恢复)"),
    (re.compile(r"^account per-order notional exceeded:?"),
     "超过账户单笔上限,跳过这个信号"),
    (re.compile(r"^account cumulative-turnover circuit breaker exceeded:?"),
     "账户滚动额度已用满,交易服务已停止"),
]

#: Written by the engine when a resting maker order ends without a fill.
GTD_EXPIRED_REASON = "post-only GTD expired or cancelled without a fill"


def _reason(raw: str) -> str:
    if not raw:
        return "(未记录)"
    for english, chinese in REASON_TEXT.items():
        if raw.startswith(english):
            extra = raw[len(english):].strip()
            return f"{chinese}{(' ' + extra) if extra else ''}"
    for pattern, chinese in REASON_PATTERNS:
        match = pattern.match(raw)
        if match:
            extra = raw[match.end():].strip()
            return f"{chinese}{chr(10) + extra if extra else ''}"
    return raw


def is_account_budget_refusal(raw: str) -> bool:
    return raw.startswith("account rolling budget exhausted")


def _decimal(value: Any, places: int = 4) -> str:
    try:
        return f"{float(value):.{places}f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(value if value is not None else "-")


def _field(label: str, value: str, short: bool = True) -> dict[str, Any]:
    return {"is_short": short, "text": {"tag": "lark_md", "content": f"**{label}**\n{value}"}}


def _card(title: str, colour: str, fields: list[dict], slug: str = "") -> dict[str, Any]:
    elements: list[dict[str, Any]] = [{"tag": "div", "fields": fields}]
    if slug:
        elements.append(
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "打开市场"},
                        "url": POLYMARKET_EVENT_URL + slug,
                        "type": "default",
                    }
                ],
            }
        )
    return {
        "config": {"wide_screen_mode": True},
        "header": {"title": {"tag": "plain_text", "content": title}, "template": colour},
        "elements": elements,
    }


def _notional(size: Any, price: Any) -> str:
    try:
        return f"{float(size) * float(price):.2f} USDC"
    except (TypeError, ValueError):
        return "-"


def _local_time(stamp: Any, fmt: str = "%H:%M:%S") -> str:
    """An RFC 3339 UTC stamp from the ledger or an envelope, in the server's
    zone (the one the journal and the ops panel show)."""

    try:
        seconds = calendar.timegm(time.strptime(str(stamp)[:19], "%Y-%m-%dT%H:%M:%S"))
    except (TypeError, ValueError):
        return str(stamp or "-")
    return time.strftime(fmt, time.localtime(seconds))


def _versus_leader(price: Any, leader_price: Any) -> str:
    try:
        ours, lead = float(price), float(leader_price)
        return f"{ours - lead:+.4f} ({(ours - lead) / lead * 100:+.1f}%)"
    except (TypeError, ValueError, ZeroDivisionError):
        return "-"


def leader_card(event: dict, label: str, short_address: str) -> dict[str, Any]:
    direction = "买入" if event["side"] == "BUY" else "卖出"
    who = f"{label} · id={event['leader_id']}"
    if short_address:
        who += f" · {short_address}"

    fields = [
        _field("Leader", who, short=False),
        _field("市场", event["title"] or "(标题未知)", short=False),
        _field("方向", f"{direction} {event['outcome'] or ''}".strip()),
        _field("价格", _decimal(event["price"])),
        _field("股数", _decimal(event["size"], 4)),
        _field("金额", _notional(event["size"], event["price"])),
    ]
    if event["realtime"]:
        colour = "green" if event["side"] == "BUY" else "orange"
        return _card(f"{direction} · Leader 出手(实时)", colour, fields, event["event_slug"])
    fields.append(_field("成交时间", _local_time(event["occurred_at"], "%m-%d %H:%M:%S")))
    fields.append(_field("来源", "REST 回填:事后补抓到的,已过时效,不会跟单", short=False))
    return _card(f"{direction} · Leader 出手(REST 回填,不跟单)", "grey", fields, event["event_slug"])


def backfill_summary_card(events: list[dict], label: str) -> dict[str, Any]:
    """Many backfilled trades at once (after a restart or an outage) become
    one card: none of them will be copied, and a card each only buries the
    live ones that will."""

    total = 0.0
    for event in events:
        try:
            total += float(event["size"]) * float(event["price"])
        except (TypeError, ValueError):
            pass
    first = _local_time(events[0]["occurred_at"], "%m-%d %H:%M")
    last = _local_time(events[-1]["occurred_at"], "%m-%d %H:%M")
    fields = [
        _field("Leader", label, short=False),
        _field("笔数", str(len(events))),
        _field("金额合计", f"{total:.2f} USDC"),
        _field("成交时间", f"{first} ~ {last}", short=False),
        _field("来源", "REST 回填:事后补抓到的,已过时效,不会跟单", short=False),
    ]
    return _card(f"Leader 成交 · REST 回填 {len(events)} 笔(不跟单)", "grey", fields)


def outcome_kind(intent: dict) -> str:
    """Which card an intent gets.

    - "fak_filled": a FAK order took liquidity and filled at once;
    - "resting": a post-only GTD is on the book (the venue accepted it);
    - "maker_filled": a resting GTD filled, fully or in part;
    - "maker_expired": a resting GTD ended without a fill;
    - "not_copied": refused or failed before or at the venue;
    - "pending": nothing to say yet.
    """

    status = intent["status"]
    gtd = intent["order_type"] == "GTD"
    if status in ("completed", "partially_filled"):
        return "maker_filled" if gtd else "fak_filled"
    if status in TERMINAL:
        if intent["reason"].startswith(GTD_EXPIRED_REASON):
            return "maker_expired"
        return "not_copied"
    if gtd and intent["attempt_status"] == "accepted":
        return "resting"
    return "pending"


def is_settled(intent: dict) -> bool:
    """No further card will ever be due. A partial fill is final once its
    attempt is finalized; until then the rest of the order may still fill."""

    if intent["status"] == "partially_filled":
        return intent["attempt_status"] in ("finalized", "rejected", "error")
    return intent["status"] in TERMINAL


def _who(intent: dict, label: str, short_address: str) -> str:
    who = f"{label} · id={intent['leader_id']}"
    return f"{who} · {short_address}" if short_address else who


def fak_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    # A FAK BUY spends its USDC budget and the venue fills as many shares as
    # that buys at or under the limit, so the price actually paid is
    # budget / shares, often below the limit.
    effective = None
    try:
        effective = float(intent["budget"]) / float(intent["filled_qty"])
    except (TypeError, ValueError, ZeroDivisionError):
        effective = None
    spent = f"{float(intent['budget']):.2f} USDC" if intent["budget"] else "-"
    fields = [
        _field("Leader", _who(intent, label, short_address), short=False),
        _field("成交股数", _decimal(intent["filled_qty"], 4)),
        _field("花费", spent),
        _field("实付均价", _decimal(effective) if effective is not None else "-"),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("限价", _decimal(intent["limit_price"], 2)),
        _field("比 Leader 价", _versus_leader(effective, intent["leader_price"]) if effective else "-"),
    ]
    return _card("FAK 吃单跟单成交", "green", fields)


def resting_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    fields = [
        _field("Leader", _who(intent, label, short_address), short=False),
        _field("挂单价", _decimal(intent["limit_price"], 3)),
        _field("股数", _decimal(intent["size"], 4)),
        _field("金额", _notional(intent["size"], intent["limit_price"])),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("比 Leader 价", _versus_leader(intent["limit_price"], intent["leader_price"])),
        _field("到期", _local_time(intent["expires_at"]) if intent["expires_at"] else "-"),
    ]
    return _card("开始挂单(maker,等对手成交)", "blue", fields)


def maker_filled_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    # A maker fill happens at the order's own price; the spend is the filled
    # shares at that price, not the whole order's budget (a partial fill
    # spends less). A fee can come off the shares received (9.9912 of 10),
    # so only a real shortfall counts as partial.
    partial = False
    try:
        partial = float(intent["filled_qty"]) < float(intent["size"]) * 0.99
    except (TypeError, ValueError):
        partial = intent["status"] == "partially_filled"
    fields = [
        _field("Leader", _who(intent, label, short_address), short=False),
        _field("成交股数", f"{_decimal(intent['filled_qty'], 4)} / {_decimal(intent['size'], 4)}"),
        _field("成交价", _decimal(intent["limit_price"], 3)),
        _field("花费", _notional(intent["filled_qty"], intent["limit_price"])),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("比 Leader 价", _versus_leader(intent["limit_price"], intent["leader_price"])),
    ]
    return _card("挂单部分成交" if partial else "挂单成交", "green", fields)


def maker_expired_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    fields = [
        _field("Leader", _who(intent, label, short_address), short=False),
        _field("挂单价", _decimal(intent["limit_price"], 3)),
        _field("股数", _decimal(intent["size"], 4)),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("结果", "到期无人成交,挂单已取消,没有买入", short=False),
    ]
    return _card("挂单到期取消(未成交)", "grey", fields)


def not_copied_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    fields = [
        _field("Leader", _who(intent, label, short_address), short=False),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("限价", _decimal(intent["limit_price"], 2)),
        _field("结果", intent["status"]),
        _field("原因", _reason(intent["reason"]), short=False),
    ]
    return _card("未跟上", "red", fields)


CARD_FOR_KIND = {
    "fak_filled": fak_card,
    "resting": resting_card,
    "maker_filled": maker_filled_card,
    "maker_expired": maker_expired_card,
    "not_copied": not_copied_card,
}


def _window_zh(seconds: int) -> str:
    if seconds % 3600 == 0:
        return f"{seconds // 3600} 小时"
    if seconds % 60 == 0:
        return f"{seconds // 60} 分钟"
    return f"{seconds} 秒"


def budget_card(budget: dict, high: bool, skipped: int = 0) -> dict[str, Any]:
    # Half up, like the ops panel (Python's format rounds half to even).
    percent = int(budget["used"] / budget["cap"] * 100 + 0.5) if budget["cap"] else 0
    usage = f"{budget['used']:.2f} / {budget['cap']:g} USDC({percent}%)"
    window = _window_zh(budget["window"])
    if high:
        release = "-"
        if budget.get("oldest"):
            try:
                oldest = calendar.timegm(time.strptime(budget["oldest"][:19], "%Y-%m-%dT%H:%M:%S"))
                release = time.strftime("%m-%d %H:%M", time.localtime(oldest + budget["window"])) + " 起逐步释放"
            except ValueError:
                release = "-"
        fields = [
            _field(f"已用(滚动 {window})", usage, short=False),
            _field("最早的占用", release, short=False),
            _field(
                "影响",
                "额度用满后不能再跟新的单,直到最早的占用释放。"
                "挂出去没成交的单也占额度,直到过了窗口。",
                short=False,
            ),
        ]
        return _card("账户额度即将用满", "orange", fields)
    fields = [_field(f"已用(滚动 {window})", usage, short=False)]
    if skipped:
        fields.append(_field("期间因额度跳过的信号", f"{skipped} 个", short=False))
    return _card("账户额度已恢复", "green", fields)


def engine_card(active: bool) -> dict[str, Any]:
    if active:
        return _card(
            "引擎已恢复运行",
            "green",
            [_field("单元", ENGINE_UNIT, short=False)],
        )
    return _card(
        "引擎已停止",
        "red",
        [
            _field("单元", ENGINE_UNIT, short=False),
            _field("影响", "不再跟任何单,直到重新启动", short=False),
        ],
    )


# --- main ----------------------------------------------------------------

TERMINAL = {"completed", "rejected", "cancelled", "failed", "expired", "dead_letter"}

#: More backfilled trades than this in one pass become a single summary card.
BACKFILL_SUMMARY_AT = 3

#: The account budget and the display names change slowly; once a minute is
#: plenty, and keeps this sidecar's reads of the ledger rare.
SLOW_CHECK_SECONDS = 60.0

#: Warn when the rolling window is this full; clear the warning only once it
#: drops below the lower mark, so usage hovering at the line cannot flap.
BUDGET_HIGH = 0.90
BUDGET_CLEAR = 0.80

TRADING_CONFIG = os.environ.get("NOTIFY_TRADING_CONFIG", "/etc/polycopy-engine/trading-config.json")


def display_names(path: str = TRADING_CONFIG) -> dict[str, str]:
    """label -> display_name from the owner's trading config. Missing or
    unreadable means every leader shows under its database label."""

    try:
        with open(path, encoding="utf-8") as handle:
            config = json.load(handle)
    except (OSError, ValueError):
        return {}
    names = {}
    for leader in config.get("leaders") or []:
        if isinstance(leader, dict) and leader.get("label") and leader.get("display_name"):
            names[str(leader["label"])] = str(leader["display_name"])
    return names


def process_events(db, state: dict, sender, names: dict[str, str]) -> bool:
    events = new_leader_events(db, state["event_cursor"])
    backfilled: dict[int, list[dict]] = {}
    for event in events:
        if not event["realtime"]:
            backfilled.setdefault(event["leader_id"], []).append(event)
    summarize = sum(len(group) for group in backfilled.values()) > BACKFILL_SUMMARY_AT
    for event in events:
        label, short = leader_identity(db, event["leader_id"])
        label = names.get(label, label)
        if summarize and not event["realtime"]:
            continue
        sender.send(
            leader_card(event, label, short),
            f"leader event {event['id']}",
            dedup=f"polycopy-ev-{event['id']}",
        )
    if summarize:
        for leader_id, group in backfilled.items():
            label, _ = leader_identity(db, leader_id)
            sender.send(
                backfill_summary_card(group, names.get(label, label)),
                f"backfill {len(group)} events",
                dedup=f"polycopy-bf-{group[0]['id']}-{group[-1]['id']}",
            )
    if events:
        state["event_cursor"] = events[-1]["id"]
    return bool(events)


def process_intents(db, state: dict, sender, names: dict[str, str]) -> bool:
    watching = [int(i) for i in state.get("watching", [])]
    rested = {int(i) for i in state.get("rested", [])}
    intents = intent_rows(db, state["intent_cursor"], watching)
    still_watching = []
    for intent in intents:
        if intent["id"] > state["intent_cursor"]:
            state["intent_cursor"] = intent["id"]
        kind = outcome_kind(intent)
        label, short = leader_identity(db, intent["leader_id"])
        label = names.get(label, label)
        if not is_settled(intent):
            still_watching.append(intent["id"])
            if kind == "resting" and intent["id"] not in rested:
                sender.send(
                    resting_card(intent, label, short),
                    f"intent {intent['id']} resting",
                    dedup=f"polycopy-in-{intent['id']}-resting",
                )
                rested.add(intent["id"])
            continue
        # Reached exactly once: an id is either newly past the cursor or
        # still on the watch list, never both, and a settled one leaves it.
        rested.discard(intent["id"])
        if is_account_budget_refusal(intent["reason"]):
            if not state.get("budget_high"):
                check_budget(db, state, sender, force_high=True)
            state["budget_skipped"] = int(state.get("budget_skipped", 0)) + 1
            log(f"intent {intent['id']} skipped for the account budget (card suppressed)")
            continue
        sender.send(
            CARD_FOR_KIND.get(kind, not_copied_card)(intent, label, short),
            f"intent {intent['id']} {kind}",
            dedup=f"polycopy-in-{intent['id']}-{intent['status']}",
        )
    state["watching"] = still_watching
    state["rested"] = sorted(rested & set(still_watching))
    return bool(intents)


def check_budget(db, state: dict, sender, force_high: bool = False) -> bool:
    """One card when the rolling window gets nearly full, one when it has
    room again. While it is full, each refused signal is counted instead of
    carded, so a full day does not become a wall of red."""

    budget = account_budget(db)
    if budget is None or budget["cap"] <= 0:
        return False
    ratio = budget["used"] / budget["cap"]
    high = bool(state.get("budget_high"))
    if not high and (force_high or ratio >= BUDGET_HIGH):
        sender.send(budget_card(budget, True), f"account budget high {ratio:.2f}")
        state["budget_high"] = True
        state["budget_skipped"] = 0
        return True
    if high and not force_high and ratio < BUDGET_CLEAR:
        sender.send(
            budget_card(budget, False, int(state.get("budget_skipped", 0))),
            f"account budget clear {ratio:.2f}",
        )
        state["budget_high"] = False
        state["budget_skipped"] = 0
        return True
    return False


#: The database poll is cheap; spawning a process is not, and the engine
#: stopping is not an event that needs two-second resolution.
ENGINE_CHECK_SECONDS = 15.0

#: Set once when systemctl cannot be run at all, so the operator is told the
#: alert is off rather than being reassured by its silence.
_engine_check_broken = False


def engine_active() -> bool:
    """Silence from the database is ambiguous -- a quiet leader and a dead
    engine look identical -- so this is the signal that tells them apart.

    A failure to run systemctl reports "active": an unknown state must not be
    announced as an outage, or a broken check becomes a nightly false alarm.
    """

    global _engine_check_broken
    try:
        done = subprocess.run(
            ["systemctl", "is-active", "--quiet", ENGINE_UNIT],
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as error:
        # Report "active" so a broken check cannot invent an outage, but say
        # so out loud: a liveness check that fails silently is the failure
        # this whole file exists to avoid.
        if not _engine_check_broken:
            _engine_check_broken = True
            log(f"engine check itself failed, engine-down alerts are OFF: {error}")
        return True
    if _engine_check_broken:
        _engine_check_broken = False
        log("engine check recovered")
    return done.returncode == 0


def main() -> int:
    credentials = _credentials()
    sender = Sender(
        credentials.get("FEISHU_APP_ID", ""),
        credentials.get("FEISHU_APP_SECRET", ""),
        credentials.get("FEISHU_CHAT_ID", ""),
    )

    # A helper rather than a separate script: finding a chat_id otherwise
    # means digging through the developer console, and this already holds the
    # credentials needed to just ask.
    if "--list-chats" in sys.argv:
        if not (sender.app_id and sender.app_secret):
            log("FEISHU_APP_ID / FEISHU_APP_SECRET are not set")
            return 2
        chats = sender.list_chats()
        if not chats:
            log("no groups visible -- is the app added to the group, and does it")
            log("hold one of im:chat / im:chat:readonly?")
            return 1
        for chat_id, name in chats:
            print(f"{chat_id}  {name}")
        return 0

    if not sender.configured:
        missing = [
            key
            for key in ("FEISHU_APP_ID", "FEISHU_APP_SECRET", "FEISHU_CHAT_ID")
            if not credentials.get(key)
        ]
        log(f"dry run, nothing will be sent -- missing {', '.join(missing)}")
    state = load_state()

    try:
        db = connect()
    except sqlite3.Error as error:
        log(f"cannot open database read-only: {error}")
        return 1

    # A first run must not replay the entire ledger into someone's phone.
    if "event_cursor" not in state or "intent_cursor" not in state:
        events, intents = max_ids(db)
        state = {"event_cursor": events, "intent_cursor": intents, "watching": []}
        save_state(state)
        log(f"first run: starting at leader_event {events}, intent {intents}")

    last_engine_state = state.get("engine_active", True)
    last_engine_check = 0.0
    last_slow_check = 0.0
    names: dict[str, str] = {}

    while True:
        try:
            now = time.monotonic()
            slow_due = now - last_slow_check >= SLOW_CHECK_SECONDS
            if slow_due:
                names = display_names()

            dirty = process_events(db, state, sender, names)
            dirty = process_intents(db, state, sender, names) or dirty

            if now - last_engine_check >= ENGINE_CHECK_SECONDS:
                last_engine_check = now
                active = engine_active()
                if active != last_engine_state:
                    sender.send(engine_card(active), f"engine active={active}")
                    last_engine_state = active
                    state["engine_active"] = active
                    dirty = True

            if slow_due:
                last_slow_check = now
                dirty = check_budget(db, state, sender) or dirty

            if dirty:
                save_state(state)

        except sqlite3.Error as error:
            # A locked or briefly unavailable database is normal while the
            # engine checkpoints WAL. Losing a poll costs nothing.
            log(f"database read failed, retrying: {error}")
        except Exception as error:  # noqa: BLE001 - the loop must outlive any one pass
            log(f"unexpected error in poll, continuing: {type(error).__name__}: {error}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    sys.exit(main())
