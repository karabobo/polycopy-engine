#!/usr/bin/env python3
"""Push leader trades and copy outcomes to Feishu, as they happen.

Runs beside the engine, never inside it. The engine's job is to copy trades;
a webhook that hangs, a malformed card, or a Feishu outage must not be able
to stall the executor or take the process down. This reads the engine's
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

import json
import os
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
POLL_SECONDS = float(os.environ.get("NOTIFY_POLL_SECONDS", "2"))
ENGINE_UNIT = os.environ.get("NOTIFY_ENGINE_UNIT", "polycopy-engine-persistent")

#: Feishu's custom-bot quota is well above anything this can generate, but a
#: bug that notifies in a loop would burn it and get the bot throttled for
#: everything else. The bucket is the backstop for that, not for normal use.
MAX_PER_MINUTE = int(os.environ.get("NOTIFY_MAX_PER_MINUTE", "20"))

STATE_PATH = os.path.join(STATE_DIR, "cursor.json")

POLYMARKET_EVENT_URL = "https://polymarket.com/event/"


def log(message: str) -> None:
    """One line per notable action, to the journal. Never the webhook URL."""

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
    rows = db.execute(
        "SELECT i.id, i.event_id, i.status, i.rejection_reason, i.planned_price,"
        "       e.price, e.leader_id, e.side,"
        "       (SELECT a.envelope_json FROM order_attempts a"
        "        WHERE a.intent_id = i.id ORDER BY a.id DESC LIMIT 1),"
        "       (SELECT a.accounted_filled_qty FROM order_attempts a"
        "        WHERE a.intent_id = i.id AND a.accounted_filled_qty > 0"
        "        ORDER BY a.id DESC LIMIT 1)"
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
                "filled_qty": row[9],
            }
        )
    return intents


# --- Feishu --------------------------------------------------------------


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


def _reason(raw: str) -> str:
    if not raw:
        return "(未记录)"
    for english, chinese in REASON_TEXT.items():
        if raw.startswith(english):
            extra = raw[len(english):].strip()
            return f"{chinese}{(' ' + extra) if extra else ''}"
    if "LeaderBudgetExhausted" in raw or "leader budget" in raw.lower():
        return f"该 leader 的滚动预算已用尽\n{raw}"
    return raw


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


def leader_card(event: dict, label: str, short_address: str) -> dict[str, Any]:
    direction = "买入" if event["side"] == "BUY" else "卖出"
    colour = "green" if event["side"] == "BUY" else "orange"
    notional = ""
    try:
        notional = f"{float(event['size']) * float(event['price']):.2f} USDC"
    except (TypeError, ValueError):
        notional = "-"

    who = f"{label} · id={event['leader_id']}"
    if short_address:
        who += f" · {short_address}"

    fields = [
        _field("Leader", who, short=False),
        _field("市场", event["title"] or "(标题未知)", short=False),
        _field("方向", f"{direction} {event['outcome'] or ''}".strip()),
        _field("价格", _decimal(event["price"])),
        _field("股数", _decimal(event["size"], 4)),
        _field("金额", notional),
    ]
    if not event["realtime"]:
        fields.append(
            _field("来源", "REST 回填(非实时,不会触发跟单)", short=False)
        )
    return _card(f"{direction} · Leader 出手", colour, fields, event["event_slug"])


def outcome_card(intent: dict, label: str, short_address: str) -> dict[str, Any]:
    who = f"{label} · id={intent['leader_id']}"
    if short_address:
        who += f" · {short_address}"

    if intent["status"] == "completed":
        effective = None
        if intent["budget"] and intent["filled_qty"]:
            try:
                effective = float(intent["budget"]) / float(intent["filled_qty"])
            except (TypeError, ValueError, ZeroDivisionError):
                effective = None
        slip = ""
        if effective is not None and intent["leader_price"]:
            try:
                lead = float(intent["leader_price"])
                slip = f"{effective - lead:+.4f} ({(effective - lead) / lead * 100:+.1f}%)"
            except (TypeError, ValueError, ZeroDivisionError):
                slip = ""
        spent = f"{float(intent['budget']):.2f} USDC" if intent["budget"] else "-"
        fields = [
            _field("Leader", who, short=False),
            _field("成交股数", _decimal(intent["filled_qty"], 4)),
            _field("花费", spent),
            _field("实付均价", _decimal(effective) if effective is not None else "-"),
            _field("Leader 价", _decimal(intent["leader_price"])),
            _field("限价", _decimal(intent["limit_price"], 2)),
            _field("滑价", slip or "-"),
        ]
        return _card("已跟单成交", "green", fields)

    fields = [
        _field("Leader", who, short=False),
        _field("Leader 价", _decimal(intent["leader_price"])),
        _field("限价", _decimal(intent["limit_price"], 2)),
        _field("结果", intent["status"]),
        _field("原因", _reason(intent["reason"]), short=False),
    ]
    return _card("未跟上", "red", fields)


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

TERMINAL = {"completed", "rejected", "cancelled", "failed", "expired"}


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

    while True:
        try:
            events = new_leader_events(db, state["event_cursor"])
            for event in events:
                label, short = leader_identity(db, event["leader_id"])
                sender.send(
                    leader_card(event, label, short),
                    f"leader event {event['id']}",
                    dedup=f"polycopy-ev-{event['id']}",
                )
                state["event_cursor"] = event["id"]

            watching = [int(i) for i in state.get("watching", [])]
            intents = intent_rows(db, state["intent_cursor"], watching)
            still_watching = []
            for intent in intents:
                if intent["id"] > state["intent_cursor"]:
                    state["intent_cursor"] = intent["id"]
                if intent["status"] in TERMINAL:
                    # Reached here exactly once: an id is either newly past the
                    # cursor or still on the watch list, never both, and a
                    # terminal one is dropped from the list below.
                    label, short = leader_identity(db, intent["leader_id"])
                    sender.send(
                        outcome_card(intent, label, short),
                        f"intent {intent['id']} {intent['status']}",
                        dedup=f"polycopy-in-{intent['id']}-{intent['status']}",
                    )
                else:
                    still_watching.append(intent["id"])
            state["watching"] = still_watching

            dirty = bool(events or intents)

            now = time.monotonic()
            if now - last_engine_check >= ENGINE_CHECK_SECONDS:
                last_engine_check = now
                active = engine_active()
                if active != last_engine_state:
                    sender.send(engine_card(active), f"engine active={active}")
                    last_engine_state = active
                    state["engine_active"] = active
                    dirty = True

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
