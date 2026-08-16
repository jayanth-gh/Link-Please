import os
import sqlite3
import threading
import time
import uuid
import json
from datetime import datetime
import hmac
import hashlib

import requests
from flask import Flask, request, jsonify

DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")
API_KEY = os.environ.get("PSEUDOGRAM_API_KEY", "").strip()
print("API KEY LOADED:", bool(API_KEY))
print("API KEY LENGTH:", len(API_KEY))
print("API KEY SHA256:", hashlib.sha256(API_KEY.encode("utf-8")).hexdigest())
BASE_URL = os.environ.get("PSEUDOGRAM_BASE_URL", "https://pseudogram-api.onrender.com")

app = Flask(__name__)

# Retry / rate limit config
MAX_ATTEMPTS = 5
RATE_LIMIT = 10
RATE_WINDOW = 60
_rate_lock = threading.Lock()
# list of timestamps (float) of recent API requests
_recent_requests = []
WORKER_STARTED = False


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    db = get_db()
    cur = db.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS rules(
            rule_id TEXT PRIMARY KEY,
            keyword TEXT NOT NULL,
            dm_message TEXT NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS deliveries(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT,
            comment_id TEXT,
            dm_id TEXT,
            status TEXT NOT NULL,
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            next_attempt_at INTEGER,
            created_at INTEGER,
            updated_at INTEGER,
            UNIQUE(rule_id, user_id)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events(
            event_id TEXT PRIMARY KEY,
            event_type TEXT,
            payload TEXT,
            created_at INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS metrics(
            key TEXT PRIMARY KEY,
            value INTEGER
        )
        """
    )
    # ensure duplicates_blocked exists
    cur.execute("INSERT OR IGNORE INTO metrics(key, value) VALUES(?,?)", ("duplicates_blocked", 0))
    db.commit()
    db.close()


def incr_metric(key, delta=1):
    db = get_db()
    cur = db.cursor()
    cur.execute("INSERT INTO metrics(key, value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value = value + ?", (key, delta, delta))
    db.commit()
    db.close()


def now_ts():
    return int(time.time())


@app.route("/rules", methods=["POST"])
def create_rule():
    body = request.get_json() or {}
    keyword = body.get("keyword")
    dm_message = body.get("dm_message")
    if not keyword or not dm_message:
        return (jsonify({"error": "keyword and dm_message required"}), 400)
    rule_id = str(uuid.uuid4())
    db = get_db()
    cur = db.cursor()
    cur.execute("INSERT INTO rules(rule_id, keyword, dm_message) VALUES(?,?,?)", (rule_id, keyword, dm_message))
    db.commit()
    db.close()
    return jsonify({"rule_id": rule_id, "keyword": keyword, "dm_message": dm_message}), 201

def verify_webhook_signature(raw_body, signature):
    if not signature:
        return False

    if not signature.startswith("sha256="):
        return False

    received = signature[len("sha256="):]

    expected = hmac.new(
        API_KEY.encode("utf-8"),
        raw_body,
        hashlib.sha256
    ).hexdigest()

    return hmac.compare_digest(expected, received)

@app.route("/webhook", methods=["POST"])
def webhook():
    # Get the exact raw request body received from Pseudogram.
    raw = request.get_data()

    # Get the signature sent by Pseudogram.
    signature = request.headers.get("X-PseudoGram-Signature")

    # Calculate our expected HMAC-SHA256 signature.
    expected = hmac.new(
        API_KEY.encode("utf-8"),
        raw,
        hashlib.sha256
    ).hexdigest()

    # Temporary debugging.
    # IMPORTANT: Do not print API_KEY itself.
    print("========== WEBHOOK DEBUG ==========")
    print("BODY:", raw.decode("utf-8", errors="replace"))
    print("BODY LENGTH:", len(raw))
    print("RECEIVED SIGNATURE:", signature)
    print("EXPECTED SIGNATURE:", "sha256=" + expected)
    print("===================================")

    # Signature must exist.
    if not signature:
        print("SIGNATURE MISSING")
        return jsonify({"error": "missing signature"}), 401

    received = signature.strip()

    # Verify signature.
    if not hmac.compare_digest(
        received,
        "sha256=" + expected
    ):
        print("SIGNATURE MISMATCH")
        return jsonify({"error": "invalid signature"}), 401

    print("SIGNATURE VALID")

    # Parse JSON only after signature verification succeeds.
    try:
        data = json.loads(raw)
    except Exception:
        return ("", 400)

    event_id = data.get("event_id")
    event_type = data.get("event_type")
    created_at = now_ts()

    db = get_db()
    cur = db.cursor()

    # Persist event idempotently.
    try:
        cur.execute(
            """
            INSERT INTO events(
                event_id,
                event_type,
                payload,
                created_at
            )
            VALUES(?,?,?,?)
            """,
            (
                event_id,
                event_type,
                json.dumps(data.get("data") or {}),
                created_at
            )
        )
        db.commit()

    except sqlite3.IntegrityError:
        # Event was already processed.
        db.close()
        return ("", 200)

    # Find matching rules and create delivery rows.
    comment = data.get("data") or {}

    text = comment.get("text", "")

    user = comment.get("from") or {}

    user_id = user.get("user_id")
    username = user.get("username")

    comment_id = comment.get("comment_id")

    cur.execute("SELECT rule_id, keyword FROM rules")
    rules = cur.fetchall()

    for r in rules:
        rule_id = r[0]
        keyword = r[1]

        # Case-insensitive keyword matching.
        if keyword.lower() in text.lower():
            ts = now_ts()

            try:
                cur.execute(
                    """
                    INSERT INTO deliveries(
                        rule_id,
                        user_id,
                        username,
                        comment_id,
                        status,
                        attempts,
                        next_attempt_at,
                        created_at,
                        updated_at
                    )
                    VALUES(?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        rule_id,
                        user_id,
                        username,
                        comment_id,
                        "queued",
                        0,
                        ts,
                        ts,
                        ts
                    )
                )

                db.commit()

            except sqlite3.IntegrityError:
                # Same user + same rule already has a delivery.
                cur.execute(
                    """
                    INSERT INTO metrics(key, value)
                    VALUES(?, ?)
                    ON CONFLICT(key)
                    DO UPDATE SET value = value + ?
                    """,
                    (
                        "duplicates_blocked",
                        1,
                        1
                    )
                )

                db.commit()
                continue

    db.close()

    return ("", 200)

def send_dm(delivery, rule_message):
    url = f"{BASE_URL}/v1/dm/send"
    headers = {}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    idempotency = f"{delivery['rule_id']}:{delivery['user_id']}"
    headers["Idempotency-Key"] = idempotency
    payload = {
        "recipient_user_id": delivery["user_id"],
        "message": rule_message,
        "comment_id": delivery["comment_id"],
    }
    # simple local rate limiter
    with _rate_lock:
        now = time.time()
        # drop old
        while _recent_requests and _recent_requests[0] <= now - RATE_WINDOW:
            _recent_requests.pop(0)
        if len(_recent_requests) >= RATE_LIMIT:
            # tell caller to wait until oldest + window
            retry_after = int(RATE_WINDOW - (now - _recent_requests[0])) + 1
            return {"ok": False, "code": 429, "body": {"error": "rate_limited"}, "headers": {"Retry-After": str(retry_after)}, "local_rate_limited": True, "retry_after": retry_after}
        # record intent
        _recent_requests.append(now)
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=6)
    except Exception as e:
        return {"ok": False, "code": None, "error": str(e)}
    try:
        body = resp.json()
    except Exception:
        body = {"error": resp.text}
    return {"ok": resp.status_code in (200, 202), "code": resp.status_code, "body": body, "headers": resp.headers}


def poll_dm_status(dm_id):
    url = f"{BASE_URL}/v1/dm/{dm_id}"
    headers = {}
    if API_KEY:
        headers["X-API-Key"] = API_KEY
    # rate limit applies to polls as well
    with _rate_lock:
        now = time.time()
        while _recent_requests and _recent_requests[0] <= now - RATE_WINDOW:
            _recent_requests.pop(0)
        if len(_recent_requests) >= RATE_LIMIT:
            return None
        _recent_requests.append(now)
    try:
        resp = requests.get(url, headers=headers, timeout=6)
        return resp.json()
    except Exception:
        return None


def worker_loop():
    while True:
        db = get_db()
        cur = db.cursor()
        now = now_ts()
        cur.execute(
            "SELECT d.*, r.dm_message FROM deliveries d JOIN rules r ON d.rule_id=r.rule_id WHERE (d.status IN ('queued','retry') AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=?)) OR (d.status='accepted' AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=?)) LIMIT 5",
            (now, now),
        )
        rows = cur.fetchall()
        if not rows:
            db.close()
            time.sleep(1)
            continue
        for row in rows:
            delivery = dict(row)
            rule_msg = row["dm_message"]
            did_update = False
            # ensure we operate on a fresh short-lived connection for updates
            conn2 = get_db()
            cur2 = conn2.cursor()
            try:
                # if not yet sent (no dm_id), attempt POST
                if not delivery.get("dm_id") and delivery["status"] in ("queued", "retry"):
                    res = send_dm(delivery, rule_msg)
                    now2 = now_ts()
                    # handle local rate limit
                    if res.get("local_rate_limited"):
                        wait = res.get("retry_after", 5)
                        cur2.execute("UPDATE deliveries SET next_attempt_at=?, updated_at=? WHERE id=?", (now_ts() + wait, now_ts(), delivery["id"]))
                        conn2.commit()
                        did_update = True
                    elif res["ok"] and res.get("body") and res["body"].get("dm_id"):
                        dm_id = res["body"]["dm_id"]
                        cur2.execute(
                            "UPDATE deliveries SET dm_id=?, status=?, attempts=attempts+1, next_attempt_at=?, updated_at=? WHERE id=?",
                            (dm_id, "accepted", now2 + 5, now2, delivery["id"],),
                        )
                        conn2.commit()
                        did_update = True
                    else:
                        code = res.get("code")
                        # network errors -> treat like 500
                        if code is None:
                            attempts = (delivery.get("attempts") or 0) + 1
                            if attempts >= MAX_ATTEMPTS:
                                cur2.execute("UPDATE deliveries SET status=?, attempts=?, last_error=?, updated_at=? WHERE id=?", ("failed", attempts, json.dumps(res.get("error")), now_ts(), delivery["id"]))
                            else:
                                backoff = min(60, 2 ** attempts)
                                cur2.execute("UPDATE deliveries SET attempts=?, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?", (attempts, now_ts() + backoff, json.dumps(res.get("error")), now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
                        elif code == 429:
                            ra = res.get("headers", {}).get("Retry-After")
                            try:
                                wait = int(ra) if ra else 5
                            except Exception:
                                wait = 5
                            cur2.execute("UPDATE deliveries SET attempts=attempts+1, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?", (now_ts() + wait, json.dumps(res.get("body")), now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
                        elif code and code >= 500:
                            attempts = (delivery.get("attempts") or 0) + 1
                            if attempts >= MAX_ATTEMPTS:
                                cur2.execute("UPDATE deliveries SET status=?, attempts=?, last_error=?, updated_at=? WHERE id=?", ("failed", attempts, json.dumps(res.get("body")), now_ts(), delivery["id"]))
                            else:
                                backoff = min(60, 2 ** attempts)
                                cur2.execute("UPDATE deliveries SET attempts=?, next_attempt_at=?, last_error=?, updated_at=? WHERE id=?", (attempts, now_ts() + backoff, json.dumps(res.get("body")), now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
                        else:
                            # client error or unknown -> mark failed
                            cur2.execute("UPDATE deliveries SET status=?, last_error=?, updated_at=? WHERE id=?", ("failed", json.dumps(res.get("body")), now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
                elif delivery.get("dm_id") and delivery["status"] == "accepted":
                    info = poll_dm_status(delivery["dm_id"])
                    if not info:
                        cur2.execute("UPDATE deliveries SET next_attempt_at=?, updated_at=? WHERE id=?", (now_ts() + 5, now_ts(), delivery["id"]))
                        conn2.commit()
                        did_update = True
                    else:
                        st = info.get("status")
                        if st == "delivered":
                            cur2.execute("UPDATE deliveries SET status=?, updated_at=? WHERE id=?", ("delivered", now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
                        elif st == "failed":
                            cur2.execute("UPDATE deliveries SET status=?, updated_at=?, last_error=? WHERE id=?", ("failed", now_ts(), json.dumps(info), delivery["id"]))
                            conn2.commit()
                            did_update = True
                        else:
                            cur2.execute("UPDATE deliveries SET next_attempt_at=?, updated_at=? WHERE id=?", (now_ts() + 5, now_ts(), delivery["id"]))
                            conn2.commit()
                            did_update = True
            finally:
                conn2.close()
            # nothing updated? avoid tight loop
            if not did_update:
                # mark to check later
                conn3 = get_db()
                c3 = conn3.cursor()
                c3.execute("UPDATE deliveries SET next_attempt_at=?, updated_at=? WHERE id=?", (now_ts() + 5, now_ts(), delivery["id"]))
                conn3.commit()
                conn3.close()
        db.close()
        # small sleep to yield
        time.sleep(0.1)


@app.route("/stats", methods=["GET"])
def stats():
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) FROM deliveries WHERE status='delivered'")
    sent = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM deliveries WHERE status='failed'")
    failed = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM deliveries WHERE status IN ('queued','accepted','retry')")
    queued = cur.fetchone()[0]
    cur.execute("SELECT value FROM metrics WHERE key='duplicates_blocked'")
    row = cur.fetchone()
    duplicates = row[0] if row else 0
    db.close()
    return jsonify({"sent": sent, "failed": failed, "queued": queued, "duplicates_blocked": duplicates})


# ensure DB is initialized when module is imported (WSGI-safe)
init_db()
# start background worker thread once
if not WORKER_STARTED:
    t = threading.Thread(target=worker_loop, daemon=True)
    t.start()
    WORKER_STARTED = True

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
