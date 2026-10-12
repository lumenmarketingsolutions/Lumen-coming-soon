"""Scheduled reminders for Kendall: email (Resend) + short text from the Lumen 208 number (SignalHouse).

A background thread checks the `reminders` table every 5 minutes and sends anything due.
Two kinds:
  - "note":        a fixed subject + text + email body
  - "kage_check":  pulls live Kage Epoxy ad numbers from Meta at send time and reports per ad

Add one from anywhere (key = GHL_SMS_WEBHOOK_KEY):
  POST /lumen-reminders/add?k=KEY  {"key": "unique-id", "due_utc": "2026-10-17T16:00", "kind": "note",
                                    "subject": "...", "text": "...", "html": "..."}
  GET  /lumen-reminders/list?k=KEY
Each reminder is claimed with an atomic UPDATE before sending, so it goes out once even with several workers.
"""
import os, json, time, sqlite3, logging, threading, datetime
import requests
from flask import Blueprint, request, jsonify

log = logging.getLogger(__name__)
reminders_bp = Blueprint("lumen_reminders", __name__, url_prefix="/lumen-reminders")

_DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_DATA_DIR, "lumen_reminders.db")
OWNER_EMAIL = "kendall@lumenmarketing.co"
GRAPH = "https://graph.facebook.com/v21.0"

# Seeded on startup (INSERT OR IGNORE, so redeploys never duplicate)
SEED = [
    {"key": "kage_v4_day5", "due_utc": "2026-10-17T16:00", "kind": "kage_check",
     "payload": {"since": "2026-10-12", "adset": "120254115612140321", "control_cpl": 49}},
]


def _db():
    con = sqlite3.connect(DB_PATH, timeout=10)
    con.execute("""CREATE TABLE IF NOT EXISTS reminders (
        key TEXT PRIMARY KEY, due_utc TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL,
        claimed_at TEXT, sent_at TEXT, result TEXT)""")
    return con


def add(key, due_utc, kind, payload):
    with _db() as con:
        con.execute("INSERT OR IGNORE INTO reminders (key, due_utc, kind, payload) VALUES (?,?,?,?)",
                    (key, due_utc, kind, json.dumps(payload)))


# ── senders ──────────────────────────────────────────────────────────────────
def _email(subject, html):
    r = requests.post("https://api.resend.com/emails", timeout=20,
                      headers={"Authorization": f"Bearer {os.environ.get('RESEND_API_KEY', '')}", "User-Agent": "lumen-reminders"},
                      json={"from": "Jarvis <notifications@lumenmarketing.co>", "to": [OWNER_EMAIL],
                            "subject": subject, "html": html})
    return r.status_code


def _text(body):
    from ghl_sms_provider import _signalhouse_send, _env
    _, err = _signalhouse_send(_env("OWNER_ALERT_NUMBER", "12085910132"), body[:600])
    return err or "ok"


# ── reminder kinds ───────────────────────────────────────────────────────────
def _kage_check(p):
    tok = os.environ.get("LUMEN_META_CAPI_TOKEN", "")
    today = datetime.date.today().isoformat()
    r = requests.get(f"{GRAPH}/{p['adset']}/insights", timeout=30, params={
        "level": "ad", "fields": "ad_name,spend,impressions,ctr,actions", "limit": 50, "access_token": tok,
        "time_range": json.dumps({"since": p["since"], "until": today})}).json()
    if "error" in r:
        raise RuntimeError(r["error"].get("message"))
    rows = []
    for x in r.get("data", []):
        leads = sum(int(a["value"]) for a in x.get("actions", []) if a["action_type"] == "lead")
        rows.append((x["ad_name"], float(x["spend"]), int(x["impressions"]), float(x.get("ctr", 0)), leads))
    rows.sort(key=lambda t: -t[1])
    spend = sum(t[1] for t in rows); leads = sum(t[4] for t in rows)
    cpl = f"${spend / leads:.0f}" if leads else "-"
    day = (datetime.date.today() - datetime.date.fromisoformat(p["since"])).days
    short = lambda n: n.replace("KE - V4 - ", "").split(" (")[0]
    starved = [short(t[0]) for t in rows if "V4" in t[0] and t[1] < 10] if day >= 4 else []
    best = min((t for t in rows if t[4]), key=lambda t: t[1] / t[4], default=None)

    sms = (f"Kage day {day}: ${spend:.0f} spent, {leads} leads, {cpl}/lead (target under ${p['control_cpl']})." if leads
           else f"Kage day {day}: ${spend:.0f} spent, no leads yet (target under ${p['control_cpl']}/lead).")
    if best:
        sms += f" Best: {short(best[0])} ${best[1] / best[4]:.0f}/lead."
    if starved:
        sms += f" Starved: {', '.join(starved)}."
    sms += " Details in email."

    tr = "".join(f"<tr><td>{n}</td><td align=right>${s:.2f}</td><td align=right>{i:,}</td><td align=right>{c:.2f}%</td>"
                 f"<td align=right>{l}</td><td align=right>{'$%.0f' % (s / l) if l else '-'}</td></tr>" for n, s, i, c, l in rows)
    steps = []
    if starved:
        steps.append(f"Starved (under $10 since launch): {', '.join(starved)}. If this includes Business Floors, move it to its own ad set at $5/day, same geo, broad.")
    if leads and spend / leads < p["control_cpl"]:
        steps.append(f"Cost per lead is beating the old ${p['control_cpl']} control. Leave it running, no edits.")
    elif spend > 0:
        steps.append(f"Not beating ${p['control_cpl']} yet. Give it to day 10-14, then keep winners and turn 5 Star Rated back on if nothing beats it.")
    steps.append("Still open: the fake 555 phone link on kageepoxy.com (Colton, Lovable).")
    html = (f"<p>Kage Epoxy, V4 creatives, {p['since']} to {today}.</p>"
            f"<table border=1 cellpadding=6 style='border-collapse:collapse;font-family:Arial;font-size:13px'>"
            f"<tr><th>Ad</th><th>Spend</th><th>Impr.</th><th>CTR</th><th>Leads</th><th>Cost/lead</th></tr>{tr}"
            f"<tr><td><b>Total</b></td><td align=right><b>${spend:.2f}</b></td><td></td><td></td><td align=right><b>{leads}</b></td><td align=right><b>{cpl}</b></td></tr></table>"
            f"<p><b>What to do</b></p><ul>{''.join(f'<li>{s}</li>' for s in steps)}</ul>")
    return f"Kage Epoxy day {day} check: {leads} leads, {cpl} per lead", html, sms


def _render(kind, p):
    if kind == "kage_check":
        return _kage_check(p)
    return p.get("subject", "Reminder"), p.get("html") or f"<p>{p.get('text', '')}</p>", p.get("text", "")


# ── loop ─────────────────────────────────────────────────────────────────────
def _tick():
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
    with _db() as con:
        due = con.execute("SELECT key, kind, payload FROM reminders WHERE sent_at IS NULL AND claimed_at IS NULL AND due_utc <= ?", (now,)).fetchall()
    for key, kind, payload in due:
        with _db() as con:
            got = con.execute("UPDATE reminders SET claimed_at=? WHERE key=? AND claimed_at IS NULL", (now, key)).rowcount
        if not got:
            continue  # another worker took it
        try:
            subject, html, sms = _render(kind, json.loads(payload))
            res = {"email": _email(subject, html), "text": _text(sms) if sms else "skipped"}
        except Exception as e:
            log.exception("reminder %s failed", key)
            res = {"error": str(e)}
            try:
                _text(f"Reminder {key} failed to build: {str(e)[:120]}")
            except Exception:
                pass
        with _db() as con:
            con.execute("UPDATE reminders SET sent_at=?, result=? WHERE key=?",
                        (datetime.datetime.utcnow().isoformat(), json.dumps(res), key))
        print(f"[Reminders] sent {key}: {res}")


def start_reminders():
    for s in SEED:
        add(s["key"], s["due_utc"], s["kind"], s["payload"])

    def loop():
        while True:
            try:
                _tick()
            except Exception as e:
                log.exception("reminders tick failed: %s", e)
            time.sleep(300)

    threading.Thread(target=loop, daemon=True).start()
    print("[Reminders] started")


# ── routes ───────────────────────────────────────────────────────────────────
def _ok():
    return request.args.get("k") and request.args.get("k") == os.environ.get("GHL_SMS_WEBHOOK_KEY")


@reminders_bp.route("/add", methods=["POST"])
def add_route():
    if not _ok():
        return "forbidden", 403
    d = request.get_json(force=True) or {}
    if not d.get("key") or not d.get("due_utc"):
        return jsonify(error="key and due_utc required"), 400
    kind = d.get("kind", "note")
    payload = d.get("payload") or {k: d[k] for k in ("subject", "text", "html") if k in d}
    add(d["key"], d["due_utc"][:16], kind, payload)
    return jsonify(ok=True)


@reminders_bp.route("/list")
def list_route():
    if not _ok():
        return "forbidden", 403
    with _db() as con:
        rows = con.execute("SELECT key, due_utc, kind, sent_at, result FROM reminders ORDER BY due_utc").fetchall()
    return jsonify([dict(zip(("key", "due_utc", "kind", "sent_at", "result"), r)) for r in rows])
