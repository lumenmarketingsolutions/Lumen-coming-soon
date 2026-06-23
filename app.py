from flask import Flask, render_template, request, jsonify, session, redirect, url_for, make_response
import os, sqlite3, datetime, uuid, json, threading, requests, csv, io, time, base64, re, hashlib
from email.mime.text import MIMEText
import phorest as mane_phorest  # Mane → Phorest client sync (no-op if env vars unset)

try:
    from zoneinfo import ZoneInfo
    _MTN_TZ = ZoneInfo("America/Denver")  # Mountain Time, DST-aware (Boise + Idaho)
except Exception:
    _MTN_TZ = None  # zoneinfo unavailable on the host; helper will fall back


def _td_record_owner_ip(ip, email):
    """Stash an IP into td_owner_ips so future /t/td events from that IP get
    is_owner=1 and are excluded from analytics. Fired on every successful
    login at /tristandare/admin (Kendall + Tristan both add their current IP)."""
    if not ip:
        return
    try:
        now = datetime.datetime.utcnow().isoformat()
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO td_owner_ips (ip, email, first_seen, last_seen)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ip) DO UPDATE SET email = excluded.email, last_seen = excluded.last_seen
        """, (ip, email, now, now))
        con.commit()
        con.close()
    except Exception as e:
        print(f"[td-owner-ip] insert failed: {e}")


def _td_is_owner_ip(ip):
    """True if the IP is in the static OWNER_IPS seed OR has been registered
    via a /tristandare/admin login. Cheap SQLite point lookup on indexed PK."""
    if not ip:
        return False
    if ip in OWNER_IPS:
        return True
    try:
        con = sqlite3.connect(DB_PATH)
        row = con.execute("SELECT 1 FROM td_owner_ips WHERE ip = ?", (ip,)).fetchone()
        con.close()
        return row is not None
    except Exception:
        return False


def _utc_iso_to_mtn(iso_utc):
    """UTC ISO timestamp -> human-readable Mountain Time (DST-aware).
    'May 13, 04:32 PM MDT' in summer, '... MST' in winter. Falls back to the
    raw UTC slice if the zoneinfo data isn't available on the host."""
    if not iso_utc:
        return ""
    raw = iso_utc[:19].replace("T", " ")
    if not _MTN_TZ:
        return raw + " UTC"
    try:
        dt = datetime.datetime.fromisoformat(iso_utc[:19])
        dt = dt.replace(tzinfo=datetime.timezone.utc).astimezone(_MTN_TZ)
        return dt.strftime("%b %d, %I:%M %p %Z")
    except Exception:
        return raw + " UTC"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "lumen-wl-key-2026")
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000  # cache static files 1 year

# Keep admin/client logins valid for 60 days so Kendall and clients don't get
# bounced back to the login screen on every visit (esp. on mobile, where
# browsers age short-lived sessions aggressively). Cookies marked Secure +
# HttpOnly + SameSite=Lax on Railway so they ride HTTPS but stay usable.
app.permanent_session_lifetime = datetime.timedelta(days=60)
if os.environ.get("RAILWAY_PROJECT_ID") or os.environ.get("RAILWAY_ENVIRONMENT"):
    app.config["SESSION_COOKIE_SECURE"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_HTTPONLY"] = True

from fathom_webhook import fathom_bp
app.register_blueprint(fathom_bp)
from meta_leads_webhook import meta_leads_bp
app.register_blueprint(meta_leads_bp)

from sce_mothersday import sce_md_bp, init_md_db
app.register_blueprint(sce_md_bp)
init_md_db()

from sce_fathersday import sce_fd_bp, init_fd_db
app.register_blueprint(sce_fd_bp)
init_fd_db()

from sce_z51 import sce_z51_bp, init_z51_db
app.register_blueprint(sce_z51_bp)
init_z51_db()

from sce_admin import sce_admin_bp
app.register_blueprint(sce_admin_bp)

# MK7 Setter CRM — see crm.py + templates/crm/* for the full module.
from crm import crm_bp
app.register_blueprint(crm_bp)

# MK7 Outreach Agent — admin-only cold-email blast tool, mounted at /crm/outreach.
# Order matters: outreach_scrapers attaches routes to outreach_bp at import time,
# so it must be imported BEFORE register_blueprint() — Flask freezes blueprints
# on registration and rejects further .route() decorators with an AssertionError.
from outreach import outreach_bp
import outreach_scrapers  # noqa: F401  (registers /crm/outreach/scrapers/* routes)
app.register_blueprint(outreach_bp)

# MK7 WhatsApp Outreach Agent — Phase 1 foundation, mounted at /crm/whatsapp.
# Send pipeline ports across from mk7media/agents/whatsapp_agent in Phase 2.
from whatsapp_outreach import whatsapp_bp
app.register_blueprint(whatsapp_bp)

ADMIN_PIN = "112501"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL = "kendall@lumenmarketing.co"
# Mane funnel leads notify Lumen AND the salon front desk so they can follow up fast
MANE_LEAD_TO = [NOTIFY_EMAIL, "manestylingstudio@gmail.com"]
OWNER_IPS = {"209.127.238.130"}

# ── Meta Conversions API (Mane funnels) ────────────────────────────────────
# Pixel ID is hardcoded since it's public anyway. Access token comes from env;
# without it, CAPI is a no-op and only the client-side Pixel fires.
# Env vars are MANE_-prefixed (matches LUMEN_ pattern below) so each client's
# token stays clearly scoped on Railway even though one System User may carry
# tokens for multiple clients.
# Get a token: Meta Business Settings → System Users → pick (or create) one →
# assign Mane's pixel → Generate Token with ads_management scope. Set as
# MANE_META_CAPI_ACCESS_TOKEN on Railway. Optionally set
# MANE_META_TEST_EVENT_CODE while QAing in Events Manager Test Events.
MANE_META_PIXEL_ID = "983900467721637"
MANE_META_CAPI_ACCESS_TOKEN = os.environ.get("MANE_META_CAPI_ACCESS_TOKEN", "")
MANE_META_TEST_EVENT_CODE  = os.environ.get("MANE_META_TEST_EVENT_CODE", "")

def _meta_hash(value):
    if not value:
        return None
    return hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()

def send_meta_capi_event(event_name, event_id, user_data, custom_data=None, source_url=None, pixel_id=None):
    """Fire a Meta Conversions API event. Fire-and-forget — logs but never
    raises. No-op when MANE_META_CAPI_ACCESS_TOKEN is unset.

    user_data: dict with optional keys: email, phone, first_name, client_ip,
               user_agent, fbp, fbc, fbclid.
    """
    if not MANE_META_CAPI_ACCESS_TOKEN:
        return  # CAPI not configured

    pixel_id = pixel_id or MANE_META_PIXEL_ID

    hashed = {}
    if user_data.get("email"):
        hashed["em"] = [_meta_hash(user_data["email"])]
    if user_data.get("phone"):
        # Strip non-digits before hashing (Meta wants E.164-ish; digits-only works)
        digits = "".join(c for c in str(user_data["phone"]) if c.isdigit())
        if digits:
            hashed["ph"] = [_meta_hash(digits)]
    if user_data.get("first_name"):
        hashed["fn"] = [_meta_hash(user_data["first_name"])]
    if user_data.get("client_ip"):
        hashed["client_ip_address"] = user_data["client_ip"]
    if user_data.get("user_agent"):
        hashed["client_user_agent"] = user_data["user_agent"]
    if user_data.get("fbp"):
        hashed["fbp"] = user_data["fbp"]
    if user_data.get("fbc"):
        hashed["fbc"] = user_data["fbc"]
    elif user_data.get("fbclid"):
        # Construct fbc from raw fbclid: fb.1.{ts}.{fbclid}
        hashed["fbc"] = f"fb.1.{int(time.time())}.{user_data['fbclid']}"

    event = {
        "event_name": event_name,
        "event_time": int(time.time()),
        "event_id": event_id or "",
        "action_source": "website",
        "user_data": hashed,
    }
    if source_url:
        event["event_source_url"] = source_url
    if custom_data:
        event["custom_data"] = custom_data

    payload = {"data": [event]}
    if MANE_META_TEST_EVENT_CODE:
        payload["test_event_code"] = MANE_META_TEST_EVENT_CODE

    url = f"https://graph.facebook.com/v18.0/{pixel_id}/events?access_token={MANE_META_CAPI_ACCESS_TOKEN}"
    try:
        r = requests.post(url, json=payload, timeout=5)
        if not r.ok:
            print(f"[meta-capi] {event_name} → HTTP {r.status_code}: {r.text[:240]}")
    except Exception as e:
        print(f"[meta-capi] {event_name} → exception: {e}")

# ── Phorest → Meta booking-attribution worker ──────────────────────────────
# Polls Phorest every MANE_PHOREST_POLL_SECS for new appointments. For any
# appointment whose clientId matches a phorest_client_id we stamped on a
# funnel lead, fire a Meta CAPI `Schedule` event back to Mane's pixel so
# Meta optimizes on real bookings, not just form fills. No-op when Phorest
# isn't configured. Worker is daemon-threaded — dies with the app, restarts
# clean on Railway redeploy (state persisted in SQLite).
MANE_PHOREST_POLL_SECS = int(os.environ.get("MANE_PHOREST_POLL_SECS", "900"))  # 15 min
# Mane's Phorest account id (shown on the branch response as accountId). Used
# to build "Open in Phorest" deep-links inside the lead notification emails.
MANE_PHOREST_ACCOUNT_ID = os.environ.get("MANE_PHOREST_ACCOUNT_ID", "27762")

def _phorest_client_url(client_id):
    if not client_id:
        return ""
    return f"https://my.phorest.com/a/{MANE_PHOREST_ACCOUNT_ID}/clients/{client_id}/overview"

def _ensure_phorest_state_table():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mane_phorest_state (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mane_phorest_fired_appts (
            appointment_id TEXT PRIMARY KEY,
            fired_at TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()

def _phorest_state_get(key, default=None):
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("SELECT v FROM mane_phorest_state WHERE k = ?", (key,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else default

def _phorest_state_set(key, value):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR REPLACE INTO mane_phorest_state (k, v) VALUES (?, ?)", (key, value))
    con.commit()
    con.close()

def _phorest_appt_already_fired(appt_id):
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("SELECT 1 FROM mane_phorest_fired_appts WHERE appointment_id = ?", (appt_id,))
    found = cur.fetchone() is not None
    con.close()
    return found

def _phorest_appt_mark_fired(appt_id):
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT OR IGNORE INTO mane_phorest_fired_appts (appointment_id, fired_at) VALUES (?, ?)",
                (appt_id, datetime.datetime.utcnow().isoformat()))
    con.commit()
    con.close()

def _phorest_lookup_lead(phorest_client_id):
    """Returns (table, email, phone, first_name, funnel_label) for the matching
    lead, or None if no funnel lead is tied to this Phorest client."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    for table, label in [("mane_extension_quiz", "extensions_funnel"),
                         ("mane_color_quiz",     "color_funnel")]:
        try:
            row = con.execute(
                f"SELECT name, email, phone FROM {table} WHERE phorest_client_id = ? LIMIT 1",
                (phorest_client_id,),
            ).fetchone()
        except sqlite3.OperationalError:
            continue
        if row:
            con.close()
            return (table, row["email"], row["phone"], row["name"], label)
    con.close()
    return None

def _phorest_poll_once():
    if not mane_phorest._configured():
        return
    _ensure_phorest_state_table()

    # Cursor: high-water-mark of updated_from. First run = now (don't fire
    # CAPI for historical appointments — only forward).
    cursor_iso = _phorest_state_get("appt_cursor")
    if not cursor_iso:
        cursor_iso = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        _phorest_state_set("appt_cursor", cursor_iso)
    try:
        cursor_dt = datetime.datetime.strptime(cursor_iso, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        cursor_dt = datetime.datetime.utcnow()

    new_high = cursor_dt
    matched = 0
    for appt in mane_phorest.list_appointments_since(cursor_dt):
        appt_id = appt.get("appointmentId")
        client_id = appt.get("clientId")
        updated = appt.get("updatedAt") or appt.get("createdAt")
        if appt_id and updated:
            try:
                u = datetime.datetime.strptime(updated[:19], "%Y-%m-%dT%H:%M:%S")
                if u > new_high:
                    new_high = u
            except Exception:
                pass
        if not appt_id or not client_id:
            continue
        if _phorest_appt_already_fired(appt_id):
            continue
        lead = _phorest_lookup_lead(client_id)
        if not lead:
            continue  # walk-in or non-funnel client, ignore
        table, email, phone, first_name, funnel_label = lead
        send_meta_capi_event(
            event_name="Schedule",
            event_id=f"phorest-schedule-{appt_id}",
            user_data={
                "email": email,
                "phone": phone,
                "first_name": first_name if first_name and first_name != "Anonymous" else "",
            },
            custom_data={
                "content_category": funnel_label,
                "phorest_appointment_id": appt_id,
            },
        )
        _phorest_appt_mark_fired(appt_id)
        matched += 1

    # Advance cursor to the latest updatedAt we saw (or "now" if no rows)
    if new_high == cursor_dt:
        new_high = datetime.datetime.utcnow()
    _phorest_state_set("appt_cursor", new_high.strftime("%Y-%m-%dT%H:%M:%SZ"))
    if matched:
        print(f"[phorest-poll] fired Schedule for {matched} matched appointment(s)")

def _phorest_poll_loop():
    # Stagger startup so we don't slam Phorest immediately on deploy
    time.sleep(30)
    while True:
        try:
            _phorest_poll_once()
        except Exception as e:
            print(f"[phorest-poll] error: {e}")
        time.sleep(MANE_PHOREST_POLL_SECS)

def _maybe_start_phorest_poller():
    if not mane_phorest._configured():
        return
    # Gated by explicit env flag so we don't fire Schedule events to Meta until
    # the client explicitly opts in. Kendall's current strategy is Lead-event
    # optimization only — no Schedule attribution. Flip to "true" later if we
    # want bookings to feed Meta's optimizer.
    if os.environ.get("MANE_PHOREST_POLL_ENABLED", "false").lower() not in ("true", "1", "yes"):
        print("[phorest-poll] worker dormant (MANE_PHOREST_POLL_ENABLED not set)")
        return
    t = threading.Thread(target=_phorest_poll_loop, daemon=True, name="mane-phorest-poller")
    t.start()
    print(f"[phorest-poll] worker started (every {MANE_PHOREST_POLL_SECS}s)")

# Start the worker once at import time. Idempotent — only starts if Phorest
# env vars are set, so it's a no-op locally without credentials.
_maybe_start_phorest_poller()

# ── Tristan Dare external-site tracking (Squarespace tristandareshop.com) ──
# Shared token gates the public /t/td endpoint; the value is also pasted into
# the tracker <script> on the Squarespace page. Override on Railway if needed.
TD_TRACK_TOKEN = os.environ.get("TD_TRACK_TOKEN", "td-pub-2026-bejetixdare")
TD_ALLOWED_ORIGINS = {
    "https://www.tristandareshop.com",
    "https://tristandareshop.com",
}
# Cap any single session's reported visible time so a runaway tab can't skew
# averages. 90 minutes is generous for a long-form sales page.
TD_MAX_SECONDS = 90 * 60
# Logins for the Tristan dashboard at /tristandare/admin. Two valid accounts:
# Kendall (so he can spot-check the client-facing view) and Tristan himself.
# Separate from Kendall's master admin (wl_auth) so Tristan can sign in here
# without ever touching anything else on the Mainframe.
TD_CLIENT_ACCOUNTS = {
    (
        (os.environ.get("TD_CLIENT_EMAIL_1") or "kendall@lumenmarketing.co").strip().lower(),
        os.environ.get("TD_CLIENT_PASSWORD_1") or "Wifimoney420!",
    ),
    (
        (os.environ.get("TD_CLIENT_EMAIL_2") or "tristandareshop@gmail.com").strip().lower(),
        os.environ.get("TD_CLIENT_PASSWORD_2") or "Meteorite26!",
    ),
}

# ── Live presence tracking (in-memory) ──
# {session_id: {"page": "home", "last_seen": timestamp, "ip": "..."}}
LIVE_VISITORS = {}
LIVE_LOCK = threading.Lock()

def clean_stale_visitors():
    """Remove visitors who haven't sent a heartbeat in 15 seconds."""
    cutoff = time.time() - 15
    with LIVE_LOCK:
        stale = [k for k, v in LIVE_VISITORS.items() if v["last_seen"] < cutoff]
        for k in stale:
            del LIVE_VISITORS[k]

# ── Marykate Agent Config ──
MK_PIN = os.environ.get("MK_PIN", "091005")
GMAIL_CLIENT_ID = os.environ.get("GMAIL_CLIENT_ID", "")
GMAIL_CLIENT_SECRET = os.environ.get("GMAIL_CLIENT_SECRET", "")
GMAIL_REDIRECT_URI = os.environ.get("GMAIL_REDIRECT_URI", "https://lumenmarketing.co/marykate/gmail/callback")
TWILIO_SID = os.environ.get("TWILIO_SID", "")
TWILIO_AUTH = os.environ.get("TWILIO_AUTH", "")
TWILIO_PHONE = os.environ.get("TWILIO_PHONE", "")


def send_email(to, subject, html_body):
    """Send an email via Resend API in a background thread."""
    def _send():
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": "Lumen <kendall@lumenmarketing.co>",
                    "to": to if isinstance(to, list) else [to],
                    "subject": subject,
                    "html": html_body,
                },
            )
            print(f"Email to {to}: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"Email send error: {e}")
    threading.Thread(target=_send).start()

# Use persistent volume on Railway (/data), fall back to local dir for dev
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "waitlist.db")

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS waitlist (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            created_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS page_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            time_on_page REAL DEFAULT 0,
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            screen TEXT DEFAULT '',
            language TEXT DEFAULT '',
            timezone TEXT DEFAULT '',
            platform TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_medium TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            city TEXT DEFAULT '',
            region TEXT DEFAULT '',
            country TEXT DEFAULT ''
        )
    """)
    con.commit()

    # Migrate: add new columns if they don't exist
    existing = [r[1] for r in con.execute("PRAGMA table_info(page_views)").fetchall()]
    new_cols = {
        "ip": "TEXT DEFAULT ''", "user_agent": "TEXT DEFAULT ''",
        "referrer": "TEXT DEFAULT ''", "screen": "TEXT DEFAULT ''",
        "language": "TEXT DEFAULT ''", "timezone": "TEXT DEFAULT ''",
        "platform": "TEXT DEFAULT ''", "utm_source": "TEXT DEFAULT ''",
        "utm_medium": "TEXT DEFAULT ''", "utm_campaign": "TEXT DEFAULT ''",
        "utm_content": "TEXT DEFAULT ''", "city": "TEXT DEFAULT ''",
        "region": "TEXT DEFAULT ''", "country": "TEXT DEFAULT ''",
        "page": "TEXT DEFAULT 'coming-soon'",
    }
    for col, dtype in new_cols.items():
        if col not in existing:
            con.execute(f"ALTER TABLE page_views ADD COLUMN {col} {dtype}")
    con.commit()

    # Funnel events table for internal analytics
    con.execute("""
        CREATE TABLE IF NOT EXISTS funnel_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT DEFAULT '',
            event TEXT NOT NULL,
            step TEXT DEFAULT '',
            value TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            timestamp TEXT NOT NULL
        )
    """)
    con.commit()

    # ── CRM tables ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            phone TEXT DEFAULT '',
            business TEXT DEFAULT '',
            revenue TEXT DEFAULT '',
            marketing TEXT DEFAULT '',
            challenge TEXT DEFAULT '',
            stage TEXT DEFAULT 'new',
            source TEXT DEFAULT 'application',
            deal_value REAL DEFAULT 0,
            follow_up_date TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS lead_activity (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            content TEXT DEFAULT '',
            metadata TEXT DEFAULT '',
            created_at TEXT NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS lead_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            lead_id INTEGER NOT NULL,
            subject TEXT DEFAULT '',
            body TEXT DEFAULT '',
            direction TEXT DEFAULT 'sent',
            created_at TEXT NOT NULL,
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        )
    """)
    con.commit()

    # ── Pipeline & stage config tables ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipelines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_stages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline_id INTEGER NOT NULL,
            slug TEXT NOT NULL,
            name TEXT NOT NULL,
            color TEXT NOT NULL DEFAULT '#7c4dff',
            position INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (pipeline_id) REFERENCES pipelines(id)
        )
    """)
    con.commit()

    # Seed default pipeline if none exists
    existing_pipeline = con.execute("SELECT id FROM pipelines LIMIT 1").fetchone()
    if not existing_pipeline:
        now = datetime.datetime.utcnow().isoformat()
        cur = con.execute("INSERT INTO pipelines (name, created_at, updated_at) VALUES ('Main Pipeline', ?, ?)", (now, now))
        pid = cur.lastrowid
        default_stages = [
            ("new", "New", "#7c4dff", 0),
            ("contacted", "Contacted", "#3b82f6", 1),
            ("discovery", "Discovery", "#f59e0b", 2),
            ("proposal", "Proposal", "#f97316", 3),
            ("won", "Won", "#22c55e", 4),
            ("lost", "Lost", "#ef4444", 5),
        ]
        for slug, name, color, pos in default_stages:
            con.execute("INSERT INTO pipeline_stages (pipeline_id, slug, name, color, position) VALUES (?, ?, ?, ?, ?)",
                        (pid, slug, name, color, pos))
        con.commit()

    # Migrate leads table if needed
    existing_leads_cols = [r[1] for r in con.execute("PRAGMA table_info(leads)").fetchall()]
    leads_new_cols = {"tags": "TEXT DEFAULT ''", "pipeline_id": "INTEGER DEFAULT 1"}
    for col, dtype in leads_new_cols.items():
        if col not in existing_leads_cols:
            con.execute(f"ALTER TABLE leads ADD COLUMN {col} {dtype}")
    con.commit()

    # ── Client dashboard tracking ──
    con.execute("""
        CREATE TABLE IF NOT EXISTS client_dashboards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            url TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS dashboard_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dashboard_slug TEXT NOT NULL,
            event TEXT NOT NULL DEFAULT 'open',
            session_id TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            duration REAL DEFAULT 0,
            timestamp TEXT NOT NULL
        )
    """)
    con.commit()

    # Seed default client dashboards
    existing_dash = con.execute("SELECT id FROM client_dashboards LIMIT 1").fetchone()
    if not existing_dash:
        now = datetime.datetime.utcnow().isoformat()
        con.execute("INSERT INTO client_dashboards (name, slug, url, created_at) VALUES (?, ?, ?, ?)",
                    ("Avalon Laser", "avalon-laser", "https://web-production-7e73c.up.railway.app", now))
        con.execute("INSERT INTO client_dashboards (name, slug, url, created_at) VALUES (?, ?, ?, ?)",
                    ("Berry Clean", "berry-clean", "", now))
        con.commit()

    # Agent consulting onboarding form submissions
    con.execute("""
        CREATE TABLE IF NOT EXISTS agent_onboarding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            business TEXT DEFAULT '',
            business_oneliner TEXT DEFAULT '',
            role TEXT DEFAULT '',
            ai_level TEXT DEFAULT '',
            claude_experience TEXT DEFAULT '',
            tools_tried TEXT DEFAULT '',
            where_stuck TEXT DEFAULT '',
            repetitive_work TEXT DEFAULT '',
            first_handoff TEXT DEFAULT '',
            hours_lost TEXT DEFAULT '',
            snap_vision TEXT DEFAULT '',
            top_three_agents TEXT DEFAULT '',
            tech_level TEXT DEFAULT '',
            learning_style TEXT DEFAULT '',
            call_outcome TEXT DEFAULT '',
            biggest_question TEXT DEFAULT '',
            anything_else TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.commit()

    # Mane Styling Studio marketing onboarding form submissions
    con.execute("""
        CREATE TABLE IF NOT EXISTS mane_onboarding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            business TEXT DEFAULT '',
            role TEXT DEFAULT '',
            business_oneliner TEXT DEFAULT '',
            services_pricing TEXT DEFAULT '',
            priority_services TEXT DEFAULT '',
            offers_deals TEXT DEFAULT '',
            ads_before TEXT DEFAULT '',
            ads_detail TEXT DEFAULT '',
            existing_content TEXT DEFAULT '',
            lead_delivery TEXT DEFAULT '',
            lead_delivery_notes TEXT DEFAULT '',
            lead_handler TEXT DEFAULT '',
            metrics_focus TEXT DEFAULT '',
            target_number TEXT DEFAULT '',
            success_60days TEXT DEFAULT '',
            anything_else TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.commit()

    # Mane Styling Studio color funnel leads
    con.execute("""
        CREATE TABLE IF NOT EXISTS mane_color_quiz (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            current_color TEXT DEFAULT '',
            dream_look TEXT DEFAULT '',
            recommendation TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            fbclid TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.commit()

    # Mane Styling Studio extensions funnel leads (consult-focused, no rec routing)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mane_extension_quiz (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            goal TEXT DEFAULT '',
            timeline TEXT DEFAULT '',
            budget TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            fbclid TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    # Idempotent column adds for tables created before each column existed
    for _table, _col in [
        ("mane_extension_quiz", "budget TEXT DEFAULT ''"),
        ("mane_extension_quiz", "phorest_client_id TEXT DEFAULT ''"),
        ("mane_color_quiz",     "phorest_client_id TEXT DEFAULT ''"),
        # A/B test tagging: '' / 'test-1' = original quiz funnels, 'test-2' = simplified v2
        ("mane_extension_quiz", "variant TEXT DEFAULT ''"),
        ("mane_color_quiz",     "variant TEXT DEFAULT ''"),
    ]:
        try:
            con.execute(f"ALTER TABLE {_table} ADD COLUMN {_col}")
        except sqlite3.OperationalError:
            pass  # column already exists
    con.commit()

    # Avalon CRM onboarding form submissions
    con.execute("""
        CREATE TABLE IF NOT EXISTS avalon_onboarding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            role TEXT DEFAULT '',
            daily_work TEXT DEFAULT '',
            moxie_likes TEXT DEFAULT '',
            moxie_frustrations TEXT DEFAULT '',
            lead_to_blvd TEXT DEFAULT '',
            ideal_workflow TEXT DEFAULT '',
            dream_features TEXT DEFAULT '',
            auto_vs_manual TEXT DEFAULT '',
            integrations TEXT DEFAULT '',
            anything_else TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.commit()

    # Marykate agent tables
    con.execute("""
        CREATE TABLE IF NOT EXISTS mk_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            tags TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mk_campaigns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            channel TEXT NOT NULL,
            subject TEXT DEFAULT '',
            body TEXT NOT NULL,
            status TEXT DEFAULT 'draft',
            sent_count INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mk_send_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_id INTEGER,
            lead_id INTEGER,
            channel TEXT NOT NULL,
            recipient TEXT NOT NULL,
            status TEXT DEFAULT 'sent',
            error TEXT DEFAULT '',
            sent_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mk_gmail_tokens (
            id INTEGER PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            expires_at TEXT,
            email TEXT DEFAULT ''
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS mk_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            channel TEXT NOT NULL DEFAULT 'email',
            subject TEXT DEFAULT '',
            body TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """)

    # Migrate mk_send_log: add gmail_msg_id column
    try:
        con.execute("ALTER TABLE mk_send_log ADD COLUMN gmail_msg_id TEXT DEFAULT ''")
    except Exception:
        pass

    # One-time migration: clear stale Gmail token missing send scope (v2)
    try:
        con.execute("CREATE TABLE IF NOT EXISTS mk_migrations (name TEXT PRIMARY KEY)")
        done = con.execute("SELECT 1 FROM mk_migrations WHERE name = 'gmail_scope_fix_v2'").fetchone()
        if not done:
            con.execute("DELETE FROM mk_gmail_tokens")
            con.execute("INSERT INTO mk_migrations (name) VALUES ('gmail_scope_fix_v2')")
    except Exception:
        pass

    # Migrate mk_leads: add new columns if they don't exist
    mk_leads_cols = [r[1] for r in con.execute("PRAGMA table_info(mk_leads)").fetchall()]
    mk_new_cols = {
        "batch_name": "TEXT DEFAULT ''",
        "batch_date": "TEXT DEFAULT ''",
        "notes": "TEXT DEFAULT ''",
        "last_contacted": "TEXT DEFAULT ''",
        "send_count": "INTEGER DEFAULT 0",
    }
    for col, dtype in mk_new_cols.items():
        if col not in mk_leads_cols:
            try:
                con.execute(f"ALTER TABLE mk_leads ADD COLUMN {col} {dtype}")
            except Exception:
                pass
    con.execute("""
        CREATE TABLE IF NOT EXISTS admin_users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT DEFAULT 'viewer',
            must_change_password INTEGER DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)

    existing = con.execute("SELECT COUNT(*) FROM admin_users").fetchone()[0]
    if existing == 0:
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        k_hash = hashlib.sha256("Wifimoney420!".encode()).hexdigest()
        m_hash = hashlib.sha256("Marykate2026".encode()).hexdigest()
        con.execute("INSERT OR IGNORE INTO admin_users (email, password_hash, role, must_change_password, created_at) VALUES (?,?,?,?,?)",
                    ("kendall@lumenmarketing.co", k_hash, "admin", 0, now))
        con.execute("INSERT OR IGNORE INTO admin_users (email, password_hash, role, must_change_password, created_at) VALUES (?,?,?,?,?)",
                    ("marykatezarehghazarian@gmail.com", m_hash, "funnels", 1, now))

    con.execute("""
        CREATE TABLE IF NOT EXISTS funnel_page_views (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page TEXT NOT NULL,
            ip TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            device TEXT DEFAULT '',
            time_on_page REAL DEFAULT 0,
            created_at TEXT NOT NULL
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS funnel_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            whatsapp TEXT DEFAULT '',
            name TEXT DEFAULT '',
            business TEXT DEFAULT '',
            need TEXT DEFAULT '',
            source_page TEXT DEFAULT '',
            market TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            status TEXT DEFAULT 'new',
            created_at TEXT NOT NULL
        )
    """)

    # Tristan Dare external tracking (Squarespace tristandareshop.com/bejetixdare).
    # One row per page-session. visitor_id is persistent across sessions (localStorage
    # on the client); session_id is per page load. visible_seconds reflects only time
    # the page was actually visible on screen, accumulated client-side and posted on
    # heartbeats + a final beacon at pagehide.
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_sessions (
            session_id TEXT PRIMARY KEY,
            visitor_id TEXT NOT NULL,
            page_url TEXT DEFAULT '',
            page_path TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_medium TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            utm_term TEXT DEFAULT '',
            fbclid TEXT DEFAULT '',
            gclid TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            screen TEXT DEFAULT '',
            language TEXT DEFAULT '',
            timezone TEXT DEFAULT '',
            platform TEXT DEFAULT '',
            visible_seconds INTEGER DEFAULT 0,
            is_owner INTEGER DEFAULT 0,
            started_at TEXT NOT NULL,
            last_seen_at TEXT,
            ended_at TEXT
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_visitor ON td_sessions(visitor_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_started ON td_sessions(started_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_campaign ON td_sessions(utm_campaign)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_medium ON td_sessions(utm_medium)")

    # Button click events for the same page. Each row is one click on a CTA.
    # `position_pct` is the element's top edge as a percentage of the document
    # height (0 = top of page, 100 = bottom), so we can separate the
    # upper vs lower placement of duplicate CTAs without needing element IDs.
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            visitor_id TEXT NOT NULL,
            label TEXT DEFAULT '',
            href TEXT DEFAULT '',
            position_pct INTEGER DEFAULT 0,
            is_owner INTEGER DEFAULT 0,
            clicked_at TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_clicks_session ON td_clicks(session_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_td_clicks_clicked ON td_clicks(clicked_at)")

    # IPs we treat as "owner" / internal for the Tristan tracker. The static
    # OWNER_IPS set (Kendall's phone) is the seed; this table accumulates more
    # IPs every time someone successfully logs into /tristandare/admin, so
    # Kendall and Tristan's own page visits never pollute the analytics.
    con.execute("""
        CREATE TABLE IF NOT EXISTS td_owner_ips (
            ip TEXT PRIMARY KEY,
            email TEXT,
            first_seen TEXT,
            last_seen TEXT
        )
    """)

    con.commit()

    con.close()

init_db()

@app.route("/t/funnel", methods=["POST"])
def track_funnel():
    data = request.get_json() or {}
    event = data.get("event", "")
    step = data.get("step", "")
    value = data.get("value", "")
    sid = data.get("sid", "")

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "")

    con = sqlite3.connect(DB_PATH)
    con.execute(
        """INSERT INTO funnel_events (session_id, event, step, value, ip, user_agent, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (sid, event, step, value, ip, user_agent, datetime.datetime.utcnow().isoformat()),
    )
    con.commit()
    con.close()
    return jsonify({"ok": True})

@app.route("/t/funnel-stats")
def funnel_stats():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT event, step, value, COUNT(*) as cnt FROM funnel_events GROUP BY event, step, value ORDER BY cnt DESC"
    ).fetchall()
    con.close()
    stats = [{"event": r[0], "step": r[1], "value": r[2], "count": r[3]} for r in rows]
    return jsonify({"stats": stats})

# ── Client Dashboard Tracking ────────────────────────────────
@app.route("/t/dash", methods=["POST", "OPTIONS"])
def track_dashboard():
    # CORS for cross-domain tracking from client dashboards
    if request.method == "OPTIONS":
        resp = jsonify({"ok": True})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp
    data = request.get_json() or {}
    slug = data.get("slug", "")
    event = data.get("event", "open")
    sid = data.get("sid", "")
    duration = float(data.get("duration", 0))
    if not slug:
        return jsonify({"ok": False}), 400
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "")
    con = sqlite3.connect(DB_PATH)
    if event == "ping" and sid:
        existing = con.execute(
            "SELECT id FROM dashboard_events WHERE session_id = ? AND dashboard_slug = ? ORDER BY id DESC LIMIT 1",
            (sid, slug)
        ).fetchone()
        if existing:
            con.execute("UPDATE dashboard_events SET duration = ? WHERE id = ?", (min(duration, 3600), existing[0]))
        else:
            con.execute(
                "INSERT INTO dashboard_events (dashboard_slug, event, session_id, ip, user_agent, duration, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (slug, "open", sid, ip, user_agent, min(duration, 3600), datetime.datetime.utcnow().isoformat()),
            )
    else:
        con.execute(
            "INSERT INTO dashboard_events (dashboard_slug, event, session_id, ip, user_agent, duration, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (slug, event, sid, ip, user_agent, 0, datetime.datetime.utcnow().isoformat()),
        )
    con.commit()
    con.close()
    resp = jsonify({"ok": True, "sid": sid})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp


# ── Tristan Dare tracker (Squarespace tristandareshop.com/bejetixdare) ──
def _td_cors_origin():
    """Mirror the caller's Origin back if it's in the whitelist, else the canonical TD host."""
    origin = (request.headers.get("Origin") or "").strip()
    if origin in TD_ALLOWED_ORIGINS:
        return origin
    return "https://www.tristandareshop.com"


@app.route("/t/td", methods=["POST", "OPTIONS"])
def track_td():
    """Three event types: 'start' (creates the session row), 'tick' (updates running
    visible_seconds + last_seen_at), 'end' (final beacon on pagehide).
    Token-gated via X-TD-Token header or `token` body field (sendBeacon can't
    set headers, so the body fallback matters)."""
    if request.method == "OPTIONS":
        resp = jsonify({"ok": True})
        resp.headers["Access-Control-Allow-Origin"] = _td_cors_origin()
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-TD-Token"
        resp.headers["Access-Control-Max-Age"] = "3600"
        return resp

    data = request.get_json(silent=True) or {}
    token = (request.headers.get("X-TD-Token") or data.get("token") or "").strip()
    if token != TD_TRACK_TOKEN:
        resp = jsonify({"ok": False, "error": "auth"})
        resp.headers["Access-Control-Allow-Origin"] = _td_cors_origin()
        return resp, 403

    event = (data.get("event") or "").strip()
    session_id = (data.get("session_id") or "").strip()
    visitor_id = (data.get("visitor_id") or "").strip()
    if not session_id or not visitor_id or event not in ("start", "tick", "end", "click"):
        resp = jsonify({"ok": False, "error": "bad-payload"})
        resp.headers["Access-Control-Allow-Origin"] = _td_cors_origin()
        return resp, 400

    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    is_owner = 1 if _td_is_owner_ip(ip) else 0

    try:
        visible_seconds = int(data.get("visible_seconds", 0))
    except (TypeError, ValueError):
        visible_seconds = 0
    visible_seconds = max(0, min(visible_seconds, TD_MAX_SECONDS))

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    try:
        if event == "start":
            # INSERT OR IGNORE means a duplicate start (e.g. SPA nav firing twice)
            # never clobbers the original row's UTMs or started_at.
            con.execute("""
                INSERT OR IGNORE INTO td_sessions
                    (session_id, visitor_id, page_url, page_path,
                     utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                     fbclid, gclid, referrer, user_agent, ip, screen, language,
                     timezone, platform, visible_seconds, is_owner, started_at, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id, visitor_id,
                (data.get("page_url") or "")[:500],
                (data.get("page_path") or "")[:200],
                (data.get("utm_source") or "")[:200],
                (data.get("utm_medium") or "")[:200],
                (data.get("utm_campaign") or "")[:200],
                (data.get("utm_content") or "")[:200],
                (data.get("utm_term") or "")[:200],
                (data.get("fbclid") or "")[:300],
                (data.get("gclid") or "")[:300],
                (data.get("referrer") or "")[:500],
                (request.headers.get("User-Agent") or "")[:500],
                ip,
                (data.get("screen") or "")[:50],
                (data.get("language") or "")[:30],
                (data.get("timezone") or "")[:60],
                (data.get("platform") or "")[:60],
                visible_seconds, is_owner, now, now,
            ))
        elif event == "tick":
            # MAX() means a stale or out-of-order tick never decrements the total.
            con.execute("""
                UPDATE td_sessions
                SET visible_seconds = MAX(visible_seconds, ?), last_seen_at = ?
                WHERE session_id = ?
            """, (visible_seconds, now, session_id))
        elif event == "end":
            con.execute("""
                UPDATE td_sessions
                SET visible_seconds = MAX(visible_seconds, ?),
                    last_seen_at = ?, ended_at = ?
                WHERE session_id = ?
            """, (visible_seconds, now, now, session_id))
        else:  # 'click'
            try:
                position_pct = int(data.get("click_position", 0))
            except (TypeError, ValueError):
                position_pct = 0
            position_pct = max(0, min(position_pct, 100))
            con.execute("""
                INSERT INTO td_clicks
                    (session_id, visitor_id, label, href, position_pct, is_owner, clicked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                session_id, visitor_id,
                (data.get("click_label") or "")[:120],
                (data.get("click_href") or "")[:500],
                position_pct, is_owner, now,
            ))
        con.commit()
    finally:
        con.close()

    resp = jsonify({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = _td_cors_origin()
    return resp


@app.route("/")
def index():
    # On the SCE subdomain, route the root to the active SCE campaign.
    # Currently: Father's Day. Switch this back to sce_md.landing for Mother's
    # Day (or another funnel) by editing the redirect target below.
    host = (request.host or "").lower()
    if host.startswith("supercarexp."):
        return redirect(url_for("sce_fd.landing"))
    return render_template("home.html")

@app.route("/qualify")
def qualify():
    return render_template("qualify.html")

LABEL_MAP = {
    "ad_spend": {
        "not_running": "Not running ads yet",
        "under_1k": "Under $1,000 / mo",
        "1_5k": "$1,000 to $5,000 / mo",
        "5_15k": "$5,000 to $15,000 / mo",
        "15k_plus": "$15,000+ / mo",
    },
    "gap": {
        "leads": "Not enough leads",
        "conversion": "Leads not converting",
        "tracking": "No tracking",
        "follow_up": "No follow up system",
    },
    "revenue": {
        "under_10k": "Under $10K / mo",
        "10_50k": "$10K to $50K / mo",
        "50_100k": "$50K to $100K / mo",
        "100k_plus": "$100K+ / mo",
    },
}

GAP_INSIGHTS = {
    "leads": "You said the gap is not enough leads. That traces back to targeting and creative. We rebuild both before any budget goes live. Volume becomes the easy part.",
    "conversion": "You said leads are coming in but not converting. That is rarely the lead. It is the speed and tone of what happens after the form gets submitted. That is the system we install.",
    "tracking": "You said the gap is not knowing what is working. We build full attribution from ad click to closed sale. Every dollar gets a name, so every decision after gets easier.",
    "follow_up": "You said the gap is follow up. Most leads decide in the first five minutes whether they get a response. We automate that window and the weeks that follow.",
}

@app.route("/qualify/submit", methods=["POST"])
def qualify_submit():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = (data.get("phone") or "").strip()
    business = (data.get("business") or "").strip()
    ad_spend = (data.get("ad_spend") or "").strip()
    gap = (data.get("gap") or "").strip()
    revenue = (data.get("revenue") or "").strip()

    if not name or not email or not phone:
        return jsonify({"ok": False, "error": "Missing contact info"}), 400
    if not business or not ad_spend or not gap or not revenue:
        return jsonify({"ok": False, "error": "Missing answers"}), 400

    ad_spend_label = LABEL_MAP["ad_spend"].get(ad_spend, ad_spend)
    gap_label = LABEL_MAP["gap"].get(gap, gap)
    revenue_label = LABEL_MAP["revenue"].get(revenue, revenue)
    insight = GAP_INSIGHTS.get(gap, "")

    now = datetime.datetime.utcnow().isoformat()
    tag_string = f"qualify,spend:{ad_spend},gap:{gap},rev:{revenue}"

    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO leads (
            name, email, phone, business, revenue, marketing, challenge,
            stage, source, tags, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, email, phone, business, revenue_label, ad_spend_label, gap_label,
        "new", "qualify", tag_string, now, now
    ))
    con.commit()
    con.close()

    notify_body = f"""
    <div style="font-family:Inter,-apple-system,sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Qualification</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">{name} applied to work with Lumen</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{email} &middot; {phone}</p>
        <table style="width:100%; border-collapse:collapse; font-size:14px;">
          <tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:35%; vertical-align:top;">Business</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; color:#e8e8f0; line-height:1.6;">{business}</td></tr>
          <tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; vertical-align:top;">Ad Spend</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; color:#e8e8f0;">{ad_spend_label}</td></tr>
          <tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; vertical-align:top;">The Gap</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; color:#e8e8f0;">{gap_label}</td></tr>
          <tr><td style="padding:12px 0; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; vertical-align:top;">Revenue</td><td style="padding:12px 0 12px 16px; color:#e8e8f0;">{revenue_label}</td></tr>
        </table>
      </div>
    </div>
    """
    send_email(NOTIFY_EMAIL, f"Qualification: {name} — {business[:60]}", notify_body)

    first_name = name.split(" ")[0] if name else "there"
    confirm_body = f"""
    <div style="font-family:Inter,-apple-system,sans-serif; background:#0a0a0f; padding:40px 20px; color:#e8e8f0;">
      <div style="max-width:580px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:40px 32px;">
        <div style="display:flex; align-items:center; gap:10px; margin-bottom:28px;">
          <div style="width:10px;height:10px;background:#6128DB;border-radius:50%;"></div>
          <div style="font-size:12px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#44445a;">Lumen</div>
        </div>
        <h1 style="font-size:28px;font-weight:800;letter-spacing:-1px;line-height:1.2;margin:0 0 18px 0;color:#f0f0f5;">We have it, {first_name}.</h1>
        <p style="font-size:16px;line-height:1.65;color:#b8b8c8;margin:0 0 28px 0;">Your application came in clean. Kendall will read it before the call so you are not starting from zero.</p>
        <div style="background:rgba(97,40,219,0.08); border:1px solid rgba(97,40,219,0.3); border-radius:12px; padding:22px; margin-bottom:28px;">
          <div style="font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">First Read</div>
          <p style="font-size:15px;line-height:1.65;color:#e8e8f0;margin:0;">{insight}</p>
        </div>
        <p style="font-size:15px;line-height:1.6;color:#b8b8c8;margin:0 0 22px 0;">The next step is a 30 minute call. You walk away knowing what we would build, what it would cost, and whether the fit is there.</p>
        <a href="https://calendly.com/lumenmarketingco/lumen" style="display:inline-block;background:#6128DB;color:#fff;font-size:15px;font-weight:700;padding:16px 30px;border-radius:10px;text-decoration:none;letter-spacing:0.3px;">Book your call &rarr;</a>
        <p style="margin-top:36px; font-size:13px; color:#6b6b80; line-height:1.55;">Reply to this email if you need to reach me directly. I read everything.<br>Kendall &middot; Lumen</p>
      </div>
    </div>
    """
    send_email(email, "Your application is in. Here's what's next.", confirm_body)

    return jsonify({"ok": True})


# ── /lumenlb — MK7-style landing page on Lumen branding, used to test the WhatsApp
#    nurture agent (+1 623 512 6504). Leads notify Kendall only. ──
@app.route("/lumenlb")
def lumenlb():
    return render_template("lumenlb.html")


def _wa_digits(value):
    return "".join(c for c in (value or "") if c.isdigit())


# ── Meta Conversions API — Lumen dataset (server-side Lead fire) ─────────────
# The lumenlb form ALREADY fires a browser-side Pixel Lead via
# fbq('track','Lead',{...},{eventID}). Browser-side alone gets stripped by
# iOS ITP / ad blockers / strict-mode browsers — realistic loss is 30-50%.
# This server-side CAPI fire is the load-bearing half of the dual-event:
# Meta dedupes the two via the shared event_id passed in the form payload.
# Inert until LUMEN_META_CAPI_TOKEN is set on Railway (Lumen-coming-soon service).
LUMEN_META_DATASET_ID = os.environ.get("LUMEN_META_DATASET_ID", "1119566303064711")
LUMEN_META_CAPI_TOKEN = os.environ.get("LUMEN_META_CAPI_TOKEN", "")
LUMEN_META_TEST_EVENT_CODE = os.environ.get("LUMEN_META_TEST_EVENT_CODE", "")


def _capi_hash(value):
    """SHA-256 hash for Meta CAPI user_data fields. Meta requires email / phone /
    first_name / last_name be hashed; IP / UA / fbp / fbc are sent plain."""
    if not value:
        return None
    return hashlib.sha256(str(value).strip().lower().encode("utf-8")).hexdigest()


def _send_lumen_lead_capi(event_id, name, phone, fbp, fbc, client_ip, client_ua, page_url):
    """Fire a server-side Meta CAPI Lead event into the Lumen dataset (1119566303064711).
    Dedupes with the browser-side fbq Lead via the shared event_id. Safe-fail:
    silently returns on any error so the form response is never blocked."""
    if not LUMEN_META_CAPI_TOKEN or not LUMEN_META_DATASET_ID:
        print("[capi-lumen-lead] skipping: LUMEN_META_CAPI_TOKEN unset")
        return
    try:
        ud = {}
        if phone:
            digits = "".join(c for c in phone if c.isdigit())
            if digits:
                ud["ph"] = [_capi_hash(digits)]
        if name:
            parts = name.strip().split(None, 1)
            if parts:
                ud["fn"] = [_capi_hash(parts[0])]
            if len(parts) > 1 and parts[1]:
                ud["ln"] = [_capi_hash(parts[1])]
        if client_ip:
            ud["client_ip_address"] = client_ip
        if client_ua:
            ud["client_user_agent"] = client_ua
        if fbp:
            ud["fbp"] = fbp
        if fbc:
            ud["fbc"] = fbc

        event = {
            "event_name": "Lead",
            "event_time": int(time.time()),
            "event_id": event_id or str(uuid.uuid4()),
            "action_source": "website",
            "user_data": ud,
            "custom_data": {
                "content_name": "Lumen inquiry",
                "lead_source": "lumenlb_inline_form",
            },
        }
        if page_url:
            event["event_source_url"] = page_url

        payload = {"data": [event]}
        if LUMEN_META_TEST_EVENT_CODE:
            payload["test_event_code"] = LUMEN_META_TEST_EVENT_CODE

        url = f"https://graph.facebook.com/v19.0/{LUMEN_META_DATASET_ID}/events?access_token={LUMEN_META_CAPI_TOKEN}"
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code >= 400:
            print(f"[capi-lumen-lead] failed {r.status_code}: {r.text[:300]}")
        else:
            print(f"[capi-lumen-lead] sent event_id={event['event_id']}")
    except Exception as e:
        print(f"[capi-lumen-lead] exception: {e}")


@app.route("/lumenlb/inquiry", methods=["POST"])
def lumenlb_inquiry():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    whatsapp = (data.get("whatsapp") or "").strip()
    website = (data.get("website") or "").strip()
    page_url = (data.get("page_url") or "").strip()

    # Min 10 digits = a real international number with a country code.
    if not name or len(_wa_digits(whatsapp)) < 10:
        return jsonify({"ok": False, "error": "Name and a full WhatsApp number with country code are required"}), 400

    now = datetime.datetime.utcnow().isoformat()
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO leads (name, email, phone, business, source, tags, stage, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (name, "", whatsapp, website, "lumenlb", "lumenlb,whatsapp-agent", "new", now, now))
        con.commit()
        con.close()
    except Exception as e:
        print(f"[lumenlb] lead insert failed: {e}")

    # Server-side CAPI Lead fire. Runs on a background thread so the form
    # response stays snappy. event_id is reused from the browser Pixel fire
    # for dedup — Meta dedupes when both events share the same event_id
    # within ~5 min. fbp / fbc come from the browser's _fbp / _fbc cookies
    # (the form JS reads them from document.cookie and includes in payload).
    event_id = (data.get("event_id") or "").strip() or str(uuid.uuid4())
    fbp = (data.get("fbp") or "").strip() or None
    fbc = (data.get("fbc") or "").strip() or None
    client_ip = (request.headers.get("X-Forwarded-For", "") or request.remote_addr or "").split(",")[0].strip()
    client_ua = request.headers.get("User-Agent", "")
    threading.Thread(
        target=_send_lumen_lead_capi,
        args=(event_id, name, whatsapp, fbp, fbc, client_ip, client_ua, page_url),
        daemon=True,
    ).start()

    dash = "—"
    wa_digits = _wa_digits(whatsapp)
    wa_button = ""
    if wa_digits:
        first_name = (name or "there").split(" ", 1)[0]
        from urllib.parse import quote
        prefill = f"Hi {first_name}, this is Kendall from Lumen. Saw your inquiry {dash} happy to dig in. When works for a quick chat?"
        wa_url = f"https://wa.me/{wa_digits}?text={quote(prefill)}"
        wa_button = (
            '<div style="margin-top:28px;text-align:center;">'
            f'<a href="{wa_url}" style="display:inline-block;background:#25D366;color:#fff;text-decoration:none;padding:14px 28px;border-radius:10px;font-weight:600;font-family:sans-serif;font-size:15px;">Reply on WhatsApp</a>'
            '<p style="margin:12px 0 0;color:#888;font-size:12px;">Opens a chat with this lead, pre-filled. (Or just wait — the agent on +1 623 512 6504 picks up if they message first.)</p>'
            '</div>'
        )

    body = (
        '<div style="font-family:Inter,-apple-system,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">'
        '<div style="max-width:560px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">'
        '<div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">New /lumenlb Inquiry</div>'
        f'<h2 style="font-size:22px;font-weight:700;margin:0 0 22px;color:#fff;">{name}</h2>'
        '<table style="width:100%;border-collapse:collapse;font-size:14px;">'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;width:130px;">WhatsApp</td><td style="padding:10px 0;color:#e8e8f0;font-weight:600;">{whatsapp or dash}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Website</td><td style="padding:10px 0;color:#e8e8f0;">{website or dash}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Source</td><td style="padding:10px 0;color:#e8e8f0;">{page_url or "lumenmarketing.co/lumenlb"}</td></tr>'
        '</table>'
        f'{wa_button}'
        '</div></div>'
    )
    send_email(NOTIFY_EMAIL, f"New /lumenlb inquiry: {name}", body)

    return jsonify({"ok": True})


# ── Harker Outdoors contact form (harkeroutdoors.com Shopify contact page) ──
# The Shopify native contact form still emails the store's contact address as
# normal. This endpoint is a PARALLEL copy: the form JS also POSTs here so we
# email kendall@ a clean notification. Best-effort — never blocks the form.
HARKER_ALLOWED_ORIGINS = {
    "https://harkeroutdoors.com",
    "https://www.harkeroutdoors.com",
    "https://harker-outdoors.myshopify.com",
}

def _harker_cors_origin():
    origin = (request.headers.get("Origin") or "").strip()
    return origin if origin in HARKER_ALLOWED_ORIGINS else "https://harkeroutdoors.com"

@app.route("/harker/contact", methods=["POST", "OPTIONS"])
def harker_contact():
    if request.method == "OPTIONS":
        resp = jsonify({"ok": True})
        resp.headers["Access-Control-Allow-Origin"] = _harker_cors_origin()
        resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp

    # The form sends the body as text/plain (so sendBeacon avoids a CORS
    # preflight), so parse JSON manually rather than relying on the
    # Content-Type header. Fall back to Flask's parser / form fields.
    data = request.get_json(silent=True)
    if not data:
        try:
            data = json.loads(request.get_data(as_text=True) or "{}")
        except Exception:
            data = request.form.to_dict() or {}
    data = data or {}
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    interest = (data.get("interest") or "").strip()
    message = (data.get("message") or "").strip()
    truck_make = (data.get("truckMake") or "").strip()
    bed_size = (data.get("bedSize") or "").strip()

    # Light validation; if it's junk we still 200 so the form UX never breaks.
    if not name and not email and not message:
        resp = jsonify({"ok": False, "error": "empty"})
        resp.headers["Access-Control-Allow-Origin"] = _harker_cors_origin()
        return resp, 200

    dash = "—"
    import html as _html
    def esc(v):
        return _html.escape(v) if v else dash
    body = (
        '<div style="font-family:Inter,-apple-system,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">'
        '<div style="max-width:560px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">'
        '<div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#f75d44;margin-bottom:10px;">New Harker Outdoors Inquiry</div>'
        f'<h2 style="font-size:22px;font-weight:700;margin:0 0 22px;color:#fff;">{esc(name)}</h2>'
        '<table style="width:100%;border-collapse:collapse;font-size:14px;">'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;width:140px;">Email</td><td style="padding:10px 0;color:#e8e8f0;font-weight:600;">{esc(email)}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Phone</td><td style="padding:10px 0;color:#e8e8f0;">{esc(phone)}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Truck make</td><td style="padding:10px 0;color:#e8e8f0;">{esc(truck_make)}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Bed size</td><td style="padding:10px 0;color:#e8e8f0;">{esc(bed_size)}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;">Inquiry about</td><td style="padding:10px 0;color:#e8e8f0;">{esc(interest)}</td></tr>'
        f'<tr><td style="padding:10px 0;color:#8b8ba0;vertical-align:top;">Message</td><td style="padding:10px 0;color:#e8e8f0;white-space:pre-wrap;">{esc(message)}</td></tr>'
        '</table>'
        '<p style="margin:22px 0 0;color:#666;font-size:12px;">Sent from the harkeroutdoors.com contact form. The store inbox also received this via Shopify.</p>'
        '</div></div>'
    )
    subj = f"New Harker inquiry: {name or email or 'website contact'}"
    send_email(NOTIFY_EMAIL, subj, body)

    resp = jsonify({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = _harker_cors_origin()
    return resp


@app.route("/about")
def about():
    return render_template("about.html")

@app.route("/funnel")
def funnel():
    return render_template("funnel.html")


@app.route("/story")
def story():
    return render_template("story.html")

@app.route("/jeremiah-newby")
def jeremiah_newby():
    return render_template("jeremiah_newby.html")

@app.route("/tristandare")
def tristan_dare():
    return render_template("tristan_dare.html")

@app.route("/jjc-dare-private-4p4xiv42")
def jackson_jet_center():
    return render_template("jackson_jet_center.html")

@app.route("/formula-dynamics")
def formula_dynamics():
    return render_template("formula_dynamics.html")

@app.route("/abraj-developments")
def abraj_proposal():
    return render_template("abraj_proposal.html")

@app.route("/rochellem/outreachonboarding/05232026")
def rochelle_onboarding():
    return render_template("rochelle_onboarding.html")

@app.route("/services")
def services():
    return render_template("services.html")

@app.route("/avaloncrm")
def avalon_crm():
    return render_template("avalon_crm.html")

@app.route("/avaloncrm/feedback")
def avalon_crm_feedback():
    return redirect("https://avalon-crm-production-eeaa.up.railway.app/feedback")

@app.route("/avaloncrm/onboarding")
def avalon_onboarding():
    return render_template("avalon_onboarding.html")

@app.route("/avaloncrm/onboarding/submit", methods=["POST"])
def avalon_onboarding_submit():
    data = request.get_json() or {}
    name = data.get("name", "").strip() or "Anonymous"
    role = data.get("role", "").strip()

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO avalon_onboarding (name, role, daily_work, moxie_likes, moxie_frustrations,
            lead_to_blvd, ideal_workflow, dream_features, auto_vs_manual, integrations, anything_else, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, role,
        data.get("daily_work", ""),
        data.get("moxie_likes", ""),
        data.get("moxie_frustrations", ""),
        data.get("lead_to_blvd", ""),
        data.get("ideal_workflow", ""),
        data.get("dream_features", ""),
        data.get("auto_vs_manual", ""),
        data.get("integrations", ""),
        data.get("anything_else", ""),
        now
    ))
    con.commit()
    con.close()

    # Email notification
    answers = f"""
    <h2>New Avalon CRM Onboarding Submission</h2>
    <p><strong>Name:</strong> {name}</p>
    <p><strong>Role:</strong> {role}</p>
    <p><strong>Daily work:</strong> {data.get('daily_work', '')}</p>
    <p><strong>What they like about Moxie:</strong> {data.get('moxie_likes', '')}</p>
    <p><strong>Moxie frustrations:</strong> {data.get('moxie_frustrations', '')}</p>
    <p><strong>Lead to Boulevard flow:</strong> {data.get('lead_to_blvd', '')}</p>
    <p><strong>Ideal workflow:</strong> {data.get('ideal_workflow', '')}</p>
    <p><strong>Dream features:</strong> {data.get('dream_features', '')}</p>
    <p><strong>Auto vs manual:</strong> {data.get('auto_vs_manual', '')}</p>
    <p><strong>Integrations wanted:</strong> {data.get('integrations', '')}</p>
    <p><strong>Anything else:</strong> {data.get('anything_else', '')}</p>
    """
    send_email(NOTIFY_EMAIL, f"Avalon Onboarding: {name} ({role})", answers)

    return jsonify({"ok": True})

@app.route("/admin/avalon-onboarding")
def admin_avalon_onboarding():
    if not session.get("wl_auth"):
        return redirect(url_for("admin"))
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM avalon_onboarding ORDER BY created_at DESC").fetchall()
    con.close()
    return render_template("admin_avalon_onboarding.html", submissions=rows)

PREFILLS = {
    "ridge": {"name": "Ridge Krauss", "email": "ridge@cinemarketer.io", "business": "Cinemarketer.io"},
}

@app.route("/agentonboarding")
def agent_onboarding():
    return render_template("agent_onboarding.html", prefill={})

@app.route("/agentonboarding/<slug>")
def agent_onboarding_prefilled(slug):
    prefill = PREFILLS.get(slug.lower(), {})
    return render_template("agent_onboarding.html", prefill=prefill)

def build_ridge_intake_email():
    return """
    <div style="margin:0; padding:0; background:#0a0a0f; font-family: -apple-system, 'Inter', Segoe UI, sans-serif;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#0a0a0f; padding:40px 20px;">
        <tr><td align="center">
          <table role="presentation" width="600" cellpadding="0" cellspacing="0" border="0" style="max-width:600px; width:100%;">
            <tr><td style="padding:0 0 32px 0;" align="left">
              <img src="https://lumenmarketing.co/static/logo-white.png" alt="Lumen" height="26" style="display:block; height:26px;">
            </td></tr>
            <tr><td style="background:#111118; border:1px solid #1a1a25; border-radius:16px; padding:44px 40px;">
              <p style="margin:0 0 14px 0; font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#a78bfa;">Before we get on the call</p>
              <h1 style="margin:0 0 24px 0; font-size:26px; font-weight:700; color:#ffffff; line-height:1.25; letter-spacing:-0.4px;">Quick thing before we meet.</h1>
              <p style="margin:0 0 18px 0; font-size:15px; color:#c8c8d8; line-height:1.7;">Hey Ridge,</p>
              <p style="margin:0 0 18px 0; font-size:15px; color:#c8c8d8; line-height:1.7;">Excited for our session. Before we hop on, I put together a short intake form I'd love for you to fill out. Takes about four minutes and it's already got your info plugged in so you can jump straight to the questions.</p>
              <p style="margin:0 0 28px 0; font-size:15px; color:#c8c8d8; line-height:1.7;">The whole point of this is simple. I don't want to show up to our call and spend the first twenty minutes figuring out where you're at, what you've tried, and what you actually want to walk away with. I want to show up already knowing. Whatever you fill out is exactly what I'll be studying beforehand so the guidance you get is specific to Cinemarketer.io, your goals, and the way you learn.</p>
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="margin:0 0 32px 0;">
                <tr><td align="left">
                  <a href="https://lumenmarketing.co/agentonboarding/ridge" style="display:inline-block; background:linear-gradient(135deg, #7c4dff, #6128DB); color:#ffffff; text-decoration:none; font-size:15px; font-weight:600; padding:16px 32px; border-radius:12px; letter-spacing:0.2px;">Open your intake form &rarr;</a>
                </td></tr>
              </table>
              <p style="margin:0 0 18px 0; font-size:15px; color:#c8c8d8; line-height:1.7;">Be honest with it. There are no wrong answers. The more real you are on this form, the more useful our time together is going to be.</p>
              <p style="margin:0 0 4px 0; font-size:15px; color:#c8c8d8; line-height:1.7;">Talk soon,</p>
              <p style="margin:0; font-size:15px; font-weight:600; color:#ffffff; line-height:1.7;">Kendall Davis</p>
              <p style="margin:2px 0 0 0; font-size:13px; color:#8b8ba0;">Founder, Lumen Marketing Solutions</p>
            </td></tr>
            <tr><td style="padding:28px 0 0 0;" align="center">
              <p style="margin:0; font-size:12px; color:#5a5a70; line-height:1.6;">Lumen Marketing Solutions &nbsp;·&nbsp; <a href="https://lumenmarketing.co" style="color:#7c4dff; text-decoration:none;">lumenmarketing.co</a></p>
            </td></tr>
          </table>
        </td></tr>
      </table>
    </div>
    """

@app.route("/admin/preview-ridge-email")
def preview_ridge_email():
    if not session.get("wl_auth"):
        return redirect(url_for("admin"))
    send_email(NOTIFY_EMAIL, "[PREVIEW] Ridge — Before our call", build_ridge_intake_email())
    return "Preview sent to " + NOTIFY_EMAIL + ". <a href='/admin'>Back</a>"

@app.route("/agentonboarding/submit", methods=["POST"])
def agent_onboarding_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    email = (data.get("email") or "").strip()
    business = (data.get("business") or "").strip()

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO agent_onboarding (
            name, email, business, business_oneliner, role,
            ai_level, claude_experience, tools_tried, where_stuck,
            repetitive_work, first_handoff, hours_lost,
            snap_vision, top_three_agents,
            tech_level, learning_style,
            call_outcome, biggest_question, anything_else, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, email, business,
        data.get("business_oneliner", ""),
        data.get("role", ""),
        data.get("ai_level", ""),
        data.get("claude_experience", ""),
        data.get("tools_tried", ""),
        data.get("where_stuck", ""),
        data.get("repetitive_work", ""),
        data.get("first_handoff", ""),
        data.get("hours_lost", ""),
        data.get("snap_vision", ""),
        data.get("top_three_agents", ""),
        data.get("tech_level", ""),
        data.get("learning_style", ""),
        data.get("call_outcome", ""),
        data.get("biggest_question", ""),
        data.get("anything_else", ""),
        now
    ))
    con.commit()
    con.close()

    def row(label, value):
        if not value:
            return ""
        safe = str(value).replace("\n", "<br>")
        return f'<tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:40%; vertical-align:top;">{label}</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; font-size:14px; color:#e8e8f0; line-height:1.6;">{safe}</td></tr>'

    body = f"""
    <div style="font-family: -apple-system, Inter, sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Intake</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">Agent Consulting Submission</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{name}{' at ' + business if business else ''}{' · ' + email if email else ''}</p>
        <table style="width:100%; border-collapse:collapse;">
          {row("Name", name)}
          {row("Email", email)}
          {row("Business", business)}
          {row("What they do", data.get("business_oneliner",""))}
          {row("Role", data.get("role",""))}
          {row("AI comfort", data.get("ai_level",""))}
          {row("Claude experience", data.get("claude_experience",""))}
          {row("Tools tried", data.get("tools_tried",""))}
          {row("Where they got stuck", data.get("where_stuck",""))}
          {row("Repetitive work", data.get("repetitive_work",""))}
          {row("First handoff", data.get("first_handoff",""))}
          {row("Hours lost/week", data.get("hours_lost",""))}
          {row("Snap-fingers vision", data.get("snap_vision",""))}
          {row("Top 3 agents", data.get("top_three_agents",""))}
          {row("Tech comfort", data.get("tech_level",""))}
          {row("Learning style", data.get("learning_style",""))}
          {row("Call outcome", data.get("call_outcome",""))}
          {row("Biggest question", data.get("biggest_question",""))}
          {row("Anything else", data.get("anything_else",""))}
        </table>
      </div>
    </div>
    """
    send_email(NOTIFY_EMAIL, f"Agent Consulting Intake: {name}{' — ' + business if business else ''}", body)

    return jsonify({"ok": True})

@app.route("/admin/agent-onboarding")
def admin_agent_onboarding():
    if not session.get("wl_auth"):
        return redirect(url_for("admin"))
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM agent_onboarding ORDER BY created_at DESC").fetchall()
    con.close()
    return render_template("admin_agent_onboarding.html", submissions=rows)

# ---------------------------------------------------------------------------
# Mane Styling Studio — marketing onboarding form
# ---------------------------------------------------------------------------
MANE_PREFILL = {"name": "Keanna Keim", "email": "", "business": "Mane Styling Studio", "role": "Owner"}
HARKER_PREFILL = {"name": "", "email": "", "business": "Harker Outdoors", "role": ""}

@app.route("/maneonboarding")
def mane_onboarding():
    return render_template("mane_onboarding.html", prefill=MANE_PREFILL)

@app.route("/maneonboarding/submit", methods=["POST"])
def mane_onboarding_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    email = (data.get("email") or "").strip()
    business = (data.get("business") or "").strip() or "Mane Styling Studio"

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO mane_onboarding (
            name, email, business, role, business_oneliner,
            services_pricing, priority_services, offers_deals,
            ads_before, ads_detail, existing_content,
            lead_delivery, lead_delivery_notes, lead_handler,
            metrics_focus, target_number, success_60days, anything_else, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, email, business,
        data.get("role", ""),
        data.get("business_oneliner", ""),
        data.get("services_pricing", ""),
        data.get("priority_services", ""),
        data.get("offers_deals", ""),
        data.get("ads_before", ""),
        data.get("ads_detail", ""),
        data.get("existing_content", ""),
        data.get("lead_delivery", ""),
        data.get("lead_delivery_notes", ""),
        data.get("lead_handler", ""),
        data.get("metrics_focus", ""),
        data.get("target_number", ""),
        data.get("success_60days", ""),
        data.get("anything_else", ""),
        now
    ))
    con.commit()
    con.close()

    delivery_labels = {"default": "CRM + email (default)", "sms": "Also wants SMS per lead",
                       "sheet": "Wants leads in a shared sheet", "other": "Something else (see notes)"}
    ads_labels = {"lots": "Has run paid ads for a while", "some": "Has run a little / experimented",
                  "never": "Never run paid ads"}

    def map_delivery(v):
        if not v:
            return ""
        return ", ".join(delivery_labels.get(x.strip(), x.strip()) for x in str(v).split(",") if x.strip())

    def row(label, value):
        if not value:
            return ""
        safe = str(value).replace("\n", "<br>")
        return f'<tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:40%; vertical-align:top;">{label}</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; font-size:14px; color:#e8e8f0; line-height:1.6;">{safe}</td></tr>'

    body = f"""
    <div style="font-family: -apple-system, Inter, sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Onboarding</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">Mane Styling Studio Onboarding</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{name}{' · ' + data.get('role','') if data.get('role') else ''}{' · ' + email if email else ''}</p>
        <table style="width:100%; border-collapse:collapse;">
          {row("Name", name)}
          {row("Email", email)}
          {row("Business", business)}
          {row("Role", data.get("role",""))}
          {row("What the business does", data.get("business_oneliner",""))}
          {row("Services and pricing", data.get("services_pricing",""))}
          {row("Priority services to push", data.get("priority_services",""))}
          {row("Current offers and deals", data.get("offers_deals",""))}
          {row("Has run paid ads before", ads_labels.get(data.get("ads_before",""), data.get("ads_before","")))}
          {row("Past ads detail", data.get("ads_detail",""))}
          {row("Existing content available", data.get("existing_content",""))}
          {row("Lead delivery preference", map_delivery(data.get("lead_delivery","")))}
          {row("Notes on lead delivery", data.get("lead_delivery_notes",""))}
          {row("Who handles incoming leads / speed", data.get("lead_handler",""))}
          {row("Other metrics they care about", data.get("metrics_focus",""))}
          {row("Specific target number", data.get("target_number",""))}
          {row("What a win looks like in 60 days", data.get("success_60days",""))}
          {row("Anything else", data.get("anything_else",""))}
        </table>
      </div>
    </div>
    """
    send_email(NOTIFY_EMAIL, f"Mane Styling Studio Onboarding: {name}", body)

    return jsonify({"ok": True})

# ---------------------------------------------------------------------------
# Mane Styling Studio — color quiz funnel (customer-facing)
# ---------------------------------------------------------------------------
@app.route("/manestyling/color-funnel")
@app.route("/mane-color")  # dev alias
def mane_color():
    # Ads point to this URL; serve the simplified v2 funnel (preserve fbclid/UTMs).
    qs = request.query_string.decode()
    return redirect("/manestyling/color-v2" + (("?" + qs) if qs else ""), code=302)

REC_LABELS = {
    "balayage": "Balayage",
    "refresh": "Color Refresh",
    "glaze": "Glaze and Gloss",
    "graycoverage": "Natural Gray Coverage",
    "consult": "Custom Color Consult",
}
CURRENT_LABELS = {
    "brunette": "Brunette",
    "highlighted": "Highlighted",
    "blonde": "Blonde",
    "red": "Red or copper",
    "black": "Very dark",
    "gray": "Going gray",
}
DREAM_LABELS = {
    "brighter": "Brighter",
    "richer": "Richer",
    "covergray": "Cover gray",
    "refresh": "Refresh",
}

@app.route("/manestyling/color-funnel/submit", methods=["POST"])
def mane_color_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    current_color = (data.get("current_color") or "").strip()
    dream_look = (data.get("dream_look") or "").strip()
    recommendation = (data.get("recommendation") or "").strip()

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO mane_color_quiz (
            name, email, phone, current_color, dream_look, recommendation,
            utm_source, utm_campaign, utm_content, fbclid, referrer, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, email, phone, current_color, dream_look, recommendation,
        data.get("utm_source", ""),
        data.get("utm_campaign", ""),
        data.get("utm_content", ""),
        data.get("fbclid", ""),
        data.get("referrer", ""),
        now,
    ))
    lead_row_id = cur.lastrowid
    con.commit()
    con.close()

    cur_l = CURRENT_LABELS.get(current_color, current_color)
    dream_l = DREAM_LABELS.get(dream_look, dream_look)
    rec_l = REC_LABELS.get(recommendation, recommendation)

    # Phorest client sync runs BEFORE the email so we can include the
    # "Open in Phorest" deep-link button. No-op if Phorest env vars unset.
    phorest_notes = (
        f"Current color: {cur_l}\nDream: {dream_l}\nRecommendation: {rec_l}"
    )
    phorest_client_id = mane_phorest.create_client(
        name=name if name != "Anonymous" else "",
        email=email,
        phone=phone,
        source="color",
        notes=phorest_notes,
        external_id=f"color-{lead_row_id}",
    )
    if phorest_client_id:
        try:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE mane_color_quiz SET phorest_client_id = ? WHERE id = ?",
                        (phorest_client_id, lead_row_id))
            con.commit()
            con.close()
        except Exception:
            pass

    def row(label, value):
        if not value:
            return ""
        safe = str(value).replace("\n", "<br>")
        return f'<tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:40%; vertical-align:top;">{label}</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; font-size:14px; color:#e8e8f0; line-height:1.6;">{safe}</td></tr>'

    phorest_url = _phorest_client_url(phorest_client_id)
    phorest_button = f'''
        <a href="{phorest_url}" target="_blank" style="display:inline-block; background:#7c4dff; color:#ffffff; text-decoration:none; font-weight:600; font-size:13px; letter-spacing:1px; text-transform:uppercase; padding:14px 26px; border-radius:10px; margin:0 0 24px 0;">Open in Phorest →</a>
    ''' if phorest_url else ""

    body = f"""
    <div style="font-family: -apple-system, Inter, sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Color Lead</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">Mane Styling Studio</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{name}{' · ' + phone if phone else ''}{' · ' + email if email else ''}</p>
        {phorest_button}
        <table style="width:100%; border-collapse:collapse;">
          {row("Name", name)}
          {row("Email", email)}
          {row("Phone", phone)}
          {row("Current color", cur_l)}
          {row("Dream", dream_l)}
          {row("Recommendation", rec_l)}
          {row("UTM source", data.get("utm_source",""))}
          {row("UTM campaign", data.get("utm_campaign",""))}
          {row("UTM content", data.get("utm_content",""))}
          {row("fbclid", data.get("fbclid",""))}
          {row("Referrer", data.get("referrer",""))}
        </table>
      </div>
    </div>
    """
    try:
        send_email(MANE_LEAD_TO, f"Mane color lead: {name}", body)
    except Exception:
        pass

    # Meta CAPI Lead event (deduped with client Pixel via event_id)
    send_meta_capi_event(
        event_name="Lead",
        event_id=data.get("event_id", ""),
        user_data={
            "email": email,
            "phone": phone,
            "first_name": name if name != "Anonymous" else "",
            "client_ip": (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip(),
            "user_agent": request.headers.get("User-Agent", ""),
            "fbp": data.get("fbp", ""),
            "fbc": data.get("fbc", ""),
            "fbclid": data.get("fbclid", ""),
        },
        custom_data={
            "content_category": "color_funnel",
            "recommendation": recommendation,
        },
        source_url=data.get("referrer", "") or "https://lumenmarketing.co/manestyling/color-funnel",
    )

    return jsonify({"ok": True})

# ---------------------------------------------------------------------------
# Mane Styling Studio — extensions consult funnel (customer-facing)
# ---------------------------------------------------------------------------
@app.route("/manestyling/extension-funnel")
def mane_extensions():
    # Ads point to this URL; serve the simplified v2 funnel (preserve fbclid/UTMs).
    qs = request.query_string.decode()
    return redirect("/manestyling/extensions-v2" + (("?" + qs) if qs else ""), code=302)

GOAL_LABELS = {
    "volume": "Volume (hair has thinned / lost fullness)",
    "length": "Length",
    "both":   "Best of both worlds",
}
TIMELINE_LABELS = {
    "soon": "In the next few weeks",
    "months": "Within a couple months",
    "exploring": "Just exploring for now",
}
BUDGET_LABELS = {
    "800-1500":  "$800 – $1,500",
    "1600-2200": "$1,600 – $2,200",
    "2300-4000": "$2,300 – $4,000",
    "open":      "Whatever it takes for dream hair",
}

@app.route("/manestyling/extension-funnel/submit", methods=["POST"])
def mane_extension_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    email = (data.get("email") or "").strip()
    phone = (data.get("phone") or "").strip()
    goal = (data.get("goal") or "").strip()
    timeline = (data.get("timeline") or "").strip()
    budget = (data.get("budget") or "").strip()

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO mane_extension_quiz (
            name, email, phone, goal, timeline, budget,
            utm_source, utm_campaign, utm_content, fbclid, referrer, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, email, phone, goal, timeline, budget,
        data.get("utm_source", ""),
        data.get("utm_campaign", ""),
        data.get("utm_content", ""),
        data.get("fbclid", ""),
        data.get("referrer", ""),
        now,
    ))
    lead_row_id = cur.lastrowid
    con.commit()
    con.close()

    goal_l = GOAL_LABELS.get(goal, goal)
    timeline_l = TIMELINE_LABELS.get(timeline, timeline)
    budget_l = BUDGET_LABELS.get(budget, budget)

    # Phorest client sync runs BEFORE the email so we can include the
    # "Open in Phorest" deep-link button. No-op if Phorest env vars unset.
    phorest_notes = (
        f"Goal: {goal_l}\nTimeline: {timeline_l}\nBudget: {budget_l}"
    )
    phorest_client_id = mane_phorest.create_client(
        name=name if name != "Anonymous" else "",
        email=email,
        phone=phone,
        source="extension",
        notes=phorest_notes,
        external_id=f"ext-{lead_row_id}",
    )
    if phorest_client_id:
        try:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE mane_extension_quiz SET phorest_client_id = ? WHERE id = ?",
                        (phorest_client_id, lead_row_id))
            con.commit()
            con.close()
        except Exception:
            pass

    def row(label, value):
        if not value:
            return ""
        safe = str(value).replace("\n", "<br>")
        return f'<tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:40%; vertical-align:top;">{label}</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; font-size:14px; color:#e8e8f0; line-height:1.6;">{safe}</td></tr>'

    phorest_url = _phorest_client_url(phorest_client_id)
    phorest_button = f'''
        <a href="{phorest_url}" target="_blank" style="display:inline-block; background:#7c4dff; color:#ffffff; text-decoration:none; font-weight:600; font-size:13px; letter-spacing:1px; text-transform:uppercase; padding:14px 26px; border-radius:10px; margin:0 0 24px 0;">Open in Phorest →</a>
    ''' if phorest_url else ""

    body = f"""
    <div style="font-family: -apple-system, Inter, sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Extensions Consult Lead</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">Mane Styling Studio</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{name}{' · ' + phone if phone else ''}{' · ' + email if email else ''}</p>
        {phorest_button}
        <table style="width:100%; border-collapse:collapse;">
          {row("Name", name)}
          {row("Email", email)}
          {row("Phone", phone)}
          {row("Goal", goal_l)}
          {row("Timeline", timeline_l)}
          {row("Budget", budget_l)}
          {row("UTM source", data.get("utm_source",""))}
          {row("UTM campaign", data.get("utm_campaign",""))}
          {row("UTM content", data.get("utm_content",""))}
          {row("fbclid", data.get("fbclid",""))}
          {row("Referrer", data.get("referrer",""))}
        </table>
      </div>
    </div>
    """
    try:
        send_email(MANE_LEAD_TO, f"Mane extensions lead: {name}", body)
    except Exception:
        pass

    # Meta CAPI Lead event (deduped with client Pixel via event_id)
    send_meta_capi_event(
        event_name="Lead",
        event_id=data.get("event_id", ""),
        user_data={
            "email": email,
            "phone": phone,
            "first_name": name if name != "Anonymous" else "",
            "client_ip": (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip(),
            "user_agent": request.headers.get("User-Agent", ""),
            "fbp": data.get("fbp", ""),
            "fbc": data.get("fbc", ""),
            "fbclid": data.get("fbclid", ""),
        },
        custom_data={
            "content_category": "extensions_funnel",
            "goal": goal,
            "timeline": timeline,
            "budget": budget,
        },
        source_url=data.get("referrer", "") or "https://lumenmarketing.co/manestyling/extension-funnel",
    )

    return jsonify({"ok": True})

# ---------------------------------------------------------------------------
# Mane Styling Studio — SIMPLIFIED v2 funnels  (A/B TEST: variant 'test-2')
# Pure form + one quick tap, name + phone only. Original quiz funnels above
# are the control (variant ''=test-1). Score the test by counting leads:
#   SELECT variant, COUNT(*) FROM mane_color_quiz GROUP BY variant;
#   SELECT variant, COUNT(*) FROM mane_extension_quiz GROUP BY variant;
# DB insert + Phorest are wrapped so a hiccup can never silently drop a lead.
# ---------------------------------------------------------------------------
def _client_ip():
    return (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()

def _phorest_email_button(phorest_client_id):
    phorest_url = _phorest_client_url(phorest_client_id)
    if not phorest_url:
        return ""
    return (f'<a href="{phorest_url}" target="_blank" style="display:inline-block;background:#7c4dff;'
            f'color:#fff;text-decoration:none;font-weight:600;font-size:13px;letter-spacing:1px;'
            f'text-transform:uppercase;padding:14px 26px;border-radius:10px;margin:0 0 20px 0;">Open in Phorest →</a>')

@app.route("/manestyling/color-v2")
def mane_color_v2():
    return render_template("mane_color_v2.html")

@app.route("/manestyling/color-v2/submit", methods=["POST"])
def mane_color_v2_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    phone = (data.get("phone") or "").strip()
    goal = (data.get("goal") or "").strip()  # brighter|richer|covergray|refresh
    now = datetime.datetime.utcnow().isoformat()

    lead_row_id = None
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("""
            INSERT INTO mane_color_quiz (
                name, email, phone, current_color, dream_look, recommendation,
                utm_source, utm_campaign, utm_content, fbclid, referrer, variant, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, "", phone, "", goal, "",
            data.get("utm_source", ""), data.get("utm_campaign", ""), data.get("utm_content", ""),
            data.get("fbclid", ""), data.get("referrer", ""), "test-2", now,
        ))
        lead_row_id = cur.lastrowid
        con.commit()
        con.close()
    except Exception as e:
        print(f"[mane-color-v2] db insert failed: {e}")

    phorest_client_id = None
    try:
        phorest_client_id = mane_phorest.create_client(
            name=name if name != "Anonymous" else "", email="", phone=phone,
            source="color", notes=f"[v2 simplified] Looking to: {goal}",
            external_id=f"color-v2-{lead_row_id}" if lead_row_id else "",
        )
        if phorest_client_id and lead_row_id:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE mane_color_quiz SET phorest_client_id = ? WHERE id = ?", (phorest_client_id, lead_row_id))
            con.commit()
            con.close()
    except Exception as e:
        print(f"[mane-color-v2] phorest sync failed: {e}")

    body = f"""
    <div style="font-family:-apple-system,Inter,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">
      <div style="max-width:560px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">
        <div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">New Color Lead</div>
        <h2 style="font-size:22px;font-weight:700;margin:0 0 6px 0;color:#fff;">Mane Styling Studio</h2>
        <p style="font-size:14px;color:#e8e8f0;margin:0 0 20px 0;">{name}{' · ' + phone if phone else ''}</p>
        {_phorest_email_button(phorest_client_id)}
        <p style="font-size:13px;color:#8b8ba0;margin:0;">Looking to: {goal or '—'}</p>
      </div>
    </div>
    """
    try:
        send_email(MANE_LEAD_TO, f"Mane color lead: {name}", body)
    except Exception:
        pass

    try:
        send_meta_capi_event(
            event_name="Lead", event_id=data.get("event_id", ""),
            user_data={
                "phone": phone, "first_name": name if name != "Anonymous" else "",
                "client_ip": _client_ip(), "user_agent": request.headers.get("User-Agent", ""),
                "fbp": data.get("fbp", ""), "fbc": data.get("fbc", ""), "fbclid": data.get("fbclid", ""),
            },
            custom_data={"content_category": "color_funnel_v2", "variant": "test-2", "goal": goal},
            source_url=data.get("referrer", "") or "https://lumenmarketing.co/manestyling/color-v2",
        )
    except Exception:
        pass

    return jsonify({"ok": True})

@app.route("/manestyling/extensions-v2")
def mane_extensions_v2():
    return render_template("mane_extensions_v2.html")

@app.route("/manestyling/extensions-v2/submit", methods=["POST"])
def mane_extensions_v2_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    phone = (data.get("phone") or "").strip()
    goal = (data.get("goal") or "").strip()  # volume|length|both
    now = datetime.datetime.utcnow().isoformat()

    lead_row_id = None
    try:
        con = sqlite3.connect(DB_PATH)
        cur = con.cursor()
        cur.execute("""
            INSERT INTO mane_extension_quiz (
                name, email, phone, goal, timeline, budget,
                utm_source, utm_campaign, utm_content, fbclid, referrer, variant, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, "", phone, goal, "", "",
            data.get("utm_source", ""), data.get("utm_campaign", ""), data.get("utm_content", ""),
            data.get("fbclid", ""), data.get("referrer", ""), "test-2", now,
        ))
        lead_row_id = cur.lastrowid
        con.commit()
        con.close()
    except Exception as e:
        print(f"[mane-ext-v2] db insert failed: {e}")

    phorest_client_id = None
    try:
        phorest_client_id = mane_phorest.create_client(
            name=name if name != "Anonymous" else "", email="", phone=phone,
            source="extension", notes=f"[v2 simplified] Goal: {goal}",
            external_id=f"ext-v2-{lead_row_id}" if lead_row_id else "",
        )
        if phorest_client_id and lead_row_id:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE mane_extension_quiz SET phorest_client_id = ? WHERE id = ?", (phorest_client_id, lead_row_id))
            con.commit()
            con.close()
    except Exception as e:
        print(f"[mane-ext-v2] phorest sync failed: {e}")

    body = f"""
    <div style="font-family:-apple-system,Inter,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">
      <div style="max-width:560px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">
        <div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">New Extensions Lead</div>
        <h2 style="font-size:22px;font-weight:700;margin:0 0 6px 0;color:#fff;">Mane Styling Studio</h2>
        <p style="font-size:14px;color:#e8e8f0;margin:0 0 20px 0;">{name}{' · ' + phone if phone else ''}</p>
        {_phorest_email_button(phorest_client_id)}
        <p style="font-size:13px;color:#8b8ba0;margin:0;">Goal: {goal or '—'}</p>
      </div>
    </div>
    """
    try:
        send_email(MANE_LEAD_TO, f"Mane extensions lead: {name}", body)
    except Exception:
        pass

    try:
        send_meta_capi_event(
            event_name="Lead", event_id=data.get("event_id", ""),
            user_data={
                "phone": phone, "first_name": name if name != "Anonymous" else "",
                "client_ip": _client_ip(), "user_agent": request.headers.get("User-Agent", ""),
                "fbp": data.get("fbp", ""), "fbc": data.get("fbc", ""), "fbclid": data.get("fbclid", ""),
            },
            custom_data={"content_category": "extensions_funnel_v2", "variant": "test-2", "goal": goal},
            source_url=data.get("referrer", "") or "https://lumenmarketing.co/manestyling/extensions-v2",
        )
    except Exception:
        pass

    return jsonify({"ok": True})

# ── Meta Lead Form ads → SAME pipeline as the funnels (Phorest client + salon
#    email). NATIVE Meta Lead Ads webhook (no Zapier): Meta posts a leadgen
#    notification, we fetch the lead via the Graph API using a page/system-user
#    token that has `leads_retrieval`, then run create_client + salon email.
#    A plain JSON POST (manual/fallback) is also accepted. variant='leadform'
#    so form leads stay separable from funnel leads when we compare volume.
MANE_LEADFORM_SECRET = os.environ.get("MANE_LEADFORM_SECRET", "")
MANE_PAGE_TOKEN = os.environ.get("MANE_PAGE_TOKEN", "")
MANE_META_APP_SECRET = os.environ.get("MANE_META_APP_SECRET", "")
# Map each instant form to its offer so routing/notes/email are correct.
MANE_FORM_OFFER = {
    "873544742065679": "extension",   # "Hair Extensions" form (name+phone)
    "2449776272499400": "color",      # "Hair Color" form (name+phone)
    "1487003163173502": "extension",  # "Hair Extensions — Qualified" (+ goal question)
    "1641764653781029": "color",      # "Hair Color — Qualified" (+ goal question)
}

def _ingest_mane_lead(*, offer, name, email="", phone="", goal="", utm_campaign="", utm_content=""):
    """Shared sink for a Mane lead-form lead: SQLite + Phorest client + salon
    email. Returns the Phorest client_id (or None)."""
    offer = "extension" if str(offer or "").lower().startswith("ext") else "color"
    name = (name or "").strip() or "Anonymous"
    email = (email or "").strip(); phone = (phone or "").strip(); goal = (goal or "").strip()
    now = datetime.datetime.utcnow().isoformat()
    lead_row_id = None
    try:
        con = sqlite3.connect(DB_PATH); cur = con.cursor()
        if offer == "extension":
            cur.execute("""INSERT INTO mane_extension_quiz
                (name,email,phone,goal,timeline,budget,utm_source,utm_campaign,utm_content,fbclid,referrer,variant,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (name, email, phone, goal, "", "", "", utm_campaign, utm_content, "", "meta_lead_form", "leadform", now))
        else:
            cur.execute("""INSERT INTO mane_color_quiz
                (name,email,phone,current_color,dream_look,recommendation,utm_source,utm_campaign,utm_content,fbclid,referrer,variant,created_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (name, email, phone, "", goal, "", "", utm_campaign, utm_content, "", "meta_lead_form", "leadform", now))
        lead_row_id = cur.lastrowid; con.commit(); con.close()
    except Exception as e:
        print(f"[mane-leadform] db insert failed: {e}")

    phorest_client_id = None
    try:
        phorest_client_id = mane_phorest.create_client(
            name=name if name != "Anonymous" else "", email=email, phone=phone,
            source=offer, notes=f"[Meta lead-form ad] {('Goal: ' + goal) if goal else ''}".strip(),
            external_id=f"{offer}-lf-{lead_row_id}" if lead_row_id else "",
        )
        if phorest_client_id and lead_row_id:
            tbl = "mane_extension_quiz" if offer == "extension" else "mane_color_quiz"
            con = sqlite3.connect(DB_PATH)
            con.execute(f"UPDATE {tbl} SET phorest_client_id = ? WHERE id = ?", (phorest_client_id, lead_row_id))
            con.commit(); con.close()
    except Exception as e:
        print(f"[mane-leadform] phorest sync failed: {e}")

    label = "Extensions" if offer == "extension" else "Color"
    contact = " · ".join(x for x in [phone, email] if x)
    body = f"""
    <div style="font-family:-apple-system,Inter,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">
      <div style="max-width:560px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">
        <div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">New {label} Lead · Instant Form</div>
        <h2 style="font-size:22px;font-weight:700;margin:0 0 6px 0;color:#fff;">Mane Styling Studio</h2>
        <p style="font-size:14px;color:#e8e8f0;margin:0 0 20px 0;">{name}{' · ' + contact if contact else ''}</p>
        {_phorest_email_button(phorest_client_id)}
        <p style="font-size:13px;color:#8b8ba0;margin:0;">{('Goal: ' + goal) if goal else 'From a Meta lead-form ad'}</p>
      </div>
    </div>
    """
    try:
        send_email(MANE_LEAD_TO, f"Mane {label.lower()} lead (form): {name}", body)
    except Exception:
        pass
    return phorest_client_id

def _process_meta_leadgen(payload):
    """Handle a native Meta Lead Ads webhook: fetch each lead's field data via
    the Graph API (needs leads_retrieval) and route it through _ingest_mane_lead."""
    for entry in (payload.get("entry") or []):
        for ch in (entry.get("changes") or []):
            if ch.get("field") != "leadgen":
                continue
            v = ch.get("value") or {}
            leadgen_id = v.get("leadgen_id"); form_id = str(v.get("form_id", "") or "")
            if not leadgen_id:
                continue
            if form_id and form_id not in MANE_FORM_OFFER:
                continue  # not a Mane form — skip silently
            try:
                lead = requests.get(
                    f"https://graph.facebook.com/v21.0/{leadgen_id}",
                    params={"access_token": MANE_PAGE_TOKEN,
                            "fields": "field_data,form_id,ad_name,campaign_name,created_time"},
                    timeout=30,
                ).json()
                if not form_id:
                    form_id = str(lead.get("form_id", "") or "")
                fd = {f.get("name"): ((f.get("values") or [""])[0]) for f in (lead.get("field_data") or [])}
                name = fd.get("full_name") or f"{fd.get('first_name','')} {fd.get('last_name','')}".strip()
                # capture the qualifying answer (key 'goal'; fall back to any custom field)
                _std = {"full_name", "first_name", "last_name", "email", "phone_number"}
                goal = fd.get("goal") or next((v for k, v in fd.items() if k not in _std and v), "")
                _ingest_mane_lead(
                    offer=MANE_FORM_OFFER.get(form_id, "color"),
                    name=name, email=fd.get("email", ""), phone=fd.get("phone_number", ""), goal=goal,
                    utm_campaign=lead.get("campaign_name", ""), utm_content=lead.get("ad_name", ""),
                )
            except Exception as e:
                print(f"[mane-leadform] fetch/ingest lead {leadgen_id} failed: {e}")

@app.route("/manestyling/leadform", methods=["GET", "POST"])
def mane_leadform_submit():
    # Meta webhook verification handshake (and a plain health check)
    if request.method == "GET":
        vt = os.environ.get("MANE_META_VERIFY_TOKEN", "")
        if request.args.get("hub.mode") == "subscribe" and vt and request.args.get("hub.verify_token") == vt:
            return request.args.get("hub.challenge", "")
        return "ok", 200

    raw = request.get_data()
    payload = request.get_json(silent=True)

    # Native Meta Lead Ads webhook
    if isinstance(payload, dict) and payload.get("object") == "page":
        if MANE_META_APP_SECRET:
            import hmac, hashlib
            sig = (request.headers.get("X-Hub-Signature-256") or "").split("sha256=")[-1]
            expected = hmac.new(MANE_META_APP_SECRET.encode(), raw, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(sig, expected):
                return "bad signature", 403
        try:
            _process_meta_leadgen(payload)
        except Exception as e:
            print(f"[mane-leadform] meta process error: {e}")
        return "ok", 200  # always 200 so Meta keeps the subscription healthy

    # Plain JSON fallback (manual test / other source) — shared-secret guarded
    secret = request.args.get("secret") or request.headers.get("X-Lumen-Secret", "")
    if MANE_LEADFORM_SECRET and secret != MANE_LEADFORM_SECRET:
        return jsonify({"ok": False, "error": "unauthorized"}), 401
    data = payload or request.form.to_dict() or {}
    offer = (data.get("offer") or "").strip().lower()
    if offer not in ("color", "extension"):
        hint = (offer + " " + (data.get("form_name") or "") + " " + (data.get("campaign_name") or "")).lower()
        offer = "extension" if ("ext" in hint or "extension" in hint) else "color"
    cid = _ingest_mane_lead(
        offer=offer,
        name=(data.get("name") or data.get("full_name") or f"{data.get('first_name','')} {data.get('last_name','')}".strip()),
        email=data.get("email", ""), phone=(data.get("phone") or data.get("phone_number") or ""),
        goal=(data.get("goal") or data.get("custom_answer") or ""),
        utm_campaign=data.get("campaign_name", ""), utm_content=data.get("ad_name", ""),
    )
    return jsonify({"ok": True, "offer": "extension" if offer == "extension" else "color", "phorest_client_id": cid})

@app.route("/admin/mane-onboarding")
def admin_mane_onboarding():
    if not session.get("wl_auth"):
        return redirect("/admin")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM mane_onboarding ORDER BY created_at DESC").fetchall()
    con.close()
    delivery_labels = {"default": "CRM + email (default)", "sms": "Also wants SMS per lead",
                       "sheet": "Wants leads in a shared sheet", "other": "Something else (see notes)"}
    subs = []
    for r in rows:
        d = dict(r)
        ld = d.get("lead_delivery", "") or ""
        d["lead_delivery_display"] = ", ".join(delivery_labels.get(x.strip(), x.strip()) for x in ld.split(",") if x.strip())
        subs.append(d)
    return render_template("admin_mane_onboarding.html", submissions=subs)

@app.route("/harker-outdoor/onboarding")
def harker_onboarding():
    return render_template("harker_onboarding.html", prefill=HARKER_PREFILL)

@app.route("/harker-outdoor/onboarding/submit", methods=["POST"])
def harker_onboarding_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip() or "Anonymous"
    email = (data.get("email") or "").strip()
    business = (data.get("business") or "").strip() or "Harker Outdoors"

    assets_labels = {
        "organized": "Yes, fully organized in Drive/Dropbox",
        "scattered": "Some, but scattered across devices",
        "content_team": "Content team manages it",
        "not_much": "Not much existing content yet",
    }
    shopify_labels = {
        "invite": "Will invite Lumen team to Shopify as staff",
        "credentials": "Will share login credentials via secure channel",
        "figure_out": "Wants to figure out access on the call",
    }

    def row(label, value):
        if not value:
            return ""
        safe = str(value).replace("\n", "<br>")
        return f'<tr><td style="padding:12px 0; border-bottom:1px solid #1a1a25; font-size:11px; font-weight:600; letter-spacing:1.5px; text-transform:uppercase; color:#7c4dff; width:40%; vertical-align:top;">{label}</td><td style="padding:12px 0 12px 16px; border-bottom:1px solid #1a1a25; font-size:14px; color:#e8e8f0; line-height:1.6;">{safe}</td></tr>'

    body = f"""
    <div style="font-family: -apple-system, Inter, sans-serif; background:#0a0a0f; padding:32px 20px; color:#e8e8f0;">
      <div style="max-width:620px; margin:0 auto; background:#111118; border:1px solid #1a1a25; border-radius:14px; padding:32px;">
        <div style="font-size:11px; font-weight:600; letter-spacing:3px; text-transform:uppercase; color:#7c4dff; margin-bottom:10px;">New Onboarding</div>
        <h2 style="font-size:22px; font-weight:700; margin:0 0 6px 0; color:#fff;">Harker Outdoors Onboarding</h2>
        <p style="font-size:13px; color:#8b8ba0; margin:0 0 24px 0;">{name}{' · ' + data.get('role','') if data.get('role') else ''}{' · ' + email if email else ''}</p>
        <table style="width:100%; border-collapse:collapse;">
          {row("Name", name)}
          {row("Email", email)}
          {row("Business", business)}
          {row("Role", data.get("role",""))}
          {row("What the business does", data.get("business_oneliner",""))}
          {row("Offers / promos / launches", data.get("offers_deals",""))}
          {row("Priority product to push", data.get("priority_product",""))}
          {row("Asset folder status", assets_labels.get(data.get("assets_status",""), data.get("assets_status","")))}
          {row("Asset folder link", data.get("assets_link",""))}
          {row("Content team contact", data.get("content_team_contact",""))}
          {row("Past Meta ad performance", data.get("past_ads_performance",""))}
          {row("Monthly ad spend", data.get("monthly_ad_spend",""))}
          {row("Target ROAS / CPP", data.get("target_metrics",""))}
          {row("Shopify store URL", data.get("shopify_url",""))}
          {row("Shopify access method", shopify_labels.get(data.get("shopify_access_method",""), data.get("shopify_access_method","")))}
          {row("Store notes", data.get("store_notes",""))}
          {row("Meta access blockers", data.get("meta_blockers",""))}
          {row("What a win looks like in 90 days", data.get("success_90days",""))}
          {row("Anything else", data.get("anything_else",""))}
        </table>
      </div>
    </div>
    """
    send_email(NOTIFY_EMAIL, f"Harker Outdoors Onboarding: {name}", body)
    return jsonify({"ok": True})

@app.route("/proposal")
def proposal():
    return render_template("avalon_crm.html")

@app.route("/join", methods=["POST"])
def join_waitlist():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"})
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "INSERT INTO waitlist (email, created_at) VALUES (?, ?)",
            (email, datetime.datetime.utcnow().isoformat()),
        )
        con.commit()
        con.close()

        if RESEND_API_KEY:
            # Send welcome email to the signup
            welcome_html = render_template("welcome_email.html")
            send_email(email, "You're in.", welcome_html)

            # Send notification to Kendall
            notify_html = render_template("notify_email.html", email=email)
            send_email(NOTIFY_EMAIL, f"New signup: {email}", notify_html)

        return jsonify({"ok": True})
    except sqlite3.IntegrityError:
        return jsonify({"ok": True})

# ── Analytics endpoints ──────────────────────────────────────
@app.route("/t/view", methods=["POST"])
def track_view():
    sid = str(uuid.uuid4())
    data = request.get_json() or {}

    # Server-side data
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    user_agent = request.headers.get("User-Agent", "")
    referrer = request.headers.get("Referer", "")

    # Client-side data
    screen = data.get("screen", "")
    language = data.get("language", "")
    timezone = data.get("timezone", "")
    platform = data.get("platform", "")
    utm_source = data.get("utm_source", "")
    utm_medium = data.get("utm_medium", "")
    utm_campaign = data.get("utm_campaign", "")
    utm_content = data.get("utm_content", "")
    page = data.get("page", "coming-soon")

    con = sqlite3.connect(DB_PATH)
    con.execute(
        """INSERT INTO page_views
        (session_id, timestamp, time_on_page, ip, user_agent, referrer,
         screen, language, timezone, platform, utm_source, utm_medium, utm_campaign, utm_content, page)
        VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (sid, datetime.datetime.utcnow().isoformat(), ip, user_agent, referrer,
         screen, language, timezone, platform, utm_source, utm_medium, utm_campaign, utm_content, page),
    )
    con.commit()
    con.close()
    return jsonify({"sid": sid})

@app.route("/t/ping", methods=["POST"])
def track_ping():
    data = request.get_json()
    sid = data.get("sid", "")
    seconds = data.get("t", 0)
    if sid and isinstance(seconds, (int, float)) and seconds > 0:
        con = sqlite3.connect(DB_PATH)
        con.execute(
            "UPDATE page_views SET time_on_page = ? WHERE session_id = ?",
            (min(seconds, 1800), sid),  # cap at 30 min
        )
        con.commit()
        con.close()
    return jsonify({"ok": True})

@app.route("/t/heartbeat", methods=["POST"])
def track_heartbeat():
    data = request.get_json() or {}
    sid = data.get("sid", "")
    page = data.get("page", "")
    active = data.get("active", False)
    if not sid:
        return jsonify({"ok": False})
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    with LIVE_LOCK:
        if active:
            LIVE_VISITORS[sid] = {"page": page, "last_seen": time.time(), "ip": ip}
        else:
            LIVE_VISITORS.pop(sid, None)
    return jsonify({"ok": True})

@app.route("/t/live")
def track_live():
    clean_stale_visitors()
    with LIVE_LOCK:
        # Exclude owner IPs
        visitors = {k: v for k, v in LIVE_VISITORS.items() if v["ip"] not in OWNER_IPS}
        total = len(visitors)
        by_page = {}
        for v in visitors.values():
            p = v["page"]
            by_page[p] = by_page.get(p, 0) + 1
    return jsonify({"live": total, "by_page": by_page})

@app.route("/t/stats")
def track_stats():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401

    hours = request.args.get("hours", None)
    con = sqlite3.connect(DB_PATH)

    if hours and hours != "max":
        cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=int(hours))).isoformat()
        time_filter = " WHERE timestamp > ?"
        time_params = (cutoff,)
    else:
        time_filter = ""
        time_params = ()

    total = con.execute(f"SELECT COUNT(*) FROM page_views{time_filter}", time_params).fetchone()[0]
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    today_count = con.execute(
        "SELECT COUNT(*) FROM page_views WHERE timestamp LIKE ?", (today + "%",)
    ).fetchone()[0]

    owner_placeholders = ",".join("?" for _ in OWNER_IPS)
    if time_filter:
        avg_time = con.execute(
            f"SELECT AVG(time_on_page) FROM page_views WHERE time_on_page > 0 AND ip NOT IN ({owner_placeholders}) AND timestamp > ?",
            tuple(OWNER_IPS) + time_params
        ).fetchone()[0] or 0
    else:
        avg_time = con.execute(
            f"SELECT AVG(time_on_page) FROM page_views WHERE time_on_page > 0 AND ip NOT IN ({owner_placeholders})",
            tuple(OWNER_IPS)
        ).fetchone()[0] or 0

    # Last 7 days breakdown
    daily = []
    for i in range(6, -1, -1):
        d = (datetime.datetime.utcnow() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        cnt = con.execute(
            "SELECT COUNT(*) FROM page_views WHERE timestamp LIKE ?", (d + "%",)
        ).fetchone()[0]
        daily.append({"date": d, "views": cnt})
    # Active now (views in last 2 minutes)
    two_min_ago = (datetime.datetime.utcnow() - datetime.timedelta(minutes=2)).isoformat()
    active = con.execute(
        "SELECT COUNT(*) FROM page_views WHERE timestamp > ?", (two_min_ago,)
    ).fetchone()[0]
    con.close()
    return jsonify({
        "total": total,
        "today": today_count,
        "avg_time": round(avg_time, 1),
        "active": active,
        "daily": daily,
    })

# ── Admin ─────────────────────────────────────────────────────
@app.route("/admin", methods=["GET", "POST"])
def admin_landing():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or request.form.get("pin") or "").strip()
        pw_hash = hashlib.sha256(password.encode()).hexdigest()

        con = sqlite3.connect(DB_PATH)
        user = con.execute("SELECT id, email, role, must_change_password FROM admin_users WHERE email=? AND password_hash=?", (email, pw_hash)).fetchone()

        if not user:
            if password == ADMIN_PIN:
                session["wl_auth"] = True
                session["admin_role"] = "admin"
                session["admin_email"] = "kendall@lumenmarketing.co"
                con.close()
                return redirect(url_for("admin_landing"))
            con.close()
            return render_template("admin.html", error=True, authed=False)

        session["wl_auth"] = True
        session["admin_role"] = user[2]
        session["admin_email"] = user[1]
        session["admin_user_id"] = user[0]
        con.close()

        if user[3] == 1:
            return redirect("/admin/change-password")

        if user[2] == "funnels":
            return redirect("/admin/funnels")
        return redirect(url_for("admin_landing"))

    if not session.get("wl_auth"):
        return render_template("admin.html", authed=False, error=False)

    if session.get("admin_role") == "funnels":
        return redirect("/admin/funnels")
    return render_template("admin.html", authed=True, error=False)


@app.route("/admin/change-password", methods=["GET", "POST"])
def admin_change_password():
    if not session.get("wl_auth"):
        return redirect("/admin")
    if request.method == "POST":
        new_pw = (request.form.get("new_password") or "").strip()
        if len(new_pw) < 6:
            return render_template("admin_change_pw.html", error="Password must be at least 6 characters")
        new_hash = hashlib.sha256(new_pw.encode()).hexdigest()
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE admin_users SET password_hash=?, must_change_password=0 WHERE id=?", (new_hash, session.get("admin_user_id")))
        con.commit()
        con.close()
        if session.get("admin_role") == "funnels":
            return redirect("/admin/funnels")
        return redirect("/admin")
    return render_template("admin_change_pw.html", error=None)


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect("/admin")

@app.route("/admin/coming-soon")
def admin_coming_soon():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("SELECT email, created_at FROM waitlist ORDER BY id DESC").fetchall()
    con.close()
    entries = [{"email": r[0], "date": r[1][:10]} for r in rows]
    return render_template("waitlist.html", authed=True, entries=entries, error=False)

@app.route("/admin/main-site")
def admin_main_site():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    return render_template("admin_site.html")


def _td_dashboard_data(hours, include_owner_in_recent=True):
    """Pull all dashboard stats + recent sessions for the TD tracker.
    Aggregates always exclude is_owner=1 sessions. The recent-sessions list
    optionally includes them (Kendall's master view) or hides them entirely
    (the client's own view at /tristandare/admin)."""
    try:
        hours_int = int(hours) if hours != "max" else None
    except (TypeError, ValueError):
        hours_int = 168
    if hours_int:
        cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=hours_int)).isoformat()
        time_clause = " AND started_at > ?"
        time_params = (cutoff,)
    else:
        time_clause = ""
        time_params = ()

    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row

    base_where = "WHERE is_owner = 0" + time_clause

    total_sessions = con.execute(
        f"SELECT COUNT(*) FROM td_sessions {base_where}", time_params
    ).fetchone()[0]
    unique_visitors = con.execute(
        f"SELECT COUNT(DISTINCT visitor_id) FROM td_sessions {base_where}", time_params
    ).fetchone()[0]
    avg_seconds = con.execute(
        f"SELECT AVG(visible_seconds) FROM td_sessions {base_where} AND visible_seconds > 0", time_params
    ).fetchone()[0] or 0
    durations = [r[0] for r in con.execute(
        f"SELECT visible_seconds FROM td_sessions {base_where} AND visible_seconds > 0 ORDER BY visible_seconds", time_params
    ).fetchall()]
    median_seconds = 0
    if durations:
        mid = len(durations) // 2
        median_seconds = durations[mid] if len(durations) % 2 else (durations[mid - 1] + durations[mid]) / 2

    medium_rows = con.execute(f"""
        SELECT COALESCE(NULLIF(utm_medium, ''), '(none)') AS medium,
               COUNT(*) AS sessions,
               COUNT(DISTINCT visitor_id) AS visitors,
               AVG(visible_seconds) AS avg_sec
        FROM td_sessions {base_where}
        GROUP BY medium
        ORDER BY sessions DESC
    """, time_params).fetchall()
    by_medium = [{"medium": r["medium"], "sessions": r["sessions"],
                  "visitors": r["visitors"],
                  "avg_sec": int(r["avg_sec"] or 0)} for r in medium_rows]

    campaign_rows = con.execute(f"""
        SELECT COALESCE(NULLIF(utm_campaign, ''), '(none)') AS campaign,
               COALESCE(NULLIF(utm_medium, ''), '(none)') AS medium,
               COUNT(*) AS sessions,
               COUNT(DISTINCT visitor_id) AS visitors,
               AVG(visible_seconds) AS avg_sec
        FROM td_sessions {base_where}
        GROUP BY campaign, medium
        ORDER BY sessions DESC
    """, time_params).fetchall()
    by_campaign = [{"campaign": r["campaign"], "medium": r["medium"],
                    "sessions": r["sessions"], "visitors": r["visitors"],
                    "avg_sec": int(r["avg_sec"] or 0)} for r in campaign_rows]

    # Button clicks — same time window as sessions, owner excluded from aggregates.
    # Group by label + placement bucket (upper / lower) so duplicate CTAs at
    # top and bottom of the page show up separately.
    click_time_clause = " AND clicked_at > ?" if time_params else ""
    click_rows = con.execute(f"""
        SELECT
            label,
            CASE WHEN position_pct < 50 THEN 'upper' ELSE 'lower' END AS placement,
            COUNT(*) AS clicks,
            COUNT(DISTINCT visitor_id) AS unique_clickers,
            COUNT(DISTINCT session_id) AS clicking_sessions,
            MIN(position_pct) AS min_pos,
            MAX(position_pct) AS max_pos
        FROM td_clicks
        WHERE is_owner = 0{click_time_clause}
        GROUP BY label, placement
        ORDER BY clicks DESC
    """, time_params).fetchall()
    clicks = [{
        "label": r["label"] or "(no label)",
        "placement": r["placement"],
        "clicks": r["clicks"],
        "unique_clickers": r["unique_clickers"],
        "clicking_sessions": r["clicking_sessions"],
    } for r in click_rows]

    # Click-through rate against unique visitors (page-level CTR per CTA).
    total_clicks = sum(c["clicks"] for c in clicks)
    if unique_visitors > 0:
        for c in clicks:
            c["ctr_pct"] = round((c["unique_clickers"] / unique_visitors) * 100, 1)
    else:
        for c in clicks:
            c["ctr_pct"] = 0.0

    if include_owner_in_recent:
        recent_clause = "WHERE 1=1" + time_clause
    else:
        recent_clause = "WHERE is_owner = 0" + time_clause
    recent_rows = con.execute(f"""
        SELECT session_id, visitor_id, utm_source, utm_medium, utm_campaign,
               utm_content, fbclid, gclid, referrer, visible_seconds, is_owner,
               started_at, ended_at, ip
        FROM td_sessions {recent_clause}
        ORDER BY started_at DESC
        LIMIT 200
    """, time_params).fetchall()
    recent = [{
        "session_id_short": r["session_id"][-8:],
        "visitor_id_short": r["visitor_id"][-8:],
        "utm_source": r["utm_source"] or "—",
        "utm_medium": r["utm_medium"] or "—",
        "utm_campaign": r["utm_campaign"] or "—",
        "utm_content": r["utm_content"] or "",
        "fbclid": (r["fbclid"] or "")[:18] + ("…" if r["fbclid"] and len(r["fbclid"]) > 18 else ""),
        "gclid": (r["gclid"] or "")[:18] + ("…" if r["gclid"] and len(r["gclid"]) > 18 else ""),
        "referrer": (r["referrer"] or "—")[:40],
        "visible_seconds": r["visible_seconds"] or 0,
        "is_owner": bool(r["is_owner"]),
        "started_at": _utc_iso_to_mtn(r["started_at"]),
        "ended_at": _utc_iso_to_mtn(r["ended_at"]) if r["ended_at"] else "",
        "ip": r["ip"] or "",
    } for r in recent_rows]

    con.close()

    return {
        "hours": hours,
        "total_sessions": total_sessions,
        "unique_visitors": unique_visitors,
        "avg_seconds": int(avg_seconds),
        "median_seconds": int(median_seconds),
        "by_medium": by_medium,
        "by_campaign": by_campaign,
        "clicks": clicks,
        "total_clicks": total_clicks,
        "recent": recent,
    }


@app.route("/admin/td-tracking")
def admin_td_tracking():
    """Kendall's master view of the Tristan Dare Squarespace tracker.
    Owner sessions excluded from aggregates, shown in recent list with a flag."""
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    hours = request.args.get("hours", "168")
    data = _td_dashboard_data(hours, include_owner_in_recent=True)
    return render_template("admin_td_tracking.html", **data)


# Per Kendall: every client admin page uses the slug pattern /<client>/admin
# (e.g. /tristandare/admin, /berryclean/admin, /makhsoom/admin). Email + password
# login validated against TD_CLIENT_ACCOUNTS; session key is separate from
# Kendall's wl_auth so clients can't see anything else on the Mainframe.
@app.route("/tristandare/admin", methods=["GET", "POST"])
def tristandare_admin():
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = (request.form.get("password") or "").strip()
        if (email, password) in TD_CLIENT_ACCOUNTS:
            session.permanent = True
            session["td_client_auth"] = True
            session["td_client_email"] = email
            # Bind this IP to the login so future /t/td events from here are
            # flagged is_owner=1 and excluded from the analytics aggregates.
            login_ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
            if "," in login_ip:
                login_ip = login_ip.split(",")[0].strip()
            _td_record_owner_ip(login_ip, email)
            return redirect(url_for("tristandare_admin"))
        return render_template("tristandare_login.html", error=True, email=email)
    if not session.get("td_client_auth"):
        return render_template("tristandare_login.html", error=False, email="")
    hours = request.args.get("hours", "168")
    data = _td_dashboard_data(hours, include_owner_in_recent=False)
    data["signed_in_as"] = session.get("td_client_email", "")
    return render_template("tristandare_dashboard.html", **data)


@app.route("/tristandare/admin/logout")
def tristandare_admin_logout():
    session.pop("td_client_auth", None)
    return redirect(url_for("tristandare_admin"))


@app.route("/admin/funnel-beta")
def admin_funnel_beta():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS funnel_beta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            agency TEXT DEFAULT '',
            clients TEXT DEFAULT '',
            spend TEXT DEFAULT '',
            bottleneck TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    rows = con.execute("SELECT name, email, agency, clients, spend, bottleneck, created_at FROM funnel_beta ORDER BY id DESC").fetchall()
    con.close()
    entries = [{"name": r[0], "email": r[1], "agency": r[2], "clients": r[3], "spend": r[4], "bottleneck": r[5], "date": r[6][:10]} for r in rows]
    return render_template("admin_funnel_beta.html", authed=True, entries=entries)

@app.route("/admin/crm")
def admin_crm():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    return render_template("admin_main.html")

@app.route("/admin/dashboard/<slug>")
def admin_dashboard(slug):
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    return render_template("admin_dashboard.html", slug=slug)

@app.route("/t/visitors")
def track_visitors():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        """SELECT ip, user_agent, referrer, screen, language, timezone, platform,
                  utm_source, utm_medium, utm_campaign, timestamp, time_on_page
           FROM page_views ORDER BY id DESC LIMIT 200"""
    ).fetchall()
    con.close()
    visitors = []
    for r in rows:
        ua = r[1]
        device = "Mobile" if any(m in ua for m in ["iPhone", "Android", "Mobile"]) else "Desktop"
        browser = "Safari" if "Safari" in ua and "Chrome" not in ua else "Chrome" if "Chrome" in ua else "Firefox" if "Firefox" in ua else "Other"
        visitors.append({
            "ip": r[0], "device": device, "browser": browser,
            "referrer": r[2], "screen": r[3], "language": r[4],
            "timezone": r[5], "platform": r[6], "utm_source": r[7],
            "utm_medium": r[8], "utm_campaign": r[9],
            "timestamp": r[10], "time_on_page": round(r[11], 1),
            "is_owner": r[0] in OWNER_IPS,
        })

    # Device breakdown
    devices = {}
    sources = {}
    for v in visitors:
        devices[v["device"]] = devices.get(v["device"], 0) + 1
        src = v["utm_source"] or v["referrer"] or "Direct"
        if len(src) > 30:
            src = src[:30] + "..."
        sources[src] = sources.get(src, 0) + 1

    return jsonify({
        "visitors": visitors,
        "devices": devices,
        "sources": sources,
    })


# ── Preview route (secret, isolated from live site) ──────────
PREVIEW_TOKEN = "2026"

@app.route(f"/preview/{PREVIEW_TOKEN}")
def preview_site():
    return render_template("site.html")

@app.route("/preview/home")
def preview_home():
    return render_template("home.html")

@app.route("/preview/home/about")
def preview_about():
    return render_template("about.html")

@app.route("/preview/home/funnel")
def preview_funnel():
    return render_template("funnel.html")

@app.route("/10daybuild")
def ten_day_build():
    return render_template("10daybuild.html")

@app.route("/robots.txt")
def robots_txt():
    return "User-agent: *\nDisallow: /audit/\nDisallow: /admin/\nDisallow: /jaredcasados\n", 200, {"Content-Type": "text/plain"}

@app.route("/audit/sublime")
def sublime_audit():
    return render_template("sublime-audit.html")

@app.route("/audit/sublime/jesse")
def sublime_jesse():
    return render_template("sublime-jesse-brief.html")

@app.route("/audit/caphardware")
def cap_hardware_audit():
    return render_template("cap-hardware-audit.html")

@app.route("/audit/caphardware/jesse")
def cap_hardware_jesse():
    return render_template("cap-hardware-jesse.html")

@app.route("/berryclean")
def berryclean_report():
    resp = make_response(render_template("berryclean-report.html"))
    resp.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
    return resp

@app.route("/jaredcasados")
def jared_scope_review():
    return render_template("brian-scope-review.html")

@app.route("/admin/test-sequence-email")
def test_sequence_email():
    """Send sequence email 1 test to kendallwdavis11@gmail.com."""
    html = render_template("sequence_email_1.html")
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Lumen <kendall@lumenmarketing.co>",
                "to": ["kendallwdavis11@gmail.com"],
                "subject": "The light always wins.",
                "html": html,
            },
        )
        return f"Status: {resp.status_code} | Response: {resp.text}"
    except Exception as e:
        return f"Error: {e}"

@app.route("/admin/test-live-email")
def test_live_email():
    """Send we're live email test to kendallwdavis11@gmail.com."""
    html = render_template("email_live.html")
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "from": "Lumen <kendall@lumenmarketing.co>",
                "to": ["kendallwdavis11@gmail.com"],
                "subject": "We're live.",
                "html": html,
            },
        )
        return f"Status: {resp.status_code} | Response: {resp.text}"
    except Exception as e:
        return f"Error: {e}"

@app.route("/admin/send-live-nate")
def send_live_nate():
    html = render_template("email_live.html")
    try:
        resp = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
            json={"from": "Lumen <kendall@lumenmarketing.co>", "to": ["n.wilkinson@launchpoint.dev"], "subject": "We're live.", "html": html},
        )
        return f"Status: {resp.status_code} | Response: {resp.text}"
    except Exception as e:
        return f"Error: {e}"

@app.route("/admin/blast-live-email")
def blast_live_email():
    """Blast we're live email to all contacts."""
    recipients = [
        "zachloos160@gmail.com",
        "allierich65@gmail.com",
        "marykatezarehghazarian@gmail.com",
        "alphonzolmajor@gmail.com",
        "kobidodd2@gmail.com",
        "n.wilkinson@launchpoint.dev",
        "kendallwdavis11@gmail.com",
        "darbimckean@gmail.com",
        "jaredponce2603@gmail.com",
    ]
    html = render_template("email_live.html")
    results = []
    for email in recipients:
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": "Lumen <kendall@lumenmarketing.co>",
                    "to": [email],
                    "subject": "We're live.",
                    "html": html,
                },
            )
            results.append(f"{email}: {resp.status_code}")
        except Exception as e:
            results.append(f"{email}: ERROR {e}")
    return "<br>".join(results)

@app.route("/admin/blast-sequence-1")
def blast_sequence_1():
    """One-time blast of sequence email 1 to waitlist."""
    recipients = [
        "zachloos160@gmail.com",
        "allierich65@gmail.com",
        "marykatezarehghazarian@gmail.com",
        "alphonzolmajor@gmail.com",
        "kobidodd2@gmail.com",
        "n.wilkinson@launchpoint.dev",
        "kendallwdavis11@gmail.com",
        "darbimckean@gmail.com",
        "jaredponce2603@gmail.com",
    ]
    html = render_template("sequence_email_1.html")
    results = []
    for email in recipients:
        try:
            resp = requests.post(
                "https://api.resend.com/emails",
                headers={
                    "Authorization": f"Bearer {RESEND_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "from": "Lumen <kendall@lumenmarketing.co>",
                    "to": [email],
                    "subject": "The light always wins.",
                    "html": html,
                },
            )
            results.append(f"{email}: {resp.status_code}")
        except Exception as e:
            results.append(f"{email}: ERROR - {e}")
    return "<br>".join(results)

@app.route("/api/funnel-signup", methods=["POST"])
def funnel_signup():
    data = request.get_json() or {}
    email = data.get("email", "").strip()
    if not email:
        return jsonify({"ok": False})
    db_path = os.path.join(os.environ.get("RAILWAY_VOLUME_MOUNT_PATH", "."), "users.db")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS funnel_signups (id INTEGER PRIMARY KEY, email TEXT, created_at TEXT)")
    conn.execute("INSERT INTO funnel_signups (email, created_at) VALUES (?, ?)", (email, datetime.datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})

@app.route("/api/funnel-beta", methods=["POST"])
def funnel_beta():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    email = data.get("email", "").strip().lower()
    agency = data.get("agency", "").strip()
    clients = data.get("clients", "")
    spend = data.get("spend", "")
    bottleneck = data.get("bottleneck", "").strip()
    if not email or "@" not in email:
        return jsonify({"ok": False})
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS funnel_beta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            agency TEXT DEFAULT '',
            clients TEXT DEFAULT '',
            spend TEXT DEFAULT '',
            bottleneck TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute(
        "INSERT INTO funnel_beta (name, email, agency, clients, spend, bottleneck, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, email, agency, clients, spend, bottleneck, now),
    )
    con.commit()
    con.close()

    # Notify
    if RESEND_API_KEY:
        send_email(
            NOTIFY_EMAIL,
            f"LumenFunnel Beta Application — {agency}",
            f"<h2>New Beta Application</h2>"
            f"<p><b>Name:</b> {name}</p>"
            f"<p><b>Email:</b> {email}</p>"
            f"<p><b>Agency:</b> {agency}</p>"
            f"<p><b>Clients:</b> {clients}</p>"
            f"<p><b>Monthly Spend:</b> {spend}</p>"
            f"<p><b>Bottleneck:</b> {bottleneck}</p>",
        )
    return jsonify({"ok": True})

@app.route("/apply", methods=["POST"])
def apply_submit():
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    email = (data.get("email") or "").strip().lower()
    phone = (data.get("phone") or "").strip()
    industry = data.get("industry", "")
    revenue = data.get("revenue", "")
    marketing = data.get("marketing", "")
    goal = data.get("goal", "")
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"})

    now = datetime.datetime.utcnow().isoformat()

    # Store application
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            name TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            industry TEXT DEFAULT '',
            revenue TEXT DEFAULT '',
            marketing TEXT DEFAULT '',
            goal TEXT DEFAULT '',
            business TEXT DEFAULT '',
            challenge TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute(
        "INSERT INTO applications (email, name, phone, industry, revenue, marketing, goal, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (email, name, phone, industry, revenue, marketing, goal, now),
    )

    # Auto-create CRM lead
    existing_lead = con.execute("SELECT id FROM leads WHERE email = ?", (email,)).fetchone()
    if not existing_lead:
        cur = con.execute(
            """INSERT INTO leads (name, email, phone, business, revenue, marketing, challenge,
               stage, source, deal_value, follow_up_date, tags, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'new', '10daybuild', 0, '', ?, ?, ?)""",
            (name, email, phone, industry, revenue, marketing, goal, industry, now, now),
        )
        lead_id = cur.lastrowid
        con.execute(
            "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'auto', 'Lead created from 10-day build application', '', ?)",
            (lead_id, now),
        )
    con.commit()
    con.close()

    # Notify Kendall
    if RESEND_API_KEY:
        notify_html = f"""
        <div style="font-family:Inter,sans-serif;background:#0a0a0f;color:#e8e8f0;padding:40px;">
            <h2 style="color:#7c4dff;">New 10-Day Build Application</h2>
            <p><strong>Name:</strong> {name}</p>
            <p><strong>Email:</strong> {email}</p>
            <p><strong>Phone:</strong> {phone or 'Not provided'}</p>
            <p><strong>Industry:</strong> {industry}</p>
            <p><strong>Revenue:</strong> {revenue}</p>
            <p><strong>Current Marketing:</strong> {marketing}</p>
            <p><strong>Goal:</strong> {goal}</p>
        </div>
        """
        send_email(NOTIFY_EMAIL, f"10-Day Build Application: {name or email}", notify_html)

    return jsonify({"ok": True})


# ── Pipeline & Stage API ─────────────────────────────────────
@app.route("/api/pipelines")
def api_pipelines():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM pipelines ORDER BY id").fetchall()
    pipelines = []
    for p in rows:
        stages = con.execute(
            "SELECT * FROM pipeline_stages WHERE pipeline_id = ? ORDER BY position", (p["id"],)
        ).fetchall()
        pipelines.append({
            "id": p["id"], "name": p["name"],
            "stages": [dict(s) for s in stages],
            "created_at": p["created_at"],
        })
    con.close()
    return jsonify({"pipelines": pipelines})

@app.route("/api/pipelines", methods=["POST"])
def api_create_pipeline():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    cur = con.execute("INSERT INTO pipelines (name, created_at, updated_at) VALUES (?, ?, ?)", (name, now, now))
    pid = cur.lastrowid
    # Seed with default stages
    defaults = [("new", "New", "#7c4dff", 0), ("won", "Won", "#22c55e", 1), ("lost", "Lost", "#ef4444", 2)]
    for slug, sname, color, pos in defaults:
        con.execute("INSERT INTO pipeline_stages (pipeline_id, slug, name, color, position) VALUES (?, ?, ?, ?, ?)",
                    (pid, slug, sname, color, pos))
    con.commit()
    con.close()
    return jsonify({"ok": True, "id": pid})

@app.route("/api/pipelines/<int:pid>/stages", methods=["PUT"])
def api_update_stages(pid):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    stages = data.get("stages", [])
    if not stages:
        return jsonify({"ok": False, "error": "At least one stage required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    # Get old stages for remapping
    old_stages = con.execute("SELECT slug FROM pipeline_stages WHERE pipeline_id = ?", (pid,)).fetchall()
    old_slugs = {r[0] for r in old_stages}
    # Delete old stages and insert new
    con.execute("DELETE FROM pipeline_stages WHERE pipeline_id = ?", (pid,))
    new_slugs = set()
    for i, s in enumerate(stages):
        slug = s.get("slug", "").strip().lower().replace(" ", "_")
        if not slug:
            slug = s.get("name", "stage").strip().lower().replace(" ", "_")
        con.execute(
            "INSERT INTO pipeline_stages (pipeline_id, slug, name, color, position) VALUES (?, ?, ?, ?, ?)",
            (pid, slug, s.get("name", slug), s.get("color", "#7c4dff"), i),
        )
        new_slugs.add(slug)
    # Move leads from deleted stages to first stage
    removed_slugs = old_slugs - new_slugs
    if removed_slugs and new_slugs:
        first_slug = stages[0].get("slug", stages[0].get("name", "").strip().lower().replace(" ", "_"))
        for old_slug in removed_slugs:
            con.execute("UPDATE leads SET stage = ? WHERE pipeline_id = ? AND stage = ?",
                        (first_slug, pid, old_slug))
    con.execute("UPDATE pipelines SET updated_at = ? WHERE id = ?", (now, pid))
    con.commit()
    con.close()
    return jsonify({"ok": True})

@app.route("/api/pipelines/<int:pid>", methods=["PUT"])
def api_update_pipeline(pid):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE pipelines SET name = ?, updated_at = ? WHERE id = ?", (name, now, pid))
    con.commit()
    con.close()
    return jsonify({"ok": True})

# ── CRM Lead API ─────────────────────────────────────────────

@app.route("/api/leads")
def api_leads():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    pipeline_id = request.args.get("pipeline_id", 1, type=int)
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM leads WHERE pipeline_id = ? ORDER BY updated_at DESC", (pipeline_id,)).fetchall()
    leads = [dict(r) for r in rows]
    con.close()
    return jsonify({"leads": leads})

@app.route("/api/leads", methods=["POST"])
def api_create_lead():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    existing = con.execute("SELECT id FROM leads WHERE email = ?", (email,)).fetchone()
    if existing:
        con.close()
        return jsonify({"ok": False, "error": "Lead with this email already exists"}), 409
    pipeline_id = int(data.get("pipeline_id", 1))
    # Get first stage of this pipeline
    first_stage = con.execute(
        "SELECT slug FROM pipeline_stages WHERE pipeline_id = ? ORDER BY position LIMIT 1", (pipeline_id,)
    ).fetchone()
    initial_stage = first_stage[0] if first_stage else "new"
    cur = con.execute(
        """INSERT INTO leads (name, email, phone, business, revenue, marketing, challenge,
           stage, source, deal_value, follow_up_date, tags, pipeline_id, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (data.get("name", ""), email, data.get("phone", ""),
         data.get("business", ""), data.get("revenue", ""),
         data.get("marketing", ""), data.get("challenge", ""),
         initial_stage, data.get("source", "manual"), float(data.get("deal_value", 0)),
         data.get("follow_up_date", ""), data.get("tags", ""), pipeline_id, now, now),
    )
    lead_id = cur.lastrowid
    con.execute(
        "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'auto', 'Lead created manually', '', ?)",
        (lead_id, now),
    )
    con.commit()
    con.close()
    return jsonify({"ok": True, "id": lead_id})

@app.route("/api/leads/<int:lead_id>", methods=["PUT"])
def api_update_lead(lead_id):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    lead = con.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        con.close()
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    # Build update
    fields = []
    params = []
    allowed = ["name", "email", "phone", "business", "revenue", "marketing",
               "challenge", "stage", "source", "deal_value", "follow_up_date", "tags"]
    for f in allowed:
        if f in data:
            fields.append(f"{f} = ?")
            params.append(data[f] if f != "deal_value" else float(data[f] or 0))
    if not fields:
        con.close()
        return jsonify({"ok": False, "error": "No fields to update"}), 400
    fields.append("updated_at = ?")
    params.append(now)
    params.append(lead_id)
    con.execute(f"UPDATE leads SET {', '.join(fields)} WHERE id = ?", params)

    # Log stage change
    old_stage_idx = 0  # default
    col_names = [desc[0] for desc in con.execute("SELECT * FROM leads LIMIT 0").description]
    if "stage" in data:
        old_stage = lead[col_names.index("stage")] if "stage" in col_names else "new"
        if data["stage"] != old_stage:
            con.execute(
                "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'stage_change', ?, ?, ?)",
                (lead_id, f"Stage changed from {old_stage} to {data['stage']}", json.dumps({"from": old_stage, "to": data["stage"]}), now),
            )

    # Log other field updates (not stage)
    updated_fields = [f for f in data if f in allowed and f != "stage"]
    if updated_fields:
        con.execute(
            "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'update', ?, '', ?)",
            (lead_id, f"Updated: {', '.join(updated_fields)}", now),
        )

    con.commit()
    con.close()
    return jsonify({"ok": True})

@app.route("/api/leads/<int:lead_id>", methods=["DELETE"])
def api_delete_lead(lead_id):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM lead_activity WHERE lead_id = ?", (lead_id,))
    con.execute("DELETE FROM lead_emails WHERE lead_id = ?", (lead_id,))
    con.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})

@app.route("/api/leads/<int:lead_id>/activity")
def api_lead_activity(lead_id):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute(
        "SELECT * FROM lead_activity WHERE lead_id = ? ORDER BY created_at DESC LIMIT 50",
        (lead_id,)
    ).fetchall()
    emails = con.execute(
        "SELECT * FROM lead_emails WHERE lead_id = ? ORDER BY created_at DESC LIMIT 20",
        (lead_id,)
    ).fetchall()
    con.close()
    return jsonify({
        "activity": [dict(r) for r in rows],
        "emails": [dict(r) for r in emails],
    })

@app.route("/api/leads/<int:lead_id>/note", methods=["POST"])
def api_add_note(lead_id):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    content = (data.get("content") or "").strip()
    if not content:
        return jsonify({"ok": False, "error": "Note content required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'note', ?, '', ?)",
        (lead_id, content, now),
    )
    con.execute("UPDATE leads SET updated_at = ? WHERE id = ?", (now, lead_id))
    con.commit()
    con.close()
    return jsonify({"ok": True})

@app.route("/api/leads/<int:lead_id>/email", methods=["POST"])
def api_send_lead_email(lead_id):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    subject = (data.get("subject") or "").strip()
    body = (data.get("body") or "").strip()
    if not subject or not body:
        return jsonify({"ok": False, "error": "Subject and body required"}), 400
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    lead = con.execute("SELECT email, name FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        con.close()
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    lead_email = lead[0]
    lead_name = lead[1] or lead_email.split("@")[0]

    # Store email record
    con.execute(
        "INSERT INTO lead_emails (lead_id, subject, body, direction, created_at) VALUES (?, ?, ?, 'sent', ?)",
        (lead_id, subject, body, now),
    )
    # Log activity
    con.execute(
        "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'email_sent', ?, ?, ?)",
        (lead_id, f"Email sent: {subject}", json.dumps({"subject": subject}), now),
    )
    con.execute("UPDATE leads SET updated_at = ? WHERE id = ?", (now, lead_id))
    con.commit()
    con.close()

    # Actually send the email via Resend
    if RESEND_API_KEY:
        email_html = f"""
        <div style="font-family:Inter,-apple-system,sans-serif;max-width:600px;margin:0 auto;padding:40px 24px;">
            {body.replace(chr(10), '<br>')}
        </div>
        """
        send_email(lead_email, subject, email_html)

    return jsonify({"ok": True})

@app.route("/api/leads/import-applications", methods=["POST"])
def api_import_applications():
    """Import existing applications as leads (one-time migration)."""
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    try:
        apps = con.execute("SELECT email, business, revenue, marketing, challenge, created_at FROM applications").fetchall()
    except Exception:
        apps = []
    imported = 0
    for a in apps:
        existing = con.execute("SELECT id FROM leads WHERE email = ?", (a[0],)).fetchone()
        if not existing:
            cur = con.execute(
                """INSERT INTO leads (name, email, phone, business, revenue, marketing, challenge,
                   stage, source, deal_value, follow_up_date, tags, created_at, updated_at)
                   VALUES ('', ?, '', ?, ?, ?, ?, 'new', 'application', 0, '', '', ?, ?)""",
                (a[0], a[1], a[2], a[3], a[4], a[5], now),
            )
            con.execute(
                "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'auto', 'Imported from application', '', ?)",
                (cur.lastrowid, now),
            )
            imported += 1
    con.commit()
    con.close()
    return jsonify({"ok": True, "imported": imported})

@app.route("/api/leads/stats")
def api_lead_stats():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    pipeline_id = request.args.get("pipeline_id", 1, type=int)
    con = sqlite3.connect(DB_PATH)
    total = con.execute("SELECT COUNT(*) FROM leads WHERE pipeline_id = ?", (pipeline_id,)).fetchone()[0]
    stages = con.execute("SELECT slug FROM pipeline_stages WHERE pipeline_id = ? ORDER BY position", (pipeline_id,)).fetchall()
    by_stage = {}
    for s in stages:
        by_stage[s[0]] = con.execute("SELECT COUNT(*) FROM leads WHERE pipeline_id = ? AND stage = ?", (pipeline_id, s[0])).fetchone()[0]
    pipeline_value = con.execute("SELECT COALESCE(SUM(deal_value), 0) FROM leads WHERE pipeline_id = ? AND stage NOT IN ('won','lost')", (pipeline_id,)).fetchone()[0]
    won_value = con.execute("SELECT COALESCE(SUM(deal_value), 0) FROM leads WHERE pipeline_id = ? AND stage = 'won'", (pipeline_id,)).fetchone()[0]
    con.close()
    return jsonify({
        "total": total,
        "by_stage": by_stage,
        "pipeline_value": pipeline_value,
        "won_value": won_value,
    })

# ── LOS Overview API ──────────────────────────────────────────
@app.route("/api/los-overview")
def api_los_overview():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    now = datetime.datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    two_min_ago = (now - datetime.timedelta(minutes=2)).isoformat()

    total_views = con.execute("SELECT COUNT(*) FROM page_views").fetchone()[0]
    views_today = con.execute("SELECT COUNT(*) FROM page_views WHERE timestamp LIKE ?", (today + "%",)).fetchone()[0]

    owner_ph = ",".join("?" for _ in OWNER_IPS)
    avg_time = con.execute(
        f"SELECT AVG(time_on_page) FROM page_views WHERE time_on_page > 0 AND ip NOT IN ({owner_ph})",
        tuple(OWNER_IPS)
    ).fetchone()[0] or 0

    active_now = con.execute("SELECT COUNT(*) FROM page_views WHERE timestamp > ?", (two_min_ago,)).fetchone()[0]
    waitlist_count = con.execute("SELECT COUNT(*) FROM waitlist").fetchone()[0]
    try:
        app_count = con.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
    except Exception:
        app_count = 0
    lead_count = con.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    pipeline_value = con.execute("SELECT COALESCE(SUM(deal_value), 0) FROM leads WHERE stage NOT IN ('won','lost')").fetchone()[0]
    won_value = con.execute("SELECT COALESCE(SUM(deal_value), 0) FROM leads WHERE stage = 'won'").fetchone()[0]

    daily = []
    for i in range(6, -1, -1):
        d = (now - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        cnt = con.execute("SELECT COUNT(*) FROM page_views WHERE timestamp LIKE ?", (d + "%",)).fetchone()[0]
        daily.append({"date": d, "views": cnt})

    try:
        avalon_onboard_count = con.execute("SELECT COUNT(*) FROM avalon_onboarding").fetchone()[0]
    except Exception:
        avalon_onboard_count = 0

    try:
        mane_onboard_count = con.execute("SELECT COUNT(*) FROM mane_onboarding").fetchone()[0]
    except Exception:
        mane_onboard_count = 0

    try:
        funnel_beta_count = con.execute("SELECT COUNT(*) FROM funnel_beta").fetchone()[0]
    except Exception:
        funnel_beta_count = 0

    dashboards = []
    try:
        dbs = con.execute("SELECT name, slug, url FROM client_dashboards ORDER BY id").fetchall()
        for db_row in dbs:
            total_opens = con.execute(
                "SELECT COUNT(*) FROM dashboard_events WHERE dashboard_slug = ? AND event = 'open'", (db_row[1],)
            ).fetchone()[0]
            last_ev = con.execute(
                "SELECT timestamp FROM dashboard_events WHERE dashboard_slug = ? ORDER BY timestamp DESC LIMIT 1", (db_row[1],)
            ).fetchone()
            dashboards.append({
                "name": db_row[0], "slug": db_row[1], "url": db_row[2],
                "total_opens": total_opens,
                "last_active": last_ev[0] if last_ev else None,
            })
    except Exception:
        pass
    con.close()
    return jsonify({
        "total_views": total_views, "views_today": views_today,
        "avg_time": round(avg_time, 1), "active_now": active_now,
        "waitlist_count": waitlist_count, "app_count": app_count,
        "lead_count": lead_count, "pipeline_value": pipeline_value,
        "won_value": won_value, "daily": daily, "dashboards": dashboards,
        "avalon_onboard_count": avalon_onboard_count,
        "mane_onboard_count": mane_onboard_count,
        "funnel_beta_count": funnel_beta_count,
    })

@app.route("/api/main-site-analytics")
def api_main_site_analytics():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    date_from = request.args.get("from", "")
    date_to = request.args.get("to", "")
    con = sqlite3.connect(DB_PATH)

    # Build date filter (dates come in as MST, DB stores UTC, MST = UTC-7)
    MST_OFFSET = 7  # hours
    date_filter = ""
    date_params = []
    if date_from:
        date_filter += " AND timestamp >= ?"
        # MST midnight = UTC 07:00
        date_params.append(date_from + f"T{MST_OFFSET:02d}:00:00")
    if date_to:
        date_filter += " AND timestamp < ?"
        # MST end of day = next day UTC 07:00
        to_dt = datetime.datetime.strptime(date_to, "%Y-%m-%d") + datetime.timedelta(days=1)
        date_params.append(to_dt.strftime("%Y-%m-%d") + f"T{MST_OFFSET:02d}:00:00")

    # Per-page views and avg time (exclude owner IPs)
    owner_ph = ",".join("?" for _ in OWNER_IPS)
    pages = ["home", "about", "funnel", "coming-soon"]
    page_stats = []
    for p in pages:
        row = con.execute(
            f"SELECT COUNT(*), COALESCE(AVG(CASE WHEN time_on_page > 0 THEN time_on_page END), 0) FROM page_views WHERE page = ? AND ip NOT IN ({owner_ph})" + date_filter,
            (p, *OWNER_IPS, *date_params)
        ).fetchone()
        page_stats.append({"page": p, "views": row[0], "avg_time": round(row[1], 1)})

    # Total views (exclude owner)
    total_row = con.execute(
        f"SELECT COUNT(*), COALESCE(AVG(CASE WHEN time_on_page > 0 THEN time_on_page END), 0) FROM page_views WHERE ip NOT IN ({owner_ph})" + date_filter,
        (*OWNER_IPS, *date_params)
    ).fetchone()
    total_views = total_row[0]
    total_avg_time = round(total_row[1], 1)

    # Daily breakdown for chart (MST days: count views between UTC 07:00 to next day 07:00)
    mst_now = datetime.datetime.utcnow() - datetime.timedelta(hours=MST_OFFSET)
    daily = []
    for i in range(13, -1, -1):
        d = (mst_now - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        day_start = d + f"T{MST_OFFSET:02d}:00:00"
        day_end_dt = datetime.datetime.strptime(d, "%Y-%m-%d") + datetime.timedelta(days=1)
        day_end = day_end_dt.strftime("%Y-%m-%d") + f"T{MST_OFFSET:02d}:00:00"
        cnt = con.execute(
            f"SELECT COUNT(*) FROM page_views WHERE timestamp >= ? AND timestamp < ? AND ip NOT IN ({owner_ph})",
            (day_start, day_end, *OWNER_IPS)
        ).fetchone()[0]
        daily.append({"date": d, "views": cnt})

    # Daily per-page breakdown
    daily_pages = {}
    for p in ["home", "about", "funnel"]:
        dp = []
        for i in range(13, -1, -1):
            d = (datetime.datetime.utcnow() - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
            if date_from and d < date_from:
                continue
            if date_to and d > date_to:
                continue
            cnt = con.execute(
                f"SELECT COUNT(*) FROM page_views WHERE page = ? AND timestamp LIKE ? AND ip NOT IN ({owner_ph})",
                (p, d + "%", *OWNER_IPS)
            ).fetchone()[0]
            dp.append({"date": d, "views": cnt})
        daily_pages[p] = dp

    # Unique visitors (by IP, exclude owner)
    unique_row = con.execute(
        f"SELECT COUNT(DISTINCT ip) FROM page_views WHERE ip NOT IN ({owner_ph})" + date_filter,
        (*OWNER_IPS, *date_params)
    ).fetchone()
    unique_visitors = unique_row[0]

    # Return visitors (IPs that have visited more than once)
    return_row = con.execute(
        f"SELECT COUNT(*) FROM (SELECT ip, COUNT(*) as cnt FROM page_views WHERE ip NOT IN ({owner_ph}) AND ip != ''" + date_filter + " GROUP BY ip HAVING cnt > 1)",
        (*OWNER_IPS, *date_params)
    ).fetchone()
    return_visitors = return_row[0] if return_row else 0

    # Device breakdown
    devices = {"mobile": 0, "desktop": 0}
    device_rows = con.execute(
        f"SELECT user_agent FROM page_views WHERE ip NOT IN ({owner_ph})" + date_filter,
        (*OWNER_IPS, *date_params)
    ).fetchall()
    for dr in device_rows:
        ua = (dr[0] or "").lower()
        if any(m in ua for m in ["iphone", "android", "mobile"]):
            devices["mobile"] += 1
        else:
            devices["desktop"] += 1

    # Top referrers (exclude internal navigation)
    ref_rows = con.execute(
        f"SELECT referrer, COUNT(*) as cnt FROM page_views WHERE referrer != '' AND referrer NOT LIKE '%lumenmarketing.co%' AND ip NOT IN ({owner_ph})" + date_filter + " GROUP BY referrer ORDER BY cnt DESC LIMIT 10",
        (*OWNER_IPS, *date_params)
    ).fetchall()
    referrers = [{"referrer": r[0], "count": r[1]} for r in ref_rows]

    # Applications
    try:
        app_filter = ""
        app_params = []
        if date_from:
            app_filter += " AND created_at >= ?"
            app_params.append(date_from)
        if date_to:
            app_filter += " AND created_at <= ?"
            app_params.append(date_to + "T23:59:59")
        apps = con.execute(
            "SELECT email, business, revenue, marketing, challenge, created_at FROM applications WHERE 1=1" + app_filter + " ORDER BY id DESC",
            app_params
        ).fetchall()
        applications = [{"email": a[0], "business": a[1], "revenue": a[2],
                         "marketing": a[3], "challenge": a[4], "created_at": a[5]} for a in apps]
    except Exception:
        applications = []

    # Funnel events
    try:
        funnel = con.execute(
            "SELECT event, step, value, COUNT(*) as cnt FROM funnel_events GROUP BY event, step, value ORDER BY cnt DESC"
        ).fetchall()
        funnel_stats = [{"event": f[0], "step": f[1], "value": f[2], "count": f[3]} for f in funnel]
    except Exception:
        funnel_stats = []

    con.close()
    return jsonify({
        "applications": applications,
        "funnel": funnel_stats,
        "total_apps": len(applications),
        "total_views": total_views,
        "total_avg_time": total_avg_time,
        "unique_visitors": unique_visitors,
        "return_visitors": return_visitors,
        "page_stats": page_stats,
        "daily": daily,
        "daily_pages": daily_pages,
        "devices": devices,
        "referrers": referrers,
    })

@app.route("/api/dashboard/<slug>/analytics")
def api_dashboard_analytics(slug):
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    now = datetime.datetime.utcnow()
    total_opens = con.execute(
        "SELECT COUNT(*) FROM dashboard_events WHERE dashboard_slug = ? AND event = 'open'", (slug,)
    ).fetchone()[0]
    unique_ips = con.execute(
        "SELECT COUNT(DISTINCT ip) FROM dashboard_events WHERE dashboard_slug = ? AND ip != ''", (slug,)
    ).fetchone()[0]
    avg_duration = con.execute(
        "SELECT AVG(duration) FROM dashboard_events WHERE dashboard_slug = ? AND duration > 0", (slug,)
    ).fetchone()[0] or 0

    daily = []
    for i in range(6, -1, -1):
        d = (now - datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        cnt = con.execute(
            "SELECT COUNT(*) FROM dashboard_events WHERE dashboard_slug = ? AND event = 'open' AND timestamp LIKE ?",
            (slug, d + "%")
        ).fetchone()[0]
        daily.append({"date": d, "opens": cnt})

    rows = con.execute(
        "SELECT ip, user_agent, duration, timestamp FROM dashboard_events WHERE dashboard_slug = ? AND event = 'open' ORDER BY timestamp DESC LIMIT 50",
        (slug,)
    ).fetchall()
    visits = []
    for r in rows:
        ua = r[1]
        device = "Mobile" if any(m in ua for m in ["iPhone", "Android", "Mobile"]) else "Desktop"
        browser = "Safari" if "Safari" in ua and "Chrome" not in ua else "Chrome" if "Chrome" in ua else "Firefox" if "Firefox" in ua else "Other"
        visits.append({"ip": r[0], "device": device, "browser": browser, "duration": round(r[2], 1), "timestamp": r[3]})

    dash = con.execute("SELECT name, slug, url FROM client_dashboards WHERE slug = ?", (slug,)).fetchone()
    dash_info = {"name": dash[0], "slug": dash[1], "url": dash[2]} if dash else {"name": slug, "slug": slug, "url": ""}
    con.close()
    return jsonify({
        "dashboard": dash_info, "total_opens": total_opens,
        "unique_visitors": unique_ips, "avg_duration": round(avg_duration, 1),
        "daily": daily, "visits": visits,
    })

@app.route("/t/reset", methods=["POST"])
def reset_stats():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    data = request.get_json() or {}
    pin = data.get("pin", "")
    if pin != ADMIN_PIN:
        return jsonify({"ok": False}), 403
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM page_views")
    con.execute("DELETE FROM waitlist")
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ════════════════════════════════════════════════════════════
#   MARYKATE AGENT
# ════════════════════════════════════════════════════════════

def mk_auth_required(f):
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("mk_auth"):
            return redirect("/marykate/login")
        return f(*args, **kwargs)
    return decorated


@app.route("/marykate/login", methods=["GET", "POST"])
def mk_login():
    if request.method == "POST":
        pin = request.form.get("pin", "")
        if pin == MK_PIN:
            session["mk_auth"] = True
            session["mk_show_notice"] = True
            return redirect("/marykate")
        return render_template("mk_login.html", error="Wrong pin")
    return render_template("mk_login.html")


@app.route("/marykate/logout")
def mk_logout():
    session.pop("mk_auth", None)
    return redirect("/marykate/login")


@app.route("/marykate")
@mk_auth_required
def mk_dashboard():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    lead_count = con.execute("SELECT COUNT(*) FROM mk_leads").fetchone()[0]
    campaign_count = con.execute("SELECT COUNT(*) FROM mk_campaigns").fetchone()[0]
    sent_count = con.execute("SELECT COUNT(*) FROM mk_send_log").fetchone()[0]
    recent = con.execute("SELECT * FROM mk_send_log ORDER BY sent_at DESC LIMIT 10").fetchall()
    recent_leads = con.execute("SELECT * FROM mk_leads ORDER BY created_at DESC LIMIT 5").fetchall()
    con.close()
    show_notice = session.pop("mk_show_notice", False)
    notice_until = datetime.datetime.utcnow().date() <= datetime.date(2026, 4, 2)
    return render_template("mk_dashboard.html", active="dashboard",
        lead_count=lead_count, campaign_count=campaign_count,
        sent_count=sent_count, recent=recent, recent_leads=recent_leads,
        show_notice=show_notice, notice_until=notice_until)


@app.route("/marykate/leads")
@mk_auth_required
def mk_leads_page():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    leads = con.execute("SELECT * FROM mk_leads ORDER BY created_at DESC").fetchall()
    batches = con.execute("SELECT DISTINCT batch_name FROM mk_leads WHERE batch_name != '' ORDER BY batch_name").fetchall()
    batch_info = con.execute("""
        SELECT batch_name, batch_date, COUNT(*) as lead_count
        FROM mk_leads WHERE batch_name != ''
        GROUP BY batch_name ORDER BY batch_date DESC, batch_name
    """).fetchall()
    con.close()
    batch_names = [b["batch_name"] for b in batches]
    batch_list = [{"name": b["batch_name"], "date": b["batch_date"] or "", "count": b["lead_count"]} for b in batch_info]
    return render_template("mk_leads.html", active="leads", leads=leads, batch_names=batch_names, batch_list=batch_list)


@app.route("/marykate/compose")
@mk_auth_required
def mk_compose_page():
    channel = request.args.get("channel", "email")
    template_id = request.args.get("template")
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    leads = con.execute("SELECT * FROM mk_leads ORDER BY name ASC").fetchall()
    batches = con.execute("SELECT DISTINCT batch_name FROM mk_leads WHERE batch_name != '' ORDER BY batch_name").fetchall()
    template = None
    if template_id:
        template = con.execute("SELECT * FROM mk_templates WHERE id = ?", (template_id,)).fetchone()
    templates = con.execute("SELECT * FROM mk_templates WHERE channel = ? ORDER BY name", (channel,)).fetchall()
    con.close()
    batch_names = [b["batch_name"] for b in batches]
    return render_template("mk_compose.html", active="compose", leads=leads, channel=channel, batch_names=batch_names, template=template, templates=templates)


@app.route("/marykate/campaigns")
@mk_auth_required
def mk_campaigns_page():
    con = sqlite3.connect(DB_PATH)
    campaigns = con.execute("SELECT * FROM mk_campaigns ORDER BY created_at DESC").fetchall()
    con.close()
    return render_template("mk_campaigns.html", active="campaigns", campaigns=campaigns)


@app.route("/marykate/campaign/<int:campaign_id>")
@mk_auth_required
def mk_campaign_detail(campaign_id):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    campaign = con.execute("SELECT * FROM mk_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    if not campaign:
        con.close()
        return redirect("/marykate/campaigns")
    logs = con.execute("""
        SELECT sl.*, l.name as lead_name, l.email as lead_email
        FROM mk_send_log sl
        LEFT JOIN mk_leads l ON sl.lead_id = l.id
        WHERE sl.campaign_id = ?
        ORDER BY sl.sent_at DESC
    """, (campaign_id,)).fetchall()
    con.close()
    return render_template("mk_campaign_detail.html", active="campaigns",
        campaign=campaign, logs=logs)


@app.route("/whatsapp-connect")
def whatsapp_connect_page():
    return render_template("whatsapp_connect.html")


@app.route("/natew/sce")
def natew_sce():
    return render_template("sce_build.html")


@app.route("/sce/sms-privacy")
def sce_sms_privacy():
    return render_template("sce_sms_privacy.html")


@app.route("/sce/sms-terms")
def sce_sms_terms():
    return render_template("sce_sms_terms.html")


@app.route("/avalon/sms-privacy")
def avalon_sms_privacy():
    return render_template("avalon_sms_privacy.html")


@app.route("/avalon/sms-terms")
def avalon_sms_terms():
    return render_template("avalon_sms_terms.html")


# ── Grow / Landing Pages ──

@app.route("/grow")
@app.route("/grow/<market>")
def grow_page(market=None):
    valid = {"lb": "Lebanon", "gcc": "GCC", "us": "General"}
    if market not in valid:
        market = "lb"
    return render_template("grow.html", market=market, market_name=valid[market])


def _cors_response(resp):
    """Tag a response with permissive CORS headers — used for funnel endpoints
    that get called cross-origin from mk7media.com and other client funnels."""
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.route("/api/grow/lead", methods=["POST", "OPTIONS"])
def grow_lead_submit():
    # CORS — MK7 (mk7media.com) and any future external funnel can POST here
    if request.method == "OPTIONS":
        return _cors_response(jsonify({"ok": True}))
    # force=True so sendBeacon's text/plain bodies still parse as JSON
    data = request.get_json(force=True, silent=True) or {}
    whatsapp = (data.get("whatsapp") or "").strip()
    name = (data.get("name") or "").strip()
    business = (data.get("business") or "").strip()
    need = (data.get("need") or "").strip()
    source_page = (data.get("source_page") or "").strip()
    market = (data.get("market") or "").strip()
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if not whatsapp:
        return _cors_response(jsonify({"error": "WhatsApp number is required"})), 400

    con = sqlite3.connect(DB_PATH)
    existing = con.execute("SELECT id FROM funnel_leads WHERE whatsapp = ? AND created_at > datetime('now', '-1 hour')", (whatsapp,)).fetchone()
    if existing:
        con.execute("UPDATE funnel_leads SET name=COALESCE(NULLIF(?,''),name), business=COALESCE(NULLIF(?,''),business), need=COALESCE(NULLIF(?,''),need) WHERE id=?",
                     (name, business, need, existing[0]))
        con.commit()
        con.close()
        return _cors_response(jsonify({"ok": True, "updated": True}))

    con.execute("INSERT INTO funnel_leads (whatsapp, name, business, need, source_page, market, ip, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (whatsapp, name, business, need, source_page, market, ip, now))
    con.commit()
    con.close()

    market_labels = {"lb": "Lebanon", "gcc": "GCC/Dubai", "us": "America", "": "Unknown"}
    ml = market_labels.get(market, market)

    if RESEND_API_KEY:
        subject = f"New Lead from Grow Page ({ml})"
        body = f"""<div style="font-family:Inter,sans-serif;color:#1a1a1a;padding:20px;">
<h2 style="margin:0 0 16px;">New Lead</h2>
<p><strong>WhatsApp:</strong> {whatsapp}</p>
<p><strong>Name:</strong> {name or 'Not provided'}</p>
<p><strong>Business:</strong> {business or 'Not provided'}</p>
<p><strong>Need:</strong> {need or 'Not provided'}</p>
<p><strong>Market:</strong> {ml}</p>
<p><strong>Source:</strong> {source_page}</p>
<p><strong>Time:</strong> {now} UTC</p>
</div>"""
        try:
            for email in [NOTIFY_EMAIL, "marykatezarehghazarian@gmail.com"]:
                requests.post("https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                    json={"from": "MK7 Media <notifications@lumenmarketing.co>", "to": [email], "subject": subject, "html": body})
        except Exception:
            pass

    return _cors_response(jsonify({"ok": True}))


@app.route("/api/grow/pageview", methods=["POST", "OPTIONS"])
def grow_pageview():
    # CORS — called from mk7media.com on every page load + beforeunload
    if request.method == "OPTIONS":
        return _cors_response(jsonify({"ok": True}))

    # force=True so sendBeacon's text/plain bodies still parse as JSON
    data = request.get_json(force=True, silent=True) or {}
    page = (data.get("page") or "").strip()
    time_on_page = float(data.get("time_on_page") or 0)
    ip = request.headers.get("X-Forwarded-For", request.remote_addr or "")
    if "," in ip:
        ip = ip.split(",")[0].strip()
    referrer = (data.get("referrer") or "")[:500]
    ua = request.headers.get("User-Agent", "")[:500]
    device = "mobile" if any(m in ua.lower() for m in ["iphone", "android", "mobile"]) else "desktop"
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    if not page:
        return _cors_response(jsonify({"ok": False})), 400

    if ip in OWNER_IPS:
        return _cors_response(jsonify({"ok": True, "skipped": True}))

    con = sqlite3.connect(DB_PATH)
    if time_on_page > 0:
        # Time-on-page update — find the most recent open page-view row for
        # this IP+page within the last hour and stamp its duration
        existing = con.execute("SELECT id FROM funnel_page_views WHERE ip=? AND page=? AND created_at > datetime('now','-1 hour') AND time_on_page=0 ORDER BY id DESC LIMIT 1", (ip, page)).fetchone()
        if existing:
            con.execute("UPDATE funnel_page_views SET time_on_page=? WHERE id=?", (time_on_page, existing[0]))
            con.commit()
            con.close()
            return _cors_response(jsonify({"ok": True, "updated": True}))

    con.execute("INSERT INTO funnel_page_views (page, ip, referrer, user_agent, device, time_on_page, created_at) VALUES (?,?,?,?,?,?,?)",
                (page, ip, referrer, ua, device, time_on_page, now))
    con.commit()
    con.close()
    return _cors_response(jsonify({"ok": True}))


@app.route("/admin/audiences")
def admin_audiences():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    con = sqlite3.connect(DB_PATH)
    pages = con.execute("SELECT DISTINCT page FROM funnel_page_views ORDER BY page").fetchall()
    page_filter = request.args.get("page", "")
    min_time = float(request.args.get("min_time", 0))

    query = "SELECT ip, page, device, time_on_page, created_at FROM funnel_page_views WHERE 1=1"
    params = []
    if page_filter:
        query += " AND page=?"
        params.append(page_filter)
    if min_time > 0:
        query += " AND time_on_page>=?"
        params.append(min_time)
    query += " ORDER BY id DESC"
    rows = con.execute(query, params).fetchall()

    page_stats = {}
    for row in con.execute("SELECT page, COUNT(*), COUNT(DISTINCT ip), AVG(CASE WHEN time_on_page>0 THEN time_on_page END) FROM funnel_page_views GROUP BY page").fetchall():
        page_stats[row[0]] = {"views": row[1], "unique": row[2], "avg_time": round(row[3] or 0, 1)}

    con.close()
    viewers = [{"ip": r[0], "page": r[1], "device": r[2], "time": round(r[3], 1), "date": r[4][:16]} for r in rows]
    return render_template("admin_audiences.html", viewers=viewers, pages=[p[0] for p in pages], page_filter=page_filter, min_time=min_time, page_stats=page_stats)


@app.route("/admin/audiences/export")
def admin_audiences_export():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    page_filter = request.args.get("page", "")
    min_time = float(request.args.get("min_time", 0))
    fmt = request.args.get("format", "csv")

    con = sqlite3.connect(DB_PATH)
    query = "SELECT DISTINCT ip, page, device, MAX(time_on_page) as max_time, MIN(created_at) as first_visit FROM funnel_page_views WHERE 1=1"
    params = []
    if page_filter:
        query += " AND page=?"
        params.append(page_filter)
    if min_time > 0:
        query += " AND time_on_page>=?"
        params.append(min_time)
    query += " GROUP BY ip, page ORDER BY first_visit DESC"
    rows = con.execute(query, params).fetchall()
    con.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["IP Address", "Page", "Device", "Max Time on Page", "First Visit"])
    for r in rows:
        writer.writerow([r[0], r[1], r[2], round(r[3], 1), r[4][:16]])

    resp = make_response(output.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f"attachment; filename=audience_{page_filter or 'all'}_{datetime.datetime.utcnow().strftime('%Y%m%d')}.csv"
    return resp


@app.route("/admin/grow-leads")
def admin_grow_leads():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))
    con = sqlite3.connect(DB_PATH)
    leads = con.execute("SELECT * FROM funnel_leads ORDER BY id DESC").fetchall()
    cols = [d[0] for d in con.execute("SELECT * FROM funnel_leads LIMIT 0").description]
    con.close()
    lead_list = [dict(zip(cols, r)) for r in leads]
    return render_template("admin_grow_leads.html", leads=lead_list)


@app.route("/admin/funnels")
def admin_funnels():
    if not session.get("wl_auth"):
        return redirect(url_for("admin_landing"))

    funnels = [
        {"id": "lumen-lb", "title": "Stop Talking to Leads Who Never Buy", "market": "Lebanon", "brand": "Lumen", "offer": "Lead quality system", "url": "/grow/lb", "domain": "lumenmarketing.co", "cta": "WhatsApp", "flag": "🇱🇧"},
        {"id": "lumen-gcc", "title": "Scale Your Business with Qualified Leads", "market": "GCC/Dubai", "brand": "Lumen", "offer": "B2B lead gen", "url": "/grow/gcc", "domain": "lumenmarketing.co", "cta": "WhatsApp", "flag": "🇦🇪"},
        {"id": "lumen-us", "title": "Your Ads Should Be Making You Money", "market": "America", "brand": "Lumen", "offer": "Full system install", "url": "/grow/us", "domain": "lumenmarketing.co", "cta": "Calendly", "flag": "🇺🇸"},
        {"id": "mk7-lb", "title": "Stop Talking to Leads Who Never Buy", "market": "Lebanon", "brand": "MK7 Media", "offer": "Lead quality system + pricing", "url": "/grow/lb", "domain": "mk7media.com", "cta": "WhatsApp", "flag": "🇱🇧"},
        {"id": "mk7-gcc", "title": "Scale Your Business with Qualified Leads", "market": "GCC/Dubai", "brand": "MK7 Media", "offer": "B2B lead gen + content creators", "url": "/grow/gcc", "domain": "mk7media.com", "cta": "WhatsApp", "flag": "🇦🇪"},
        {"id": "mk7-home", "title": "Meta Ads That Deliver Qualified Buyers", "market": "All", "brand": "MK7 Media", "offer": "Main site + quiz funnel", "url": "/", "domain": "mk7media.com", "cta": "Quiz", "flag": "🌐"},
    ]

    con = sqlite3.connect(DB_PATH)
    total_leads = con.execute("SELECT COUNT(*) FROM funnel_leads").fetchone()[0]
    leads_by_market = {}
    for row in con.execute("SELECT market, COUNT(*) FROM funnel_leads GROUP BY market").fetchall():
        leads_by_market[row[0]] = row[1]
    leads_today = con.execute("SELECT COUNT(*) FROM funnel_leads WHERE created_at >= date('now')").fetchone()[0]
    leads_week = con.execute("SELECT COUNT(*) FROM funnel_leads WHERE created_at >= date('now', '-7 days')").fetchone()[0]
    recent_leads = con.execute("SELECT whatsapp, name, market, source_page, created_at FROM funnel_leads ORDER BY id DESC LIMIT 5").fetchall()
    con.close()

    stats = {
        "total": total_leads,
        "today": leads_today,
        "week": leads_week,
        "by_market": leads_by_market,
        "recent": [{"whatsapp": r[0], "name": r[1], "market": r[2], "source": r[3], "date": r[4][:16]} for r in recent_leads]
    }

    return render_template("admin_funnels.html", funnels=funnels, stats=stats)


@app.route("/privacy")
def privacy_policy():
    return render_template("privacy.html")


@app.route("/marykate/whatsapp")
@mk_auth_required
def mk_whatsapp_page():
    return render_template("mk_whatsapp.html", active="whatsapp")


@app.route("/marykate/templates")
@mk_auth_required
def mk_templates_page():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    templates = con.execute("SELECT * FROM mk_templates ORDER BY created_at DESC").fetchall()
    con.close()
    return render_template("mk_templates.html", active="templates", templates=templates)


@app.route("/marykate/api/templates", methods=["POST"])
@mk_auth_required
def mk_create_template():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    channel = data.get("channel", "email")
    subject = data.get("subject", "").strip()
    body = data.get("body", "").strip()
    if not name or not body:
        return jsonify({"ok": False, "error": "Name and body required"})
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO mk_templates (name, channel, subject, body, created_at) VALUES (?, ?, ?, ?, ?)",
                (name, channel, subject, body, now))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.route("/marykate/api/templates/<int:tpl_id>", methods=["POST"])
@mk_auth_required
def mk_update_template(tpl_id):
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    channel = data.get("channel", "email")
    subject = data.get("subject", "").strip()
    body = data.get("body", "").strip()
    if not name or not body:
        return jsonify({"ok": False, "error": "Name and body required"})
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE mk_templates SET name=?, channel=?, subject=?, body=? WHERE id=?",
                (name, channel, subject, body, tpl_id))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.route("/marykate/api/templates/<int:tpl_id>", methods=["DELETE"])
@mk_auth_required
def mk_delete_template(tpl_id):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM mk_templates WHERE id=?", (tpl_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Marykate API: Leads ──

def _detect_column(headers, patterns):
    """Find the best matching column header from a list of regex patterns."""
    for h in headers:
        h_clean = h.strip().lower()
        for p in patterns:
            if re.fullmatch(p, h_clean):
                return h
    return None

def _parse_uploaded_file(file):
    """Parse CSV, XLSX, or XLS file into a list of dicts."""
    filename = file.filename.lower()
    if filename.endswith(('.xlsx', '.xls')):
        import openpyxl
        file_bytes = file.read()
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
        ws = wb.active
        rows_iter = ws.iter_rows(values_only=True)
        header_row = next(rows_iter, None)
        if not header_row:
            return [], []
        headers = [str(h).strip() if h is not None else "" for h in header_row]
        data = []
        for row in rows_iter:
            d = {}
            for i, val in enumerate(row):
                if i < len(headers):
                    d[headers[i]] = str(val).strip() if val is not None else ""
            data.append(d)
        wb.close()
        return headers, data
    else:
        content = file.read().decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(content))
        headers = reader.fieldnames or []
        data = list(reader)
        return headers, data


@app.route("/marykate/api/leads/upload", methods=["POST"])
@mk_auth_required
def mk_upload_leads():
    file = request.files.get("file")
    if not file:
        return jsonify({"ok": False, "error": "No file"})

    filename = file.filename.lower()
    if not filename.endswith(('.csv', '.xlsx', '.xls')):
        return jsonify({"ok": False, "error": "Unsupported file type. Use CSV, XLSX, or XLS."})

    headers, data = _parse_uploaded_file(file)
    if not headers:
        return jsonify({"ok": False, "error": "File is empty or has no headers"})

    # Smart column detection
    name_patterns = [r'name', r'full\s*name', r'full_name', r'contact\s*name', r'lead\s*name',
                     r'client', r'customer', r'contact', r'person']
    first_name_patterns = [r'first\s*name', r'first_name', r'first', r'fname', r'given\s*name']
    last_name_patterns = [r'last\s*name', r'last_name', r'last', r'lname', r'surname', r'family\s*name']
    email_patterns = [r'e?\-?mail', r'email\s*address', r'email_address', r'contact\s*email', r'e_mail']
    phone_patterns = [r'phone', r'phone\s*number', r'phone_number', r'mobile', r'cell',
                      r'telephone', r'tel', r'contact\s*phone', r'number', r'cell\s*phone',
                      r'mobile\s*number', r'mobile_number', r'phone_no']
    tag_patterns = [r'tags?', r'source', r'category', r'type', r'label', r'labels', r'group', r'status']

    name_col = _detect_column(headers, name_patterns)
    first_col = _detect_column(headers, first_name_patterns)
    last_col = _detect_column(headers, last_name_patterns)
    email_col = _detect_column(headers, email_patterns)
    phone_col = _detect_column(headers, phone_patterns)
    tag_col = _detect_column(headers, tag_patterns)

    # Build mapping info for response
    mappings = {}
    combine_name = False
    if name_col:
        mappings["name"] = name_col
    elif first_col:
        mappings["name"] = first_col + (" + " + last_col if last_col else "")
        combine_name = True
    if email_col:
        mappings["email"] = email_col
    if phone_col:
        mappings["phone"] = phone_col
    if tag_col:
        mappings["tags"] = tag_col

    now = datetime.datetime.utcnow().isoformat()
    batch_name = (request.form.get("batch_name") or "").strip()
    if not batch_name:
        batch_name = "Upload " + datetime.datetime.utcnow().strftime("%b %d")
    batch_date = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    con = sqlite3.connect(DB_PATH)
    count = 0
    for row in data:
        if combine_name:
            fn = (row.get(first_col, "") or "").strip() if first_col else ""
            ln = (row.get(last_col, "") or "").strip() if last_col else ""
            name = (fn + " " + ln).strip()
        else:
            name = (row.get(name_col, "") or "").strip() if name_col else ""
        email = (row.get(email_col, "") or "").strip().lower() if email_col else ""
        phone = (row.get(phone_col, "") or "").strip() if phone_col else ""
        tags = (row.get(tag_col, "") or "").strip() if tag_col else ""
        if not email and not phone:
            continue
        con.execute("INSERT INTO mk_leads (name, email, phone, tags, batch_name, batch_date, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (name, email, phone, tags, batch_name, batch_date, now))
        count += 1
    con.commit()
    con.close()
    return jsonify({"ok": True, "count": count, "batch_name": batch_name, "mappings": mappings})


@app.route("/marykate/api/leads/bulk-delete", methods=["POST"])
@mk_auth_required
def mk_bulk_delete_leads():
    data = request.get_json() or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "error": "No IDs provided"})
    con = sqlite3.connect(DB_PATH)
    placeholders = ",".join("?" * len(ids))
    con.execute(f"DELETE FROM mk_leads WHERE id IN ({placeholders})", ids)
    con.commit()
    con.close()
    return jsonify({"ok": True, "deleted": len(ids)})


@app.route("/marykate/api/leads/add", methods=["POST"])
@mk_auth_required
def mk_add_lead():
    data = request.get_json() or {}
    name = data.get("name", "").strip()
    email = data.get("email", "").strip().lower()
    phone = data.get("phone", "").strip()
    tags = data.get("tags", "").strip()
    if not email and not phone:
        return jsonify({"ok": False, "error": "Email or phone required"})
    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO mk_leads (name, email, phone, tags, created_at) VALUES (?, ?, ?, ?, ?)",
                (name, email, phone, tags, now))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.route("/marykate/api/leads/<int:lead_id>", methods=["DELETE"])
@mk_auth_required
def mk_delete_lead(lead_id):
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM mk_leads WHERE id = ?", (lead_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.route("/marykate/api/leads/<int:lead_id>/notes", methods=["POST"])
@mk_auth_required
def mk_save_notes(lead_id):
    data = request.get_json() or {}
    notes = data.get("notes", "")
    con = sqlite3.connect(DB_PATH)
    con.execute("UPDATE mk_leads SET notes = ? WHERE id = ?", (notes, lead_id))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@app.route("/marykate/lead/<int:lead_id>")
@mk_auth_required
def mk_lead_detail(lead_id):
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    lead = con.execute("SELECT * FROM mk_leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        con.close()
        return redirect("/marykate/leads")
    send_logs = con.execute(
        "SELECT * FROM mk_send_log WHERE lead_id = ? ORDER BY sent_at DESC", (lead_id,)
    ).fetchall()
    con.close()
    return render_template("mk_lead_detail.html", active="leads", lead=lead, send_logs=send_logs)


@app.route("/marykate/api/send/check-duplicates", methods=["POST"])
@mk_auth_required
def mk_check_duplicates():
    data = request.get_json() or {}
    lead_ids = data.get("lead_ids", [])
    channel = data.get("channel", "email")
    subject = data.get("subject", "")
    body = data.get("body", "")
    if not lead_ids:
        return jsonify({"duplicates": []})
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    duplicates = []
    for lid in lead_ids:
        # Check if this lead has already received a message with same subject/body on same channel
        if channel == "email" and subject:
            row = con.execute(
                """SELECT sl.id FROM mk_send_log sl
                   JOIN mk_campaigns c ON sl.campaign_id = c.id
                   WHERE sl.lead_id = ? AND sl.channel = 'email' AND c.subject = ? AND sl.status = 'sent'
                   LIMIT 1""",
                (lid, subject)
            ).fetchone()
        elif channel == "sms" and body:
            row = con.execute(
                """SELECT sl.id FROM mk_send_log sl
                   JOIN mk_campaigns c ON sl.campaign_id = c.id
                   WHERE sl.lead_id = ? AND sl.channel = 'sms' AND c.body = ? AND sl.status = 'sent'
                   LIMIT 1""",
                (lid, body)
            ).fetchone()
        else:
            row = None
        if row:
            lead = con.execute("SELECT id, name, email, phone FROM mk_leads WHERE id = ?", (lid,)).fetchone()
            if lead:
                duplicates.append({"id": lead["id"], "name": lead["name"] or lead["email"] or lead["phone"]})
    con.close()
    return jsonify({"duplicates": duplicates})


# ── Marykate Gmail OAuth ──
@app.route("/marykate/gmail/connect")
@mk_auth_required
def mk_gmail_connect():
    if not GMAIL_CLIENT_ID:
        return "Gmail not configured. Set GMAIL_CLIENT_ID and GMAIL_CLIENT_SECRET.", 400
    from urllib.parse import urlencode
    params = {
        "client_id": GMAIL_CLIENT_ID,
        "redirect_uri": GMAIL_REDIRECT_URI,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/gmail.send https://www.googleapis.com/auth/userinfo.email",
        "access_type": "offline",
        "prompt": "consent",
    }
    return redirect("https://accounts.google.com/o/oauth2/auth?" + urlencode(params))


@app.route("/marykate/gmail/callback")
def mk_gmail_callback():
    code = request.args.get("code")
    if not code:
        return "No code provided by Google", 400
    try:
        # Exchange code for tokens directly via HTTP — no PKCE
        token_resp = requests.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": GMAIL_CLIENT_ID,
            "client_secret": GMAIL_CLIENT_SECRET,
            "redirect_uri": GMAIL_REDIRECT_URI,
            "grant_type": "authorization_code",
        })
        token_data = token_resp.json()
        if "error" in token_data:
            return f"Gmail token error: {token_data.get('error_description', token_data['error'])}", 400
        access_token = token_data["access_token"]
        refresh_token = token_data.get("refresh_token", "")
        expires_in = token_data.get("expires_in", 3600)
        expires_at = (datetime.datetime.utcnow() + datetime.timedelta(seconds=expires_in)).isoformat()

        # Get user email
        user_resp = requests.get("https://www.googleapis.com/oauth2/v2/userinfo",
                                 headers={"Authorization": f"Bearer {access_token}"})
        gmail_email = user_resp.json().get("email", "")

        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM mk_gmail_tokens")
        con.execute(
            "INSERT INTO mk_gmail_tokens (id, access_token, refresh_token, expires_at, email) VALUES (1, ?, ?, ?, ?)",
            (access_token, refresh_token, expires_at, gmail_email)
        )
        con.commit()
        con.close()
        session["mk_auth"] = True
        return redirect("/marykate/compose?channel=email")
    except Exception as e:
        return f"Gmail connection error: {e}", 500


def mk_get_gmail_creds():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT access_token, refresh_token, expires_at, email FROM mk_gmail_tokens WHERE id = 1").fetchone()
    con.close()
    if not row or not row[0]:
        return None, ""
    access_token = row[0]
    refresh_token = row[1]
    expires_at = row[2]
    email = row[3]
    # Check if expired and refresh
    if expires_at:
        try:
            exp = datetime.datetime.fromisoformat(expires_at)
            if datetime.datetime.utcnow() >= exp and refresh_token:
                resp = requests.post("https://oauth2.googleapis.com/token", data={
                    "client_id": GMAIL_CLIENT_ID,
                    "client_secret": GMAIL_CLIENT_SECRET,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                })
                td = resp.json()
                if "access_token" in td:
                    access_token = td["access_token"]
                    new_exp = (datetime.datetime.utcnow() + datetime.timedelta(seconds=td.get("expires_in", 3600))).isoformat()
                    con = sqlite3.connect(DB_PATH)
                    con.execute("UPDATE mk_gmail_tokens SET access_token = ?, expires_at = ? WHERE id = 1",
                                (access_token, new_exp))
                    con.commit()
                    con.close()
        except Exception:
            pass
    from google.oauth2.credentials import Credentials
    creds = Credentials(
        token=access_token, refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GMAIL_CLIENT_ID, client_secret=GMAIL_CLIENT_SECRET,
    )
    return creds, email


@app.route("/marykate/api/gmail/status")
@mk_auth_required
def mk_gmail_status():
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT email FROM mk_gmail_tokens WHERE id = 1").fetchone()
    con.close()
    if row and row[0]:
        return jsonify({"connected": True, "email": row[0]})
    return jsonify({"connected": False})


@app.route("/marykate/api/gmail/disconnect", methods=["POST"])
@mk_auth_required
def mk_gmail_disconnect():
    con = sqlite3.connect(DB_PATH)
    con.execute("DELETE FROM mk_gmail_tokens")
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Marykate: Background send helpers ──
def _mk_refresh_gmail_token(creds_refresh):
    """Refresh Gmail OAuth token and return new access token."""
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": GMAIL_CLIENT_ID,
        "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": creds_refresh,
        "grant_type": "refresh_token",
    })
    td = resp.json()
    if "access_token" in td:
        new_token = td["access_token"]
        new_exp = (datetime.datetime.utcnow() + datetime.timedelta(seconds=td.get("expires_in", 3600))).isoformat()
        try:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE mk_gmail_tokens SET access_token = ?, expires_at = ? WHERE id = 1", (new_token, new_exp))
            con.commit()
            con.close()
        except Exception:
            pass
        return new_token
    return None


def _mk_build_gmail_service(creds_token, creds_refresh):
    """Build a fresh Gmail service with current credentials."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials(
        token=creds_token, refresh_token=creds_refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GMAIL_CLIENT_ID, client_secret=GMAIL_CLIENT_SECRET,
    )
    return build("gmail", "v1", credentials=creds)


def _mk_send_emails_bg(campaign_id, subject, body, lead_ids, creds_token, creds_refresh, sender_email):
    """Send emails in background thread so request doesn't time out."""
    service = _mk_build_gmail_service(creds_token, creds_refresh)
    con = sqlite3.connect(DB_PATH)
    sent = 0
    errors = 0
    consecutive_failures = 0
    batch_size = len(lead_ids)
    for i, lid in enumerate(lead_ids):
        lead = con.execute("SELECT id, name, email FROM mk_leads WHERE id = ?", (lid,)).fetchone()
        if not lead or not lead[2]:
            continue
        now = datetime.datetime.utcnow().isoformat()
        personalized_body = body.replace("{{name}}", lead[1] or "there")
        personalized_subject = subject.replace("{{name}}", lead[1] or "there")
        msg = MIMEText(personalized_body, "html")
        msg["to"] = lead[2]
        msg["from"] = sender_email
        msg["subject"] = personalized_subject
        max_retries = 3
        for attempt in range(max_retries):
            try:
                raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
                result = service.users().messages().send(userId="me", body={"raw": raw}).execute()
                gmail_msg_id = result.get("id", "")
                con.execute(
                    "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, sent_at, gmail_msg_id) VALUES (?, ?, 'email', ?, 'sent', ?, ?)",
                    (campaign_id, lead[0], lead[2], now, gmail_msg_id))
                con.execute(
                    "UPDATE mk_leads SET last_contacted = ?, send_count = send_count + 1 WHERE id = ?",
                    (now, lead[0]))
                sent += 1
                consecutive_failures = 0
                break
            except Exception as e:
                error_str = str(e)
                if attempt < max_retries - 1:
                    if "invalid_grant" in error_str.lower() or "token" in error_str.lower() or "401" in error_str or "403" in error_str:
                        new_token = _mk_refresh_gmail_token(creds_refresh)
                        if new_token:
                            creds_token = new_token
                            service = _mk_build_gmail_service(creds_token, creds_refresh)
                    time.sleep(3 * (attempt + 1))
                else:
                    con.execute(
                        "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, error, sent_at) VALUES (?, ?, 'email', ?, 'failed', ?, ?)",
                        (campaign_id, lead[0], lead[2], error_str[:200], now))
                    errors += 1
                    consecutive_failures += 1
        con.commit()
        if consecutive_failures >= 10:
            con.execute(
                "UPDATE mk_campaigns SET status = 'failed', sent_count = ? WHERE id = ?", (sent, campaign_id))
            con.commit()
            con.close()
            return
        if batch_size > 50:
            time.sleep(2)
        else:
            time.sleep(1)
    con.execute("UPDATE mk_campaigns SET status = 'sent', sent_count = ? WHERE id = ?", (sent, campaign_id))
    con.commit()
    con.close()


def _mk_send_sms_bg(campaign_id, body, lead_ids):
    """Send SMS in background thread so request doesn't time out."""
    from twilio.rest import Client
    client = Client(TWILIO_SID, TWILIO_AUTH)
    con = sqlite3.connect(DB_PATH)
    sent = 0
    errors = 0
    for lid in lead_ids:
        lead = con.execute("SELECT id, name, phone FROM mk_leads WHERE id = ?", (lid,)).fetchone()
        if not lead or not lead[2]:
            continue
        now = datetime.datetime.utcnow().isoformat()
        personalized = body.replace("{{name}}", lead[1] or "there")
        try:
            client.messages.create(body=personalized, from_=TWILIO_PHONE, to=lead[2])
            con.execute(
                "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, sent_at) VALUES (?, ?, 'sms', ?, 'sent', ?)",
                (campaign_id, lead[0], lead[2], now))
            con.execute(
                "UPDATE mk_leads SET last_contacted = ?, send_count = send_count + 1 WHERE id = ?",
                (now, lead[0]))
            sent += 1
        except Exception as e:
            con.execute(
                "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, error, sent_at) VALUES (?, ?, 'sms', ?, 'failed', ?, ?)",
                (campaign_id, lead[0], lead[2], str(e)[:200], now))
            errors += 1
        con.commit()
        time.sleep(0.5)
    con.execute("UPDATE mk_campaigns SET status = 'sent', sent_count = ? WHERE id = ?", (sent, campaign_id))
    con.commit()
    con.close()


# ── Marykate API: Send Email ──
@app.route("/marykate/api/send/email", methods=["POST"])
@mk_auth_required
def mk_send_email():
    data = request.get_json() or {}
    subject = data.get("subject", "")
    body = data.get("body", "")
    lead_ids = data.get("lead_ids", [])
    if not subject or not body or not lead_ids:
        return jsonify({"ok": False, "error": "Subject, body, and recipients required"})
    creds, sender_email = mk_get_gmail_creds()
    if not creds:
        return jsonify({"ok": False, "error": "Gmail not connected"})
    con = sqlite3.connect(DB_PATH)
    now = datetime.datetime.utcnow().isoformat()
    cur = con.execute(
        "INSERT INTO mk_campaigns (name, channel, subject, body, status, created_at) VALUES (?, 'email', ?, ?, 'sending', ?)",
        (subject[:60], subject, body, now)
    )
    campaign_id = cur.lastrowid
    con.commit()
    con.close()
    threading.Thread(target=_mk_send_emails_bg, args=(
        campaign_id, subject, body, lead_ids, creds.token, creds.refresh_token, sender_email
    )).start()
    return jsonify({"ok": True, "sent": len(lead_ids), "errors": 0, "queued": True})


# ── Marykate API: Send SMS ──
@app.route("/marykate/api/send/sms", methods=["POST"])
@mk_auth_required
def mk_send_sms():
    data = request.get_json() or {}
    body = data.get("body", "")
    lead_ids = data.get("lead_ids", [])
    if not body or not lead_ids:
        return jsonify({"ok": False, "error": "Message and recipients required"})
    if not TWILIO_SID or not TWILIO_AUTH or not TWILIO_PHONE:
        return jsonify({"ok": False, "error": "Twilio not configured"})
    con = sqlite3.connect(DB_PATH)
    now = datetime.datetime.utcnow().isoformat()
    cur = con.execute(
        "INSERT INTO mk_campaigns (name, channel, body, status, created_at) VALUES (?, 'sms', ?, 'sending', ?)",
        (body[:60], body, now))
    campaign_id = cur.lastrowid
    con.commit()
    con.close()
    threading.Thread(target=_mk_send_sms_bg, args=(campaign_id, body, lead_ids)).start()
    return jsonify({"ok": True, "sent": len(lead_ids), "errors": 0, "queued": True})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
