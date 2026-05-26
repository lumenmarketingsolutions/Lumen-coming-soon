"""
MK7 Setter CRM — simplified lead/appointment tracker for the Lumen/MK7 outreach team.

Mounted on the host app as a Flask Blueprint at /crm.
Storage: a dedicated SQLite file (CRM_DB_PATH), separate from waitlist.db / whatsapp.db.

Auth model (v0.2 — switched away from Google sign-in to avoid Google's
verification gate for the calendar.events scope):
  - Setters log in with email + password (Werkzeug hashed). 60-day session.
  - First admin (kendall@lumenmarketing.co) is auto-seeded on first boot from
    CRM_BOOTSTRAP_ADMIN_PASSWORD env var. Forced to change on first login.
  - Calendar OAuth happens ONCE on the SHARED service account
    (kendall@lumenmarketing.co), connected from the admin page. Refresh
    token stored in app_settings. Every meeting booked by any setter creates
    an event on this calendar with MaryKate + Mary + the lead + the setter
    all listed as attendees, so it lands on their calendars via invite.

See ~/.claude/projects/-Users-kendalldavis-avalon-crm/memory/mk7-setter-crm.md for full intent.
"""

from flask import (
    Blueprint, render_template, request, jsonify, session,
    redirect, url_for, abort, current_app
)
import os, sqlite3, datetime, json, re, secrets, string, threading, requests
from functools import wraps
from urllib.parse import urlencode, quote_plus
from werkzeug.security import generate_password_hash as _gph_raw, check_password_hash


def generate_password_hash(pw):
    """Wrap werkzeug to force pbkdf2:sha256 — the default ('scrypt') depends
    on the stdlib hashlib having scrypt compiled in, which isn't true on
    older macOS Python builds. pbkdf2:sha256 is universally available and
    still strong (Werkzeug default iterations = 600k as of v3)."""
    return _gph_raw(pw, method="pbkdf2:sha256")

crm_bp = Blueprint("crm", __name__, url_prefix="/crm")


@crm_bp.app_template_filter("fromjson")
def _fromjson(s):
    """Jinja filter: parse a JSON string into a dict (used for activity payloads)."""
    if not s:
        return {}
    if isinstance(s, dict):
        return s
    try:
        return json.loads(s)
    except Exception:
        return {}


@crm_bp.app_template_filter("urlencode_with")
def _urlencode_with(overrides, base):
    """Jinja filter: merge `overrides` over filter dict `base`, drop empties,
    return a urlencoded querystring. Lets templates write chip links like
    `?{{ {'view':'mine'}|urlencode_with(f) }}` without rebuilding the full querystring."""
    merged = {}
    try:
        for k, v in (base or {}).items():
            if v not in (None, "", []):
                merged[k] = v
    except AttributeError:
        pass
    for k, v in (overrides or {}).items():
        if v in (None, "", []):
            merged.pop(k, None)
        else:
            merged[k] = v
    return urlencode(merged)

# ── Config ────────────────────────────────────────────────────────────────────
# Reuse the host app's RESEND_API_KEY (already loaded). We re-read from env so
# the blueprint stays self-contained if anyone imports it in isolation.
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")

# Dedicated Google OAuth client for the CRM. Distinct from GMAIL_* used by the
# Marykate agent because the redirect URI and scope set differ.
GOOGLE_CLIENT_ID = os.environ.get("CRM_GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("CRM_GOOGLE_CLIENT_SECRET", "")
GOOGLE_REDIRECT_URI = os.environ.get(
    "CRM_GOOGLE_REDIRECT_URI",
    "https://lumenmarketing.co/crm/auth/google/callback",
)
GOOGLE_SCOPES = " ".join([
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/calendar.events",
])

# First admin auto-provisioned on first boot. Email + password come from env so
# we never put plaintext credentials in code or git history. If the password
# env var is missing we still create the row but with a *long random* password
# that has to be reset via Settings — the admin won't be able to log in until
# the env var is populated.
BOOTSTRAP_ADMIN_EMAIL = os.environ.get(
    "CRM_BOOTSTRAP_ADMIN_EMAIL", "kendall@lumenmarketing.co"
).strip().lower()
BOOTSTRAP_ADMIN_PASSWORD = os.environ.get("CRM_BOOTSTRAP_ADMIN_PASSWORD", "")

# Admins who receive notification emails on lead-add / meeting-booked.
# Mary (mary@mk7media.com) was dropped per Kendall's request — only Kendall
# + MaryKate get the notifications now. Override via CRM_NOTIFY_ADMINS env.
NOTIFY_ADMINS = [
    e.strip() for e in
    os.environ.get("CRM_NOTIFY_ADMINS",
                   "kendall@lumenmarketing.co,marykatezarehghazarian@gmail.com")
        .split(",")
    if e.strip()
]

# Every Google Calendar event created by any setter is auto-attended by these
# emails. They show up on the invite + get the event on their own GCal. The
# event organizer (the setter who booked) is suppressed from this list so
# Google doesn't double-invite them. Mary removed per request — only Kendall
# (organizer) + MaryKate get the calendar invites now.
ALWAYS_INVITE = [
    e.strip().lower() for e in
    os.environ.get("CRM_ALWAYS_INVITE",
                   "kendall@lumenmarketing.co,marykatezarehghazarian@gmail.com")
        .split(",")
    if e.strip()
]

# Persistent volume on Railway, local file otherwise.
_DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("CRM_DB_PATH", os.path.join(_DATA_DIR, "setters.db"))

# ── Enums ─────────────────────────────────────────────────────────────────────
INDUSTRIES = [
    "Finance", "Manufacturing", "Real Estate", "Retail", "Education",
    "Construction", "Healthcare", "Hospitality", "Technology", "Other",
]
COUNTRIES = [
    "Australia", "Bahrain", "Canada", "Egypt", "France", "Germany",
    "India", "Iraq", "Jordan", "Kuwait", "Lebanon", "Morocco", "Oman",
    "Pakistan", "Qatar", "Saudi Arabia", "Turkey", "UAE",
    "United Kingdom", "United States",
]
SOURCES = ["Inbound", "Outbound"]
STATUSES = [
    "New", "Contacted", "Qualified", "Meeting Scheduled",
    "Proposal Sent", "Negotiation", "Closed",
]
ROLES = ["admin", "sales", "setter"]
POSITIONS = ["Meeting Setter", "Sales", "Admin"]

# Map UI role labels to internal role codes when admins edit users.
POSITION_TO_ROLE = {
    "Meeting Setter": "setter",
    "Sales": "sales",
    "Admin": "admin",
}

# ── DB ────────────────────────────────────────────────────────────────────────
def db():
    """Open a fresh connection. row_factory makes results behave like dicts."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


def init_db():
    """Idempotent schema init. Safe to call on every boot."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = db()
    cur = con.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        google_sub TEXT UNIQUE,
        first_name TEXT,
        last_name TEXT,
        picture_url TEXT,
        role TEXT NOT NULL DEFAULT 'setter',           -- admin | sales | setter
        position TEXT,                                  -- display label
        country TEXT,
        timezone TEXT,
        phone TEXT,
        password_hash TEXT,
        must_change_password INTEGER NOT NULL DEFAULT 0,
        google_access_token TEXT,
        google_refresh_token TEXT,
        google_token_expires_at TEXT,
        calendar_connected INTEGER NOT NULL DEFAULT 0,
        is_active INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        last_login_at TEXT
    );

    CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS login_attempts (
        ip TEXT NOT NULL,
        attempted_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_login_ip_time ON login_attempts(ip, attempted_at);

    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        ig_handle TEXT,
        full_name TEXT,
        job_title TEXT,
        email TEXT,
        phone_country_code TEXT,
        phone TEXT,
        country TEXT,
        industry TEXT,
        source TEXT NOT NULL DEFAULT 'Outbound',
        status TEXT NOT NULL DEFAULT 'New',
        do_not_approach INTEGER NOT NULL DEFAULT 0,
        do_not_approach_reason TEXT,
        notes TEXT,
        created_by_user_id INTEGER REFERENCES users(id),
        assigned_to_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_country ON leads(country);
    CREATE INDEX IF NOT EXISTS idx_leads_created_by ON leads(created_by_user_id);
    CREATE INDEX IF NOT EXISTS idx_leads_dna ON leads(do_not_approach);

    CREATE TABLE IF NOT EXISTS lead_dedupe_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
        key_type TEXT NOT NULL,         -- ig | email | phone
        key_value TEXT NOT NULL,
        UNIQUE(key_type, key_value)
    );
    CREATE INDEX IF NOT EXISTS idx_dedupe_lead ON lead_dedupe_keys(lead_id);

    CREATE TABLE IF NOT EXISTS lead_activity (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
        user_id INTEGER REFERENCES users(id),
        activity_type TEXT NOT NULL,
        payload TEXT,                   -- JSON blob
        created_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_activity_lead ON lead_activity(lead_id);

    CREATE TABLE IF NOT EXISTS meetings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
        setter_user_id INTEGER NOT NULL REFERENCES users(id),
        title TEXT,
        scheduled_at TEXT NOT NULL,     -- ISO UTC
        duration_min INTEGER NOT NULL DEFAULT 30,
        timezone TEXT,                  -- IANA name when booked
        gmeet_link TEXT,
        google_event_id TEXT,
        status TEXT NOT NULL DEFAULT 'scheduled',  -- scheduled | completed | no_show | canceled
        notes TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_meetings_scheduled ON meetings(scheduled_at);
    CREATE INDEX IF NOT EXISTS idx_meetings_setter ON meetings(setter_user_id);
    CREATE INDEX IF NOT EXISTS idx_meetings_lead ON meetings(lead_id);

    CREATE TABLE IF NOT EXISTS invitations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        role TEXT NOT NULL DEFAULT 'setter',
        position TEXT,
        country TEXT,
        invited_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );
    """)
    # ── Migrations for DBs created under v0.1 (Google-only schema) ──
    cols = {r[1] for r in cur.execute("PRAGMA table_info(users)").fetchall()}
    if "password_hash" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN password_hash TEXT")
    if "must_change_password" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN must_change_password INTEGER NOT NULL DEFAULT 0")
    con.commit()

    # ── Seed / sync bootstrap admin from env (idempotent) ──
    #
    # Behavior: every boot, IF CRM_BOOTSTRAP_ADMIN_PASSWORD is set, we ensure
    # the bootstrap admin row exists AND its password matches the env var.
    # This guarantees the env-var password is the source of truth: if the row
    # got created with a different password, was reset, or was corrupted,
    # the next boot restores the env-var password. The hash is what's stored
    # in the DB — the plaintext env var is only used during the hash step.
    existing = cur.execute("SELECT id, password_hash FROM users WHERE email = ?",
                           (BOOTSTRAP_ADMIN_EMAIL,)).fetchone()
    if not existing:
        # No admin yet — create one. If env var is missing, use an unguessable
        # placeholder so the row exists but no one can log in until env is set.
        pw = BOOTSTRAP_ADMIN_PASSWORD or ("locked-" + secrets.token_urlsafe(32))
        cur.execute("""
            INSERT INTO users (email, first_name, last_name, role, position,
                must_change_password, password_hash, is_active, created_at)
            VALUES (?, 'Kendall', 'Davis', 'admin', 'Admin', 0, ?, 1, ?)
        """, (BOOTSTRAP_ADMIN_EMAIL, generate_password_hash(pw),
              datetime.datetime.utcnow().isoformat()))
        con.commit()
        if BOOTSTRAP_ADMIN_PASSWORD:
            print(f"[crm] Bootstrapped admin {BOOTSTRAP_ADMIN_EMAIL} from CRM_BOOTSTRAP_ADMIN_PASSWORD")
        else:
            print(f"[crm] WARNING: created {BOOTSTRAP_ADMIN_EMAIL} with placeholder password. "
                  "Set CRM_BOOTSTRAP_ADMIN_PASSWORD env var and restart.")
    elif BOOTSTRAP_ADMIN_PASSWORD:
        # Existing admin — re-sync the password from env var so it's always
        # current. We only rewrite if the new hash differs from stored, to
        # avoid pointless writes on every boot (also: check_password_hash
        # is the only way to compare since each hash has its own salt).
        if not existing["password_hash"] or \
           not check_password_hash(existing["password_hash"], BOOTSTRAP_ADMIN_PASSWORD):
            cur.execute("UPDATE users SET password_hash = ?, must_change_password = 0, is_active = 1 WHERE id = ?",
                        (generate_password_hash(BOOTSTRAP_ADMIN_PASSWORD), existing["id"]))
            con.commit()
            print(f"[crm] Synced admin {BOOTSTRAP_ADMIN_EMAIL} password from env.")

    con.close()


def now_iso():
    return datetime.datetime.utcnow().isoformat()


# ── Auth helpers ──────────────────────────────────────────────────────────────
def current_user():
    """Return the logged-in user row (sqlite3.Row) or None. Cached on g via session id."""
    uid = session.get("crm_user_id")
    if not uid:
        return None
    con = db()
    row = con.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (uid,)).fetchone()
    con.close()
    return row


def login_required(f):
    @wraps(f)
    def wrapped(*a, **kw):
        if not current_user():
            # Preserve return path so post-login lands the user where they were trying to go.
            nxt = request.full_path if request.method == "GET" else request.path
            return redirect(url_for("crm.login", next=nxt))
        return f(*a, **kw)
    return wrapped


def admin_required(f):
    @wraps(f)
    def wrapped(*a, **kw):
        u = current_user()
        if not u:
            return redirect(url_for("crm.login", next=request.full_path))
        if u["role"] != "admin":
            abort(403)
        return f(*a, **kw)
    return wrapped


# ── Normalization for dedupe ──────────────────────────────────────────────────
def norm_email(s):
    if not s:
        return ""
    return s.strip().lower()


def norm_handle(s):
    if not s:
        return ""
    s = s.strip().lower()
    if s.startswith("@"):
        s = s[1:]
    # Strip a trailing slash or trailing query (in case someone pastes a URL)
    s = s.rstrip("/").split("?")[0].split("/")[-1]
    return s


def norm_phone(country_code, phone):
    """Strip to E.164-ish digits-only string for comparison. Don't be too clever."""
    digits = re.sub(r"\D", "", (phone or ""))
    if not digits:
        return ""
    cc = re.sub(r"\D", "", (country_code or ""))
    # If phone already starts with country code, don't double it.
    if cc and not digits.startswith(cc):
        digits = cc + digits
    return "+" + digits


def dedupe_keys_for(lead):
    """Returns list of (key_type, key_value) tuples (non-empty only)."""
    out = []
    if lead.get("email"):
        v = norm_email(lead["email"])
        if v:
            out.append(("email", v))
    if lead.get("ig_handle"):
        v = norm_handle(lead["ig_handle"])
        if v:
            out.append(("ig", v))
    if lead.get("phone"):
        v = norm_phone(lead.get("phone_country_code"), lead.get("phone"))
        if v and len(v) > 3:
            out.append(("phone", v))
    return out


def find_dupe(con, payload, exclude_lead_id=None):
    """Returns the first matching dupe lead row, or None."""
    keys = dedupe_keys_for(payload)
    if not keys:
        return None
    for (kt, kv) in keys:
        q = """
        SELECT l.*, u.first_name AS creator_first, u.last_name AS creator_last
        FROM lead_dedupe_keys k
        JOIN leads l ON l.id = k.lead_id
        LEFT JOIN users u ON u.id = l.created_by_user_id
        WHERE k.key_type = ? AND k.key_value = ?
        """
        params = [kt, kv]
        if exclude_lead_id:
            q += " AND l.id != ?"
            params.append(exclude_lead_id)
        q += " LIMIT 1"
        row = con.execute(q, params).fetchone()
        if row:
            return row, (kt, kv)
    return None


def write_dedupe_keys(con, lead_id, payload):
    """Upsert dedupe keys for a lead. Caller commits."""
    con.execute("DELETE FROM lead_dedupe_keys WHERE lead_id = ?", (lead_id,))
    for (kt, kv) in dedupe_keys_for(payload):
        try:
            con.execute(
                "INSERT INTO lead_dedupe_keys (lead_id, key_type, key_value) VALUES (?, ?, ?)",
                (lead_id, kt, kv),
            )
        except sqlite3.IntegrityError:
            # Another lead already owns this key; skip silently — caller chose to
            # override the dupe warning, so we don't block on the secondary key.
            pass


# ── Activity log ──────────────────────────────────────────────────────────────
def log_activity(con, lead_id, user_id, activity_type, payload=None):
    con.execute(
        "INSERT INTO lead_activity (lead_id, user_id, activity_type, payload, created_at) VALUES (?, ?, ?, ?, ?)",
        (lead_id, user_id, activity_type, json.dumps(payload) if payload else None, now_iso()),
    )


# ── Email notifications (admin) ───────────────────────────────────────────────
def _send_email(to, subject, html):
    if not RESEND_API_KEY:
        print(f"[crm-email] RESEND_API_KEY missing — would send to {to}: {subject}")
        return
    def _bg():
        try:
            r = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json={"from": "MK7 Setter CRM <kendall@lumenmarketing.co>",
                      "to": [to], "subject": subject, "html": html},
                timeout=10,
            )
            print(f"[crm-email] {to} -> {r.status_code}")
        except Exception as e:
            print(f"[crm-email] send error: {e}")
    threading.Thread(target=_bg, daemon=True).start()


def notify_new_lead(lead_id):
    con = db()
    row = con.execute("""
        SELECT l.*, u.first_name AS creator_first, u.last_name AS creator_last, u.email AS creator_email
        FROM leads l LEFT JOIN users u ON u.id = l.created_by_user_id
        WHERE l.id = ?
    """, (lead_id,)).fetchone()
    con.close()
    if not row:
        return
    setter = f"{row['creator_first'] or ''} {row['creator_last'] or ''}".strip() or row["creator_email"] or "Unknown"
    handle = f" // @{row['ig_handle']}" if row["ig_handle"] else ""
    subject = f"[MK7 CRM] {setter} added a lead: {row['company_name']}{handle}"
    base = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co")
    link = f"{base}/crm/leads/{lead_id}"

    # Instagram row: stripped handle + a clickable link to the IG profile.
    # On mobile, instagram.com URLs open the IG app automatically when installed.
    ig_row = ""
    if row["ig_handle"]:
        h = str(row["ig_handle"]).lstrip("@")
        ig_row = (
            f'<tr><td style="padding:4px 12px 4px 0;color:#888">Instagram</td>'
            f'<td><a href="https://instagram.com/{h}" '
            f'style="color:#128fc4;text-decoration:none">@{h}</a></td></tr>'
        )

    # Email and phone get their own rows + clickable mailto/tel links.
    email_row = (
        f'<tr><td style="padding:4px 12px 4px 0;color:#888">Email</td>'
        f'<td><a href="mailto:{row["email"]}" style="color:#128fc4;text-decoration:none">{row["email"]}</a></td></tr>'
    ) if row["email"] else ""
    phone_row = (
        f'<tr><td style="padding:4px 12px 4px 0;color:#888">Phone</td>'
        f'<td><a href="tel:{row["phone"]}" style="color:#128fc4;text-decoration:none">{row["phone"]}</a></td></tr>'
    ) if row["phone"] else ""

    # Notes section (only if present)
    notes_html = ""
    if row["notes"]:
        notes_html = (
            '<div style="margin-top:14px;padding:12px 14px;background:#f6f8fb;'
            'border-radius:8px;font-size:13px;line-height:1.5;color:#333;white-space:pre-wrap">'
            '<div style="font-size:10px;font-weight:700;letter-spacing:1.5px;'
            'text-transform:uppercase;color:#888;margin-bottom:6px">Notes</div>'
            f'{row["notes"]}'
            '</div>'
        )

    html = f"""
    <div style="font-family:Inter,system-ui,sans-serif;max-width:560px">
      <h2 style="margin:0 0 12px;font-size:18px">New lead added</h2>
      <p style="margin:0 0 16px;color:#444">
        <strong>{setter}</strong> just added a new lead in the CRM.
      </p>
      <table style="font-size:14px;color:#333;border-collapse:collapse">
        <tr><td style="padding:4px 12px 4px 0;color:#888">Company</td><td>{row['company_name']}</td></tr>
        {ig_row}
        <tr><td style="padding:4px 12px 4px 0;color:#888">Industry</td><td>{row['industry'] or '—'}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Country</td><td>{row['country'] or '—'}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Source</td><td>{row['source']}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Status</td><td>{row['status']}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Contact</td><td>{row['full_name'] or '—'}</td></tr>
        {email_row}
        {phone_row}
      </table>
      {notes_html}
      <p style="margin:20px 0 0">
        <a href="{link}" style="background:#128fc4;color:#fff;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600">Open lead</a>
      </p>
    </div>
    """
    for to in NOTIFY_ADMINS:
        _send_email(to, subject, html)


def notify_meeting_booked(meeting_id):
    con = db()
    row = con.execute("""
        SELECT m.*,
               l.company_name, l.ig_handle, l.country AS lead_country,
               u.first_name AS setter_first, u.last_name AS setter_last,
               u.email AS setter_email, u.country AS setter_country
        FROM meetings m
        JOIN leads l ON l.id = m.lead_id
        JOIN users u ON u.id = m.setter_user_id
        WHERE m.id = ?
    """, (meeting_id,)).fetchone()
    con.close()
    if not row:
        return
    setter = f"{row['setter_first'] or ''} {row['setter_last'] or ''}".strip() or row["setter_email"]
    handle = f" // @{row['ig_handle']}" if row["ig_handle"] else ""
    subject = f"[MK7 CRM] {setter} booked a meeting with {row['company_name']}"
    when = row["scheduled_at"]
    try:
        dt = datetime.datetime.fromisoformat(when.replace("Z", ""))
        when_pretty = dt.strftime("%a %b %d, %Y · %I:%M %p UTC")
    except Exception:
        when_pretty = when
    base = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co")
    link = f"{base}/crm/leads/{row['lead_id']}"
    gmeet_html = f'<p style="margin:8px 0 0"><a href="{row["gmeet_link"]}">{row["gmeet_link"]}</a></p>' if row["gmeet_link"] else ""
    html = f"""
    <div style="font-family:Inter,system-ui,sans-serif;max-width:560px">
      <h2 style="margin:0 0 12px;font-size:18px">Meeting booked</h2>
      <p style="margin:0 0 16px;color:#444">
        <strong>{setter}</strong> booked a meeting with <strong>{row['company_name']}{handle}</strong>.
      </p>
      <table style="font-size:14px;color:#333;border-collapse:collapse">
        <tr><td style="padding:4px 12px 4px 0;color:#888">When</td><td>{when_pretty}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Duration</td><td>{row['duration_min']} min</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Setter country</td><td>{row['setter_country'] or '—'}</td></tr>
        <tr><td style="padding:4px 12px 4px 0;color:#888">Lead country</td><td>{row['lead_country'] or '—'}</td></tr>
      </table>
      {gmeet_html}
      <p style="margin:20px 0 0">
        <a href="{link}" style="background:#128fc4;color:#fff;padding:10px 18px;border-radius:8px;text-decoration:none;font-weight:600">Open lead</a>
      </p>
    </div>
    """
    for to in NOTIFY_ADMINS:
        _send_email(to, subject, html)


def send_password_reset_email(email, first_name, temp_password, reset_by_first):
    """Notify a user that their password was reset by an admin. Same brand
    as the invite email, different copy."""
    base = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co")
    login_url = f"{base}/crm/login"
    name = (first_name or "").strip() or "there"
    by = (reset_by_first or "").strip() or "An admin"
    subject = "Your MK7 CRM password was reset"
    html = f"""\
<div style="font-family:Inter,system-ui,sans-serif;background:#080809;padding:32px 16px;color:#e8e8f0">
  <div style="max-width:480px;margin:0 auto;background:#111114;border:1px solid #1c1c24;border-radius:18px;padding:32px;color:#e8e8f0">
    <div style="font-size:11px;letter-spacing:2px;text-transform:uppercase;color:#56566a;margin-bottom:6px">
      Lumen × MK7
    </div>
    <h1 style="margin:0 0 16px;font-size:22px;font-weight:700;letter-spacing:-0.4px">
      Password reset
    </h1>
    <p style="margin:0 0 16px;font-size:14px;line-height:1.6;color:#b8b8c8">
      Hi {name} — {by} just reset your MK7 Setter CRM password. Use the new
      temporary password below to sign in, then choose a new one of your own.
    </p>
    <table style="width:100%;font-size:14px;border-collapse:collapse;background:#15151a;border:1px solid #1c1c24;border-radius:10px;margin:0 0 22px">
      <tr><td style="padding:14px 16px 6px;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#56566a">Email</td></tr>
      <tr><td style="padding:0 16px 12px;font-family:ui-monospace,Menlo,monospace;color:#e8e8f0">{email}</td></tr>
      <tr><td style="padding:8px 16px 6px;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#56566a;border-top:1px solid #1c1c24">New temporary password</td></tr>
      <tr><td style="padding:0 16px 16px;font-family:ui-monospace,Menlo,monospace;font-size:16px;color:#e8e8f0;letter-spacing:0.5px">{temp_password}</td></tr>
    </table>
    <p style="margin:0 0 22px">
      <a href="{login_url}"
         style="display:inline-block;background:#128fc4;color:#fff;padding:12px 22px;border-radius:10px;text-decoration:none;font-weight:600;font-size:14px">
        Sign in
      </a>
    </p>
    <p style="margin:0;font-size:12px;color:#56566a;line-height:1.6">
      If you didn't ask for a reset, contact Kendall right away.
    </p>
  </div>
</div>
"""
    _send_email(email, subject, html)


def send_invite_email(email, first_name, temp_password, invited_by_first):
    """Send a welcome/invite email to a newly-created user with their temp
    credentials. They'll be forced to change the password on first login."""
    base = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co")
    login_url = f"{base}/crm/login"
    name = (first_name or "").strip() or "there"
    by = (invited_by_first or "").strip() or "An admin"
    subject = "You've been invited to the MK7 CRM"
    html = f"""\
<div style="font-family:Inter,system-ui,sans-serif;background:#080809;padding:32px 16px;color:#e8e8f0">
  <div style="max-width:480px;margin:0 auto;background:#111114;border:1px solid #1c1c24;border-radius:18px;padding:32px;color:#e8e8f0">
    <div style="font-size:11px;letter-spacing:2px;text-transform:uppercase;color:#56566a;margin-bottom:6px">
      Lumen × MK7
    </div>
    <h1 style="margin:0 0 16px;font-size:22px;font-weight:700;letter-spacing:-0.4px">
      You're in.
    </h1>
    <p style="margin:0 0 18px;font-size:14px;line-height:1.6;color:#b8b8c8">
      Hi {name} — {by} just added you to the <strong>MK7 Setter CRM</strong>.
      Use the credentials below to sign in. You'll be asked to choose a new
      password as soon as you're in.
    </p>
    <table style="width:100%;font-size:14px;border-collapse:collapse;background:#15151a;border:1px solid #1c1c24;border-radius:10px;margin:0 0 22px">
      <tr>
        <td style="padding:14px 16px 6px;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#56566a">Email</td>
      </tr>
      <tr><td style="padding:0 16px 12px;font-family:ui-monospace,Menlo,monospace;color:#e8e8f0">{email}</td></tr>
      <tr><td style="padding:8px 16px 6px;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#56566a;border-top:1px solid #1c1c24">Temporary password</td></tr>
      <tr><td style="padding:0 16px 16px;font-family:ui-monospace,Menlo,monospace;font-size:16px;color:#e8e8f0;letter-spacing:0.5px">{temp_password}</td></tr>
    </table>
    <p style="margin:0 0 22px">
      <a href="{login_url}"
         style="display:inline-block;background:#128fc4;color:#fff;padding:12px 22px;border-radius:10px;text-decoration:none;font-weight:600;font-size:14px">
        Sign in to MK7 CRM
      </a>
    </p>
    <p style="margin:0;font-size:12px;color:#56566a;line-height:1.6">
      Sessions stay signed in for 60 days. If you didn't expect this, just
      ignore this email — no account is active until you sign in.
    </p>
    <div style="margin-top:24px;padding-top:16px;border-top:1px solid #1c1c24;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#56566a">
      Lumen × MK7 — Powered by Lumen Marketing
    </div>
  </div>
</div>
"""
    _send_email(email, subject, html)


# ── Email + password auth ─────────────────────────────────────────────────────
LOGIN_THROTTLE_WINDOW_MIN = 15
LOGIN_THROTTLE_MAX_ATTEMPTS = 8


def _client_ip():
    return (request.headers.get("X-Forwarded-For", request.remote_addr) or "").split(",")[0].strip()


def _login_locked(ip):
    if not ip:
        return False
    con = db()
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(minutes=LOGIN_THROTTLE_WINDOW_MIN)).isoformat()
    n = con.execute(
        "SELECT COUNT(*) FROM login_attempts WHERE ip = ? AND attempted_at > ?",
        (ip, cutoff),
    ).fetchone()[0]
    con.close()
    return n >= LOGIN_THROTTLE_MAX_ATTEMPTS


def _record_failed_login(ip):
    if not ip:
        return
    con = db()
    con.execute("INSERT INTO login_attempts (ip, attempted_at) VALUES (?, ?)",
                (ip, now_iso()))
    # Garbage-collect old rows occasionally so the table doesn't grow forever.
    con.execute("DELETE FROM login_attempts WHERE attempted_at < ?",
                ((datetime.datetime.utcnow() - datetime.timedelta(hours=24)).isoformat(),))
    con.commit()
    con.close()


def _clear_login_attempts(ip):
    if not ip:
        return
    con = db()
    con.execute("DELETE FROM login_attempts WHERE ip = ?", (ip,))
    con.commit()
    con.close()


def _looks_like_safe_next(nxt):
    """Only accept internal /crm paths as post-login redirects (no open redirect)."""
    return bool(nxt) and nxt.startswith("/crm") and "://" not in nxt and "\n" not in nxt


@crm_bp.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        return redirect(url_for("crm.leads"))
    ip = _client_ip()
    error = request.args.get("error")
    nxt = request.values.get("next", "/crm/leads")

    if request.method == "POST":
        if _login_locked(ip):
            error = "Too many attempts. Wait 15 minutes and try again."
        else:
            email = norm_email(request.form.get("email") or "")
            pw = (request.form.get("password") or "")
            con = db()
            row = con.execute(
                "SELECT * FROM users WHERE email = ? AND is_active = 1", (email,)
            ).fetchone()
            con.close()
            if row and row["password_hash"] and check_password_hash(row["password_hash"], pw):
                session.permanent = True
                session["crm_user_id"] = row["id"]
                con = db()
                con.execute("UPDATE users SET last_login_at = ? WHERE id = ?", (now_iso(), row["id"]))
                con.commit()
                con.close()
                _clear_login_attempts(ip)
                if row["must_change_password"]:
                    return redirect(url_for("crm.settings_page", change_password="1"))
                if not _looks_like_safe_next(nxt):
                    nxt = "/crm/leads"
                return redirect(nxt)
            _record_failed_login(ip)
            error = "That email and password didn't match an active account."
    return render_template("crm/login.html", error=error, next=nxt)


@crm_bp.route("/logout")
def logout():
    session.pop("crm_user_id", None)
    return redirect(url_for("crm.login"))


# ── App-settings helpers (singleton key/value store for shared GCal tokens) ───
def get_setting(key):
    con = db()
    row = con.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
    con.close()
    return row["value"] if row else None


def set_setting(key, value):
    con = db()
    con.execute("""INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at""",
                (key, value, now_iso()))
    con.commit()
    con.close()


# ── Admin-only: connect THE SHARED Google Calendar account ───────────────────
@crm_bp.route("/admin/calendar/connect")
@admin_required
def admin_calendar_connect():
    if not GOOGLE_CLIENT_ID:
        return ("Google OAuth not configured. Set CRM_GOOGLE_CLIENT_ID + "
                "CRM_GOOGLE_CLIENT_SECRET on the server."), 503
    state = secrets.token_urlsafe(24)
    session["crm_oauth_state"] = state
    params = {
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": GOOGLE_REDIRECT_URI,
        "response_type": "code",
        "scope": GOOGLE_SCOPES,
        "access_type": "offline",
        "include_granted_scopes": "true",
        # Force consent so we always get a refresh_token (Google only issues
        # one on first consent; subsequent grants return access_token only).
        "prompt": "consent",
        "state": state,
        # Pre-fill the email field so admin doesn't sign in with the wrong account.
        "login_hint": BOOTSTRAP_ADMIN_EMAIL,
    }
    return redirect("https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params))


@crm_bp.route("/auth/google/callback")
@admin_required
def auth_google_callback():
    """OAuth callback for the shared service-calendar connection. Admin-only.
    We never use Google for *user* login anymore — this endpoint exists solely
    to capture a refresh token for the one shared service account whose calendar
    hosts all meetings."""
    if request.args.get("state") != session.get("crm_oauth_state"):
        return "OAuth state mismatch — please try again from Admin → Calendar.", 400
    code = request.args.get("code")
    if not code:
        err = request.args.get("error", "no code returned")
        return f"Google connection canceled: {err}", 400

    try:
        token_resp = requests.post("https://oauth2.googleapis.com/token", data={
            "code": code,
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uri": GOOGLE_REDIRECT_URI,
            "grant_type": "authorization_code",
        }, timeout=10)
        tok = token_resp.json()
        if "error" in tok:
            return f"Google token error: {tok.get('error_description', tok['error'])}", 400
        access_token = tok["access_token"]
        refresh_token = tok.get("refresh_token", "")
        expires_at = (datetime.datetime.utcnow() +
                      datetime.timedelta(seconds=tok.get("expires_in", 3600))).isoformat()

        # Capture the *Google account* that just authorized us so admin can
        # see which calendar events will land on.
        u_resp = requests.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        u = u_resp.json()
        email = (u.get("email") or "").lower()
    except Exception as e:
        return f"Google connection failed: {e}", 500

    # If we didn't receive a refresh_token (because the user previously
    # authorized this app and Google declined to re-issue one), fall back to
    # whatever's already stored. If nothing is stored, ask them to revoke
    # and re-grant.
    if not refresh_token:
        existing = get_setting("gcal_refresh_token")
        if existing:
            refresh_token = existing
        else:
            return ("Google didn't return a refresh token. This usually means the "
                    "app is already authorized for this account. Revoke access at "
                    "https://myaccount.google.com/permissions and retry."), 400

    set_setting("gcal_access_token", access_token)
    set_setting("gcal_refresh_token", refresh_token)
    set_setting("gcal_token_expires_at", expires_at)
    set_setting("gcal_account_email", email)
    return redirect("/crm/admin?calendar=connected")


# ── Shared Google Calendar helpers ────────────────────────────────────────────
# All Google Calendar work uses ONE service account whose tokens live in
# app_settings (connected once by an admin from /crm/admin/calendar/connect).
def _refresh_shared_token():
    rt = get_setting("gcal_refresh_token")
    if not rt:
        return None
    try:
        r = requests.post("https://oauth2.googleapis.com/token", data={
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "refresh_token": rt,
            "grant_type": "refresh_token",
        }, timeout=10)
        td = r.json()
        if "access_token" not in td:
            print(f"[crm-gcal] refresh failed: {td}")
            return None
        new = td["access_token"]
        new_exp = (datetime.datetime.utcnow() +
                   datetime.timedelta(seconds=td.get("expires_in", 3600))).isoformat()
        set_setting("gcal_access_token", new)
        set_setting("gcal_token_expires_at", new_exp)
        return new
    except Exception as e:
        print(f"[crm-gcal] refresh error: {e}")
        return None


def _shared_access_token():
    tok = get_setting("gcal_access_token")
    exp = get_setting("gcal_token_expires_at")
    if not tok and not get_setting("gcal_refresh_token"):
        return None
    if exp:
        try:
            if datetime.datetime.fromisoformat(exp) <= datetime.datetime.utcnow() + datetime.timedelta(seconds=30):
                tok = _refresh_shared_token() or tok
        except Exception:
            pass
    if not tok:
        tok = _refresh_shared_token()
    return tok


def shared_calendar_status():
    """For admin UI: returns dict with connected bool + which account."""
    return {
        "connected": bool(get_setting("gcal_refresh_token")),
        "account_email": get_setting("gcal_account_email") or "",
        "last_token_at": get_setting("gcal_token_expires_at") or "",
    }


def create_google_calendar_event(setter_row, *, summary, description, start_iso_utc, duration_min, lead_email=None):
    """Create an event on the SHARED service calendar with a Google Meet link.
    Attendees:
      - the lead's email (when known)
      - ALWAYS_INVITE list (Kendall, Mary, MaryKate)
    The setter is intentionally NOT invited — the CRM is their system of record
    for bookings, and they don't need a calendar invite for every meeting they
    set. The shared service account is the organizer, so it's dropped from
    the attendee list to avoid Google double-inviting itself.
    Returns (event_id, meet_link, error)."""
    tok = _shared_access_token()
    if not tok:
        return None, None, "Calendar not connected — ask an admin to connect it from /crm/admin"
    try:
        start = datetime.datetime.fromisoformat(start_iso_utc.replace("Z", ""))
        end = start + datetime.timedelta(minutes=duration_min)
        organizer_email = (get_setting("gcal_account_email") or "").lower()
        invited = set()
        attendees = []
        if lead_email:
            e = lead_email.strip().lower()
            if e and e != organizer_email and e not in invited:
                attendees.append({"email": lead_email.strip()})
                invited.add(e)
        for e in ALWAYS_INVITE:
            if e and e != organizer_email and e not in invited:
                attendees.append({"email": e})
                invited.add(e)
        body = {
            "summary": summary,
            "description": description or "",
            "start": {"dateTime": start.isoformat() + "Z"},
            "end":   {"dateTime": end.isoformat() + "Z"},
            "conferenceData": {
                "createRequest": {
                    "requestId": secrets.token_hex(12),
                    "conferenceSolutionKey": {"type": "hangoutsMeet"},
                }
            },
            "reminders": {"useDefault": True},
            "guestsCanSeeOtherGuests": True,
            "guestsCanInviteOthers": False,
        }
        if attendees:
            body["attendees"] = attendees
        r = requests.post(
            "https://www.googleapis.com/calendar/v3/calendars/primary/events"
            "?conferenceDataVersion=1&sendUpdates=all",
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json"},
            json=body, timeout=15,
        )
        if r.status_code >= 400:
            return None, None, f"Calendar API: {r.status_code} {r.text[:200]}"
        ev = r.json()
        meet = ""
        for ep in (ev.get("conferenceData") or {}).get("entryPoints", []):
            if ep.get("entryPointType") == "video":
                meet = ep.get("uri", "")
                break
        return ev.get("id"), meet, None
    except Exception as e:
        return None, None, f"Calendar create error: {e}"


def delete_google_calendar_event(event_id):
    tok = _shared_access_token()
    if not tok or not event_id:
        return
    try:
        requests.delete(
            f"https://www.googleapis.com/calendar/v3/calendars/primary/events/{event_id}?sendUpdates=all",
            headers={"Authorization": f"Bearer {tok}"}, timeout=10,
        )
    except Exception as e:
        print(f"[crm-cal] delete failed: {e}")


# ── Pages: leads list ─────────────────────────────────────────────────────────
@crm_bp.route("/")
@login_required
def index():
    return redirect(url_for("crm.leads"))


@crm_bp.route("/leads")
@login_required
def leads():
    u = current_user()
    args = request.args
    # Filters
    source = args.get("source", "")
    industry = args.get("industry", "")
    country = args.get("country", "")
    status = args.get("status", "")
    dna = args.get("dna", "exclude")    # exclude | only | all
    view = args.get("view", "everyone") # everyone | mine
    q = (args.get("q") or "").strip()

    where = ["1=1"]
    params = []
    if source:
        where.append("l.source = ?"); params.append(source)
    if industry:
        where.append("l.industry = ?"); params.append(industry)
    if country:
        where.append("l.country = ?"); params.append(country)
    if status:
        where.append("l.status = ?"); params.append(status)
    if dna == "exclude":
        where.append("l.do_not_approach = 0")
    elif dna == "only":
        where.append("l.do_not_approach = 1")
    if view == "mine":
        where.append("(l.created_by_user_id = ? OR l.assigned_to_user_id = ?)")
        params += [u["id"], u["id"]]
    if q:
        where.append("""(
            l.company_name LIKE ? OR l.ig_handle LIKE ? OR l.full_name LIKE ?
            OR l.email LIKE ? OR l.phone LIKE ?
        )""")
        like = f"%{q}%"
        params += [like, like, like, like, like]

    con = db()
    rows = con.execute(f"""
        SELECT l.*,
               cu.first_name AS creator_first, cu.last_name AS creator_last, cu.email AS creator_email,
               au.first_name AS assignee_first, au.last_name AS assignee_last
        FROM leads l
        LEFT JOIN users cu ON cu.id = l.created_by_user_id
        LEFT JOIN users au ON au.id = l.assigned_to_user_id
        WHERE {' AND '.join(where)}
        ORDER BY l.created_at DESC
        LIMIT 500
    """, params).fetchall()
    total = con.execute(f"SELECT COUNT(*) FROM leads l WHERE {' AND '.join(where)}", params).fetchone()[0]
    users = con.execute("SELECT id, first_name, last_name, email, role FROM users WHERE is_active = 1 ORDER BY first_name").fetchall()
    con.close()

    return render_template(
        "crm/leads.html",
        u=u, leads=rows, total=total, users=users,
        f={"source": source, "industry": industry, "country": country,
           "status": status, "dna": dna, "view": view, "q": q},
        INDUSTRIES=INDUSTRIES, COUNTRIES=COUNTRIES, SOURCES=SOURCES, STATUSES=STATUSES,
    )


# ── API: create lead (with dupe check) ────────────────────────────────────────
@crm_bp.route("/api/leads", methods=["POST"])
@login_required
def api_create_lead():
    u = current_user()
    data = request.get_json(silent=True) or {}
    company = (data.get("company_name") or "").strip()
    if not company:
        return jsonify({"ok": False, "error": "Company name is required."}), 400

    payload = {
        "company_name": company,
        "ig_handle": norm_handle(data.get("ig_handle") or ""),
        "full_name": (data.get("full_name") or "").strip(),
        "job_title": (data.get("job_title") or "").strip(),
        "email": (data.get("email") or "").strip(),
        "phone_country_code": (data.get("phone_country_code") or "").strip(),
        "phone": (data.get("phone") or "").strip(),
        "country": (data.get("country") or "").strip(),
        "industry": (data.get("industry") or "").strip(),
        "source": (data.get("source") or "Outbound").strip(),
        "status": (data.get("status") or "New").strip(),
        "notes": (data.get("notes") or "").strip(),
    }
    override = bool(data.get("override_dupe"))

    con = db()
    if not override:
        hit = find_dupe(con, payload)
        if hit:
            dupe_row, (kt, kv) = hit
            creator = f"{dupe_row['creator_first'] or ''} {dupe_row['creator_last'] or ''}".strip() or "—"
            con.close()
            return jsonify({
                "ok": False,
                "duplicate": True,
                "match_type": kt,
                "match_value": kv,
                "existing_lead": {
                    "id": dupe_row["id"],
                    "company_name": dupe_row["company_name"],
                    "ig_handle": dupe_row["ig_handle"],
                    "status": dupe_row["status"],
                    "country": dupe_row["country"],
                    "created_by": creator,
                    "created_at": dupe_row["created_at"],
                },
            }), 409

    now = now_iso()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO leads (
            company_name, ig_handle, full_name, job_title, email,
            phone_country_code, phone, country, industry, source, status, notes,
            created_by_user_id, assigned_to_user_id, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        payload["company_name"], payload["ig_handle"] or None, payload["full_name"] or None,
        payload["job_title"] or None, payload["email"] or None,
        payload["phone_country_code"] or None, payload["phone"] or None,
        payload["country"] or None, payload["industry"] or None,
        payload["source"], payload["status"], payload["notes"] or None,
        u["id"], u["id"], now, now,
    ))
    lead_id = cur.lastrowid
    write_dedupe_keys(con, lead_id, payload)
    log_activity(con, lead_id, u["id"], "created", {"override_dupe": override})
    # If the creator typed an initial note in the Add Lead modal, log it as
    # a 'note' activity entry too so it shows in the activity timeline on
    # the lead detail page (otherwise it'd only live on the leads.notes
    # column and never get surfaced).
    if payload["notes"]:
        log_activity(con, lead_id, u["id"], "note", {"text": payload["notes"]})
    con.commit()
    con.close()

    notify_new_lead(lead_id)
    return jsonify({"ok": True, "id": lead_id})


# ── Pages: lead detail ────────────────────────────────────────────────────────
@crm_bp.route("/leads/<int:lead_id>")
@login_required
def lead_detail(lead_id):
    u = current_user()
    con = db()
    lead = con.execute("""
        SELECT l.*,
               cu.first_name AS creator_first, cu.last_name AS creator_last, cu.email AS creator_email,
               au.first_name AS assignee_first, au.last_name AS assignee_last
        FROM leads l
        LEFT JOIN users cu ON cu.id = l.created_by_user_id
        LEFT JOIN users au ON au.id = l.assigned_to_user_id
        WHERE l.id = ?
    """, (lead_id,)).fetchone()
    if not lead:
        con.close()
        abort(404)
    activity = con.execute("""
        SELECT a.*, u.first_name AS u_first, u.last_name AS u_last, u.email AS u_email
        FROM lead_activity a LEFT JOIN users u ON u.id = a.user_id
        WHERE a.lead_id = ?
        ORDER BY a.created_at DESC
    """, (lead_id,)).fetchall()
    meetings = con.execute("""
        SELECT m.*, u.first_name AS setter_first, u.last_name AS setter_last
        FROM meetings m LEFT JOIN users u ON u.id = m.setter_user_id
        WHERE m.lead_id = ?
        ORDER BY m.scheduled_at DESC
    """, (lead_id,)).fetchall()
    users = con.execute("SELECT id, first_name, last_name, email, role FROM users WHERE is_active = 1 ORDER BY first_name").fetchall()
    con.close()
    return render_template(
        "crm/lead_detail.html",
        u=u, lead=lead, activity=activity, meetings=meetings, users=users,
        INDUSTRIES=INDUSTRIES, COUNTRIES=COUNTRIES, SOURCES=SOURCES, STATUSES=STATUSES,
    )


@crm_bp.route("/api/leads/<int:lead_id>", methods=["PATCH"])
@admin_required   # SECURITY: only admins can edit lead fields. Setters can
                  # still ADD new leads and ADD notes, but they cannot mutate
                  # existing lead data — prevents accidental or malicious
                  # sabotage in a multi-setter team. Add-note has its own
                  # endpoint that remains setter-accessible.
def api_update_lead(lead_id):
    u = current_user()
    data = request.get_json(silent=True) or {}

    con = db()
    lead = con.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        con.close()
        abort(404)

    editable = ["company_name", "ig_handle", "full_name", "job_title", "email",
                "phone_country_code", "phone", "country", "industry",
                "source", "status", "notes",
                "do_not_approach", "do_not_approach_reason",
                "assigned_to_user_id"]
    updates = {}
    for k in editable:
        if k in data:
            v = data[k]
            if k == "do_not_approach":
                v = 1 if v else 0
            elif k == "ig_handle" and isinstance(v, str):
                v = norm_handle(v) or None
            elif isinstance(v, str):
                v = v.strip() or None
            updates[k] = v

    if not updates:
        con.close()
        return jsonify({"ok": True, "noop": True})

    # Status change -> activity row
    if "status" in updates and updates["status"] != lead["status"]:
        log_activity(con, lead_id, u["id"], "status_change",
                     {"from": lead["status"], "to": updates["status"]})
    if "do_not_approach" in updates and updates["do_not_approach"] != lead["do_not_approach"]:
        log_activity(con, lead_id, u["id"],
                     "dna_on" if updates["do_not_approach"] else "dna_off",
                     {"reason": updates.get("do_not_approach_reason")})
    if "assigned_to_user_id" in updates and updates["assigned_to_user_id"] != lead["assigned_to_user_id"]:
        log_activity(con, lead_id, u["id"], "assigned",
                     {"user_id": updates["assigned_to_user_id"]})

    set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
    params = list(updates.values()) + [now_iso(), lead_id]
    con.execute(f"UPDATE leads SET {set_clause}, updated_at = ? WHERE id = ?", params)

    # If any dedupe-relevant field changed, rewrite keys.
    if any(k in updates for k in ("email", "ig_handle", "phone", "phone_country_code")):
        new_payload = dict(lead)
        new_payload.update(updates)
        write_dedupe_keys(con, lead_id, new_payload)

    con.commit()
    con.close()
    return jsonify({"ok": True})


@crm_bp.route("/api/leads/<int:lead_id>/notes", methods=["POST"])
@login_required
def api_add_note(lead_id):
    u = current_user()
    text = ((request.get_json(silent=True) or {}).get("text") or "").strip()
    if not text:
        return jsonify({"ok": False, "error": "Empty note"}), 400
    con = db()
    if not con.execute("SELECT 1 FROM leads WHERE id = ?", (lead_id,)).fetchone():
        con.close(); abort(404)
    log_activity(con, lead_id, u["id"], "note", {"text": text})
    con.commit()
    con.close()
    return jsonify({"ok": True})


@crm_bp.route("/api/leads/<int:lead_id>", methods=["DELETE"])
@admin_required
def api_delete_lead(lead_id):
    con = db()
    con.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Calendar ──────────────────────────────────────────────────────────────────
@crm_bp.route("/calendar")
@login_required
def calendar():
    u = current_user()
    # Default to current month.
    today = datetime.date.today()
    try:
        year = int(request.args.get("y", today.year))
        month = int(request.args.get("m", today.month))
    except Exception:
        year, month = today.year, today.month

    # Range for the month grid (we render Sun-anchored 6-week grid like trifid).
    first = datetime.date(year, month, 1)
    # last day of month
    if month == 12:
        next_first = datetime.date(year + 1, 1, 1)
    else:
        next_first = datetime.date(year, month + 1, 1)
    last = next_first - datetime.timedelta(days=1)

    # Stats for the header
    con = db()
    month_range = (first.isoformat(), (next_first).isoformat())
    total_month = con.execute(
        "SELECT COUNT(*) FROM meetings WHERE scheduled_at >= ? AND scheduled_at < ?",
        month_range).fetchone()[0]
    completed_month = con.execute(
        "SELECT COUNT(*) FROM meetings WHERE status = 'completed' AND scheduled_at >= ? AND scheduled_at < ?",
        month_range).fetchone()[0]
    my_month = con.execute(
        "SELECT COUNT(*) FROM meetings WHERE setter_user_id = ? AND scheduled_at >= ? AND scheduled_at < ?",
        (u["id"], *month_range)).fetchone()[0]

    # Pull all meetings overlapping the visible 6-week window.
    grid_start = first - datetime.timedelta(days=first.weekday() + 1 if first.weekday() != 6 else 0)
    # weekday(): Mon=0..Sun=6; we want Sun-anchored, so back up to most recent Sunday.
    if first.weekday() == 6:
        grid_start = first
    else:
        grid_start = first - datetime.timedelta(days=first.weekday() + 1)
    grid_end = grid_start + datetime.timedelta(days=42)
    meetings = con.execute("""
        SELECT m.id, m.lead_id, m.scheduled_at, m.duration_min, m.status, m.setter_user_id,
               m.gmeet_link, m.notes AS meeting_notes, m.timezone AS meeting_tz,
               l.company_name, l.ig_handle, l.full_name AS lead_full_name,
               l.email AS lead_email, l.phone AS lead_phone,
               l.country AS lead_country, l.industry, l.status AS lead_status,
               l.notes AS lead_notes,
               u.first_name AS setter_first, u.last_name AS setter_last,
               u.country AS setter_country, u.email AS setter_email
        FROM meetings m
        JOIN leads l ON l.id = m.lead_id
        JOIN users u ON u.id = m.setter_user_id
        WHERE m.scheduled_at >= ? AND m.scheduled_at < ?
        ORDER BY m.scheduled_at ASC
    """, (grid_start.isoformat(), grid_end.isoformat())).fetchall()

    # Recent leads for the "+ Book Meeting" lead-selector on this page.
    recent_leads = con.execute("""
        SELECT id, company_name, ig_handle, country, industry, status, email
        FROM leads
        WHERE do_not_approach = 0
        ORDER BY created_at DESC
        LIMIT 250
    """).fetchall()
    con.close()

    # Group meetings by date string
    by_day = {}
    for m in meetings:
        try:
            d = datetime.datetime.fromisoformat(m["scheduled_at"][:19]).date().isoformat()
        except Exception:
            continue
        by_day.setdefault(d, []).append(m)

    # Build 6-week grid
    grid = []
    cur_day = grid_start
    for _ in range(6):
        week = []
        for _ in range(7):
            week.append({
                "date": cur_day,
                "in_month": (cur_day.month == month),
                "iso": cur_day.isoformat(),
                "meetings": by_day.get(cur_day.isoformat(), []),
            })
            cur_day += datetime.timedelta(days=1)
        grid.append(week)

    # prev/next month
    prev_y, prev_m = (year - 1, 12) if month == 1 else (year, month - 1)
    next_y, next_m = (year + 1, 1) if month == 12 else (year, month + 1)
    month_label = first.strftime("%B %Y")

    # Embed meeting + lead data for client-side popups.
    meetings_json = json.dumps({m["id"]: dict(m) for m in meetings}, default=str)
    leads_json = json.dumps([dict(l) for l in recent_leads], default=str)

    return render_template(
        "crm/calendar.html",
        u=u, grid=grid, month_label=month_label,
        year=year, month=month,
        prev_y=prev_y, prev_m=prev_m, next_y=next_y, next_m=next_m,
        meetings_json=meetings_json,
        leads_json=leads_json,
        stats={"total_month": total_month, "completed_month": completed_month,
               "my_month": my_month,
               "rate": int((completed_month * 100 / total_month)) if total_month else 0},
    )


@crm_bp.route("/api/meetings", methods=["POST"])
@login_required
def api_create_meeting():
    u = current_user()
    data = request.get_json(silent=True) or {}
    lead_id = data.get("lead_id")
    scheduled_at = (data.get("scheduled_at") or "").strip()
    duration_min = int(data.get("duration_min") or 30)
    notes = (data.get("notes") or "").strip()
    timezone = (data.get("timezone") or "").strip()
    if not lead_id or not scheduled_at:
        return jsonify({"ok": False, "error": "lead_id and scheduled_at required"}), 400

    con = db()
    lead = con.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    if not lead:
        con.close()
        return jsonify({"ok": False, "error": "Lead not found"}), 404

    setter_row = con.execute("SELECT * FROM users WHERE id = ?", (u["id"],)).fetchone()
    summary = f"MK7 — {lead['company_name']}"
    desc_parts = [
        f"Booked by {(u['first_name'] or '').strip()} {(u['last_name'] or '').strip()} ({u['email']}) via MK7 CRM.",
        f"Lead: {lead['company_name']}",
    ]
    if lead["ig_handle"]:
        desc_parts.append(f"IG: @{lead['ig_handle']}")
    if notes:
        desc_parts.append(f"Notes: {notes}")
    description = "\n".join(desc_parts)
    event_id, meet_link, err = create_google_calendar_event(
        setter_row, summary=summary, description=description,
        start_iso_utc=scheduled_at, duration_min=duration_min,
        lead_email=lead["email"],
    )

    cur = con.cursor()
    cur.execute("""
        INSERT INTO meetings (
            lead_id, setter_user_id, title, scheduled_at, duration_min,
            timezone, gmeet_link, google_event_id, status, notes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'scheduled', ?, ?, ?)
    """, (lead_id, u["id"], summary, scheduled_at, duration_min,
          timezone or None, meet_link or None, event_id or None,
          notes or None, now_iso(), now_iso()))
    meeting_id = cur.lastrowid

    # Auto-advance status
    if lead["status"] in ("New", "Contacted", "Qualified"):
        con.execute("UPDATE leads SET status = 'Meeting Scheduled', updated_at = ? WHERE id = ?",
                    (now_iso(), lead_id))
        log_activity(con, lead_id, u["id"], "status_change",
                     {"from": lead["status"], "to": "Meeting Scheduled", "via": "meeting"})

    log_activity(con, lead_id, u["id"], "meeting_booked",
                 {"meeting_id": meeting_id, "scheduled_at": scheduled_at,
                  "meet_link": meet_link, "gcal_error": err})
    con.commit()
    con.close()

    notify_meeting_booked(meeting_id)

    return jsonify({"ok": True, "id": meeting_id, "gmeet_link": meet_link,
                    "google_event_id": event_id, "gcal_error": err})


@crm_bp.route("/api/meetings/<int:meeting_id>", methods=["PATCH"])
@login_required
def api_update_meeting(meeting_id):
    u = current_user()
    data = request.get_json(silent=True) or {}
    con = db()
    m = con.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    if not m:
        con.close(); abort(404)
    # Only the booking setter or an admin can update meeting status.
    if u["role"] != "admin" and m["setter_user_id"] != u["id"]:
        con.close()
        return jsonify({"ok": False, "error": "Only the booking setter or an admin can update this meeting."}), 403
    new_status = data.get("status")
    if new_status and new_status in ("scheduled", "completed", "no_show", "canceled"):
        con.execute("UPDATE meetings SET status = ?, updated_at = ? WHERE id = ?",
                    (new_status, now_iso(), meeting_id))
        log_activity(con, m["lead_id"], u["id"], f"meeting_{new_status}",
                     {"meeting_id": meeting_id})
        # cascade DNA-like signals back to lead
        if new_status == "completed":
            con.execute("UPDATE leads SET status = 'Negotiation', updated_at = ? WHERE id = ? AND status = 'Meeting Scheduled'",
                        (now_iso(), m["lead_id"]))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@crm_bp.route("/api/meetings/<int:meeting_id>", methods=["DELETE"])
@admin_required   # SECURITY: cancel/delete a meeting is admin-only. Setters
                  # can still mark a meeting completed or no-show via PATCH;
                  # only an admin can wipe the record (and its GCal event).
def api_delete_meeting(meeting_id):
    u = current_user()
    con = db()
    m = con.execute("SELECT * FROM meetings WHERE id = ?", (meeting_id,)).fetchone()
    if not m:
        con.close(); abort(404)
    delete_google_calendar_event(m["google_event_id"])
    con.execute("DELETE FROM meetings WHERE id = ?", (meeting_id,))
    log_activity(con, m["lead_id"], u["id"], "meeting_canceled", {"meeting_id": meeting_id})
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Admin: Meeting Setters dashboard + user management ────────────────────────
@crm_bp.route("/admin")
@admin_required
def admin():
    u = current_user()
    country_filter = (request.args.get("country") or "").strip()
    setter_filter = request.args.get("setter")  # user id or empty

    con = db()
    where = ["u.is_active = 1", "u.role IN ('setter', 'sales')"]
    params = []
    if country_filter:
        where.append("u.country = ?"); params.append(country_filter)
    if setter_filter:
        where.append("u.id = ?"); params.append(int(setter_filter))

    rows = con.execute(f"""
        SELECT u.id, u.first_name, u.last_name, u.email, u.country, u.position, u.role,
               (SELECT COUNT(*) FROM leads l WHERE l.created_by_user_id = u.id) AS total_leads,
               (SELECT COUNT(*) FROM leads l WHERE l.created_by_user_id = u.id AND l.status NOT IN ('New')) AS contacted,
               (SELECT COUNT(*) FROM meetings m WHERE m.setter_user_id = u.id) AS meetings_booked,
               (SELECT COUNT(*) FROM meetings m WHERE m.setter_user_id = u.id AND m.status = 'completed') AS meetings_attended,
               (SELECT COUNT(*) FROM leads l WHERE l.created_by_user_id = u.id AND l.status = 'Closed') AS converted
        FROM users u
        WHERE {' AND '.join(where)}
        ORDER BY total_leads DESC, u.first_name
    """, params).fetchall()
    all_users = con.execute("SELECT * FROM users ORDER BY first_name").fetchall()
    con.close()
    total = sum(r["total_leads"] for r in rows)
    contacted = sum(r["contacted"] for r in rows)
    booked = sum(r["meetings_booked"] for r in rows)
    attended = sum(r["meetings_attended"] for r in rows)
    converted = sum(r["converted"] for r in rows)
    cal = shared_calendar_status()
    return render_template(
        "crm/admin.html",
        u=u, setters=rows, all_users=all_users,
        country_filter=country_filter, setter_filter=setter_filter,
        COUNTRIES=COUNTRIES, ROLES=ROLES, POSITIONS=POSITIONS,
        calendar=cal,
        stats={"total": total, "contacted": contacted, "booked": booked,
               "attended": attended, "converted": converted,
               "rate": int((converted * 100 / total)) if total else 0},
    )


def _gen_temp_password(n=12):
    """Generate a memorable-ish temp password: 3 short word-chunks + 4 digits.
    Avoids ambiguous chars (0/O, 1/l/I)."""
    alphabet = string.ascii_lowercase.replace("l", "")
    chunks = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(2)]
    digits = "".join(secrets.choice("23456789") for _ in range(4))
    return f"{chunks[0]}-{chunks[1]}-{digits}"


@crm_bp.route("/api/users", methods=["POST"])
@admin_required
def api_create_user():
    """Admin creates a setter/sales/admin user directly with a temp password.
    Returns the password ONCE — admin shares it however they want; we only
    store the hash. The user must change it on first login.

    Position is the only user-facing field; role is derived server-side from
    POSITION_TO_ROLE so the UI doesn't show a redundant 'Role' dropdown."""
    data = request.get_json(silent=True) or {}
    email = norm_email(data.get("email") or "")
    first = (data.get("first_name") or "").strip()
    last = (data.get("last_name") or "").strip()
    position = (data.get("position") or "Meeting Setter").strip()
    # Fall back to an explicit 'role' if some old client still sends it,
    # otherwise derive from position.
    role = (data.get("role") or POSITION_TO_ROLE.get(position, "setter")).strip().lower()
    country = (data.get("country") or "").strip() or None
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"}), 400
    if role not in ROLES:
        return jsonify({"ok": False, "error": "Invalid role"}), 400
    con = db()
    if con.execute("SELECT 1 FROM users WHERE email = ?", (email,)).fetchone():
        con.close()
        return jsonify({"ok": False, "error": "A user with that email already exists"}), 409
    temp_pw = _gen_temp_password()
    con.execute("""
        INSERT INTO users (email, first_name, last_name, role, position, country,
            password_hash, must_change_password, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, 1, 1, ?)
    """, (email, first or None, last or None, role, position, country,
          generate_password_hash(temp_pw), now_iso()))
    con.commit()
    con.close()

    # Fire the welcome email with login credentials. Best-effort — if Resend
    # is unconfigured we just log it; the admin can still copy the temp
    # password from the modal that just rendered.
    inviter = current_user()
    try:
        send_invite_email(email, first, temp_pw,
                          invited_by_first=(inviter["first_name"] if inviter else ""))
    except Exception as e:
        print(f"[crm-invite] send failed for {email}: {e}")

    return jsonify({"ok": True, "email": email, "temp_password": temp_pw,
                    "email_sent": bool(RESEND_API_KEY)})


@crm_bp.route("/api/users/<int:user_id>", methods=["DELETE"])
@admin_required
def api_delete_user(user_id):
    """Hard-delete a user. Preserves the leads they created (their FK becomes
    NULL, showing as "—" for added-by) but drops their meetings outright
    (and the corresponding GCal events, if any). Frees the email for re-add."""
    me = current_user()
    if me["id"] == user_id:
        return jsonify({"ok": False, "error": "You can't delete your own account."}), 400
    con = db()
    target = con.execute("SELECT email FROM users WHERE id = ?", (user_id,)).fetchone()
    if not target:
        con.close(); abort(404)
    # Find this user's meetings to remove their GCal events too.
    mtgs = con.execute(
        "SELECT id, google_event_id FROM meetings WHERE setter_user_id = ?",
        (user_id,)
    ).fetchall()
    for m in mtgs:
        if m["google_event_id"]:
            delete_google_calendar_event(m["google_event_id"])
    # Orphan the leads + activity rows (preserves data, just removes FK).
    con.execute("UPDATE leads SET created_by_user_id = NULL WHERE created_by_user_id = ?", (user_id,))
    con.execute("UPDATE leads SET assigned_to_user_id = NULL WHERE assigned_to_user_id = ?", (user_id,))
    con.execute("UPDATE lead_activity SET user_id = NULL WHERE user_id = ?", (user_id,))
    # Drop the user's meetings + the user row.
    con.execute("DELETE FROM meetings WHERE setter_user_id = ?", (user_id,))
    con.execute("DELETE FROM users WHERE id = ?", (user_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True, "email": target["email"]})


@crm_bp.route("/api/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def api_reset_password(user_id):
    """Admin force-resets a user's password. Returns the new temp password once
    AND emails it to the user so the admin doesn't need to manually relay it."""
    me = current_user()
    con = db()
    row = con.execute("SELECT email, first_name FROM users WHERE id = ?", (user_id,)).fetchone()
    if not row:
        con.close(); abort(404)
    temp_pw = _gen_temp_password()
    con.execute("UPDATE users SET password_hash = ?, must_change_password = 1 WHERE id = ?",
                (generate_password_hash(temp_pw), user_id))
    con.commit()
    con.close()
    try:
        send_password_reset_email(row["email"], row["first_name"], temp_pw,
                                  reset_by_first=(me["first_name"] if me else ""))
    except Exception as e:
        print(f"[crm-reset] email failed for {row['email']}: {e}")
    return jsonify({"ok": True, "email": row["email"], "temp_password": temp_pw,
                    "email_sent": bool(RESEND_API_KEY)})


@crm_bp.route("/api/users/<int:user_id>", methods=["PATCH"])
@admin_required
def api_update_user(user_id):
    data = request.get_json(silent=True) or {}
    fields = {}
    for k in ("first_name", "last_name", "country", "position", "role", "is_active", "phone", "timezone"):
        if k in data:
            v = data[k]
            if k == "is_active":
                v = 1 if v else 0
            elif k == "role" and v not in ROLES:
                continue
            elif isinstance(v, str):
                v = v.strip() or None
            fields[k] = v
    # If position changed but role wasn't explicitly set, derive role.
    if "position" in fields and "role" not in fields:
        derived = POSITION_TO_ROLE.get(fields["position"] or "")
        if derived:
            fields["role"] = derived
    if not fields:
        return jsonify({"ok": True, "noop": True})
    con = db()
    sets = ", ".join([f"{k} = ?" for k in fields])
    con.execute(f"UPDATE users SET {sets} WHERE id = ?", list(fields.values()) + [user_id])
    con.commit()
    con.close()
    return jsonify({"ok": True})


@crm_bp.route("/api/me/password", methods=["POST"])
@login_required
def api_change_my_password():
    u = current_user()
    data = request.get_json(silent=True) or {}
    cur_pw = data.get("current_password") or ""
    new_pw = data.get("new_password") or ""
    # Allow setting from blank when must_change_password is set (just-issued
    # temp password they already used to get in here).
    if u["must_change_password"] and not cur_pw:
        cur_pw = data.get("temp_password") or ""
    if not check_password_hash(u["password_hash"], cur_pw):
        return jsonify({"ok": False, "error": "Current password didn't match."}), 403
    if len(new_pw) < 10:
        return jsonify({"ok": False, "error": "New password must be at least 10 characters."}), 400
    con = db()
    con.execute("UPDATE users SET password_hash = ?, must_change_password = 0 WHERE id = ?",
                (generate_password_hash(new_pw), u["id"]))
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Settings ──────────────────────────────────────────────────────────────────
@crm_bp.route("/settings")
@login_required
def settings_page():
    u = current_user()
    return render_template("crm/settings.html", u=u, COUNTRIES=COUNTRIES, POSITIONS=POSITIONS)


@crm_bp.route("/api/me", methods=["PATCH"])
@login_required
def api_update_me():
    u = current_user()
    data = request.get_json(silent=True) or {}
    fields = {}
    # Setters can only edit a few of their own fields.
    allowed_self = ("first_name", "last_name", "phone", "timezone")
    for k in allowed_self:
        if k in data:
            v = data[k]
            if isinstance(v, str):
                v = v.strip() or None
            fields[k] = v
    if not fields:
        return jsonify({"ok": True, "noop": True})
    con = db()
    sets = ", ".join([f"{k} = ?" for k in fields])
    con.execute(f"UPDATE users SET {sets} WHERE id = ?", list(fields.values()) + [u["id"]])
    con.commit()
    con.close()
    return jsonify({"ok": True})


# Initialize DB on import
init_db()
