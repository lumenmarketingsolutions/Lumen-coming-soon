"""
MK7 Outreach Agent — cold-email blast + tracking, admin-only.

Mounted at /crm/outreach as a Flask Blueprint. Shares the CRM's setters.db
(same DB connection helpers from crm.py) so we don't fork storage. Sending is
done through Resend on a dedicated cold-outreach subdomain (go.lumenmarketing.co)
so spam complaints don't degrade the apex transactional reputation.

Architecture:
  - Audiences   = CSV-uploaded recipient lists (or "derived" lists like opened/clicked).
  - Templates   = subject + HTML body with {{first_name}} / {{last_name}} / {{company}} merge tags.
  - Campaigns   = (audience × template) blast, paced over N hours.
  - Sends       = one row per (campaign, recipient). Status: queued → sent → delivered/opened/clicked/bounced/complained.
  - Suppressions = global email blocklist. Auto-populated on unsubscribe / bounce / complaint.
  - A single background worker thread drains the queue at a rate-limited pace and
    talks to Resend. Resend webhook posts back per-send events.

Compliance:
  - Every email injects a List-Unsubscribe header + visible unsub link.
  - Hard bounces and spam complaints auto-suppress globally.
  - Mailing address is rendered in the footer from OUTREACH_MAILING_ADDRESS.
"""

from flask import (
    Blueprint, render_template, request, jsonify, abort, make_response,
)
import os, datetime, json, re, secrets, threading, time, hmac, hashlib, base64, csv, io, html as html_lib, random
import sqlite3  # for catching OperationalError in dashboard when scraper tables aren't initialized yet
import requests

# Reuse the CRM's DB + auth helpers — outreach lives in the same SQLite file.
from crm import (
    db, current_user, login_required, admin_required,
    now_iso, RESEND_API_KEY,
)

outreach_bp = Blueprint("outreach", __name__, url_prefix="/crm/outreach")


# ── Config ────────────────────────────────────────────────────────────────────
# All config is env-driven so we can swap sending domain without code changes.
FROM_DOMAIN   = os.environ.get("OUTREACH_FROM_DOMAIN", "go.lumenmarketing.co")
# Generic identity for cold outreach so recipients don't see a personal name.
# Per-template overrides exist if a campaign wants a different sender persona.
FROM_LOCAL    = os.environ.get("OUTREACH_FROM_LOCAL", "info")
FROM_NAME     = os.environ.get("OUTREACH_FROM_NAME", "Lumen Marketing")
# info@ is a generic Workspace address — Kendall + Mary both subscribe (or forward)
# so neither name leaks to cold recipients but replies still reach both inboxes.
DEFAULT_REPLY_TO = os.environ.get("OUTREACH_REPLY_TO", "info@lumenmarketing.co")
PUBLIC_BASE_URL  = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co").rstrip("/")
# Required by CAN-SPAM. Left blank by default so we never ship a fake address —
# the dashboard surfaces a warning banner when it's unset (see dashboard template).
MAILING_ADDRESS  = os.environ.get("OUTREACH_MAILING_ADDRESS", "").strip()
# Resend Pro is 10/sec; we stay at 8 to leave headroom for retries/webhooks.
RESEND_RATE_PER_SEC = float(os.environ.get("OUTREACH_RATE_PER_SEC", "8"))
RESEND_WEBHOOK_SECRET = os.environ.get("RESEND_WEBHOOK_SECRET", "")


def from_email():
    """Default From: address; campaigns can override via template fields."""
    return f"{FROM_LOCAL}@{FROM_DOMAIN}"


# ── DB schema ─────────────────────────────────────────────────────────────────
def init_outreach_db():
    """Idempotent. Called on import."""
    con = db()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS outreach_audiences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        note TEXT,
        source TEXT NOT NULL DEFAULT 'csv',  -- csv | derived
        parent_audience_id INTEGER REFERENCES outreach_audiences(id) ON DELETE SET NULL,
        parent_campaign_id INTEGER,           -- for derived audiences
        derivation_type TEXT,                 -- opened | not_opened | clicked | bounced
        member_count INTEGER NOT NULL DEFAULT 0,
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outreach_audience_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audience_id INTEGER NOT NULL REFERENCES outreach_audiences(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        extra_json TEXT,            -- any CSV columns we didn't map
        created_at TEXT NOT NULL,
        UNIQUE(audience_id, email)  -- in-audience dedupe; across audiences allowed
    );
    CREATE INDEX IF NOT EXISTS idx_oam_audience ON outreach_audience_members(audience_id);
    CREATE INDEX IF NOT EXISTS idx_oam_email    ON outreach_audience_members(email);

    CREATE TABLE IF NOT EXISTS outreach_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        subject TEXT NOT NULL,
        body_html TEXT NOT NULL,
        body_text TEXT,             -- optional plaintext; derived from html on send if blank
        from_name TEXT,             -- overrides global default
        from_local TEXT,            -- overrides local part (still @go.lumenmarketing.co)
        reply_to TEXT,              -- overrides global reply-to
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outreach_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        template_id INTEGER NOT NULL REFERENCES outreach_templates(id),
        audience_id INTEGER NOT NULL REFERENCES outreach_audiences(id),
        status TEXT NOT NULL DEFAULT 'draft',
            -- draft | scheduled | sending | paused | completed | canceled
        send_window_hours REAL NOT NULL DEFAULT 4.0,
        scheduled_start TEXT,        -- ISO UTC; null = send immediately on launch
        started_at TEXT,
        completed_at TEXT,
        total_queued INTEGER NOT NULL DEFAULT 0,
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outreach_sends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL REFERENCES outreach_campaigns(id) ON DELETE CASCADE,
        member_id INTEGER REFERENCES outreach_audience_members(id) ON DELETE SET NULL,
        email TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        extra_json TEXT,
        status TEXT NOT NULL DEFAULT 'queued',
            -- queued | sending | sent | failed | suppressed | canceled
        scheduled_at TEXT NOT NULL,
        sent_at TEXT,
        delivered_at TEXT,
        opened_at TEXT,
        open_count INTEGER NOT NULL DEFAULT 0,
        first_click_at TEXT,
        click_count INTEGER NOT NULL DEFAULT 0,
        bounced_at TEXT,
        complained_at TEXT,
        unsubscribed_at TEXT,
        resend_message_id TEXT,
        error TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0,
        unsub_token TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_os_campaign     ON outreach_sends(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_os_queue        ON outreach_sends(status, scheduled_at);
    CREATE INDEX IF NOT EXISTS idx_os_message      ON outreach_sends(resend_message_id);
    CREATE INDEX IF NOT EXISTS idx_os_unsub        ON outreach_sends(unsub_token);
    CREATE INDEX IF NOT EXISTS idx_os_email        ON outreach_sends(email);

    CREATE TABLE IF NOT EXISTS outreach_suppressions (
        email TEXT PRIMARY KEY,
        reason TEXT NOT NULL,        -- unsubscribe | bounce | complaint | manual
        source_campaign_id INTEGER,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outreach_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        resend_message_id TEXT,
        event_type TEXT NOT NULL,
        payload_json TEXT,
        received_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_oe_message ON outreach_events(resend_message_id);
    """)
    con.commit()
    con.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)


def norm_email(s):
    return (s or "").strip().lower()


def valid_email(s):
    return bool(s and EMAIL_RE.match(s))


def unsub_secret():
    """Lazily generated and persisted in app_settings so URL tokens stay
    valid across restarts. Never expose — it's the HMAC key."""
    from crm import get_setting, set_setting
    s = get_setting("outreach_unsub_secret")
    if not s:
        s = secrets.token_urlsafe(48)
        set_setting("outreach_unsub_secret", s)
    return s


def make_unsub_token(send_id, email):
    """HMAC over (send_id:email) so the token is forge-proof and tied to a
    specific recipient. The encoded send_id lets us look up the row without
    storing the email in the URL."""
    msg = f"{send_id}:{norm_email(email)}".encode()
    sig = hmac.new(unsub_secret().encode(), msg, hashlib.sha256).digest()
    payload = base64.urlsafe_b64encode(f"{send_id}:{base64.urlsafe_b64encode(sig[:18]).decode().rstrip('=')}".encode()).decode().rstrip("=")
    return payload


def parse_unsub_token(token):
    """Returns (send_id, expected_sig_prefix) or (None, None) on garbage."""
    try:
        padded = token + "=" * (-len(token) % 4)
        raw = base64.urlsafe_b64decode(padded).decode()
        send_id_str, sig_b64 = raw.split(":", 1)
        return int(send_id_str), sig_b64
    except Exception:
        return None, None


def verify_unsub_token(token):
    """Returns the send row if token is valid, else None."""
    send_id, sig_b64 = parse_unsub_token(token)
    if not send_id:
        return None
    con = db()
    row = con.execute("SELECT * FROM outreach_sends WHERE id = ?", (send_id,)).fetchone()
    con.close()
    if not row:
        return None
    expected = make_unsub_token(send_id, row["email"])
    if not hmac.compare_digest(expected, token):
        return None
    return row


def render_template_body(body, ctx):
    """Replace merge tags with values from the recipient context. Sensible
    fallbacks when fields are blank so emails never look like 'Hi ,'."""
    if not body:
        return ""
    repl = {
        "{{first_name}}": (ctx.get("first_name") or "there").strip(),
        "{{last_name}}":  (ctx.get("last_name") or "").strip(),
        "{{company}}":    (ctx.get("company") or "your business").strip(),
        "{{email}}":      (ctx.get("email") or "").strip(),
    }
    out = body
    for k, v in repl.items():
        out = out.replace(k, v)
    return out


def html_to_plaintext(s):
    """Cheap fallback for body_text when the template only has HTML.
    Not perfect; good enough for inbox-provider sniffing."""
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.I)
    s = re.sub(r"</p>", "\n\n", s, flags=re.I)
    s = re.sub(r"<[^>]+>", "", s)
    s = html_lib.unescape(s)
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def inject_unsub_html(body_html, unsub_url):
    """Append a small, compliant unsub footer if the template author didn't
    already include {{unsubscribe_url}}. We never let mail go out without one.
    If MAILING_ADDRESS is unset, the footer skips that line — dashboard banner
    surfaces the missing config so admin sees it."""
    if "{{unsubscribe_url}}" in body_html:
        return body_html.replace("{{unsubscribe_url}}", unsub_url)
    addr_line = (f'{html_lib.escape(MAILING_ADDRESS)}<br>'
                 if MAILING_ADDRESS else '')
    footer = (
        '<div style="margin-top:36px;padding-top:18px;border-top:1px solid #eee;'
        'font-family:Inter,system-ui,sans-serif;font-size:11px;line-height:1.6;color:#888">'
        f'{addr_line}'
        'You\'re receiving this because we believed it was relevant to your business. '
        f'<a href="{unsub_url}" style="color:#888;text-decoration:underline">'
        'Unsubscribe</a> to stop hearing from us.'
        '</div>'
    )
    return body_html + footer


def inject_unsub_text(body_text, unsub_url):
    if "{{unsubscribe_url}}" in body_text:
        return body_text.replace("{{unsubscribe_url}}", unsub_url)
    addr = f"{MAILING_ADDRESS}\n" if MAILING_ADDRESS else ""
    return (body_text or "") + f"\n\n—\n{addr}Unsubscribe: {unsub_url}\n"


def is_suppressed(con, email):
    e = norm_email(email)
    if not e:
        return True
    return bool(con.execute(
        "SELECT 1 FROM outreach_suppressions WHERE email = ?", (e,)
    ).fetchone())


def add_suppression(con, email, reason, source_campaign_id=None):
    e = norm_email(email)
    if not e:
        return
    con.execute(
        "INSERT OR IGNORE INTO outreach_suppressions (email, reason, source_campaign_id, created_at) "
        "VALUES (?, ?, ?, ?)",
        (e, reason, source_campaign_id, now_iso()),
    )


def refresh_audience_count(con, audience_id):
    n = con.execute(
        "SELECT COUNT(*) FROM outreach_audience_members WHERE audience_id = ?",
        (audience_id,)
    ).fetchone()[0]
    con.execute("UPDATE outreach_audiences SET member_count = ? WHERE id = ?",
                (n, audience_id))
    return n


# ── Stat aggregation ──────────────────────────────────────────────────────────
def campaign_stats(con, campaign_id):
    """Computed on read — at tens of thousands of rows this is plenty fast and
    means webhook handlers don't need transactional counter updates."""
    row = con.execute("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status='sent' OR sent_at IS NOT NULL THEN 1 ELSE 0 END) AS sent,
            SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered,
            SUM(CASE WHEN opened_at IS NOT NULL THEN 1 ELSE 0 END) AS opened,
            SUM(CASE WHEN first_click_at IS NOT NULL THEN 1 ELSE 0 END) AS clicked,
            SUM(CASE WHEN bounced_at IS NOT NULL THEN 1 ELSE 0 END) AS bounced,
            SUM(CASE WHEN complained_at IS NOT NULL THEN 1 ELSE 0 END) AS complained,
            SUM(CASE WHEN unsubscribed_at IS NOT NULL THEN 1 ELSE 0 END) AS unsubscribed,
            SUM(CASE WHEN status='suppressed' THEN 1 ELSE 0 END) AS suppressed,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status='queued' THEN 1 ELSE 0 END) AS queued
        FROM outreach_sends WHERE campaign_id = ?
    """, (campaign_id,)).fetchone()
    d = {k: (row[k] or 0) for k in row.keys()}
    d["delivered_pct"] = round(100 * d["delivered"] / d["sent"], 1) if d["sent"] else 0.0
    d["open_pct"]      = round(100 * d["opened"]    / d["sent"], 1) if d["sent"] else 0.0
    d["click_pct"]     = round(100 * d["clicked"]   / d["sent"], 1) if d["sent"] else 0.0
    d["bounce_pct"]    = round(100 * d["bounced"]   / d["sent"], 1) if d["sent"] else 0.0
    return d


def _lifetime(con):
    row = con.execute("""
        SELECT
            SUM(CASE WHEN sent_at IS NOT NULL THEN 1 ELSE 0 END) AS sent,
            SUM(CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END) AS delivered,
            SUM(CASE WHEN opened_at IS NOT NULL THEN 1 ELSE 0 END) AS opened,
            SUM(CASE WHEN first_click_at IS NOT NULL THEN 1 ELSE 0 END) AS clicked,
            SUM(CASE WHEN bounced_at IS NOT NULL THEN 1 ELSE 0 END) AS bounced,
            SUM(CASE WHEN unsubscribed_at IS NOT NULL THEN 1 ELSE 0 END) AS unsubscribed
        FROM outreach_sends
    """).fetchone()
    d = {k: (row[k] or 0) for k in row.keys()}
    d["delivered_pct"] = round(100 * d["delivered"] / d["sent"], 1) if d["sent"] else 0.0
    d["open_pct"]      = round(100 * d["opened"]    / d["sent"], 1) if d["sent"] else 0.0
    d["click_pct"]     = round(100 * d["clicked"]   / d["sent"], 1) if d["sent"] else 0.0
    return d


# ── Pages: dashboard ──────────────────────────────────────────────────────────
@outreach_bp.route("/")
@admin_required
def dashboard():
    u = current_user()
    con = db()
    lt = _lifetime(con)

    # Today's activity at a glance (UTC). Lightweight queries, all on indices.
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    today_stats = con.execute("""
        SELECT
            SUM(CASE WHEN sent_at >= ? THEN 1 ELSE 0 END) AS sent_today,
            SUM(CASE WHEN opened_at >= ? THEN 1 ELSE 0 END) AS opened_today,
            SUM(CASE WHEN first_click_at >= ? THEN 1 ELSE 0 END) AS clicked_today
        FROM outreach_sends
    """, (today, today, today)).fetchone()
    today_stats = {k: (today_stats[k] or 0) for k in today_stats.keys()}

    # Scraper / "agent" status. These tables exist only after outreach_scrapers
    # is imported — guard so a fresh DB still renders.
    scrapers_active = scrapers_paused = 0
    next_run_at = None
    agents_recent = []
    try:
        scrapers_active = con.execute(
            "SELECT COUNT(*) FROM outreach_scrapers WHERE status = 'active'"
        ).fetchone()[0]
        scrapers_paused = con.execute(
            "SELECT COUNT(*) FROM outreach_scrapers WHERE status = 'paused'"
        ).fetchone()[0]
        # Next-scheduled-run: any active scraper whose schedule_hour is later
        # today (or first thing tomorrow if all are past).
        active_scrapers = con.execute(
            "SELECT id, name, schedule_hour_utc, last_run_date FROM outreach_scrapers "
            "WHERE status = 'active'"
        ).fetchall()
        if active_scrapers:
            now = datetime.datetime.utcnow()
            today_str = now.strftime("%Y-%m-%d")
            candidates = []
            for s in active_scrapers:
                if s["last_run_date"] == today_str:
                    # Already ran today; next is tomorrow at schedule_hour.
                    nxt = (now + datetime.timedelta(days=1)).replace(
                        hour=s["schedule_hour_utc"] or 13, minute=0, second=0, microsecond=0
                    )
                else:
                    # Hasn't run yet today.
                    nxt = now.replace(
                        hour=s["schedule_hour_utc"] or 13, minute=0, second=0, microsecond=0
                    )
                    if nxt <= now:
                        # Schedule hour already passed today — scheduler will pick up next tick.
                        nxt = now
                candidates.append((nxt, s["name"]))
            candidates.sort()
            next_run_at = candidates[0][0].isoformat()

        # Recent agent activity (last 8 runs across all scrapers).
        agents_recent = con.execute("""
            SELECT r.*, s.name AS scraper_name
            FROM outreach_scraper_runs r
            JOIN outreach_scrapers s ON s.id = r.scraper_id
            ORDER BY r.started_at DESC LIMIT 8
        """).fetchall()
    except sqlite3.OperationalError:
        # outreach_scraper tables not yet created (init order race on first boot).
        pass

    # Recent campaigns (still useful as a secondary feed).
    recent = con.execute("""
        SELECT c.*, t.name AS template_name, a.name AS audience_name
        FROM outreach_campaigns c
        LEFT JOIN outreach_templates t ON t.id = c.template_id
        LEFT JOIN outreach_audiences a ON a.id = c.audience_id
        ORDER BY c.created_at DESC LIMIT 5
    """).fetchall()

    counts = {
        "audiences": con.execute("SELECT COUNT(*) FROM outreach_audiences").fetchone()[0],
        "templates": con.execute("SELECT COUNT(*) FROM outreach_templates").fetchone()[0],
        "campaigns": con.execute("SELECT COUNT(*) FROM outreach_campaigns").fetchone()[0],
        "suppressed": con.execute("SELECT COUNT(*) FROM outreach_suppressions").fetchone()[0],
        "scrapers_active": scrapers_active,
        "scrapers_paused": scrapers_paused,
    }
    recent_stats = []
    for r in recent:
        s = campaign_stats(con, r["id"])
        recent_stats.append({"c": dict(r), "s": s})
    con.close()
    return render_template(
        "crm/outreach_dashboard.html",
        u=u, lt=lt, today=today_stats, counts=counts, recent=recent_stats,
        agents_recent=agents_recent, next_run_at=next_run_at,
        from_email=from_email(),
        mailing_address=MAILING_ADDRESS,
    )


# ── Pages: audiences ──────────────────────────────────────────────────────────
@outreach_bp.route("/audiences")
@admin_required
def audiences():
    u = current_user()
    con = db()
    rows = con.execute("""
        SELECT a.*, u.first_name AS creator_first, u.last_name AS creator_last
        FROM outreach_audiences a
        LEFT JOIN users u ON u.id = a.created_by_user_id
        ORDER BY a.created_at DESC
    """).fetchall()
    con.close()
    return render_template("crm/outreach_audiences.html", u=u, audiences=rows)


@outreach_bp.route("/audiences/<int:audience_id>")
@admin_required
def audience_detail(audience_id):
    u = current_user()
    con = db()
    a = con.execute("SELECT * FROM outreach_audiences WHERE id = ?", (audience_id,)).fetchone()
    if not a:
        con.close(); abort(404)
    # Just a sample so the page doesn't list 30k rows. Full list is for export.
    sample = con.execute("""
        SELECT email, first_name, last_name, company, created_at
        FROM outreach_audience_members WHERE audience_id = ?
        ORDER BY id ASC LIMIT 25
    """, (audience_id,)).fetchall()
    # Count how many of this audience are suppressed (informational).
    sup = con.execute("""
        SELECT COUNT(*) FROM outreach_audience_members m
        JOIN outreach_suppressions s ON s.email = m.email
        WHERE m.audience_id = ?
    """, (audience_id,)).fetchone()[0]
    # Past campaigns that targeted this audience.
    campaigns = con.execute("""
        SELECT c.*, t.name AS template_name
        FROM outreach_campaigns c
        LEFT JOIN outreach_templates t ON t.id = c.template_id
        WHERE c.audience_id = ?
        ORDER BY c.created_at DESC
    """, (audience_id,)).fetchall()
    con.close()
    return render_template(
        "crm/outreach_audience_detail.html",
        u=u, a=a, sample=sample, suppressed_in_audience=sup, campaigns=campaigns,
    )


@outreach_bp.route("/audiences", methods=["POST"])
@admin_required
def api_create_audience_from_csv():
    """Multipart form: name + csv file. Streams in, dedupes, ignores invalid."""
    u = current_user()
    name = (request.form.get("name") or "").strip()
    note = (request.form.get("note") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Audience name required"}), 400
    f = request.files.get("file")
    if not f:
        return jsonify({"ok": False, "error": "CSV file required"}), 400

    # Read & sniff. We accept UTF-8 with optional BOM.
    raw = f.read()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        return jsonify({"ok": False, "error": "CSV looks empty"}), 400

    # Fuzzy-match column headers (case/space-insensitive).
    field_map = {}
    for fn in reader.fieldnames:
        norm = re.sub(r"[^a-z0-9]", "", (fn or "").lower())
        if norm in ("email", "emailaddress", "emails"):
            field_map["email"] = fn
        elif norm in ("firstname", "fname", "givenname"):
            field_map["first_name"] = fn
        elif norm in ("lastname", "lname", "surname", "familyname"):
            field_map["last_name"] = fn
        elif norm in ("company", "companyname", "business", "businessname", "organization", "org"):
            field_map["company"] = fn
    if "email" not in field_map:
        return jsonify({
            "ok": False,
            "error": f"No email column found. Detected headers: {', '.join(reader.fieldnames)}"
        }), 400

    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_audiences (name, note, source, created_by_user_id, created_at)
        VALUES (?, ?, 'csv', ?, ?)
    """, (name, note or None, u["id"], now_iso()))
    audience_id = cur.lastrowid

    seen_in_file = set()
    accepted = 0
    invalid = 0
    duped = 0
    rows_to_insert = []
    for row in reader:
        email = norm_email(row.get(field_map["email"], ""))
        if not valid_email(email):
            invalid += 1
            continue
        if email in seen_in_file:
            duped += 1
            continue
        seen_in_file.add(email)
        first = (row.get(field_map.get("first_name", ""), "") or "").strip() or None
        last  = (row.get(field_map.get("last_name", ""), "") or "").strip() or None
        comp  = (row.get(field_map.get("company", ""), "") or "").strip() or None
        # Stash any extra columns as JSON so they can be merge-tagged later.
        extras = {k: v for k, v in row.items()
                  if k and k not in field_map.values() and (v or "").strip()}
        rows_to_insert.append((
            audience_id, email, first, last, comp,
            json.dumps(extras) if extras else None, now_iso()
        ))
        # Batch commit every 5k rows to keep memory bounded on huge files.
        if len(rows_to_insert) >= 5000:
            cur.executemany("""
                INSERT OR IGNORE INTO outreach_audience_members
                    (audience_id, email, first_name, last_name, company, extra_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows_to_insert)
            accepted += cur.rowcount
            rows_to_insert = []
            con.commit()
    if rows_to_insert:
        cur.executemany("""
            INSERT OR IGNORE INTO outreach_audience_members
                (audience_id, email, first_name, last_name, company, extra_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, rows_to_insert)
        accepted += cur.rowcount

    refresh_audience_count(con, audience_id)
    con.commit()
    con.close()
    return jsonify({
        "ok": True, "audience_id": audience_id,
        "accepted": accepted, "invalid": invalid, "duped_in_file": duped,
    })


@outreach_bp.route("/audiences/<int:audience_id>", methods=["DELETE"])
@admin_required
def api_delete_audience(audience_id):
    """Hard-delete an audience and its members. Past campaigns that referenced
    it keep their sends history (sends rows aren't audience-FK'd)."""
    con = db()
    con.execute("DELETE FROM outreach_audiences WHERE id = ?", (audience_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/audiences/<int:audience_id>/export")
@admin_required
def audience_export(audience_id):
    """CSV export so admin can grab the list back out."""
    con = db()
    a = con.execute("SELECT name FROM outreach_audiences WHERE id = ?", (audience_id,)).fetchone()
    if not a:
        con.close(); abort(404)
    rows = con.execute("""
        SELECT email, first_name, last_name, company
        FROM outreach_audience_members WHERE audience_id = ? ORDER BY id
    """, (audience_id,)).fetchall()
    con.close()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["email", "first_name", "last_name", "company"])
    for r in rows:
        w.writerow([r["email"], r["first_name"] or "", r["last_name"] or "", r["company"] or ""])
    safe = re.sub(r"[^a-z0-9]+", "-", a["name"].lower()).strip("-")
    resp = make_response(buf.getvalue())
    resp.headers["Content-Type"] = "text/csv"
    resp.headers["Content-Disposition"] = f'attachment; filename="{safe}.csv"'
    return resp


@outreach_bp.route("/audiences/<int:audience_id>/derive", methods=["POST"])
@admin_required
def api_derive_audience(audience_id):
    """Create a child audience from a campaign result: 'opened' / 'not_opened'
    / 'clicked' / 'bounced'. Source: any campaign on the parent audience."""
    data = request.get_json(silent=True) or {}
    kind = (data.get("kind") or "").strip()
    campaign_id = data.get("campaign_id")
    name = (data.get("name") or "").strip()
    if kind not in ("opened", "not_opened", "clicked", "bounced"):
        return jsonify({"ok": False, "error": "Invalid kind"}), 400
    if not campaign_id:
        return jsonify({"ok": False, "error": "campaign_id required"}), 400
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400

    u = current_user()
    con = db()
    parent = con.execute("SELECT * FROM outreach_audiences WHERE id = ?", (audience_id,)).fetchone()
    if not parent:
        con.close(); return jsonify({"ok": False, "error": "Audience missing"}), 404
    if kind == "opened":
        cond = "opened_at IS NOT NULL"
    elif kind == "not_opened":
        cond = "opened_at IS NULL AND sent_at IS NOT NULL"
    elif kind == "clicked":
        cond = "first_click_at IS NOT NULL"
    else:  # bounced
        cond = "bounced_at IS NOT NULL"

    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_audiences
            (name, source, parent_audience_id, parent_campaign_id, derivation_type,
             created_by_user_id, created_at)
        VALUES (?, 'derived', ?, ?, ?, ?, ?)
    """, (name, audience_id, campaign_id, kind, u["id"], now_iso()))
    new_id = cur.lastrowid

    # Copy member fields from the matching sends.
    cur.execute(f"""
        INSERT OR IGNORE INTO outreach_audience_members
            (audience_id, email, first_name, last_name, company, created_at)
        SELECT ?, email, first_name, last_name, company, ?
        FROM outreach_sends
        WHERE campaign_id = ? AND {cond}
    """, (new_id, now_iso(), campaign_id))
    refresh_audience_count(con, new_id)
    con.commit()
    con.close()
    return jsonify({"ok": True, "audience_id": new_id})


# ── Pages: templates ──────────────────────────────────────────────────────────
@outreach_bp.route("/templates")
@admin_required
def templates_list():
    u = current_user()
    con = db()
    rows = con.execute("""
        SELECT t.*, COUNT(c.id) AS use_count
        FROM outreach_templates t
        LEFT JOIN outreach_campaigns c ON c.template_id = t.id
        GROUP BY t.id ORDER BY t.updated_at DESC
    """).fetchall()
    con.close()
    return render_template("crm/outreach_templates.html", u=u, templates=rows)


@outreach_bp.route("/templates/new", methods=["GET"])
@outreach_bp.route("/templates/<int:template_id>", methods=["GET"])
@admin_required
def template_edit(template_id=None):
    u = current_user()
    t = None
    if template_id:
        con = db()
        t = con.execute("SELECT * FROM outreach_templates WHERE id = ?", (template_id,)).fetchone()
        con.close()
        if not t:
            abort(404)
    return render_template(
        "crm/outreach_template_edit.html",
        u=u, t=t, from_email=from_email(),
        default_reply_to=DEFAULT_REPLY_TO,
    )


@outreach_bp.route("/api/templates", methods=["POST"])
@admin_required
def api_create_template():
    u = current_user()
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    subject = (data.get("subject") or "").strip()
    body_html = data.get("body_html") or ""
    body_text = data.get("body_text") or ""
    if not (name and subject and body_html):
        return jsonify({"ok": False, "error": "Name, subject, and body are required"}), 400
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_templates
            (name, subject, body_html, body_text, from_name, from_local, reply_to,
             created_by_user_id, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (name, subject, body_html, body_text or None,
          (data.get("from_name") or "").strip() or None,
          (data.get("from_local") or "").strip() or None,
          (data.get("reply_to") or "").strip() or None,
          u["id"], now_iso(), now_iso()))
    tid = cur.lastrowid
    con.commit()
    con.close()
    return jsonify({"ok": True, "id": tid})


@outreach_bp.route("/api/templates/<int:template_id>", methods=["PATCH"])
@admin_required
def api_update_template(template_id):
    data = request.get_json(silent=True) or {}
    fields = {}
    for k in ("name", "subject", "body_html", "body_text",
              "from_name", "from_local", "reply_to"):
        if k in data:
            v = data[k]
            if isinstance(v, str):
                v = v.strip() or None
            fields[k] = v
    if not fields:
        return jsonify({"ok": True, "noop": True})
    fields["updated_at"] = now_iso()
    con = db()
    sets = ", ".join(f"{k} = ?" for k in fields)
    con.execute(f"UPDATE outreach_templates SET {sets} WHERE id = ?",
                list(fields.values()) + [template_id])
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/templates/<int:template_id>", methods=["DELETE"])
@admin_required
def api_delete_template(template_id):
    con = db()
    in_use = con.execute(
        "SELECT COUNT(*) FROM outreach_campaigns WHERE template_id = ?",
        (template_id,)
    ).fetchone()[0]
    if in_use:
        con.close()
        return jsonify({
            "ok": False,
            "error": f"Template is used by {in_use} campaign(s). Delete or rename those first."
        }), 409
    con.execute("DELETE FROM outreach_templates WHERE id = ?", (template_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Pages: campaigns ──────────────────────────────────────────────────────────
@outreach_bp.route("/campaigns")
@admin_required
def campaigns_list():
    u = current_user()
    con = db()
    rows = con.execute("""
        SELECT c.*, t.name AS template_name, a.name AS audience_name,
               a.member_count AS audience_size
        FROM outreach_campaigns c
        LEFT JOIN outreach_templates t ON t.id = c.template_id
        LEFT JOIN outreach_audiences a ON a.id = c.audience_id
        ORDER BY c.created_at DESC
    """).fetchall()
    # Stats per row — N+1 is fine for the campaign-count scale (tens, not millions).
    out = []
    for r in rows:
        out.append({"c": dict(r), "s": campaign_stats(con, r["id"])})
    con.close()
    return render_template("crm/outreach_campaigns.html", u=u, rows=out)


@outreach_bp.route("/campaigns/new")
@admin_required
def campaign_new():
    u = current_user()
    con = db()
    audiences = con.execute("""
        SELECT id, name, member_count, source, derivation_type, created_at
        FROM outreach_audiences ORDER BY created_at DESC
    """).fetchall()
    templates = con.execute("""
        SELECT id, name, subject, updated_at FROM outreach_templates ORDER BY updated_at DESC
    """).fetchall()
    con.close()
    return render_template(
        "crm/outreach_campaign_new.html",
        u=u, audiences=audiences, templates=templates, from_email=from_email(),
        mailing_address=MAILING_ADDRESS,
    )


@outreach_bp.route("/campaigns/<int:campaign_id>")
@admin_required
def campaign_detail(campaign_id):
    u = current_user()
    con = db()
    c = con.execute("""
        SELECT c.*, t.name AS template_name, t.subject AS template_subject,
               a.name AS audience_name, a.member_count AS audience_size
        FROM outreach_campaigns c
        LEFT JOIN outreach_templates t ON t.id = c.template_id
        LEFT JOIN outreach_audiences a ON a.id = c.audience_id
        WHERE c.id = ?
    """, (campaign_id,)).fetchone()
    if not c:
        con.close(); abort(404)
    s = campaign_stats(con, campaign_id)
    # A tiny recent activity slice for the detail page — last 30 events.
    recent_events = con.execute("""
        SELECT s.email, s.sent_at, s.delivered_at, s.opened_at, s.first_click_at,
               s.bounced_at, s.unsubscribed_at, s.status, s.error
        FROM outreach_sends s
        WHERE s.campaign_id = ?
          AND (s.sent_at IS NOT NULL OR s.status IN ('failed','suppressed'))
        ORDER BY COALESCE(s.first_click_at, s.opened_at, s.delivered_at, s.sent_at) DESC NULLS LAST
        LIMIT 30
    """, (campaign_id,)).fetchall()
    con.close()
    return render_template(
        "crm/outreach_campaign_detail.html",
        u=u, c=c, s=s, recent=recent_events,
    )


@outreach_bp.route("/api/campaigns", methods=["POST"])
@admin_required
def api_create_campaign():
    """Create + launch a campaign in one step. Queues a send row per audience
    member, scheduled evenly across send_window_hours from now (with small
    jitter so we don't pile up on second boundaries)."""
    u = current_user()
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    template_id = data.get("template_id")
    audience_id = data.get("audience_id")
    window_h = float(data.get("send_window_hours") or 4)
    launch_now = bool(data.get("launch", False))

    if not (name and template_id and audience_id):
        return jsonify({"ok": False, "error": "Name, template, and audience are required"}), 400
    if window_h < 0.1 or window_h > 72:
        return jsonify({"ok": False, "error": "Send window must be between 0.1 and 72 hours"}), 400

    con = db()
    a = con.execute("SELECT * FROM outreach_audiences WHERE id = ?", (audience_id,)).fetchone()
    t = con.execute("SELECT * FROM outreach_templates WHERE id = ?", (template_id,)).fetchone()
    if not a or not t:
        con.close()
        return jsonify({"ok": False, "error": "Audience or template missing"}), 404

    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_campaigns
            (name, template_id, audience_id, status, send_window_hours,
             created_by_user_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (name, template_id, audience_id,
          "sending" if launch_now else "draft",
          window_h, u["id"], now_iso()))
    campaign_id = cur.lastrowid

    queued = 0
    if launch_now:
        queued = _queue_campaign_sends(con, campaign_id, audience_id, window_h)
        cur.execute("UPDATE outreach_campaigns SET started_at = ?, total_queued = ? WHERE id = ?",
                    (now_iso(), queued, campaign_id))
    con.commit()
    con.close()
    return jsonify({"ok": True, "campaign_id": campaign_id, "queued": queued})


def _queue_campaign_sends(con, campaign_id, audience_id, window_h):
    """Pull every member, dedupe against global suppressions, schedule each
    one at an evenly-spaced offset from now. Returns the queued count."""
    members = con.execute("""
        SELECT m.id, m.email, m.first_name, m.last_name, m.company, m.extra_json
        FROM outreach_audience_members m
        WHERE m.audience_id = ?
          AND m.email NOT IN (SELECT email FROM outreach_suppressions)
        ORDER BY m.id
    """, (audience_id,)).fetchall()
    if not members:
        return 0
    start = datetime.datetime.utcnow()
    window_s = max(window_h * 3600.0, 1.0)
    n = len(members)
    step = window_s / max(n, 1)
    rows = []
    for i, m in enumerate(members):
        offset = i * step + random.uniform(-step * 0.15, step * 0.15) if n > 1 else 0
        sched = start + datetime.timedelta(seconds=max(offset, 0))
        rows.append((
            campaign_id, m["id"], m["email"], m["first_name"], m["last_name"],
            m["company"], m["extra_json"], "queued", sched.isoformat(),
        ))
    con.executemany("""
        INSERT INTO outreach_sends
            (campaign_id, member_id, email, first_name, last_name, company,
             extra_json, status, scheduled_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    return len(rows)


@outreach_bp.route("/api/campaigns/<int:campaign_id>/launch", methods=["POST"])
@admin_required
def api_launch_campaign(campaign_id):
    """Promote a draft campaign to 'sending' and queue the sends."""
    con = db()
    c = con.execute("SELECT * FROM outreach_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    if not c:
        con.close(); abort(404)
    if c["status"] not in ("draft", "paused"):
        con.close()
        return jsonify({"ok": False, "error": f"Cannot launch from status '{c['status']}'"}), 400
    queued = 0
    if c["status"] == "draft":
        queued = _queue_campaign_sends(con, campaign_id, c["audience_id"], c["send_window_hours"])
    con.execute("UPDATE outreach_campaigns SET status='sending', started_at=COALESCE(started_at, ?), "
                "total_queued = total_queued + ? WHERE id = ?",
                (now_iso(), queued, campaign_id))
    con.commit()
    con.close()
    return jsonify({"ok": True, "queued": queued})


@outreach_bp.route("/api/campaigns/<int:campaign_id>/pause", methods=["POST"])
@admin_required
def api_pause_campaign(campaign_id):
    con = db()
    con.execute("UPDATE outreach_campaigns SET status='paused' WHERE id = ? AND status='sending'",
                (campaign_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/campaigns/<int:campaign_id>/cancel", methods=["POST"])
@admin_required
def api_cancel_campaign(campaign_id):
    """Cancel a running campaign. Already-sent emails stay in their final state;
    queued sends are marked canceled and skipped by the worker."""
    con = db()
    con.execute("UPDATE outreach_campaigns SET status='canceled', completed_at = ? WHERE id = ?",
                (now_iso(), campaign_id))
    con.execute("UPDATE outreach_sends SET status='canceled' WHERE campaign_id = ? AND status='queued'",
                (campaign_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/campaigns/<int:campaign_id>", methods=["DELETE"])
@admin_required
def api_delete_campaign(campaign_id):
    con = db()
    con.execute("DELETE FROM outreach_campaigns WHERE id = ?", (campaign_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/campaigns/<int:campaign_id>/stats")
@admin_required
def api_campaign_stats(campaign_id):
    """For live-refresh polling on the campaign detail page."""
    con = db()
    s = campaign_stats(con, campaign_id)
    c = con.execute("SELECT status FROM outreach_campaigns WHERE id = ?", (campaign_id,)).fetchone()
    con.close()
    return jsonify({"ok": True, "stats": s, "status": c["status"] if c else None})


# ── Suppressions ──────────────────────────────────────────────────────────────
@outreach_bp.route("/suppressions")
@admin_required
def suppressions_page():
    u = current_user()
    con = db()
    rows = con.execute("""
        SELECT * FROM outreach_suppressions ORDER BY created_at DESC LIMIT 500
    """).fetchall()
    total = con.execute("SELECT COUNT(*) FROM outreach_suppressions").fetchone()[0]
    con.close()
    return render_template("crm/outreach_suppressions.html",
                           u=u, rows=rows, total=total)


@outreach_bp.route("/api/suppressions", methods=["POST"])
@admin_required
def api_add_suppression():
    """Manually add an email to the suppression list."""
    data = request.get_json(silent=True) or {}
    email = norm_email(data.get("email") or "")
    if not valid_email(email):
        return jsonify({"ok": False, "error": "Invalid email"}), 400
    con = db()
    add_suppression(con, email, "manual")
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/suppressions", methods=["DELETE"])
@admin_required
def api_remove_suppression():
    email = norm_email((request.get_json(silent=True) or {}).get("email") or "")
    if not email:
        return jsonify({"ok": False, "error": "email required"}), 400
    con = db()
    con.execute("DELETE FROM outreach_suppressions WHERE email = ?", (email,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


# ── Public: unsubscribe (no login) ────────────────────────────────────────────
@outreach_bp.route("/u/<token>", methods=["GET", "POST"])
def unsubscribe(token):
    """One-click unsubscribe. GET shows a confirmation page (so email scanners
    that pre-fetch links don't accidentally unsubscribe users). POST commits
    — that's also what Gmail/Yahoo's List-Unsubscribe-Post hits."""
    row = verify_unsub_token(token)
    if not row:
        return render_template("crm/outreach_unsubscribed.html",
                               status="invalid", email=None), 400
    if request.method == "POST":
        con = db()
        add_suppression(con, row["email"], "unsubscribe",
                        source_campaign_id=row["campaign_id"])
        con.execute("UPDATE outreach_sends SET unsubscribed_at = COALESCE(unsubscribed_at, ?) "
                    "WHERE id = ?", (now_iso(), row["id"]))
        con.commit()
        con.close()
        return render_template("crm/outreach_unsubscribed.html",
                               status="done", email=row["email"])
    # GET — show confirm screen
    return render_template("crm/outreach_unsubscribed.html",
                           status="confirm", email=row["email"], token=token)


# ── Resend webhook ────────────────────────────────────────────────────────────
def _verify_resend_signature(raw_body, headers):
    """Resend uses Svix-style signing. The header carries one or more
    'v1,base64sig' entries; we accept if any of them matches our HMAC."""
    if not RESEND_WEBHOOK_SECRET:
        # If unconfigured, accept anything. Useful for local dev — disable in prod
        # by setting RESEND_WEBHOOK_SECRET to the value Resend gave you.
        return True
    sig_header = headers.get("svix-signature") or headers.get("Svix-Signature") or ""
    svix_id = headers.get("svix-id") or headers.get("Svix-Id") or ""
    svix_ts = headers.get("svix-timestamp") or headers.get("Svix-Timestamp") or ""
    if not (sig_header and svix_id and svix_ts):
        return False
    secret = RESEND_WEBHOOK_SECRET
    if secret.startswith("whsec_"):
        secret = secret[len("whsec_"):]
    try:
        key = base64.b64decode(secret)
    except Exception:
        return False
    signed = f"{svix_id}.{svix_ts}.{raw_body.decode('utf-8', 'replace')}".encode()
    expected = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
    for entry in sig_header.split():
        parts = entry.split(",", 1)
        if len(parts) == 2 and hmac.compare_digest(parts[1], expected):
            return True
    return False


@outreach_bp.route("/webhooks/resend", methods=["POST"])
def resend_webhook():
    raw = request.get_data() or b""
    if not _verify_resend_signature(raw, request.headers):
        return ("bad signature", 401)
    try:
        evt = json.loads(raw.decode("utf-8"))
    except Exception:
        return ("bad json", 400)

    event_type = evt.get("type") or ""
    data = evt.get("data") or {}
    msg_id = data.get("email_id") or data.get("id") or ""

    con = db()
    con.execute(
        "INSERT INTO outreach_events (resend_message_id, event_type, payload_json, received_at) "
        "VALUES (?, ?, ?, ?)",
        (msg_id or None, event_type, json.dumps(evt), now_iso())
    )

    if msg_id:
        send = con.execute(
            "SELECT * FROM outreach_sends WHERE resend_message_id = ?", (msg_id,)
        ).fetchone()
        if send:
            sid = send["id"]
            ts = now_iso()
            if event_type == "email.delivered":
                con.execute("UPDATE outreach_sends SET delivered_at = COALESCE(delivered_at, ?) "
                            "WHERE id = ?", (ts, sid))
            elif event_type == "email.opened":
                con.execute("UPDATE outreach_sends SET opened_at = COALESCE(opened_at, ?), "
                            "open_count = open_count + 1 WHERE id = ?", (ts, sid))
            elif event_type == "email.clicked":
                con.execute("UPDATE outreach_sends SET first_click_at = COALESCE(first_click_at, ?), "
                            "click_count = click_count + 1 WHERE id = ?", (ts, sid))
            elif event_type == "email.bounced":
                con.execute("UPDATE outreach_sends SET bounced_at = COALESCE(bounced_at, ?) "
                            "WHERE id = ?", (ts, sid))
                add_suppression(con, send["email"], "bounce", send["campaign_id"])
            elif event_type == "email.complained":
                con.execute("UPDATE outreach_sends SET complained_at = COALESCE(complained_at, ?) "
                            "WHERE id = ?", (ts, sid))
                add_suppression(con, send["email"], "complaint", send["campaign_id"])
    con.commit()
    con.close()
    return ("", 204)


# ── Send worker ───────────────────────────────────────────────────────────────
_worker_started = False
_worker_lock = threading.Lock()


def _send_one(send_row, campaign_row, template_row):
    """Build + POST one email to Resend. Returns (ok, message_id_or_error)."""
    ctx = {
        "email": send_row["email"],
        "first_name": send_row["first_name"],
        "last_name": send_row["last_name"],
        "company": send_row["company"],
    }
    unsub_url = f"{PUBLIC_BASE_URL}/crm/outreach/u/{make_unsub_token(send_row['id'], send_row['email'])}"
    subject = render_template_body(template_row["subject"], ctx)
    body_html = render_template_body(template_row["body_html"], ctx)
    body_text = render_template_body(
        template_row["body_text"] or html_to_plaintext(template_row["body_html"]),
        ctx,
    )
    body_html = inject_unsub_html(body_html, unsub_url)
    body_text = inject_unsub_text(body_text, unsub_url)

    from_local = template_row["from_local"] or FROM_LOCAL
    from_name  = template_row["from_name"]  or FROM_NAME
    from_addr  = f"{from_local}@{FROM_DOMAIN}"
    reply_to   = template_row["reply_to"]   or DEFAULT_REPLY_TO

    payload = {
        "from": f"{from_name} <{from_addr}>",
        "to": [send_row["email"]],
        "subject": subject,
        "html": body_html,
        "text": body_text,
        "reply_to": [reply_to],
        "headers": {
            "List-Unsubscribe": f"<{unsub_url}>",
            "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
        },
        "tags": [
            {"name": "campaign_id", "value": str(campaign_row["id"])},
            {"name": "send_id",     "value": str(send_row["id"])},
        ],
    }
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload, timeout=20,
        )
    except requests.RequestException as e:
        return False, f"network: {e}"
    if r.status_code == 429:
        return False, "rate_limited"
    if r.status_code >= 500:
        return False, f"resend_5xx: {r.status_code} {r.text[:200]}"
    if r.status_code >= 400:
        # 4xx (e.g. invalid recipient) — don't retry, mark failed.
        return False, f"resend_4xx: {r.status_code} {r.text[:200]}"
    try:
        return True, (r.json() or {}).get("id", "")
    except Exception:
        return True, ""


def _worker_tick():
    """Process up to N due sends. Returns whether we did anything (to inform
    sleep duration in the main loop)."""
    BATCH = max(int(RESEND_RATE_PER_SEC * 2), 1)  # ~2s worth of sends per tick
    now = now_iso()
    con = db()

    # Claim a batch atomically: select queued+due, then UPDATE to 'sending'
    # filtered on still-queued so concurrent workers (if any) don't double-claim.
    rows = con.execute("""
        SELECT s.*, c.status AS campaign_status, c.template_id
        FROM outreach_sends s
        JOIN outreach_campaigns c ON c.id = s.campaign_id
        WHERE s.status = 'queued'
          AND s.scheduled_at <= ?
          AND c.status = 'sending'
        ORDER BY s.scheduled_at ASC
        LIMIT ?
    """, (now, BATCH)).fetchall()
    if not rows:
        # Mark any campaigns whose queue is empty as completed.
        con.execute("""
            UPDATE outreach_campaigns
            SET status = 'completed', completed_at = COALESCE(completed_at, ?)
            WHERE status = 'sending'
              AND NOT EXISTS (
                SELECT 1 FROM outreach_sends s
                WHERE s.campaign_id = outreach_campaigns.id
                  AND s.status IN ('queued', 'sending')
              )
        """, (now,))
        con.commit()
        con.close()
        return False

    ids = [r["id"] for r in rows]
    placeholders = ",".join("?" * len(ids))
    con.execute(f"UPDATE outreach_sends SET status='sending' "
                f"WHERE id IN ({placeholders}) AND status='queued'", ids)
    con.commit()

    # Preload templates so we don't hit DB per row.
    tmpl_ids = {r["template_id"] for r in rows}
    tmpls = {t["id"]: t for t in con.execute(
        f"SELECT * FROM outreach_templates WHERE id IN ({','.join('?'*len(tmpl_ids))})",
        list(tmpl_ids)
    ).fetchall()}

    sleep_between = 1.0 / max(RESEND_RATE_PER_SEC, 0.1)

    for r in rows:
        # Re-check suppression at send time (someone may have unsubscribed
        # between queueing and now).
        if is_suppressed(con, r["email"]):
            con.execute("UPDATE outreach_sends SET status='suppressed' WHERE id = ?", (r["id"],))
            con.commit()
            continue
        # And re-check campaign status — admin may have paused/canceled mid-batch.
        cs = con.execute("SELECT status FROM outreach_campaigns WHERE id = ?",
                         (r["campaign_id"],)).fetchone()
        if not cs or cs["status"] != "sending":
            con.execute("UPDATE outreach_sends SET status='queued' WHERE id = ? AND status='sending'",
                        (r["id"],))
            con.commit()
            continue

        tmpl = tmpls.get(r["template_id"])
        if not tmpl:
            con.execute("UPDATE outreach_sends SET status='failed', error=? WHERE id = ?",
                        ("template missing", r["id"]))
            con.commit()
            continue

        ok, info = _send_one(r, {"id": r["campaign_id"]}, tmpl)
        if ok:
            con.execute(
                "UPDATE outreach_sends SET status='sent', sent_at=?, resend_message_id=? WHERE id = ?",
                (now_iso(), info or None, r["id"])
            )
        elif info == "rate_limited" or (info or "").startswith("resend_5xx") or (info or "").startswith("network"):
            # Transient — back off and retry. Push scheduled_at out.
            retries = (r["retry_count"] or 0) + 1
            if retries >= 5:
                con.execute("UPDATE outreach_sends SET status='failed', error=?, retry_count=? "
                            "WHERE id = ?", (info, retries, r["id"]))
            else:
                next_at = (datetime.datetime.utcnow() +
                           datetime.timedelta(seconds=30 * retries)).isoformat()
                con.execute("UPDATE outreach_sends SET status='queued', retry_count=?, "
                            "scheduled_at=? WHERE id = ?",
                            (retries, next_at, r["id"]))
        else:
            con.execute("UPDATE outreach_sends SET status='failed', error=? WHERE id = ?",
                        (info, r["id"]))
        con.commit()
        time.sleep(sleep_between)

    con.close()
    return True


def _worker_loop():
    """Daemon thread. Polls the queue, paces sends, sleeps when idle."""
    print(f"[outreach] send worker started (rate={RESEND_RATE_PER_SEC}/s, from={from_email()})")
    while True:
        try:
            did_work = _worker_tick()
            if not did_work:
                time.sleep(5)
        except Exception as e:
            print(f"[outreach] worker error: {e}")
            time.sleep(10)


def start_worker():
    """Start the worker thread once per process. Safe to call multiple times."""
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        if not RESEND_API_KEY:
            print("[outreach] WARN: RESEND_API_KEY unset, worker not started")
            return
        t = threading.Thread(target=_worker_loop, daemon=True, name="outreach-worker")
        t.start()
        _worker_started = True


# Initialize on import
init_outreach_db()
start_worker()
