from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os, sqlite3, datetime, uuid, json, threading, requests, csv, io, time, base64, re
from email.mime.text import MIMEText

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "lumen-wl-key-2026")
app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 31536000  # cache static files 1 year

ADMIN_PIN = "112501"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL = "kendall@lumenmarketing.co"
OWNER_IPS = {"209.127.238.130"}

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
                    "to": [to],
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

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/story")
def story():
    return render_template("story.html")

@app.route("/avaloncrm")
def avalon_crm():
    return render_template("avalon_crm.html")

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
        pin = (request.form.get("pin") or "").strip()
        if pin == ADMIN_PIN:
            session["wl_auth"] = True
            return redirect(url_for("admin_landing"))
        return render_template("admin.html", error=True, authed=False)
    if not session.get("wl_auth"):
        return render_template("admin.html", authed=False, error=False)
    return render_template("admin.html", authed=True, error=False)

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
    business = data.get("business", "")
    revenue = data.get("revenue", "")
    marketing = data.get("marketing", "")
    challenge = data.get("challenge", "")
    email = (data.get("email") or "").strip().lower()
    if not email or "@" not in email:
        return jsonify({"ok": False, "error": "Valid email required"})

    now = datetime.datetime.utcnow().isoformat()

    # Store application
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS applications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            business TEXT DEFAULT '',
            revenue TEXT DEFAULT '',
            marketing TEXT DEFAULT '',
            challenge TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute(
        "INSERT INTO applications (email, business, revenue, marketing, challenge, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (email, business, revenue, marketing, challenge, now),
    )

    # Auto-create CRM lead
    existing_lead = con.execute("SELECT id FROM leads WHERE email = ?", (email,)).fetchone()
    if not existing_lead:
        cur = con.execute(
            """INSERT INTO leads (name, email, phone, business, revenue, marketing, challenge,
               stage, source, deal_value, follow_up_date, tags, created_at, updated_at)
               VALUES (?, ?, '', ?, ?, ?, ?, 'new', 'application', 0, '', '', ?, ?)""",
            ("", email, business, revenue, marketing, challenge, now, now),
        )
        lead_id = cur.lastrowid
        con.execute(
            "INSERT INTO lead_activity (lead_id, type, content, metadata, created_at) VALUES (?, 'auto', 'Lead created from application', '', ?)",
            (lead_id, now),
        )
    con.commit()
    con.close()

    # Notify Kendall
    if RESEND_API_KEY:
        notify_html = f"""
        <div style="font-family:Inter,sans-serif;background:#0a0a0f;color:#e8e8f0;padding:40px;">
            <h2 style="color:#7c4dff;">New Application</h2>
            <p><strong>Email:</strong> {email}</p>
            <p><strong>Business:</strong> {business}</p>
            <p><strong>Revenue:</strong> {revenue}</p>
            <p><strong>Current Marketing:</strong> {marketing}</p>
            <p><strong>Biggest Challenge:</strong> {challenge}</p>
        </div>
        """
        send_email(NOTIFY_EMAIL, f"New application: {email}", notify_html)

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
    })

@app.route("/api/main-site-analytics")
def api_main_site_analytics():
    if not session.get("wl_auth"):
        return jsonify({"ok": False}), 401
    con = sqlite3.connect(DB_PATH)
    try:
        apps = con.execute(
            "SELECT email, business, revenue, marketing, challenge, created_at FROM applications ORDER BY id DESC"
        ).fetchall()
        applications = [{"email": a[0], "business": a[1], "revenue": a[2],
                         "marketing": a[3], "challenge": a[4], "created_at": a[5]} for a in apps]
    except Exception:
        applications = []
    try:
        funnel = con.execute(
            "SELECT event, step, value, COUNT(*) as cnt FROM funnel_events GROUP BY event, step, value ORDER BY cnt DESC"
        ).fetchall()
        funnel_stats = [{"event": f[0], "step": f[1], "value": f[2], "count": f[3]} for f in funnel]
    except Exception:
        funnel_stats = []
    con.close()
    return jsonify({"applications": applications, "funnel": funnel_stats, "total_apps": len(applications)})

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
    return render_template("mk_dashboard.html", active="dashboard",
        lead_count=lead_count, campaign_count=campaign_count,
        sent_count=sent_count, recent=recent, recent_leads=recent_leads)


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
def _mk_send_emails_bg(campaign_id, subject, body, lead_ids, creds_token, creds_refresh, sender_email):
    """Send emails in background thread so request doesn't time out."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials(
        token=creds_token, refresh_token=creds_refresh,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GMAIL_CLIENT_ID, client_secret=GMAIL_CLIENT_SECRET,
    )
    service = build("gmail", "v1", credentials=creds)
    con = sqlite3.connect(DB_PATH)
    sent = 0
    errors = 0
    for lid in lead_ids:
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
        try:
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
            service.users().messages().send(userId="me", body={"raw": raw}).execute()
            con.execute(
                "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, sent_at) VALUES (?, ?, 'email', ?, 'sent', ?)",
                (campaign_id, lead[0], lead[2], now))
            con.execute(
                "UPDATE mk_leads SET last_contacted = ?, send_count = send_count + 1 WHERE id = ?",
                (now, lead[0]))
            sent += 1
        except Exception as e:
            con.execute(
                "INSERT INTO mk_send_log (campaign_id, lead_id, channel, recipient, status, error, sent_at) VALUES (?, ?, 'email', ?, 'failed', ?, ?)",
                (campaign_id, lead[0], lead[2], str(e)[:200], now))
            errors += 1
        con.commit()
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
