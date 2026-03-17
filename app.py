from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os, sqlite3, datetime, uuid, json, threading, requests

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "lumen-wl-key-2026")

ADMIN_PIN = "112501"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL = "kendall@lumenmarketing.co"
OWNER_IPS = {"209.127.238.130"}


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

@app.route("/")
def index():
    return render_template("index.html")

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

    con = sqlite3.connect(DB_PATH)
    con.execute(
        """INSERT INTO page_views
        (session_id, timestamp, time_on_page, ip, user_agent, referrer,
         screen, language, timezone, platform, utm_source, utm_medium, utm_campaign, utm_content)
        VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (sid, datetime.datetime.utcnow().isoformat(), ip, user_agent, referrer,
         screen, language, timezone, platform, utm_source, utm_medium, utm_campaign, utm_content),
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
        return render_template("admin.html", error=True, authed=False, waitlist_count=0, views_today=0, app_count=0)
    if not session.get("wl_auth"):
        return render_template("admin.html", authed=False, error=False, waitlist_count=0, views_today=0, app_count=0)
    con = sqlite3.connect(DB_PATH)
    waitlist_count = con.execute("SELECT COUNT(*) FROM waitlist").fetchone()[0]
    today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    views_today = con.execute("SELECT COUNT(*) FROM page_views WHERE timestamp LIKE ?", (today + "%",)).fetchone()[0]
    try:
        app_count = con.execute("SELECT COUNT(*) FROM applications").fetchone()[0]
    except Exception:
        app_count = 0
    con.close()
    return render_template("admin.html", authed=True, error=False,
                           waitlist_count=waitlist_count, views_today=views_today, app_count=app_count)

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
    return render_template("admin_main.html")

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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
