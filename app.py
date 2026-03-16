from flask import Flask, render_template, request, jsonify, session, redirect, url_for
import os, sqlite3, datetime, uuid, json, threading, requests

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "lumen-wl-key-2026")

ADMIN_PIN = "112501"
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL = "kendall@lumenmarketing.co"


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

    if time_filter:
        avg_time = con.execute(
            f"SELECT AVG(time_on_page) FROM page_views WHERE time_on_page > 0 AND timestamp > ?", time_params
        ).fetchone()[0] or 0
    else:
        avg_time = con.execute(
            "SELECT AVG(time_on_page) FROM page_views WHERE time_on_page > 0"
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
    con = sqlite3.connect(DB_PATH)
    try:
        apps = con.execute("SELECT email, business, revenue, marketing, challenge, created_at FROM applications ORDER BY id DESC").fetchall()
    except Exception:
        apps = []
    try:
        funnel = con.execute("SELECT event, step, value, COUNT(*) as cnt FROM funnel_events GROUP BY event, step, value ORDER BY cnt DESC").fetchall()
    except Exception:
        funnel = []
    con.close()
    applications = [{"email": a[0], "business": a[1], "revenue": a[2], "marketing": a[3], "challenge": a[4], "date": a[5][:10]} for a in apps]
    funnel_stats = [{"event": f[0], "step": f[1], "value": f[2], "count": f[3]} for f in funnel]
    return render_template("admin_main.html", authed=True, applications=applications, funnel_stats=funnel_stats)

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
        (email, business, revenue, marketing, challenge, datetime.datetime.utcnow().isoformat()),
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
