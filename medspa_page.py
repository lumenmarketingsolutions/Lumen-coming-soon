"""Med spa landing page (The Booked Out System) at lumenmarketing.co/medspa.

React build from ~/booked-out-site (Vite base /medspa/) lives in medspa_site/.
  /medspa/            the page (plus /medspa/media/vsl.mp4, served with range requests)
  /medspa/api/vsl     VSL watch tracking: view, play, pause, progress (25/50/75/95), heartbeat every 10s, complete, leave
  /medspa/apply       application form: stored + emailed to Kendall
  /crm/vsl            stats for Kendall (CRM login): plays, watch time, drop off, applications
"""
import datetime
import html
import os
import sqlite3

from flask import Blueprint, jsonify, make_response, redirect, request, send_from_directory

from crm import login_required

medspa_bp = Blueprint("medspa", __name__)

SITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "medspa_site")
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DATA_DIR, "waitlist.db")
KENDALL_IPS = {"209.127.238.130"}


def _db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS vsl_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, visit TEXT, event TEXT, max_s REAL, duration REAL, pct INTEGER,
        at_s REAL, ip TEXT, ua TEXT, page TEXT, ref TEXT, created_at TEXT)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_vsl_visit ON vsl_events(visit)")
    con.execute("""CREATE TABLE IF NOT EXISTS medspa_applications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, spa TEXT, city TEXT, phone TEXT, email TEXT, sms_consent INTEGER,
        revenue TEXT, speed TEXT, visit TEXT, vsl_watched INTEGER, vsl_length INTEGER, page TEXT, ip TEXT, created_at TEXT)""")
    return con


def _ip():
    return (request.headers.get("X-Forwarded-For", request.remote_addr or "") or "").split(",")[0].strip()


def _num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


@medspa_bp.route("/medspa/api/vsl", methods=["POST"])
def vsl_event():
    d = request.get_json(silent=True, force=True) or {}
    event = str(d.get("event") or "")[:20]
    visit = str(d.get("visit") or "")[:64]
    if not visit or event not in {"view", "play", "pause", "progress", "heartbeat", "complete", "leave"}:
        return jsonify({"ok": False}), 400
    con = _db()
    con.execute("INSERT INTO vsl_events (visit, event, max_s, duration, pct, at_s, ip, ua, page, ref, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (visit, event, _num(d.get("max")), _num(d.get("duration")), d.get("pct"), _num(d.get("at")), _ip(),
                 (request.headers.get("User-Agent") or "")[:300], str(d.get("page") or "")[:500], str(d.get("ref") or "")[:500],
                 datetime.datetime.utcnow().isoformat()))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@medspa_bp.route("/medspa/apply", methods=["POST"])
def medspa_apply():
    d = request.get_json(silent=True) or {}
    v = {k: str(d.get(k) or "").strip()[:300] for k in ["name", "spa", "city", "phone", "email", "revenue", "speed", "visit", "page"]}
    if not v["name"] or not v["phone"]:
        return jsonify({"ok": False, "error": "missing"}), 400
    watched, length = int(_num(d.get("vsl_watched")) or 0), int(_num(d.get("vsl_length")) or 0)
    con = _db()
    con.execute("""INSERT INTO medspa_applications (name, spa, city, phone, email, sms_consent, revenue, speed, visit, vsl_watched, vsl_length, page, ip, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (v["name"], v["spa"], v["city"], v["phone"], v["email"], 1 if d.get("sms") else 0, v["revenue"], v["speed"],
                 v["visit"], watched, length, v["page"], _ip(), datetime.datetime.utcnow().isoformat()))
    con.commit()
    con.close()
    seen = f"{watched // 60}:{watched % 60:02d} of {length // 60}:{length % 60:02d}" if length else "did not play it"
    rows = "".join(
        f'<tr><td style="padding:8px 0;color:#6b7079;width:150px;">{label}</td><td style="padding:8px 0;color:#16191F;font-weight:600;">{html.escape(val) or "&ndash;"}</td></tr>'
        for label, val in [("Name", v["name"]), ("Med spa", v["spa"]), ("City", v["city"]), ("Mobile", v["phone"]), ("Email", v["email"]),
                           ("Monthly revenue", v["revenue"]), ("Reply speed today", v["speed"]), ("SMS consent", "Yes" if d.get("sms") else "No"),
                           ("Watched the VSL", seen)])
    body = ('<div style="font-family:Inter,-apple-system,sans-serif;background:#F6F2EC;padding:32px 20px;">'
            '<div style="max-width:560px;margin:0 auto;background:#fff;border:1px solid #E4DDD2;border-radius:14px;padding:32px;">'
            '<div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#0E6F99;margin-bottom:10px;">New med spa application</div>'
            f'<h2 style="font-size:22px;margin:0 0 18px;color:#16191F;">{html.escape(v["spa"] or v["name"])}</h2>'
            f'<table style="width:100%;border-collapse:collapse;font-size:14px;">{rows}</table>'
            '<p style="margin:22px 0 0;color:#6b7079;font-size:13px;">They were sent to your calendar next. From lumenmarketing.co/medspa</p></div></div>')
    try:
        from app import send_email, NOTIFY_EMAIL
        send_email(NOTIFY_EMAIL, f"Med spa application: {v['spa'] or v['name']} ({v['city']})", body)
    except Exception as e:
        print("[medspa] email failed:", e)
    return jsonify({"ok": True})


@medspa_bp.route("/medspa")
def medspa_root():
    return redirect("/medspa/", code=301)


@medspa_bp.route("/medspa/")
@medspa_bp.route("/medspa/<path:filename>")
def medspa_site(filename="index.html"):
    full = os.path.join(SITE_DIR, filename)
    if not os.path.isfile(full):
        filename = "index.html"
    resp = make_response(send_from_directory(SITE_DIR, filename, conditional=True))
    if filename == "index.html":
        resp.headers["Cache-Control"] = "no-cache"
    elif filename.startswith("media/"):
        resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp


def _fmt(s):
    s = int(round(s or 0))
    return f"{s // 60}:{s % 60:02d}"


@medspa_bp.route("/crm/vsl")
@login_required
def vsl_stats():
    show_me = request.args.get("me") == "1"
    con = _db()
    rows = con.execute("""SELECT visit, MIN(created_at) first, MAX(COALESCE(max_s,0)) max_s, MAX(COALESCE(duration,0)) duration,
                                 SUM(event='play') plays, SUM(event='complete') completes, MAX(ip) ip, MAX(ua) ua, MAX(ref) ref
                          FROM vsl_events GROUP BY visit ORDER BY first DESC""").fetchall()
    apps = con.execute("SELECT * FROM medspa_applications ORDER BY id DESC").fetchall()
    con.close()
    visits = [r for r in rows if show_me or r["ip"] not in KENDALL_IPS]
    applied = {a["visit"] for a in apps}
    played = [r for r in visits if r["plays"] or r["max_s"] > 0]
    length = max([r["duration"] for r in visits] + [0]) or 186
    avg_s = sum(r["max_s"] for r in played) / len(played) if played else 0
    done = [r for r in played if r["max_s"] >= 0.95 * length]

    def reached(p):
        return sum(1 for r in played if r["max_s"] >= p / 100 * length)

    marks = "".join(
        f'<div class="bar"><span>{p}%</span><div><i style="width:{(reached(p) / len(played) * 100) if played else 0:.0f}%"></i></div><b>{reached(p)}</b></div>'
        for p in (10, 25, 50, 75, 95))
    buckets = list(range(0, int(length) + 1, 15))
    curve = "".join(
        f'<div class="col" title="{_fmt(t)}: {sum(1 for r in played if r["max_s"] >= t)} still watching">'
        f'<i style="height:{(sum(1 for r in played if r["max_s"] >= t) / len(played) * 100) if played else 0:.0f}%"></i><span>{_fmt(t)}</span></div>'
        for t in buckets)
    table = "".join(
        f'<tr><td>{html.escape((r["first"] or "")[:16].replace("T", " "))} UTC</td><td>{"Played" if r in played else "Did not play"}</td>'
        f'<td>{_fmt(r["max_s"])}</td><td>{(r["max_s"] / length * 100):.0f}%</td><td>{"Yes" if r["visit"] in applied else ""}</td>'
        f'<td>{"Phone" if "Mobile" in (r["ua"] or "") else "Desktop"}</td><td class="muted">{html.escape((r["ref"] or "")[:40])}</td></tr>'
        for r in visits[:200])
    app_rows = "".join(
        f'<tr><td>{html.escape((a["created_at"] or "")[:16].replace("T", " "))}</td><td>{html.escape(a["spa"] or "")}</td><td>{html.escape(a["city"] or "")}</td>'
        f'<td>{html.escape(a["revenue"] or "")}</td><td>{_fmt(a["vsl_watched"])}</td></tr>' for a in apps[:100])
    stat = lambda v, l: f'<div class="stat"><div class="v">{v}</div><div class="l">{l}</div></div>'
    page = f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>VSL stats</title><style>
body{{margin:0;background:#F6F2EC;color:#16191F;font:15px/1.5 -apple-system,Inter,sans-serif}} main{{max-width:1000px;margin:0 auto;padding:32px 16px 80px}}
h1{{font-size:28px;margin:0 0 4px}} .sub{{color:#6b7079;margin:0 0 24px}} h2{{font-size:17px;margin:36px 0 12px}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}} .stat{{background:#fff;border:1px solid #E4DDD2;border-radius:14px;padding:16px}}
.stat .v{{font-size:26px;font-weight:700}} .stat .l{{color:#6b7079;font-size:13px}}
.bar{{display:grid;grid-template-columns:48px 1fr 40px;gap:10px;align-items:center;margin:6px 0}} .bar div{{background:#E4DDD2;border-radius:99px;height:12px;overflow:hidden}} .bar i{{display:block;height:100%;background:#0E6F99}}
.curve{{display:flex;align-items:flex-end;gap:4px;height:180px;background:#fff;border:1px solid #E4DDD2;border-radius:14px;padding:16px 12px 30px}}
.col{{flex:1;height:100%;display:flex;flex-direction:column;justify-content:flex-end;position:relative}} .col i{{display:block;background:#C9A46A;border-radius:4px 4px 0 0}} .col span{{position:absolute;bottom:-22px;left:0;right:0;text-align:center;font-size:10px;color:#6b7079}}
.wrap{{overflow-x:auto;background:#fff;border:1px solid #E4DDD2;border-radius:14px}} table{{width:100%;border-collapse:collapse;font-size:13px}} td,th{{padding:9px 12px;border-bottom:1px solid #eee;text-align:left;white-space:nowrap}} th{{color:#6b7079;font-weight:500}} .muted{{color:#9a9fa8}}
a{{color:#0E6F99}}</style></head><body><main>
<h1>VSL stats</h1><p class="sub">lumenmarketing.co/medspa · video length {_fmt(length)} · {"including" if show_me else "excluding"} your own visits (<a href="?me={'0' if show_me else '1'}">{"hide" if show_me else "show"} mine</a>)</p>
<div class="stats">{stat(len(visits), "page visits")}{stat(len(played), "played the video")}{stat(f"{(len(played) / len(visits) * 100) if visits else 0:.0f}%", "play rate")}
{stat(_fmt(avg_s), "average watch time")}{stat(f"{(avg_s / length * 100):.0f}%", "average % watched")}{stat(len(done), "watched to the end")}{stat(len(apps), "applications")}</div>
<h2>How far people got</h2>{marks}
<h2>Drop off, every 15 seconds</h2><div class="curve">{curve}</div>
<h2>Applications</h2><div class="wrap"><table><tr><th>When (UTC)</th><th>Spa</th><th>City</th><th>Revenue</th><th>Watched</th></tr>{app_rows or '<tr><td colspan=5 class="muted">None yet</td></tr>'}</table></div>
<h2>Every visit</h2><div class="wrap"><table><tr><th>When</th><th>Video</th><th>Watched to</th><th>%</th><th>Applied</th><th>Device</th><th>From</th></tr>{table or '<tr><td colspan=7 class="muted">No visits yet</td></tr>'}</table></div>
</main></body></html>"""
    return page
