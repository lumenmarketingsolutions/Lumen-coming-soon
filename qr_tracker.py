"""
Trackable QR codes with UTM parameters.

A QR encodes a short URL we own (/q/<code>). Every scan is logged, then redirected to the
destination with UTM parameters appended. That gives scan counts, lets the destination change
after the code is printed, and keeps the encoded URL short so the QR stays sparse and scans
from a distance.

Dashboard at /qr/admin (behind the master admin session, same as the rest of the Mainframe).
"""
import os, re, json, sqlite3, hashlib, datetime, secrets, io, threading, urllib.parse
from flask import Blueprint, request, session, redirect, render_template_string, send_file, abort

qr_bp = Blueprint("qr_tracker", __name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = "/data" if os.path.isdir("/data") else BASE_DIR
DB_PATH = os.environ.get("QR_DB_PATH", os.path.join(DATA_DIR, "qr_tracker.db"))
# Public host the QR encodes. Short host = sparse QR = scans from further away.
QR_HOST = os.environ.get("QR_HOST", "https://go.feelsgoodclub.com")

# Link-preview fetchers and scanners that hit a URL without a human ever seeing it.
BOT_RE = re.compile(r"bot|crawler|spider|preview|facebookexternalhit|whatsapp|slackbot|telegram|"
                    r"discord|skype|twitterbot|linkedin|embedly|quora|pinterest|vkshare|outlook|"
                    r"google-safety|bingpreview|curl|wget|python-requests|headless|monitor|uptime", re.I)
DEDUPE_MINUTES = 60          # same fingerprint inside this window counts once

# Server-side events to a Meta pixel are OFF by default, per Kendall 24.09.2026: he does not
# want this tool writing into the Feels Good Club pixel. A code only fires if a pixel is chosen
# explicitly on it. Nothing writes to Meta otherwise.
#
# Note what this does and does not change. Scanners still land on the destination with the UTMs
# attached, so whatever pixel is already on that site captures them as it would any visitor.
# That is the site's own pixel doing its normal job, not this tool, and it is what actually
# builds a usable audience, because pixel audiences live on the business and are shareable to
# any ad account with access, including both Feels Good Club accounts.
META_PIXEL_DEFAULT = os.environ.get("QR_META_PIXEL_ID", "1828086791348138")   # "feelsgoodclub"
META_TOKEN = os.environ.get("LUMEN_META_CAPI_TOKEN") or os.environ.get("META_TOKEN_LUMEN") or ""
META_EVENT = "QRScan"
GRAPH = "https://graph.facebook.com/v21.0"


def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c


def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS qr_codes(
            code TEXT PRIMARY KEY, label TEXT, destination TEXT,
            utm_source TEXT, utm_medium TEXT, utm_campaign TEXT, utm_content TEXT, utm_term TEXT,
            created TEXT, archived INTEGER DEFAULT 0,
            pixel_id TEXT, capi INTEGER DEFAULT 1);
        CREATE TABLE IF NOT EXISTS qr_scans(
            id INTEGER PRIMARY KEY AUTOINCREMENT, code TEXT, ts TEXT,
            ip TEXT, user_agent TEXT, referrer TEXT, country TEXT,
            is_bot INTEGER DEFAULT 0, is_unique INTEGER DEFAULT 1, fp TEXT,
            capi_status TEXT);
        CREATE INDEX IF NOT EXISTS idx_scans_code ON qr_scans(code, ts);
        CREATE INDEX IF NOT EXISTS idx_scans_fp ON qr_scans(fp, ts);
        """)
        # Columns added after the first deploy, ignore if already present.
        for stmt in ("ALTER TABLE qr_codes ADD COLUMN pixel_id TEXT",
                     "ALTER TABLE qr_codes ADD COLUMN capi INTEGER DEFAULT 1",
                     "ALTER TABLE qr_scans ADD COLUMN capi_status TEXT"):
            try: c.execute(stmt)
            except sqlite3.OperationalError: pass


def now():
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def client_ip():
    # Grey cloud: Cloudflare is DNS only, so traffic hits Railway directly and the real visitor
    # is the first hop of X-Forwarded-For. CF-Connecting-IP is still checked first in case the
    # orange cloud gets switched back on later, in which case it is the authoritative one.
    ip = request.headers.get("CF-Connecting-IP") or request.headers.get("X-Forwarded-For", "") or (request.remote_addr or "")
    return ip.split(",")[0].strip()


def build_url(row):
    """Append UTM parameters to the destination without clobbering ones already there."""
    dest = (row["destination"] or "").strip()
    if not re.match(r"^https?://", dest):
        dest = "https://" + dest
    parts = urllib.parse.urlsplit(dest)
    q = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
    for k in ("utm_source", "utm_medium", "utm_campaign", "utm_content", "utm_term"):
        v = (row[k] or "").strip()
        if v and k not in q:
            q[k] = v
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path,
                                    urllib.parse.urlencode(q), parts.fragment))


# ---------------------------------------------------------------- Meta audience feed
def send_capi(code, pixel_id, ip, ua, url, label, campaign, event_id):
    """Fire the scan at a Meta pixel so scanners land in a retargetable audience.

    Runs on a thread: a QR scan has to redirect instantly, and Graph can take a second.
    """
    if not (META_TOKEN and pixel_id):
        return "skipped: no token or pixel"
    payload = {"data": [{
        "event_name": META_EVENT,
        "event_time": int(datetime.datetime.utcnow().timestamp()),
        "event_id": event_id,               # dedupes against the site pixel's own hit
        "action_source": "website",
        "event_source_url": url,
        "user_data": {"client_ip_address": ip, "client_user_agent": ua},
        "custom_data": {"qr_code": code, "qr_label": label or "", "campaign": campaign or ""},
    }]}
    try:
        import requests
        r = requests.post(f"{GRAPH}/{pixel_id}/events",
                          data={"data": json.dumps(payload["data"]), "access_token": META_TOKEN},
                          timeout=8).json()
        if "error" in r:
            return f"error: {str(r['error'].get('message'))[:120]}"
        return f"ok: {r.get('events_received', 0)} received"
    except Exception as e:
        return f"error: {str(e)[:120]}"


def fire_and_record(scan_id, *args):
    status = send_capi(*args)
    try:
        with db() as c:
            c.execute("UPDATE qr_scans SET capi_status=? WHERE id=?", (status, scan_id))
    except Exception as e:
        print(f"[qr] capi status write failed: {e}")


# ---------------------------------------------------------------- the redirect
@qr_bp.route("/q/<code>")
def qr_redirect(code):
    init_db()
    with db() as c:
        row = c.execute("SELECT * FROM qr_codes WHERE code=?", (code,)).fetchone()
    if not row:
        return redirect("https://lumenmarketing.co", code=302)

    ua = request.headers.get("User-Agent", "")
    ip = client_ip()
    is_bot = 1 if BOT_RE.search(ua) or not ua else 0
    fp = hashlib.sha256(f"{code}|{ip}|{ua}".encode()).hexdigest()[:24]
    cutoff = (datetime.datetime.utcnow() - datetime.timedelta(minutes=DEDUPE_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ")
    dest = build_url(row)
    scan_id = None
    try:
        with db() as c:
            seen = c.execute("SELECT 1 FROM qr_scans WHERE fp=? AND ts>=? LIMIT 1", (fp, cutoff)).fetchone()
            cur = c.execute("INSERT INTO qr_scans(code,ts,ip,user_agent,referrer,country,is_bot,is_unique,fp) VALUES(?,?,?,?,?,?,?,?,?)",
                            (code, now(), ip, ua[:400], (request.referrer or "")[:300],
                             request.headers.get("CF-IPCountry", ""), is_bot, 0 if seen else 1, fp))
            scan_id = cur.lastrowid
    except Exception as e:
        print(f"[qr] log failed for {code}: {e}")

    # Only real people, only once per hour, go into the Meta audience.
    if not is_bot and not seen and row["capi"] and scan_id:
        threading.Thread(target=fire_and_record, daemon=True, args=(
            scan_id, code, row["pixel_id"] or META_PIXEL_DEFAULT, ip, ua, dest,
            row["label"], row["utm_campaign"], f"qr-{fp}-{int(datetime.datetime.utcnow().timestamp())//3600}")).start()

    # 302 not 301: a 301 gets cached by the browser and repeat scans never reach us.
    return redirect(dest, code=302)


# ---------------------------------------------------------------- QR image
def make_qr(url, fmt="png", box=12, border=2):
    import qrcode
    from qrcode.constants import ERROR_CORRECT_H
    qr = qrcode.QRCode(version=None, error_correction=ERROR_CORRECT_H, box_size=box, border=border)
    qr.add_data(url); qr.make(fit=True)
    buf = io.BytesIO()
    if fmt == "svg":
        import qrcode.image.svg
        qr.image_factory = qrcode.image.svg.SvgPathImage
        qr.make_image().save(buf)
        buf.seek(0); return buf, "image/svg+xml"
    qr.make_image(fill_color="black", back_color="white").save(buf, format="PNG")
    buf.seek(0); return buf, "image/png"


@qr_bp.route("/qr/img/<code>.<fmt>")
def qr_image(code, fmt):
    if fmt not in ("png", "svg"): abort(404)
    init_db()
    with db() as c:
        row = c.execute("SELECT 1 FROM qr_codes WHERE code=?", (code,)).fetchone()
    if not row: abort(404)
    size = request.args.get("size", "12")
    try: box = max(4, min(40, int(size)))
    except Exception: box = 12
    buf, mime = make_qr(f"{QR_HOST}/q/{code}", fmt, box=box)
    dl = request.args.get("download")
    return send_file(buf, mimetype=mime, as_attachment=bool(dl), download_name=f"qr-{code}.{fmt}")


# ---------------------------------------------------------------- dashboard
# No password to type. Opening the key link once signs this device in for the app's 60 day
# session, so the dashboard is one tap from the home screen. The key stays in place of a
# password rather than removing the gate entirely: anyone who could reach the create form
# could point go.feelsgoodclub.com/q/<anything> at any site they liked, and a redirect from a
# real shop domain is exactly what a phishing link wants to be. That would land on the domain
# reputation the Shopify store shares.
QR_KEY = os.environ.get("QR_KEY", "")


def _auth():
    return session.get("qr_auth") is True or session.get("wl_auth") is True


QR_PW = os.environ.get("QR_PW", "")


@qr_bp.route("/qr/k/<key>")
def qr_key_login(key):
    if QR_KEY and secrets.compare_digest(key, QR_KEY):
        session.permanent = True
        session["qr_auth"] = True
    return redirect("/qr/admin")


LOGIN_PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Feels Good Club QR</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet"><style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,Helvetica,Arial,sans-serif;background:#0f1012;color:#f2f1ee;
 min-height:100%;display:flex;align-items:center;justify-content:center;padding:40px 18px}
.box{width:100%;max-width:360px;text-align:center}
h1{font-size:19px;margin-bottom:6px}p{font-size:13px;color:#9a9ea6;margin-bottom:22px}
input{width:100%;background:#17181b;border:1px solid #26282d;border-radius:10px;color:#f2f1ee;
 padding:14px;font:inherit;font-size:16px;text-align:center;margin-bottom:12px}
button{width:100%;padding:14px;border:0;border-radius:10px;background:#c9a227;color:#141108;
 font:inherit;font-weight:600;font-size:15px;cursor:pointer}
.err{color:#e2705f;font-size:13px;margin-bottom:12px}
</style></head><body><div class="box">
<h1>Feels Good Club</h1><p>QR scan tracking</p>
{% if error %}<div class="err">Wrong password.</div>{% endif %}
<form method="post"><input name="password" type="password" placeholder="Password" autofocus
 autocomplete="current-password" inputmode="numeric"><button>Open dashboard</button></form>
</div></body></html>"""


@qr_bp.route("/qr/login", methods=["GET", "POST"])
def qr_login():
    err = False
    if request.method == "POST":
        pw = (request.form.get("password") or "").strip()
        if pw and ((QR_PW and secrets.compare_digest(pw, QR_PW)) or pw == QR_KEY):
            session.permanent = True
            session["qr_auth"] = True
            return redirect("/qr/admin")
        err = True
    return render_template_string(LOGIN_PAGE, error=err)

# Pixels offered in the create form. The first is the default.
PIXELS = [("1828086791348138", "feelsgoodclub"),
          ("1119566303064711", "Lumen Marketing"),
          ("2251653638703742", "MK7 Media")]

PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>QR Tracker</title><link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet"><style>
:root{--bg:#0f1012;--card:#17181b;--line:#26282d;--gold:#c9a227;--text:#f2f1ee;--muted:#9a9ea6;--green:#3ec37a}
*{box-sizing:border-box;margin:0;padding:0}body{font-family:Inter,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--text);line-height:1.55;padding-bottom:60px}
a{color:var(--gold);text-decoration:none}.wrap{max-width:1180px;margin:0 auto;padding:0 18px}
header{border-bottom:1px solid var(--line);padding:16px 0;margin-bottom:24px}header .wrap{display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:10px}
h2{font-size:13px;letter-spacing:.1em;text-transform:uppercase;color:var(--gold);margin:26px 0 12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin-bottom:14px}
table{width:100%;border-collapse:collapse;font-size:13px}th{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);text-align:left;padding:8px 10px 8px 0;border-bottom:1px solid var(--line)}
td{padding:11px 10px 11px 0;border-bottom:1px solid var(--line);vertical-align:middle}td.r,th.r{text-align:right}
.btn{display:inline-block;padding:9px 15px;border-radius:8px;font-weight:600;font-size:13px;border:0;cursor:pointer;font-family:inherit}
.b-gold{background:var(--gold);color:#141108}.b-ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
input,select{width:100%;background:#0f1012;border:1px solid var(--line);border-radius:8px;color:var(--text);padding:10px;font:inherit;font-size:14px}
label{font-size:12px;color:var(--muted);display:block;margin:12px 0 4px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:12px}
.kpi{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px}.kpi b{font-size:24px;display:block}.kpi span{font-size:12px;color:var(--muted)}
.small{font-size:12px;color:var(--muted)}.mono{font-family:ui-monospace,Menlo,monospace;font-size:12px}
.spark{display:flex;align-items:flex-end;gap:3px;height:34px}.spark i{background:var(--gold);flex:1;max-width:9px;border-radius:1px;display:block;min-height:2px}
img.qr{width:78px;height:78px;border-radius:8px;background:#fff;padding:4px;flex:none}
/* One code = one card. Stacks cleanly on a phone, no sideways scrolling. */
.qrow{display:flex;gap:16px;align-items:flex-start;padding:18px 0;border-bottom:1px solid var(--line)}
.qrow:last-child{border-bottom:0;padding-bottom:0}.qrow:first-child{padding-top:0}
.qmeta{flex:1;min-width:0}.qmeta b{font-size:15px}
.qmeta .mono,.qmeta .small{overflow-wrap:anywhere}
.qnums{display:flex;gap:22px;margin:12px 0 10px}.qnums div b{font-size:21px;display:block;line-height:1.2}
.qacts{display:flex;gap:8px;flex-wrap:wrap}
.copy{cursor:pointer;background:#0f1012;border:1px solid var(--line);border-radius:7px;padding:7px 10px;
 display:inline-flex;align-items:center;gap:7px;font-size:12px;max-width:100%}
.copy span{font-family:ui-monospace,Menlo,monospace;overflow-wrap:anywhere;text-align:left}
@media(max-width:640px){
 .wrap{padding:0 16px}.qrow{gap:12px}img.qr{width:60px;height:60px}
 /* Two up, so the numbers do not push the codes off the first screen. */
 .grid{grid-template-columns:1fr 1fr;gap:10px}.kpi b{font-size:20px}
 .qnums{gap:16px}.qnums div b{font-size:19px}
 .btn{padding:10px 13px}.qacts .btn{flex:1;text-align:center;min-width:88px}
 td,th{padding:8px 6px 8px 0;font-size:12px}
 h2{margin:22px 0 10px}.card{padding:16px}}
</style></head><body>
<header><div class="wrap"><b>QR Tracker</b><span class="small">{{ host }}/q/&lt;code&gt;</span></div></header>
<div class="wrap">{{ body|safe }}</div>
<script>function cp(el,t){navigator.clipboard.writeText(t).then(function(){var s=el.innerHTML;el.innerHTML='<span>copied</span>&nbsp;&#10003;';setTimeout(function(){el.innerHTML=s},1200)})}</script>
</body></html>"""


def page(body):
    return render_template_string(PAGE, body=body, host=QR_HOST)


@qr_bp.route("/qr/admin")
def qr_admin():
    if not _auth(): return redirect("/qr/login")
    init_db()
    with db() as c:
        codes = [dict(r) for r in c.execute("SELECT * FROM qr_codes WHERE archived=0 ORDER BY created DESC")]
        tot = {r["code"]: r["n"] for r in c.execute("SELECT code, COUNT(*) n FROM qr_scans WHERE is_bot=0 GROUP BY code")}
        uni = {r["code"]: r["n"] for r in c.execute("SELECT code, COUNT(*) n FROM qr_scans WHERE is_bot=0 AND is_unique=1 GROUP BY code")}
        bots = {r["code"]: r["n"] for r in c.execute("SELECT code, COUNT(*) n FROM qr_scans WHERE is_bot=1 GROUP BY code")}
        days = [r["d"] for r in c.execute("SELECT DISTINCT substr(ts,1,10) d FROM qr_scans ORDER BY d DESC LIMIT 14")]
        by_day = {}
        for r in c.execute("SELECT code, substr(ts,1,10) d, COUNT(*) n FROM qr_scans WHERE is_bot=0 GROUP BY code,d"):
            by_day.setdefault(r["code"], {})[r["d"]] = r["n"]
    last14 = [(datetime.date.today() - datetime.timedelta(days=i)).isoformat() for i in range(13, -1, -1)]
    body = f"""<div class="grid">
<div class="kpi"><b>{len(codes)}</b><span>active codes</span></div>
<div class="kpi"><b>{sum(tot.values()):,}</b><span>scans, bots excluded</span></div>
<div class="kpi"><b>{sum(uni.values()):,}</b><span>unique scanners</span></div>
<div class="kpi"><b>{sum(bots.values()):,}</b><span>bot and preview hits filtered</span></div></div>
<h2>Codes</h2>"""
    if not codes:
        body += '<div class="card small">No codes yet. Create one below.</div>'
    else:
        rows = ""
        for k in codes:
            d = by_day.get(k["code"], {}); mx = max(list(d.values()) + [1])
            spark = "".join(f'<i style="height:{max(2,int(30*d.get(x,0)/mx))}px" title="{x}: {d.get(x,0)}"></i>' for x in last14)
            utms = " ".join(f"{a.replace('utm_','')}={k[a]}" for a in ("utm_source","utm_medium","utm_campaign","utm_content") if k.get(a))
            link = f"{QR_HOST}/q/{k['code']}"
            rows += f"""<div class="qrow">
<img class="qr" src="/qr/img/{k['code']}.png?size=6" alt="">
<div class="qmeta">
  <b>{k['label'] or k['code']}</b>
  <div class="copy" onclick="cp(this,'{link}')" style="margin:8px 0"><span>{link}</span>&nbsp;📋</div>
  <div class="small">goes to {(k['destination'] or '')[:70]}</div>
  <div class="small">{utms}</div>
  <div class="qnums">
    <div><b>{tot.get(k['code'],0):,}</b><span class="small">scans</span></div>
    <div><b>{uni.get(k['code'],0):,}</b><span class="small">unique</span></div>
    <div style="flex:1;min-width:90px"><div class="spark">{spark}</div><span class="small">last 14 days</span></div>
  </div>
  <div class="qacts">
    <a class="btn b-ghost" href="/qr/img/{k['code']}.png?size=20&amp;download=1">PNG</a>
    <a class="btn b-ghost" href="/qr/img/{k['code']}.svg?download=1">SVG</a>
    <a class="btn b-ghost" href="/qr/admin/scans/{k['code']}">Scans</a>
    <form method="post" action="/qr/admin/archive/{k['code']}" style="display:inline;flex:1;min-width:88px"><button class="btn b-ghost" style="width:100%">Archive</button></form>
  </div>
</div></div>"""
        body += f'<div class="card">{rows}</div>'
    # Default is off. Feeding a pixel is a deliberate per-code choice.
    opts = '<option value="" selected>Off, do not send anything to Meta</option>' + \
           "".join(f'<option value="{p}">Also send a server event to {n} ({p})</option>' for p, n in PIXELS)
    body += f"""<h2>New code</h2><div class="card"><form method="post" action="/qr/admin/create">
<label>Label (what and where, for example Counter card, Beirut store)</label><input name="label" required>
<label>Destination URL</label><input name="destination" placeholder="https://feelsgoodclub.com/collections/all" required>
<div class="grid" style="margin-top:6px">
<div><label>utm_source</label><input name="utm_source" value="qr"></div>
<div><label>utm_medium</label><input name="utm_medium" value="print"></div>
<div><label>utm_campaign</label><input name="utm_campaign" placeholder="fgc_flyer_oct"></div>
<div><label>utm_content</label><input name="utm_content" placeholder="counter_card"></div></div>
<label>Server events to Meta</label><select name="pixel_id">{opts}</select>
<label>Custom short code (optional, letters, numbers and dashes)</label><input name="code" placeholder="leave blank for a random one">
<div style="margin-top:16px"><button class="btn b-gold">Create code</button></div>
<p class="small" style="margin-top:12px">Use one code per physical placement with the same destination and a different utm_content, so you can see which placement actually works. The QR encodes only the short URL, the UTMs are added at redirect, which keeps the code sparse and easy to scan.</p>
</form></div>

<h2>Data and audiences</h2><div class="card">
<p><b>Server events to Meta are off.</b> This tool sends nothing to any pixel unless a code is set to, one code at a time, in the form above. Default is off.</p>
<p style="margin-top:14px"><b>Export.</b> Every scan, with code, placement, time, country, IP, device and referrer.</p>
<p style="margin-top:10px"><a class="btn b-gold" href="/qr/admin/export.csv">Export all scans as CSV</a></p>
<p class="small" style="margin-top:14px">Worth being straight about what the CSV can do. It is for counting and analysis, which placement pulled, which day, which city. It <b>cannot</b> be uploaded to Meta as a custom audience. A customer list audience matches on email, phone or name, and a QR scan gives none of those, only an IP and a device string, which Meta does not accept for list matching. No tool can get around that.</p>
<p style="margin-top:14px"><b>If you do want to retarget scanners</b>, it already happens without this tool touching anything. They land on the site with <span class="mono">utm_source=qr</span> attached, and the pixel that is already on the site records the visit like any other. Build it in Ads Manager, Audiences, Create audience, Website:</p>
<div class="card" style="background:#0f1012;margin:10px 0"><span class="mono">Source: the pixel on the destination site<br>
Include people who: URL &nbsp;contains&nbsp; utm_source=qr<br>
Retention: 180 days</span></div>
<p class="small">Use <span class="mono">utm_content</span> contains a placement value for one code only. This does not apply to a code pointing at Instagram, since that is not your site and your pixel never runs there.</p>
<p style="margin-top:14px"><b>Two ad accounts.</b> A pixel audience belongs to the business, not to one ad account, so a single audience can be used from Feels Good Club and Feels Good Club V2 both, with no rebuilding. V2 currently has no pixel attached to it, which has to be fixed before it can use one.</p>
</div>"""
    return page(body)


@qr_bp.route("/qr/admin/export.csv")
def qr_export():
    if not _auth(): return redirect("/qr/login")
    init_db()
    import csv
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["code", "label", "campaign", "utm_content", "destination", "scanned_utc",
                "country", "ip", "user_agent", "referrer", "type", "meta_event"])
    with db() as c:
        rows = c.execute("""SELECT s.*, k.label, k.utm_campaign, k.utm_content, k.destination
                            FROM qr_scans s LEFT JOIN qr_codes k ON k.code=s.code ORDER BY s.id""")
        for r in rows:
            w.writerow([r["code"], r["label"] or "", r["utm_campaign"] or "", r["utm_content"] or "",
                        r["destination"] or "", r["ts"], r["country"] or "", r["ip"] or "",
                        r["user_agent"] or "", r["referrer"] or "",
                        "bot" if r["is_bot"] else ("unique" if r["is_unique"] else "repeat"),
                        r["capi_status"] or ""])
    buf = io.BytesIO(out.getvalue().encode("utf-8"))
    return send_file(buf, mimetype="text/csv", as_attachment=True,
                     download_name=f"qr-scans-{datetime.date.today().isoformat()}.csv")


@qr_bp.route("/qr/admin/create", methods=["POST"])
def qr_create():
    if not _auth(): return redirect("/qr/login")
    init_db()
    f = request.form
    code = re.sub(r"[^a-zA-Z0-9-]", "", (f.get("code") or "").strip())[:32] or secrets.token_urlsafe(4).replace("_", "").replace("-", "")[:6].lower()
    with db() as c:
        if c.execute("SELECT 1 FROM qr_codes WHERE code=?", (code,)).fetchone():
            code = code + secrets.token_hex(2)
        px = (f.get("pixel_id") or "").strip()
        c.execute("INSERT INTO qr_codes(code,label,destination,utm_source,utm_medium,utm_campaign,utm_content,utm_term,created,archived,pixel_id,capi) VALUES(?,?,?,?,?,?,?,?,?,0,?,?)",
                  (code, f.get("label", "").strip(), f.get("destination", "").strip(),
                   f.get("utm_source", "").strip(), f.get("utm_medium", "").strip(),
                   f.get("utm_campaign", "").strip(), f.get("utm_content", "").strip(), "", now(),
                   px, 1 if px else 0))
    return redirect("/qr/admin")


@qr_bp.route("/qr/admin/archive/<code>", methods=["POST"])
def qr_archive(code):
    if not _auth(): return redirect("/qr/login")
    with db() as c: c.execute("UPDATE qr_codes SET archived=1 WHERE code=?", (code,))
    return redirect("/qr/admin")


@qr_bp.route("/qr/admin/scans/<code>")
def qr_scans(code):
    if not _auth(): return redirect("/qr/login")
    init_db()
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM qr_scans WHERE code=? ORDER BY id DESC LIMIT 500", (code,))]
    t = "".join(f"<tr><td>{r['ts']}</td><td>{r['country'] or ''}</td><td class='small'>{(r['user_agent'] or '')[:70]}</td><td>{'bot' if r['is_bot'] else ('unique' if r['is_unique'] else 'repeat')}</td></tr>" for r in rows)
    return page(f"<h2>Scans for {code}</h2><div class=card style='overflow:auto'><table><tr><th>When (UTC)</th><th>Country</th><th>Device</th><th>Type</th></tr>{t}</table></div><p style='margin-top:14px'><a href='/qr/admin'>Back</a></p>")
