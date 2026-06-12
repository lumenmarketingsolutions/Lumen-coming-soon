"""
Supercar Experience Boise — Corvette C8 Z51 rental inquiry funnel.

Single-page inquiry flow:
  GET  /z51                  → landing with car hero, description, specs, duration picker, lead form
  POST /z51/optin            → save lead, fire Meta Lead event, redirect to Valara iframe checkout
  POST /z51/track            → visit-time tracking

Checkout happens in Valara (real availability calendar + bookings land directly
in the rental platform). Meta optimization target is the Lead event we fire
when the customer submits our form — Valara doesn't surface Purchase events
into Meta, but we don't need that for ad optimization.

Pricing config is pulled from Valara `bookings_calendar/available_slots`
(verified 2026-06-12). Update RENTAL_PRICING below when Nate adjusts rates.
"""

import os
import sqlite3
import datetime
import uuid
import json
import time
import hashlib
import threading
import requests
try:
    from zoneinfo import ZoneInfo
    _BOISE_TZ = ZoneInfo("America/Boise")
except Exception:
    _BOISE_TZ = datetime.timezone(datetime.timedelta(hours=-6))
from flask import Blueprint, render_template, request, redirect, url_for, jsonify

sce_z51_bp = Blueprint("sce_z51", __name__)


# ─────────────────── Configuration ───────────────────

BASE_URL = os.environ.get("Z51_BASE_URL", "https://supercarexp.lumenmarketing.co")

# The full Valara iframe booking URL (single-use signed link for this product).
# Update if Nate regenerates the signed link.
VALARA_IFRAME_URL = (
    "https://app.valara.io/iframe/products/68dc7cb7-8028-4ee6-865a-4a0b32a3d62c"
    "?sig=aHEvb2R4YmxYWEVnV0dMUmNqVnBKSDNRT1pSeW92WFI5c2JjTTZRL3F3d2VraUFvZzhpVXM4VU9BcWZxWGw2"
    "d2xjVHJJaytIeENmWE9nNVVMTEg3djJrdTE0ZnJJSzVESW8yY1ZDRHowRXRQM29hUEdwM1BwMnRMUnRac2pkZ242"
    "TW8xL2dkTHRmbXVBeTBCL28zU2xsZkdpOXZ0NFl3cUpZMW01RmJzYjNFYUtNZUFCVWFJTk0yMUhUNkYrR3U1Z0s2"
    "ZGwzZGthUzd5VVJpczFLZFg2Z0NtdjNZQmlWU2xEMGZjMTRaTlhoaitLQ3J1RmZySmFFbWs5ZFBwRktCY29rc1Z2"
    "Si9lNC96NlVkRUh4RE80V2FIYWtrM3cyM3E1b0JZUW1TUHE4S1k4Q1ZDNFhvND0tLWZ6aWp4TDQrbUlaS25iSlAt"
    "LW11Y00wQjhPYXEwekNodmRMM1I4a2c9PQ"
    "&text=Show+Availability&color=%23ff4d00&text_color=%23FFFFFF"
    "&ref=https%3A%2F%2Fsupercarexp.lumenmarketing.co%2Fz51"
)

# Duration menu — labels + price displayed in the picker.
# Prices sourced from Valara bookings_calendar/available_slots, 2026-06-12.
DURATIONS = [
    {"id": "4h",  "label": "4 hours",  "sub": "Quick rip",                "price": 350},
    {"id": "8h",  "label": "8 hours",  "sub": "Half-day cruise",          "price": 475},
    {"id": "1d",  "label": "1 day",    "sub": "Full 24 hours",            "price": 599},
    {"id": "2d",  "label": "2 days",   "sub": "Weekend run",              "price": 899},
    {"id": "3d",  "label": "3 days",   "sub": "Long weekend",             "price": 1198},
]
DURATION_BY_ID = {d["id"]: d for d in DURATIONS}

# Pixel + CAPI — shared with other SCE funnels
META_PIXEL_ID   = os.environ.get("META_PIXEL_ID_SCE", "1514374663034732")
META_CAPI_TOKEN = os.environ.get("META_CAPI_TOKEN_SCE", "")

# Resend lead notification
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAILS  = ["kendall@lumenmarketing.co", "n.wilkinson@launchpoint.dev"]
NOTIFY_FROM    = "SCE Boise <kendall@lumenmarketing.co>"

# DB
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH  = os.path.join(DATA_DIR, "waitlist.db")


# ─────────────────────────── DB ───────────────────────────

def init_z51_db():
    """Idempotent DB setup."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS z51_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            session_id TEXT DEFAULT '',
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            phone TEXT DEFAULT '',
            duration TEXT NOT NULL,
            price_dollars INTEGER NOT NULL,
            special_notes TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_medium TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            utm_term TEXT DEFAULT '',
            fbp TEXT DEFAULT '',
            fbc TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_z51_leads_email ON z51_leads(email)")
    con.execute("""
        CREATE TABLE IF NOT EXISTS z51_visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            page TEXT NOT NULL,
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            entered_at TEXT NOT NULL,
            time_on_page_ms INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()


# ─────────────────────────── Helpers ───────────────────────────

def _sha256(v):
    if not v:
        return ""
    return hashlib.sha256(v.strip().lower().encode("utf-8")).hexdigest()


def _digits_only(v):
    return "".join(c for c in (v or "") if c.isdigit())


def _client_ip():
    fwd = request.headers.get("X-Forwarded-For", "") or ""
    if fwd:
        return fwd.split(",")[0].strip()
    return request.remote_addr or ""


def _format_boise_now():
    now = datetime.datetime.now(_BOISE_TZ)
    return now.strftime("%b %-d, %Y · %-I:%M %p %Z").replace("  ", " ")


def _build_valara_url(*, email, name, phone):
    """Append customer info to Valara iframe URL as a maybe-prefill attempt.

    Valara may or may not honor these params — if they do, the customer
    skips re-entering their info at checkout. If they don't, the params
    are ignored and the customer enters info normally. Either way no harm.
    """
    extras = []
    if email:
        extras.append(f"customer_email={requests.utils.quote(email)}")
        extras.append(f"email={requests.utils.quote(email)}")
    if name:
        extras.append(f"customer_name={requests.utils.quote(name)}")
        extras.append(f"name={requests.utils.quote(name)}")
    if phone:
        extras.append(f"customer_phone={requests.utils.quote(phone)}")
        extras.append(f"phone={requests.utils.quote(phone)}")
    sep = "&" if "?" in VALARA_IFRAME_URL else "?"
    return VALARA_IFRAME_URL + (sep + "&".join(extras) if extras else "")


# ─────────────────────── Lead email ───────────────────────

def _build_lead_email_html(c):
    notes_html = (c["notes"] or "<span style='color:#9aa0a6'>None</span>").replace("\n", "<br>")
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f1ec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1A1A1A;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f4f1ec;padding:32px 16px;">
<tr><td align="center">
<table cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.06);">
<tr><td style="background:#ff4d00;padding:22px 32px;color:#fff;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;font-weight:600;opacity:0.9;">SCE Boise · Corvette C8 Z51</div>
<div style="font-size:22px;font-weight:700;margin-top:6px;">New rental inquiry</div></td></tr>
<tr><td style="padding:28px 32px 8px;">
<div style="font-size:24px;font-weight:700;">{c['name'] or '(no name given)'}</div>
<div style="color:#5C5C5C;font-size:14px;margin-top:6px;">Corvette C8 Z51 · {c['duration_label']}</div>
<div style="font-size:38px;font-weight:700;color:#ff4d00;margin-top:14px;line-height:1;">${c['price_str']}</div>
</td></tr>
<tr><td style="padding:28px 32px 0;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Contact</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:14px;">
<tr><td style="padding:6px 0;color:#5C5C5C;width:90px;">Email</td><td style="color:#1A1A1A;"><a href="mailto:{c['email']}" style="color:#1A1A1A;text-decoration:none;">{c['email']}</a></td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;">Phone</td><td style="color:#1A1A1A;"><a href="tel:{c['phone_tel']}" style="color:#1A1A1A;text-decoration:none;">{c['phone'] or '(none)'}</a></td></tr>
</table></td></tr>
<tr><td style="padding:24px 32px 0;">
  <a href="tel:{c['phone_tel']}" style="display:inline-block;background-color:#ff4d00;color:#ffffff;text-decoration:none;padding:14px 24px;border-radius:10px;font-weight:600;font-size:14px;margin-right:8px;margin-bottom:8px;">Call them</a>
  <a href="sms:{c['phone_tel']}" style="display:inline-block;background-color:#ffffff;color:#ff4d00;text-decoration:none;padding:12.5px 22.5px;border-radius:10px;font-weight:600;font-size:14px;border:1.5px solid #ff4d00;margin-bottom:8px;">Text them</a>
</td></tr>
<tr><td style="padding:28px 32px 0;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Notes</div>
<div style="background:#FAF7F2;border-radius:10px;padding:14px 16px;">
<div style="color:#1A1A1A;font-size:15px;line-height:1.55;">{notes_html}</div></div></td></tr>
<tr><td style="padding:28px 32px 28px;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Attribution</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:12px;color:#5C5C5C;">
<tr><td style="padding:4px 0;">Source</td><td style="text-align:right;color:#1A1A1A;">{c['utm_source'] or '—'}</td></tr>
<tr><td style="padding:4px 0;">Campaign</td><td style="text-align:right;color:#1A1A1A;">{c['utm_campaign'] or '—'}</td></tr>
<tr><td style="padding:4px 0;">Event ID</td><td style="text-align:right;font-family:'SF Mono',Menlo,monospace;color:#1A1A1A;">{c['event_id']}</td></tr>
<tr><td style="padding:4px 0;">Submitted</td><td style="text-align:right;color:#1A1A1A;">{c['submitted_at']}</td></tr>
</table></td></tr>
<tr><td style="background:#FAF7F2;padding:16px 32px;text-align:center;">
<div style="font-size:11px;color:#5C5C5C;letter-spacing:0.06em;">Lumen Mainframe · SCE Z51 funnel</div>
</td></tr>
</table></td></tr></table>
</body></html>"""


def _send_lead_email(ctx):
    if not RESEND_API_KEY:
        print("[SCE-Z51 email] RESEND_API_KEY not set, skipping notification")
        return
    subject = f"New Z51 Inquiry · {ctx['name'] or 'Anonymous'} · {ctx['duration_label']} · ${ctx['price_str']}"
    payload = {
        "from": NOTIFY_FROM, "to": NOTIFY_EMAILS, "subject": subject,
        "html": _build_lead_email_html(ctx), "reply_to": ctx["email"],
    }
    def _send():
        try:
            r = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"},
                json=payload, timeout=8,
            )
            if r.status_code >= 300:
                print(f"[SCE-Z51 email] {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"[SCE-Z51 email] exception: {e}")
    threading.Thread(target=_send, daemon=True).start()


# ─────────────────── Meta CAPI ───────────────────

def _capi_fire(event_name, event_id, source_url, user_data, custom_data=None):
    if not META_CAPI_TOKEN or not META_PIXEL_ID:
        return
    payload_event = {
        "event_name": event_name, "event_time": int(time.time()),
        "event_id": event_id, "action_source": "website",
        "event_source_url": source_url,
        "user_data": {k: v for k, v in user_data.items() if v},
    }
    if custom_data:
        payload_event["custom_data"] = custom_data
    body = {"data": [payload_event], "access_token": META_CAPI_TOKEN}
    def _send():
        try:
            r = requests.post(f"https://graph.facebook.com/v19.0/{META_PIXEL_ID}/events", json=body, timeout=4)
            if r.status_code != 200:
                print(f"[CAPI] {event_name} {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"[CAPI] {event_name} exception: {e}")
    threading.Thread(target=_send, daemon=True).start()


def _ctx():
    return {
        "pixel_id": META_PIXEL_ID,
        "support_phone": "(208) 914-5640",
        "support_phone_tel": "+12089145640",
    }


# ─────────────────────────── Routes ───────────────────────────

@sce_z51_bp.route("/z51")
def landing():
    # Step 1 — pick duration. Preserve any preset choice via ?dur=...
    preset = (request.args.get("dur") or "").strip()
    return render_template(
        "sce_z51_landing.html",
        durations=DURATIONS,
        preset_duration=preset if preset in DURATION_BY_ID else "",
        **_ctx(),
    )


@sce_z51_bp.route("/z51/info")
def info():
    # Step 2 — contact form. Requires a duration in query params.
    duration_id = (request.args.get("dur") or "").strip()
    if duration_id not in DURATION_BY_ID:
        return redirect(url_for("sce_z51.landing"))
    duration = DURATION_BY_ID[duration_id]
    event_id = uuid.uuid4().hex
    return render_template(
        "sce_z51_info.html",
        duration=duration,
        event_id=event_id,
        back_url=url_for("sce_z51.landing") + f"?dur={duration_id}",
        **_ctx(),
    )


@sce_z51_bp.route("/z51/optin", methods=["POST"])
def optin():
    duration_id = (request.form.get("duration") or "").strip()
    name        = (request.form.get("name") or "").strip()
    email       = (request.form.get("email") or "").strip().lower()
    phone       = (request.form.get("phone") or "").strip()
    notes       = (request.form.get("special_notes") or "").strip()[:1000]
    event_id    = (request.form.get("event_id") or "").strip() or uuid.uuid4().hex
    session_id  = (request.form.get("session_id") or "").strip()[:64]

    if duration_id not in DURATION_BY_ID:
        return redirect(url_for("sce_z51.landing") + "?err=duration")
    if not email or "@" not in email:
        return redirect(url_for("sce_z51.landing") + "?err=email")

    duration = DURATION_BY_ID[duration_id]
    price = duration["price"]

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO z51_leads
                (event_id, session_id, name, email, phone, duration, price_dollars, special_notes,
                 ip, user_agent, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                 fbp, fbc, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, session_id, name, email, phone,
            duration_id, price, notes,
            request.headers.get("X-Forwarded-For", request.remote_addr or ""),
            request.headers.get("User-Agent", ""),
            request.referrer or "",
            request.form.get("utm_source", ""),
            request.form.get("utm_medium", ""),
            request.form.get("utm_campaign", ""),
            request.form.get("utm_content", ""),
            request.form.get("utm_term", ""),
            request.cookies.get("_fbp", ""),
            request.cookies.get("_fbc", ""),
            now,
        ))
        con.commit()
    finally:
        con.close()

    phone_digits = _digits_only(phone)
    if len(phone_digits) == 10:
        phone_tel = f"+1{phone_digits}"
    elif len(phone_digits) == 11 and phone_digits.startswith("1"):
        phone_tel = f"+{phone_digits}"
    else:
        phone_tel = phone_digits

    _send_lead_email({
        "name": name, "email": email, "phone": phone, "phone_tel": phone_tel,
        "duration_label": duration["label"],
        "price_str": "{:,}".format(price),
        "utm_source":   request.form.get("utm_source", ""),
        "utm_medium":   request.form.get("utm_medium", ""),
        "utm_campaign": request.form.get("utm_campaign", ""),
        "event_id": event_id,
        "submitted_at": _format_boise_now(),
        "notes": notes,
    })

    # Meta CAPI Lead — same event_id as browser fbq Lead so they dedupe
    name_parts = name.split()
    fn = name_parts[0] if name_parts else ""
    ln = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    _capi_fire(
        event_name="Lead", event_id=event_id,
        source_url=f"{request.scheme}://{request.host}/z51",
        user_data={
            "em": _sha256(email),
            "ph": _sha256(_digits_only(phone)),
            "fn": _sha256(fn),
            "ln": _sha256(ln),
            "client_ip_address": _client_ip(),
            "client_user_agent": request.headers.get("User-Agent", ""),
            "fbp": request.cookies.get("_fbp", ""),
            "fbc": request.cookies.get("_fbc", ""),
        },
        custom_data={
            "currency": "USD", "value": price,
            "content_ids": [f"Z51-{duration_id.upper()}"],
            "content_name": f"Corvette C8 Z51 · {duration['label']}",
            "content_type": "product",
        },
    )

    # Redirect to Valara iframe — append customer info as maybe-prefill
    return redirect(_build_valara_url(email=email, name=name, phone=phone))


@sce_z51_bp.route("/z51/track", methods=["POST", "OPTIONS"])
def track():
    if request.method == "OPTIONS":
        resp = jsonify({"ok": True})
        resp.headers["Access-Control-Allow-Origin"] = "*"
        return resp
    try:
        data = request.get_json(silent=True) or {}
    except Exception:
        data = {}
    sid    = (data.get("sid") or "").strip()[:64]
    page   = (data.get("page") or "").strip()[:32]
    kind   = (data.get("kind") or "view").strip()[:16]
    if not sid or not page:
        return jsonify({"ok": False, "err": "bad_payload"}), 400

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    try:
        if kind == "view":
            con.execute("""
                INSERT INTO z51_visits
                    (session_id, page, ip, user_agent, referrer, entered_at, time_on_page_ms, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, 0, ?)
            """, (
                sid, page,
                _client_ip(),
                request.headers.get("User-Agent", ""),
                (data.get("ref") or "")[:256],
                now, now,
            ))
        elif kind == "time":
            ms = int(data.get("ms") or 0)
            if ms > 0:
                con.execute("""
                    UPDATE z51_visits SET time_on_page_ms = ?, updated_at = ?
                    WHERE id = (
                        SELECT id FROM z51_visits
                        WHERE session_id = ? AND page = ?
                        ORDER BY id DESC LIMIT 1
                    )
                """, (ms, now, sid, page))
        con.commit()
    finally:
        con.close()
    resp = jsonify({"ok": True})
    resp.headers["Access-Control-Allow-Origin"] = "*"
    return resp
