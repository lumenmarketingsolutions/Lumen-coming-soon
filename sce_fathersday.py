"""
Supercar Experience Boise — Father's Day funnel.

Multi-step build-your-own package:
  Step 1 — Pick a car (C8 Z06, GT3RS, Urus S, G63)
  Step 2 — Pick a duration (4 hour, 8 hour, 24 hour)
  Step 3 — Add Anderson Ranch dinner gift card ($100 / $150 / $200) or skip
  Step 4 — Add Modern BBQ Supply gift card ($25 / $50 / $100) or skip
  Step 5 — Review the package + capture contact info → Stripe Checkout

State lives in URL query params (no sessions). Back button works naturally.
Server is the source of truth on price. The `ar` query-param key dates
from when step 3 was Anderson Reserve bourbon; the initials still fit
Anderson Ranch dinner, so the internal key stays `ar` to avoid a wide
rename across the leads table, Stripe metadata, and email templates.

Routes:
  GET  /fathersday                    → landing
  GET  /fathersday/car                → step 1 (car)
  GET  /fathersday/duration           → step 2 (?car=…)
  GET  /fathersday/dinner             → step 3 (?car=…&dur=…)
  GET  /fathersday/bbq                → step 4 (?car=…&dur=…&ar=…)
  GET  /fathersday/review             → step 5 (?car=…&dur=…&ar=…&mbs=…)
  POST /fathersday/optin              → save lead → Stripe Checkout Session → redirect
  GET  /fathersday/booked             → thank-you (after Stripe success)
  POST /fathersday/stripe-webhook     → Stripe event sink (Purchase CAPI)

`ar=0` or absent → no Anderson Ranch dinner gift card. Same for `mbs=0`.
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

sce_fd_bp = Blueprint("sce_fd", __name__)

# ─────────────────── Configuration (edit pricing here) ───────────────────

BASE_URL = os.environ.get("FD_BASE_URL", "https://supercarexp.lumenmarketing.co")

STRIPE_SECRET_KEY = (
    os.environ.get("STRIPE_SECRET_KEY_SCE")
    or os.environ.get("STRIPE_SECRET_KEY")
    or ""
)

CARS = [
    {"id": "c8z06", "name": "Corvette C8 Z06",       "short": "C8 Z06",  "image": "c8z06.jpg",
     "blurb": "Naturally aspirated V8. 670hp. Sounds like a Ferrari."},
    {"id": "gt3rs", "name": "Porsche 911 GT3RS",      "short": "GT3RS",   "image": "gt3rs.jpg",
     "blurb": "Track-focused flat-six. The sharpest car in the fleet."},
    {"id": "urus",  "name": "Lamborghini Urus S",     "short": "Urus S",  "image": "urus.jpg",
     "blurb": "Twin-turbo V8 SUV. 5 seats. Loud, fast, comfortable."},
    {"id": "g63",   "name": "Mercedes-AMG G63",       "short": "G63",     "image": "g63.jpg",
     "blurb": "V8 brick on wheels. 5 seats. Pure presence."},
]
CAR_BY_ID = {c["id"]: c for c in CARS}

DURATIONS = [
    {"id": "4h",  "label": "4 hours",  "hours": 4,
     "desc": "Half day. Cruise the mountains, dinner downtown."},
    {"id": "8h",  "label": "8 hours",  "hours": 8,
     "desc": "Full day. Real road trip, take the long way home."},
    {"id": "24h", "label": "24 hours", "hours": 24,
     "desc": "Keep it for the full Father's Day weekend run."},
]
DURATION_BY_ID = {d["id"]: d for d in DURATIONS}

# Base SCE rental pricing in whole dollars, per (car_id, duration_id).
# Placeholder values from Valara base rates — update when Nate confirms final pricing.
RENTAL_PRICING = {
    ("c8z06", "4h"):  399,
    ("c8z06", "8h"):  599,
    ("c8z06", "24h"): 799,
    ("gt3rs", "4h"):  599,
    ("gt3rs", "8h"):  799,
    ("gt3rs", "24h"): 1099,
    ("urus",  "4h"):  399,
    ("urus",  "8h"):  549,
    ("urus",  "24h"): 699,
    ("g63",   "4h"):  199,
    ("g63",   "8h"):  250,
    ("g63",   "24h"): 350,
}

ANDERSON_RANCH_VALUES     = [100, 150, 200]   # 0 (or absent) = skipped
MODERN_BBQ_SUPPLY_VALUES  = [25, 50, 100]     # 0 (or absent) = skipped

BUNDLE_PREMIUM = 0  # Set >0 if SCE wants a flat bundle markup

# ─── 10% Father's Day discount ───
# Discount displayed on the review page AND applied at Stripe Checkout. Two
# wire-up paths and we support both:
#   1) STRIPE_FD_COUPON env var set → attach that coupon ID to the Checkout
#      Session so Stripe applies it server-side (cleanest — appears on the
#      Stripe receipt as a discount line).
#   2) STRIPE_FD_COUPON unset → fall back to reducing each line item by
#      FD_DISCOUNT_PCT inline, so the funnel still ships the discount even
#      without the coupon configured. Receipt won't show "10% off" in this
#      mode, just the reduced prices.
FD_DISCOUNT_PCT  = 10
STRIPE_FD_COUPON = os.environ.get("STRIPE_FD_COUPON", "").strip()

# Pixel + CAPI
META_PIXEL_ID   = os.environ.get("META_PIXEL_ID_SCE", "1514374663034732")
META_CAPI_TOKEN = os.environ.get("META_CAPI_TOKEN_SCE", "")

# Resend lead notification
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAILS  = ["kendall@lumenmarketing.co", "n.wilkinson@launchpoint.dev"]
NOTIFY_FROM    = "SCE Father's Day <kendall@lumenmarketing.co>"

# DB
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH  = os.path.join(DATA_DIR, "waitlist.db")


# ─────────────────────────── DB ───────────────────────────

def init_fd_db():
    """Idempotent DB setup for the Father's Day funnel."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fathersday_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            session_id TEXT DEFAULT '',
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            phone TEXT DEFAULT '',
            car TEXT NOT NULL,
            duration TEXT NOT NULL,
            ar_value INTEGER NOT NULL,
            mbs_value INTEGER NOT NULL,
            sku TEXT NOT NULL,
            price_cents INTEGER NOT NULL,
            stripe_session_id TEXT DEFAULT '',
            stripe_url TEXT DEFAULT '',
            status TEXT DEFAULT 'opt-in',
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
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_fd_leads_email ON fathersday_leads(email)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_fd_leads_status ON fathersday_leads(status)")
    con.execute("""
        CREATE TABLE IF NOT EXISTS fathersday_visits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            page TEXT NOT NULL,
            car TEXT DEFAULT '',
            duration TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            entered_at TEXT NOT NULL,
            time_on_page_ms INTEGER DEFAULT 0,
            updated_at TEXT NOT NULL
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS fathersday_purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stripe_session_id TEXT UNIQUE NOT NULL,
            stripe_payment_intent TEXT DEFAULT '',
            stripe_customer TEXT DEFAULT '',
            email TEXT DEFAULT '',
            sku TEXT DEFAULT '',
            amount_cents INTEGER DEFAULT 0,
            currency TEXT DEFAULT 'usd',
            lead_id INTEGER,
            raw TEXT DEFAULT '',
            created_at TEXT NOT NULL
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


def _parse_int(v, allowed=None):
    """Return int(v) if it's in `allowed` (or any non-negative int if `allowed` is None).
    Returns 0 on any parse failure."""
    try:
        n = int(v)
    except (TypeError, ValueError):
        return 0
    if n < 0:
        return 0
    if allowed is not None and n not in allowed:
        return 0
    return n


def _read_build_from_request():
    """Read current build state from query params (for GET) or form (for POST).
    Returns a normalized dict. Skipped gift cards come back as 0."""
    g = request.values  # works for both args + form
    car      = (g.get("car") or "").strip()
    duration = (g.get("dur") or g.get("duration") or "").strip()
    ar       = _parse_int(g.get("ar"),  allowed=[0] + ANDERSON_RANCH_VALUES)
    mbs      = _parse_int(g.get("mbs"), allowed=[0] + MODERN_BBQ_SUPPLY_VALUES)
    return {
        "car": car if car in CAR_BY_ID else "",
        "duration": duration if duration in DURATION_BY_ID else "",
        "ar": ar, "mbs": mbs,
    }


def _step_url(step, build, **extra):
    """Build a URL for any step preserving the build state. `extra` overrides params."""
    params = {}
    if build.get("car"):      params["car"] = build["car"]
    if build.get("duration"): params["dur"] = build["duration"]
    # ar/mbs always sent so 0 is preserved (skip = 0)
    params["ar"]  = str(build.get("ar", 0))
    params["mbs"] = str(build.get("mbs", 0))
    params.update({k: str(v) for k, v in extra.items()})
    path = url_for(f"sce_fd.{step}")
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    return f"{path}?{qs}" if qs else path


def _calc_total(car_id, duration_id, ar_value, mbs_value):
    base = RENTAL_PRICING.get((car_id, duration_id), 0)
    return base + int(ar_value) + int(mbs_value) + BUNDLE_PREMIUM


def _build_sku(car_id, duration_id, ar_value, mbs_value):
    return f"FD-{car_id.upper()}-{duration_id.upper()}-AR{ar_value}-MBS{mbs_value}"


# ─────────────────── Stripe Checkout Session ───────────────────

def _create_stripe_session(*, email, name, phone, event_id, car, duration, ar_value, mbs_value, total_dollars):
    if not STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY not set.")

    base_rental = RENTAL_PRICING.get((car["id"], duration["id"]), 0)
    sku = _build_sku(car["id"], duration["id"], ar_value, mbs_value)

    line_items = [(f"{car['name']} · {duration['label']}", base_rental)]
    if int(ar_value) > 0:
        line_items.append((f"Anderson Ranch Dinner · ${ar_value} gift card", int(ar_value)))
    if int(mbs_value) > 0:
        line_items.append((f"Modern BBQ Supply · ${mbs_value} gift card", int(mbs_value)))
    if BUNDLE_PREMIUM > 0:
        line_items.append(("Father's Day Bundle", BUNDLE_PREMIUM))

    line_total = sum(amt for _, amt in line_items)
    if line_total != total_dollars:
        raise RuntimeError(f"Internal price mismatch: {line_total} vs {total_dollars}")

    data = [
        ("mode", "payment"),
        ("customer_email", email),
        ("client_reference_id", event_id),
        ("success_url", f"{BASE_URL}/fathersday/booked?session_id={{CHECKOUT_SESSION_ID}}"),
        ("cancel_url",  f"{BASE_URL}/fathersday/review?canceled=1&car={car['id']}&dur={duration['id']}&ar={ar_value}&mbs={mbs_value}"),
        ("metadata[event_id]", event_id),
        ("metadata[sku]",      sku),
        ("metadata[car]",      car["id"]),
        ("metadata[duration]", duration["id"]),
        ("metadata[ar]",       str(ar_value)),
        ("metadata[mbs]",      str(mbs_value)),
        ("metadata[name]",     name[:100]),
        ("metadata[phone]",    phone[:32]),
        ("phone_number_collection[enabled]", "true"),
    ]

    # 10% Father's Day discount — applied via Stripe coupon when configured.
    # If STRIPE_FD_COUPON is set we send full-price line items + a coupon ID,
    # and Stripe applies the discount server-side (shows on receipt as a line
    # item discount). If unset, we reduce each line item by 10% inline so the
    # funnel still ships the right charged amount.
    use_coupon = bool(STRIPE_FD_COUPON)
    if use_coupon:
        data.append(("discounts[0][coupon]", STRIPE_FD_COUPON))
    else:
        # Fall back to letting customers paste their own promotion codes too,
        # so a manually-created promo code can ride this funnel if needed.
        data.append(("allow_promotion_codes", "true"))

    discount_factor = 1.0 if use_coupon else (1.0 - FD_DISCOUNT_PCT / 100.0)
    for i, (name_line, dollars) in enumerate(line_items):
        cents = int(round(dollars * 100 * discount_factor))
        data.append((f"line_items[{i}][quantity]", "1"))
        data.append((f"line_items[{i}][price_data][currency]", "usd"))
        data.append((f"line_items[{i}][price_data][unit_amount]", str(cents)))
        data.append((f"line_items[{i}][price_data][product_data][name]", name_line))

    r = requests.post(
        "https://api.stripe.com/v1/checkout/sessions",
        auth=(STRIPE_SECRET_KEY, ""),
        data=data, timeout=15,
    )
    if r.status_code >= 300:
        # Surface enough detail in the exception to debug from Railway logs.
        # Most common failure with the new coupon flow: 'No such coupon: FD10'
        # → coupon was created in test mode but secret key is live (or vice
        # versa). Easy fix: recreate coupon in the matching mode.
        coupon_dbg = f"coupon={STRIPE_FD_COUPON or '(none)'}"
        raise RuntimeError(
            f"Stripe error {r.status_code} ({coupon_dbg}): {r.text[:600]}"
        )
    body = r.json()
    return body.get("url", ""), body.get("id", "")


# ─────────────────────────── Lead email ───────────────────────────

def _build_lead_email_html(c):
    notes_html = (c["notes"] or "<span style='color:#9aa0a6'>None</span>").replace("\n", "<br>")
    ar_row = ""
    mbs_row = ""
    if c["ar_value"] > 0:
        ar_row = f"<tr><td style='padding:6px 0;color:#5C5C5C;'>Anderson Ranch</td><td style='color:#1A1A1A;text-align:right;'>${c['ar_value']}</td></tr>"
    if c["mbs_value"] > 0:
        mbs_row = f"<tr><td style='padding:6px 0;color:#5C5C5C;'>Modern BBQ Supply</td><td style='color:#1A1A1A;text-align:right;'>${c['mbs_value']}</td></tr>"
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f1ec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1A1A1A;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f4f1ec;padding:32px 16px;">
<tr><td align="center">
<table cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.06);">
<tr><td style="background:#ff4d00;padding:22px 32px;color:#fff;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;font-weight:600;opacity:0.9;">SCE Boise · Father's Day</div>
<div style="font-size:22px;font-weight:700;margin-top:6px;">New lead</div></td></tr>
<tr><td style="padding:28px 32px 8px;">
<div style="font-size:24px;font-weight:700;">{c['name'] or '(no name given)'}</div>
<div style="color:#5C5C5C;font-size:14px;margin-top:6px;">{c['car_name']} · {c['duration_label']}</div>
<div style="font-size:38px;font-weight:700;color:#ff4d00;margin-top:14px;line-height:1;">${c['price_str']}</div>
</td></tr>
<tr><td style="padding:28px 32px 0;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Contact</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:14px;">
<tr><td style="padding:6px 0;color:#5C5C5C;width:90px;">Email</td><td style="color:#1A1A1A;"><a href="mailto:{c['email']}" style="color:#1A1A1A;text-decoration:none;">{c['email']}</a></td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;">Phone</td><td style="color:#1A1A1A;"><a href="tel:{c['phone_tel']}" style="color:#1A1A1A;text-decoration:none;">{c['phone'] or '(none)'}</a></td></tr>
</table></td></tr>
<tr><td style="padding:28px 32px 0;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Build</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:14px;">
<tr><td style="padding:6px 0;color:#5C5C5C;width:50%;">Car</td><td style="color:#1A1A1A;text-align:right;">{c['car_name']}</td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;">Duration</td><td style="color:#1A1A1A;text-align:right;">{c['duration_label']}</td></tr>
{ar_row}
{mbs_row}
<tr><td style="padding:6px 0;color:#5C5C5C;">SKU</td><td style="color:#1A1A1A;text-align:right;font-family:'SF Mono',Menlo,monospace;font-size:12px;">{c['sku']}</td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;border-top:1px solid #eee;">Subtotal</td><td style="color:#1A1A1A;text-align:right;border-top:1px solid #eee;">${c['subtotal_str']}</td></tr>
<tr><td style="padding:6px 0;color:#ff4d00;font-weight:600;">Father's Day {c['discount_pct']}% off</td><td style="color:#ff4d00;text-align:right;font-weight:600;">-${c['discount_str']}</td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;font-weight:700;">Total charged</td><td style="color:#1A1A1A;text-align:right;font-weight:800;font-size:16px;">${c['price_str']}</td></tr>
<tr><td style="padding:6px 0;color:#5C5C5C;">Stripe status</td><td style="color:#1A1A1A;text-align:right;">{c['stripe_status']}</td></tr>
</table></td></tr>
<tr><td style="padding:28px 32px 0;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Notes from gifter</div>
<div style="background:#FAF7F2;border-radius:10px;padding:14px 16px;">
<div style="color:#1A1A1A;font-size:15px;line-height:1.55;">{notes_html}</div></div></td></tr>
<tr><td style="padding:24px 32px 0;">
  <a href="tel:{c['phone_tel']}" style="display:inline-block;background-color:#ff4d00;color:#ffffff;text-decoration:none;padding:14px 24px;border-radius:10px;font-weight:600;font-size:14px;margin-right:8px;margin-bottom:8px;">Call them</a>
  <a href="sms:{c['phone_tel']}" style="display:inline-block;background-color:#ffffff;color:#ff4d00;text-decoration:none;padding:12.5px 22.5px;border-radius:10px;font-weight:600;font-size:14px;border:1.5px solid #ff4d00;margin-bottom:8px;">Text them</a>
</td></tr>
<tr><td style="padding:28px 32px 28px;">
<div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#ff4d00;font-weight:700;margin-bottom:10px;">Attribution</div>
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:12px;color:#5C5C5C;">
<tr><td style="padding:4px 0;">Source</td><td style="text-align:right;color:#1A1A1A;">{c['utm_source'] or '—'}</td></tr>
<tr><td style="padding:4px 0;">Campaign</td><td style="text-align:right;color:#1A1A1A;">{c['utm_campaign'] or '—'}</td></tr>
<tr><td style="padding:4px 0;">Event ID</td><td style="text-align:right;font-family:'SF Mono',Menlo,monospace;color:#1A1A1A;">{c['event_id']}</td></tr>
<tr><td style="padding:4px 0;">Submitted</td><td style="text-align:right;color:#1A1A1A;">{c['submitted_at']}</td></tr>
</table></td></tr>
<tr><td style="background:#FAF7F2;padding:16px 32px;text-align:center;">
<div style="font-size:11px;color:#5C5C5C;letter-spacing:0.06em;">Lumen Mainframe · SCE Father's Day funnel</div>
</td></tr>
</table></td></tr></table>
</body></html>"""


def _send_lead_email(ctx):
    if not RESEND_API_KEY:
        print("[SCE-FD email] RESEND_API_KEY not set, skipping notification")
        return
    subject = f"New SCE FD Lead · {ctx['name'] or 'Anonymous'} · ${ctx['price_str']} · {ctx['car_short']}"
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
                print(f"[SCE-FD email] {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"[SCE-FD email] exception: {e}")
    threading.Thread(target=_send, daemon=True).start()


# ─────────────────────────── Meta CAPI ───────────────────────────

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


# ─────────────────────────── Shared context ───────────────────────────

def _ctx():
    return {
        "pixel_id": META_PIXEL_ID,
        "support_phone": "(208) 914-5640",
        "support_phone_tel": "+12089145640",
    }


# ─────────────────────────── Routes ───────────────────────────

@sce_fd_bp.route("/fathersday")
def landing():
    """Pre-funnel warm-up landing — hero + selling beats + CTAs to step_car."""
    return render_template(
        "sce_fd_landing.html",
        start_url=url_for("sce_fd.step_car"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/car")
def step_car():
    """Step 1 — pick the car. Preserve any preset state via params."""
    build = _read_build_from_request()
    return render_template(
        "sce_fd_step_car.html",
        cars=CARS, build=build,
        step=1, step_total=5,
        next_url_base=url_for("sce_fd.step_duration"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/duration")
def step_duration():
    """Step 2 — pick duration. Requires `car`."""
    build = _read_build_from_request()
    if not build["car"]:
        return redirect(url_for("sce_fd.step_car"))
    car = CAR_BY_ID[build["car"]]
    # Annotate each duration with the base price for this car so the chip shows it.
    durations_priced = [
        {**d, "base_price": RENTAL_PRICING.get((car["id"], d["id"]), 0)}
        for d in DURATIONS
    ]
    return render_template(
        "sce_fd_step_duration.html",
        car=car, durations=durations_priced, build=build,
        step=2, step_total=5,
        back_url=_step_url("step_car", {"car": build["car"]}),
        next_url_base=url_for("sce_fd.step_dinner"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/dinner")
def step_dinner():
    """Step 3 — pick Anderson Ranch dinner gift card (or skip)."""
    build = _read_build_from_request()
    if not build["car"] or not build["duration"]:
        return redirect(url_for("sce_fd.step_car"))
    car = CAR_BY_ID[build["car"]]
    duration = DURATION_BY_ID[build["duration"]]
    return render_template(
        "sce_fd_step_dinner.html",
        car=car, duration=duration, build=build,
        ar_values=ANDERSON_RANCH_VALUES,
        step=3, step_total=5,
        back_url=_step_url("step_duration", build),
        next_url_base=url_for("sce_fd.step_bbq"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/bbq")
def step_bbq():
    """Step 4 — pick Modern BBQ Supply gift card (or skip)."""
    build = _read_build_from_request()
    if not build["car"] or not build["duration"]:
        return redirect(url_for("sce_fd.step_car"))
    car = CAR_BY_ID[build["car"]]
    duration = DURATION_BY_ID[build["duration"]]
    return render_template(
        "sce_fd_step_bbq.html",
        car=car, duration=duration, build=build,
        mbs_values=MODERN_BBQ_SUPPLY_VALUES,
        step=4, step_total=5,
        back_url=_step_url("step_dinner", build),
        next_url_base=url_for("sce_fd.review"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/review")
def review():
    """Step 5 — review package + capture contact info."""
    build = _read_build_from_request()
    if not build["car"] or not build["duration"]:
        return redirect(url_for("sce_fd.step_car"))
    car = CAR_BY_ID[build["car"]]
    duration = DURATION_BY_ID[build["duration"]]
    base_rental = RENTAL_PRICING.get((car["id"], duration["id"]), 0)
    total = _calc_total(build["car"], build["duration"], build["ar"], build["mbs"])
    discount    = int(round(total * FD_DISCOUNT_PCT / 100))
    final_total = total - discount
    event_id = uuid.uuid4().hex
    return render_template(
        "sce_fd_review.html",
        car=car, duration=duration, build=build,
        base_rental=base_rental, bundle_premium=BUNDLE_PREMIUM,
        total=total, discount=discount, final_total=final_total,
        discount_pct=FD_DISCOUNT_PCT,
        event_id=event_id,
        step=5, step_total=5,
        back_url=_step_url("step_bbq", build),
        canceled=(request.args.get("canceled") == "1"),
        **_ctx(),
    )


@sce_fd_bp.route("/fathersday/optin", methods=["POST"])
def optin():
    build = _read_build_from_request()
    car_id      = build["car"]
    duration_id = build["duration"]
    ar_value    = build["ar"]
    mbs_value   = build["mbs"]

    name     = (request.form.get("name") or "").strip()
    email    = (request.form.get("email") or "").strip().lower()
    phone    = (request.form.get("phone") or "").strip()
    notes    = (request.form.get("special_notes") or "").strip()[:1000]
    event_id = (request.form.get("event_id") or "").strip() or uuid.uuid4().hex
    session_id = (request.form.get("session_id") or "").strip()[:64]

    if not car_id or not duration_id:
        return redirect(url_for("sce_fd.step_car"))
    if not email or "@" not in email:
        return redirect(_step_url("review", build) + "&err=email")

    car      = CAR_BY_ID[car_id]
    duration = DURATION_BY_ID[duration_id]
    total_dollars       = _calc_total(car_id, duration_id, ar_value, mbs_value)
    discount_dollars    = int(round(total_dollars * FD_DISCOUNT_PCT / 100))
    final_total_dollars = total_dollars - discount_dollars
    sku = _build_sku(car_id, duration_id, ar_value, mbs_value)

    now = datetime.datetime.utcnow().isoformat()
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO fathersday_leads
                (event_id, session_id, name, email, phone,
                 car, duration, ar_value, mbs_value, sku, price_cents,
                 special_notes,
                 ip, user_agent, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                 fbp, fbc, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, session_id, name, email, phone,
            car_id, duration_id, ar_value, mbs_value, sku, final_total_dollars * 100,
            notes,
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
            now, now,
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

    stripe_url = ""
    stripe_session_id = ""
    stripe_status = "created"
    try:
        stripe_url, stripe_session_id = _create_stripe_session(
            email=email, name=name, phone=phone, event_id=event_id,
            car=car, duration=duration,
            ar_value=ar_value, mbs_value=mbs_value,
            total_dollars=total_dollars,
        )
    except Exception as e:
        print(f"[SCE-FD stripe] failure: {e}")
        stripe_status = f"failed: {str(e)[:200]}"

    con = sqlite3.connect(DB_PATH)
    try:
        con.execute(
            "UPDATE fathersday_leads SET stripe_session_id=?, stripe_url=?, updated_at=? WHERE event_id=?",
            (stripe_session_id, stripe_url, datetime.datetime.utcnow().isoformat(), event_id),
        )
        con.commit()
    finally:
        con.close()

    _send_lead_email({
        "name": name, "email": email, "phone": phone, "phone_tel": phone_tel,
        "car_name": car["name"], "car_short": car["short"],
        "duration_label": duration["label"],
        "ar_value": ar_value, "mbs_value": mbs_value, "sku": sku,
        "price_str": "{:,}".format(final_total_dollars),
        "subtotal_str": "{:,}".format(total_dollars),
        "discount_str": "{:,}".format(discount_dollars),
        "discount_pct": FD_DISCOUNT_PCT,
        "stripe_status": stripe_status,
        "utm_source":   request.form.get("utm_source", ""),
        "utm_medium":   request.form.get("utm_medium", ""),
        "utm_campaign": request.form.get("utm_campaign", ""),
        "event_id": event_id, "submitted_at": _format_boise_now(),
        "notes": notes,
    })

    name_parts = name.split()
    fn = name_parts[0] if name_parts else ""
    ln = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    _capi_fire(
        event_name="Lead", event_id=event_id,
        source_url=f"{request.scheme}://{request.host}{request.path}",
        user_data={
            "em": _sha256(email), "ph": _sha256(_digits_only(phone)),
            "fn": _sha256(fn), "ln": _sha256(ln),
            "client_ip_address": _client_ip(),
            "client_user_agent": request.headers.get("User-Agent", ""),
            "fbp": request.cookies.get("_fbp", ""),
            "fbc": request.cookies.get("_fbc", ""),
        },
        custom_data={
            "currency": "USD", "value": final_total_dollars,
            "content_ids": [sku],
            "content_name": f"{car['name']} · {duration['label']} · Father's Day",
            "content_type": "product",
        },
    )

    if stripe_url:
        return redirect(stripe_url)
    # Surface the Stripe error in the redirect URL so it's visible without
    # digging through Railway logs. The status string is built from
    # _create_stripe_session's RuntimeError and already includes the coupon ID
    # plus Stripe's response body.
    from urllib.parse import quote
    err_msg = quote(stripe_status.replace("failed: ", "")[:400])
    return redirect(
        _step_url("review", build)
        + "&err=stripe&event_id=" + event_id
        + "&err_msg=" + err_msg
    )


@sce_fd_bp.route("/fathersday/booked")
def booked():
    sess_id = (request.args.get("session_id") or "").strip()
    return render_template("sce_fd_booked.html", session_id=sess_id, **_ctx())


@sce_fd_bp.route("/fathersday/stripe-webhook", methods=["POST"])
def stripe_webhook():
    try:
        payload = request.get_data(as_text=True)
        evt = json.loads(payload) if payload else {}
    except Exception:
        return jsonify({"ok": False, "err": "bad_payload"}), 400

    if evt.get("type") == "checkout.session.completed":
        sess = evt.get("data", {}).get("object", {}) or {}
        sid = sess.get("id", "")
        email = (sess.get("customer_details") or {}).get("email") or sess.get("customer_email") or ""
        amount = sess.get("amount_total") or 0
        client_ref = sess.get("client_reference_id") or ""
        meta = (sess.get("metadata") or {})
        sku = meta.get("sku") or ""

        lead = None
        if sid:
            con = sqlite3.connect(DB_PATH)
            try:
                con.execute("""
                    INSERT OR IGNORE INTO fathersday_purchases
                        (stripe_session_id, stripe_payment_intent, stripe_customer,
                         email, sku, amount_cents, currency, raw, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sid, sess.get("payment_intent") or "", sess.get("customer") or "",
                    email, sku, amount, sess.get("currency") or "usd",
                    payload[:8000], datetime.datetime.utcnow().isoformat(),
                ))
                if client_ref:
                    con.execute(
                        "UPDATE fathersday_leads SET status='paid', updated_at=? WHERE event_id=?",
                        (datetime.datetime.utcnow().isoformat(), client_ref),
                    )
                    row = con.execute(
                        """SELECT email, phone, name, sku, ip, user_agent, fbp, fbc
                           FROM fathersday_leads WHERE event_id = ?""",
                        (client_ref,),
                    ).fetchone()
                    if row:
                        lead = {
                            "email": row[0] or email, "phone": row[1] or "",
                            "name": row[2] or "", "sku": row[3] or sku,
                            "ip": row[4] or "", "ua": row[5] or "",
                            "fbp": row[6] or "", "fbc": row[7] or "",
                        }
                con.commit()
            finally:
                con.close()

        purchase_event_id = client_ref or sid or uuid.uuid4().hex
        ud_email = (lead and lead["email"]) or email
        ud_phone = (lead and lead["phone"]) or ""
        name = (lead and lead["name"]) or ""
        parts = name.split()
        fn = parts[0] if parts else ""
        ln = " ".join(parts[1:]) if len(parts) > 1 else ""
        _capi_fire(
            event_name="Purchase", event_id=purchase_event_id,
            source_url=f"{BASE_URL}/fathersday/booked",
            user_data={
                "em": _sha256(ud_email), "ph": _sha256(_digits_only(ud_phone)),
                "fn": _sha256(fn), "ln": _sha256(ln),
                "client_ip_address": (lead and lead["ip"]) or "",
                "client_user_agent": (lead and lead["ua"]) or "",
                "fbp": (lead and lead["fbp"]) or "",
                "fbc": (lead and lead["fbc"]) or "",
            },
            custom_data={
                "currency": (sess.get("currency") or "usd").upper(),
                "value": (amount or 0) / 100.0,
                "content_ids": [(lead and lead["sku"]) or sku] if ((lead and lead["sku"]) or sku) else [],
                "content_type": "product", "order_id": sid,
            },
        )

    return jsonify({"ok": True})
