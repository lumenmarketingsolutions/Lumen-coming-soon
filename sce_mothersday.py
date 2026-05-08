"""
Supercar Experience Boise — Mother's Day funnel.

Funnel flow:
  / (or /mothersday)                    → landing page (both tiers visible)
  /mothersday/tier/<tier>               → 3 cars for that tier
  /mothersday/reserve/<tier>/<car>      → opt-in form (lead fires here)
  POST /mothersday/optin                → lead handler, writes DB, redirect to Stripe
  /mothersday/booked                    → thank-you (after Stripe success)
  POST /mothersday/stripe-webhook       → Stripe event sink (Purchase CAPI event)

State is encoded in the URL (tier + car) so there is no session dependency.
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
from flask import Blueprint, render_template, request, redirect, url_for, abort, jsonify

sce_md_bp = Blueprint("sce_md", __name__)

# ── Stripe Payment Links (SCE account, created 2026-05-07) ──
STRIPE_LINKS = {
    "full-day": {
        "g63":    "https://buy.stripe.com/00wbJ35ChbCO2Q63Rv6wE05",
        "urus":   "https://buy.stripe.com/3cI6oJ6GlayKcqG3Rv6wE06",
        "gt3rs":  "https://buy.stripe.com/bJefZjaWB36i9eugEh6wE07",
    },
    "day-out": {
        "g63":    "https://buy.stripe.com/dRm8wRggV8qC76m0Fj6wE08",
        "urus":   "https://buy.stripe.com/9B66oJaWB5eqcqGafT6wE09",
        "gt3rs":  "https://buy.stripe.com/bJe8wRd4JfT4eyOgEh6wE0a",
    },
}

# ── Tier + car config ──
TIERS = {
    "full-day": {
        "id": "full-day",
        "name": "The Full Day",
        "tagline": "The full day. Pulled up in style, pampered after.",
        "starting_at": 500,
        "value_up_to": 1019,
        "spa_label": "Hand & Stone spa gift card included",
        "spa_value_line": "$120 spa value, included",
        "restaurant_value": 100,
        "restaurants": [
            "Percy",
            "Fork",
            "Eight Thirty Common",
            "The Wylder",
        ],
        "restaurants_note": "Your choice from these or any restaurant she loves.",
        "cars": [
            {
                "id": "g63",
                "name": "Mercedes-AMG G63",
                "duration": "24 hour rental",
                "image": "g63.jpg",
                "package_price": 500,
                "total_value": 570,
                "savings": 70,
                "sku": "MD-T1-G63-24",
            },
            {
                "id": "urus",
                "name": "Lamborghini Urus",
                "duration": "24 hour rental",
                "image": "urus.jpg",
                "package_price": 780,
                "total_value": 920,
                "savings": 140,
                "sku": "MD-T1-URUS-24",
            },
            {
                "id": "gt3rs",
                "name": "Porsche GT3RS",
                "duration": "8 hour rental",
                "image": "gt3rs.jpg",
                "package_price": 859,
                "total_value": 1019,
                "savings": 160,
                "sku": "MD-T1-GT3RS-8",
            },
        ],
    },
    "day-out": {
        "id": "day-out",
        "name": "The Day Out",
        "tagline": "Lunch, the spa, and a supercar she gets to drive.",
        "starting_at": 330,
        "value_up_to": 969,
        "spa_label": "Hand & Stone spa gift card included",
        "spa_value_line": "$120 spa value, $80 through our partnership",
        "restaurant_value": 50,
        "restaurants": [
            "Don & Charly's",
            "Zullee",
            "The Local",
            "The Wylder",
        ],
        "restaurants_note": "Your choice from these or any restaurant she loves.",
        "cars": [
            {
                "id": "g63",
                "name": "Mercedes-AMG G63",
                "duration": "8 hour rental",
                "image": "g63.jpg",
                "package_price": 330,
                "total_value": 420,
                "savings": 90,
                "sku": "MD-T2-G63-8",
            },
            {
                "id": "urus",
                "name": "Lamborghini Urus",
                "duration": "8 hour rental",
                "image": "urus.jpg",
                "package_price": 570,
                "total_value": 720,
                "savings": 150,
                "sku": "MD-T2-URUS-8",
            },
            {
                "id": "gt3rs",
                "name": "Porsche GT3RS",
                "duration": "8 hour rental",
                "image": "gt3rs.jpg",
                "package_price": 769,
                "total_value": 969,
                "savings": 200,
                "sku": "MD-T2-GT3RS-8",
            },
        ],
    },
}

# Pixel + CAPI configuration (env-driven so we can swap real IDs without code change)
META_PIXEL_ID  = os.environ.get("META_PIXEL_ID_SCE", "1514374663034732")
META_CAPI_TOKEN = os.environ.get("META_CAPI_TOKEN_SCE", "")

# DB path resolved by the host app's DATA_DIR convention
DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "waitlist.db")


def init_md_db():
    """Idempotent DB setup for the Mother's Day funnel."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS mothersday_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT UNIQUE NOT NULL,
            name TEXT DEFAULT '',
            email TEXT NOT NULL,
            phone TEXT DEFAULT '',
            tier TEXT NOT NULL,
            car TEXT NOT NULL,
            sku TEXT NOT NULL,
            price_cents INTEGER NOT NULL,
            stripe_link TEXT NOT NULL,
            status TEXT DEFAULT 'opt-in',
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
    con.execute("""
        CREATE TABLE IF NOT EXISTS mothersday_purchases (
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


def _get_combo(tier_id, car_id):
    tier = TIERS.get(tier_id)
    if not tier:
        return None, None
    car = next((c for c in tier["cars"] if c["id"] == car_id), None)
    return tier, car


# ─────────────────── Meta CAPI helpers ───────────────────

def _sha256(v):
    """Lowercase + trim + SHA-256 hex. Empty string for empty input."""
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


def _capi_fire(event_name, event_id, source_url, user_data, custom_data=None):
    """Fire a single CAPI event in a background thread. Silent no-op if token missing."""
    if not META_CAPI_TOKEN or not META_PIXEL_ID:
        return
    payload_event = {
        "event_name": event_name,
        "event_time": int(time.time()),
        "event_id": event_id,
        "action_source": "website",
        "event_source_url": source_url,
        "user_data": {k: v for k, v in user_data.items() if v},
    }
    if custom_data:
        payload_event["custom_data"] = custom_data
    body = {"data": [payload_event], "access_token": META_CAPI_TOKEN}

    def _send():
        try:
            r = requests.post(
                f"https://graph.facebook.com/v19.0/{META_PIXEL_ID}/events",
                json=body,
                timeout=4,
            )
            if r.status_code != 200:
                print(f"[CAPI] {event_name} {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"[CAPI] {event_name} exception: {e}")

    threading.Thread(target=_send, daemon=True).start()


def _ctx():
    """Common template context."""
    return {
        "pixel_id": META_PIXEL_ID,
        "support_phone": "(208) 914-5640",
        "support_phone_tel": "+12089145640",
        "delivery_radius_copy": "Free delivery within 10 miles of Ten Mile & Franklin.",
    }


# ─────────────────────────── Routes ───────────────────────────

@sce_md_bp.route("/mothersday")
def landing():
    return render_template(
        "sce_md_landing.html",
        tiers=[TIERS["full-day"], TIERS["day-out"]],
        **_ctx(),
    )


@sce_md_bp.route("/mothersday/tier/<tier_id>")
def tier(tier_id):
    tier = TIERS.get(tier_id)
    if not tier:
        return redirect(url_for("sce_md.landing"))
    return render_template("sce_md_cars.html", tier=tier, **_ctx())


@sce_md_bp.route("/mothersday/reserve/<tier_id>/<car_id>")
def reserve(tier_id, car_id):
    tier, car = _get_combo(tier_id, car_id)
    if not tier or not car:
        return redirect(url_for("sce_md.landing"))
    # Generate event_id at page render so client-side fbq Lead and server-side
    # CAPI Lead share the same id and Meta dedupes them within the 5-minute window.
    event_id = uuid.uuid4().hex
    return render_template(
        "sce_md_optin.html",
        tier=tier, car=car, event_id=event_id,
        **_ctx(),
    )


@sce_md_bp.route("/mothersday/optin", methods=["POST"])
def optin():
    tier_id = (request.form.get("tier") or "").strip()
    car_id  = (request.form.get("car") or "").strip()
    name    = (request.form.get("name") or "").strip()
    email   = (request.form.get("email") or "").strip().lower()
    phone   = (request.form.get("phone") or "").strip()
    event_id = (request.form.get("event_id") or "").strip() or uuid.uuid4().hex

    tier, car = _get_combo(tier_id, car_id)
    if not tier or not car:
        return redirect(url_for("sce_md.landing"))
    if not email or "@" not in email:
        return redirect(url_for("sce_md.reserve", tier_id=tier_id, car_id=car_id) + "?err=email")

    stripe_url = STRIPE_LINKS[tier_id][car_id]
    now = datetime.datetime.utcnow().isoformat()

    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("""
            INSERT INTO mothersday_leads
                (event_id, name, email, phone, tier, car, sku, price_cents, stripe_link,
                 ip, user_agent, referrer, utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                 fbp, fbc, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            event_id, name, email, phone,
            tier_id, car_id, car["sku"], car["package_price"] * 100, stripe_url,
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

    # Fire server-side CAPI Lead (background thread, doesn't block redirect).
    name_parts = name.split()
    fn = name_parts[0] if name_parts else ""
    ln = " ".join(name_parts[1:]) if len(name_parts) > 1 else ""
    _capi_fire(
        event_name="Lead",
        event_id=event_id,
        source_url=f"{request.scheme}://{request.host}{request.path}",
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
            "currency": "USD",
            "value": car["package_price"],
            "content_ids": [car["sku"]],
            "content_name": f"{car['name']} {tier['name']}",
            "content_type": "product",
        },
    )

    # Pass event_id and email into Stripe via prefilled_email so attribution stays clean.
    sep = "&" if "?" in stripe_url else "?"
    return redirect(f"{stripe_url}{sep}prefilled_email={email}&client_reference_id={event_id}")


@sce_md_bp.route("/mothersday/booked")
def booked():
    sku = (request.args.get("sku") or "").strip()
    return render_template("sce_md_booked.html", sku=sku, **_ctx())


@sce_md_bp.route("/mothersday/stripe-webhook", methods=["POST"])
def stripe_webhook():
    """
    Stripe Purchase event handler. Webhook signing secret + CAPI fire wired in
    follow-up commit once we have the SCE webhook signing key.
    """
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

        # Look up the original lead so we can recover phone/name/IP for CAPI match.
        lead = None
        if sid:
            con = sqlite3.connect(DB_PATH)
            try:
                con.execute("""
                    INSERT OR IGNORE INTO mothersday_purchases
                        (stripe_session_id, stripe_payment_intent, stripe_customer,
                         email, sku, amount_cents, currency, raw, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sid,
                    sess.get("payment_intent") or "",
                    sess.get("customer") or "",
                    email, sku, amount,
                    sess.get("currency") or "usd",
                    payload[:8000],
                    datetime.datetime.utcnow().isoformat(),
                ))
                if client_ref:
                    con.execute(
                        "UPDATE mothersday_leads SET status='paid', updated_at=? WHERE event_id=?",
                        (datetime.datetime.utcnow().isoformat(), client_ref),
                    )
                    row = con.execute(
                        """SELECT email, phone, name, sku, ip, user_agent, fbp, fbc
                           FROM mothersday_leads WHERE event_id = ?""",
                        (client_ref,),
                    ).fetchone()
                    if row:
                        lead = {
                            "email": row[0] or email,
                            "phone": row[1] or "",
                            "name":  row[2] or "",
                            "sku":   row[3] or sku,
                            "ip":    row[4] or "",
                            "ua":    row[5] or "",
                            "fbp":   row[6] or "",
                            "fbc":   row[7] or "",
                        }
                con.commit()
            finally:
                con.close()

        # Fire CAPI Purchase server-side using the highest-fidelity user data we have.
        purchase_event_id = client_ref or sid or uuid.uuid4().hex
        ud_email = (lead and lead["email"]) or email
        ud_phone = lead and lead["phone"] or ""
        name = (lead and lead["name"]) or ""
        parts = name.split()
        fn = parts[0] if parts else ""
        ln = " ".join(parts[1:]) if len(parts) > 1 else ""
        _capi_fire(
            event_name="Purchase",
            event_id=purchase_event_id,
            source_url=f"https://supercarexp.lumenmarketing.co/mothersday/booked",
            user_data={
                "em": _sha256(ud_email),
                "ph": _sha256(_digits_only(ud_phone)),
                "fn": _sha256(fn),
                "ln": _sha256(ln),
                "client_ip_address": (lead and lead["ip"]) or "",
                "client_user_agent": (lead and lead["ua"]) or "",
                "fbp": (lead and lead["fbp"]) or "",
                "fbc": (lead and lead["fbc"]) or "",
            },
            custom_data={
                "currency": (sess.get("currency") or "usd").upper(),
                "value": (amount or 0) / 100.0,
                "content_ids": [(lead and lead["sku"]) or sku] if ((lead and lead["sku"]) or sku) else [],
                "content_type": "product",
                "order_id": sid,
            },
        )

    return jsonify({"ok": True})
