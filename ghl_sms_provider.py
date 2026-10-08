"""
GHL custom SMS provider backed by SignalHouse (Lumen's own 10DLC number).

The native SignalHouse GHL app kept binding to an empty SignalHouse account, so this
replaces it: a private GHL Marketplace app whose conversation provider (type SMS,
"custom provider" unticked, so it replaces the default SMS channel) points here.

  GHL user/workflow sends SMS  -> POST /lumen-sms/outbound?k=KEY  -> SignalHouse /message/sms
  SignalHouse delivery events  -> POST /lumen-sms/signalhouse?k=KEY -> GHL message status
  Lead replies by text         -> POST /lumen-sms/signalhouse?k=KEY -> GHL inbound message
  App install (OAuth)          -> GET  /lumen-sms/oauth/callback   -> tokens stored in ghl_sms.db

Env vars:
  GHL_SMS_CLIENT_ID, GHL_SMS_CLIENT_SECRET   Marketplace app credentials
  GHL_SMS_WEBHOOK_KEY                        shared secret, passed as ?k= on both webhook URLs
  GHL_SMS_FROM_NUMBER                        the SignalHouse 208 number texts go out from
  GHL_SMS_PROVIDER_ID                        conversationProviderId (optional, sent on inbound)
  LUMEN_SIGNALHOUSE_API_KEY                  Lumen's SignalHouse key (NOT Avalon's)
  SIGNALHOUSE_BASE_URL                       default https://v2.signalhouse.io
"""

import hmac
import logging
import os
import re
import sqlite3
import threading
import time

import requests
from flask import Blueprint, abort, request

log = logging.getLogger(__name__)
ghl_sms_bp = Blueprint("ghl_sms", __name__, url_prefix="/lumen-sms")

GHL = "https://services.leadconnectorhq.com"
SH_BASE = os.environ.get("SIGNALHOUSE_BASE_URL", "https://v2.signalhouse.io")
_DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(_DATA_DIR, "ghl_sms.db")


def _env(k, default=""):
    return os.environ.get(k, default)


# ── Storage ───────────────────────────────────────────────────────────────────
def _db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_ghl_sms_db():
    with _db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS tokens (
            location_id TEXT PRIMARY KEY, access_token TEXT, refresh_token TEXT, expires_at REAL)""")
        con.execute("""CREATE TABLE IF NOT EXISTS messages (
            sh_id TEXT PRIMARY KEY, ghl_message_id TEXT, location_id TEXT, direction TEXT,
            phone TEXT, body TEXT, status TEXT, created_at REAL)""")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _check_key():
    key = _env("GHL_SMS_WEBHOOK_KEY")
    if not key or not hmac.compare_digest(request.args.get("k", ""), key):
        abort(403)


def _digits11(phone):
    d = re.sub(r"\D", "", str(phone or ""))
    if len(d) == 10:
        d = "1" + d
    return d if len(d) == 11 else None


def _save_tokens(key, data):
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO tokens VALUES (?,?,?,?)",
                    (key, data["access_token"], data.get("refresh_token", ""),
                     time.time() + int(data.get("expires_in", 86399)) - 300))


def _row(key):
    with _db() as con:
        return con.execute("SELECT * FROM tokens WHERE location_id=?", (key,)).fetchone()


def _refresh(row, user_type):
    r = requests.post(f"{GHL}/oauth/token", data={
        "client_id": _env("GHL_SMS_CLIENT_ID"), "client_secret": _env("GHL_SMS_CLIENT_SECRET"),
        "grant_type": "refresh_token", "refresh_token": row["refresh_token"], "user_type": user_type,
    }, timeout=15).json()
    if "access_token" not in r:
        log.error("GHL token refresh failed (%s): %s", user_type, r.get("error_description") or r.get("message"))
        return None
    _save_tokens(row["location_id"], r)
    return r["access_token"]


def _company():
    """Agency-level token from a bulk (agency) install, stored under key company|<companyId>."""
    with _db() as con:
        row = con.execute("SELECT * FROM tokens WHERE location_id LIKE 'company|%'").fetchone()
    if not row:
        return None, None
    tok = row["access_token"] if time.time() < row["expires_at"] else _refresh(row, "Company")
    return row["location_id"].split("|", 1)[1], tok


def _mint_location(location_id):
    company_id, ctok = _company()
    if not ctok:
        raise RuntimeError(f"app not installed on location {location_id}")
    r = requests.post(f"{GHL}/oauth/locationToken", data={"companyId": company_id, "locationId": location_id},
                      headers={"Authorization": f"Bearer {ctok}", "Version": "2021-07-28"}, timeout=15).json()
    if "access_token" not in r:
        raise RuntimeError(f"location token failed: {r.get('message') or r.get('error')}")
    _save_tokens(location_id, r)
    return r["access_token"]


def _token(location_id):
    row = _row(location_id)
    if row and time.time() < row["expires_at"]:
        return row["access_token"]
    if row and row["refresh_token"]:
        tok = _refresh(row, "Location")
        if tok:
            return tok
    return _mint_location(location_id)


def _ghl(method, path, location_id, version="2021-04-15", **kw):
    headers = {"Authorization": f"Bearer {_token(location_id)}", "Version": version, "Accept": "application/json"}
    r = requests.request(method, f"{GHL}{path}", headers=headers, timeout=15, **kw)
    if r.status_code >= 300:
        log.error("GHL %s %s -> %s %s", method, path, r.status_code, r.text[:300])
    return r


def _pretty(d11):
    return f"({d11[1:4]}) {d11[4:7]}-{d11[7:]}" if d11 and len(d11) == 11 else d11


def _set_status(location_id, ghl_message_id, status, error=None):
    body = {"status": status}
    if error:
        body["error"] = {"code": "1", "type": "saas", "message": str(error)[:200]}
    _ghl("PUT", f"/conversations/messages/{ghl_message_id}/status", location_id, json=body)


_last_balance_alert = 0.0


def _balance_alert(err):
    """SignalHouse is prepaid: when the balance hits zero every text fails. Email Kendall, at most every 3 hours."""
    global _last_balance_alert
    if "insufficient" not in str(err).lower() or time.time() - _last_balance_alert < 3 * 3600:
        return
    _last_balance_alert = time.time()
    key = _env("RESEND_API_KEY")
    if not key:
        return
    try:
        requests.post("https://api.resend.com/emails", timeout=15, headers={"Authorization": f"Bearer {key}"}, json={
            "from": "Lumen SMS <notifications@lumenmarketing.co>",
            "to": ["kendall@lumenmarketing.co", "kendallwdavis11@gmail.com"],
            "subject": "URGENT: SignalHouse balance is empty, texts are failing",
            "html": f"<p>SignalHouse rejected a text with: <b>{err}</b></p><p>Every text from the 208 number (Contractor OS leads, "
                    "booking confirmations, your alerts) and Avalon's CRM texts fail until the balance is topped up. "
                    "Add funds and turn on auto-recharge in SignalHouse.</p>"})
    except Exception as exc:
        log.error("balance alert email failed: %s", exc)


def _signalhouse_send(to_number, body, media=None):
    payload = {"senderPhoneNumber": _digits11(_env("GHL_SMS_FROM_NUMBER")),
               "recipientPhoneNumber": [_digits11(to_number)], "messageBody": body or ""}
    path = "/message/sms"
    if media:
        payload["mediaUrls"] = media
        path = "/message/mms"
    r = requests.post(f"{SH_BASE}{path}", json=payload, timeout=15, headers={
        "Authorization": f"Bearer {_env('LUMEN_SIGNALHOUSE_API_KEY')}", "Content-Type": "application/json"})
    data = r.json() if r.content else {}
    if r.status_code == 201:
        return (data.get("insertedMessages") or [{}])[0].get("_id"), None
    err = data.get("message") or data.get("error") or f"HTTP {r.status_code}"
    _balance_alert(err)
    return None, err


def notify_owner(text):
    """Text Kendall's own cell from the 208 number (lead alerts, forwarded replies)."""
    to = _env("OWNER_ALERT_NUMBER", "12085910132")
    if not to:
        return
    _, err = _signalhouse_send(to, text[:600])
    if err:
        log.error("Owner alert failed: %s", err)


# ── OAuth install ─────────────────────────────────────────────────────────────
@ghl_sms_bp.route("/oauth/callback")
def oauth_callback():
    code = request.args.get("code")
    if not code:
        return "Missing code", 400
    r = requests.post(f"{GHL}/oauth/token", data={
        "client_id": _env("GHL_SMS_CLIENT_ID"), "client_secret": _env("GHL_SMS_CLIENT_SECRET"),
        "grant_type": "authorization_code", "code": code, "user_type": "Location",
    }, timeout=15).json()
    if "access_token" not in r:
        log.error("GHL SMS install failed: %s", r.get("error_description") or r.get("message"))
        return f"Install failed: {r.get('error_description') or r.get('message') or 'no token returned'}", 400
    if r.get("userType") == "Company" or not r.get("locationId"):
        # Agency (bulk) install: keep the agency token, then mint a token for our sub-account
        _save_tokens(f"company|{r['companyId']}", r)
        loc = _env("GHL_SMS_LOCATION_ID", "6b4I6ILHBVcWQYlmPj3i")
        try:
            _mint_location(loc)
        except Exception as exc:
            log.error("GHL SMS location token failed: %s", exc)
            return f"Installed at agency level, but could not get access to sub-account {loc}: {exc}", 400
    else:
        loc = r["locationId"]
        _save_tokens(loc, r)
    log.info("GHL SMS provider installed on location %s", loc)
    return f"Lumen SMS installed on location {loc}. You can close this tab."


# ── GHL -> SignalHouse ────────────────────────────────────────────────────────
@ghl_sms_bp.route("/outbound", methods=["POST"])
def outbound():
    _check_key()
    d = request.get_json(silent=True) or {}
    if (d.get("type") or "SMS").upper() != "SMS":
        return "", 200
    loc, msg_id, phone = d.get("locationId"), d.get("messageId"), d.get("phone")
    sh_id, err = _signalhouse_send(phone, d.get("message"), d.get("attachments") or None)
    if err:
        log.error("SignalHouse send failed for GHL message %s: %s", msg_id, err)
        if loc and msg_id:
            _set_status(loc, msg_id, "failed", err)
        return "", 200
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)",
                    (sh_id, msg_id, loc, "outbound", _digits11(phone), d.get("message"), "sent", time.time()))
    return "", 200


# ── SignalHouse -> GHL ────────────────────────────────────────────────────────
OPT_OUT_WORDS = {"STOP", "STOPALL", "UNSUBSCRIBE", "CANCEL", "END", "QUIT", "OPTOUT", "OPT OUT", "REVOKE"}
STATUS_EVENTS = {"MESSAGE_DELIVERED": "delivered", "SMS_FAILED": "failed",
                 "MMS_FAILED": "failed", "MESSAGE_FAILED": "failed"}


def _contact_for(location_id, phone):
    """Returns (contact_id, display name)."""
    e164 = "+" + phone
    r = _ghl("GET", "/contacts/search/duplicate", location_id, version="2021-07-28",
             params={"locationId": location_id, "number": e164})
    contact = (r.json() if r.ok else {}).get("contact")
    if contact:
        name = contact.get("contactName") or " ".join(filter(None, [contact.get("firstName"), contact.get("lastName")]))
        return contact["id"], (name or "").title()
    r = _ghl("POST", "/contacts/", location_id, version="2021-07-28",
             json={"locationId": location_id, "phone": e164, "source": "Inbound SMS"})
    return ((r.json().get("contact") or {}).get("id") if r.ok else None), ""


def _conversation_for(location_id, contact_id):
    r = _ghl("GET", "/conversations/search", location_id, params={"locationId": location_id, "contactId": contact_id})
    convs = (r.json() if r.ok else {}).get("conversations") or []
    if convs:
        return convs[0]["id"]
    r = _ghl("POST", "/conversations/", location_id, json={"locationId": location_id, "contactId": contact_id})
    return (r.json().get("conversation") or {}).get("id") if r.ok else None


def _signature_ok():
    """SignalHouse signs group webhooks: hex HMAC-SHA256 over "<timestamp>.<raw body>" (timestamp in ms)."""
    secret = _env("LUMEN_SIGNALHOUSE_WEBHOOK_SECRET")
    if not secret:
        return True
    sig = request.headers.get("X-SignalHouse-Signature", "")
    ts = request.headers.get("X-SignalHouse-Timestamp", "")
    if not sig or not ts or abs(time.time() * 1000 - int(ts or 0)) > 5 * 60 * 1000:
        return False
    expected = hmac.new(secret.encode(), f"{ts}.".encode() + request.get_data(), "sha256").hexdigest()
    return hmac.compare_digest(sig, expected)


@ghl_sms_bp.route("/signalhouse", methods=["POST"])
def signalhouse_event():
    _check_key()
    if not _signature_ok():
        abort(403)
    data = request.get_json(silent=True) or {}
    delivery_id = request.headers.get("X-SignalHouse-Delivery-Id", "")
    # SignalHouse gives a receiver 3 seconds, so acknowledge now and do the GHL work afterwards
    threading.Thread(target=_handle_signalhouse, args=(data, delivery_id), daemon=True).start()
    return "", 200


def _handle_signalhouse(data, delivery_id):
    event = (data.get("event") or data.get("type") or data.get("eventType") or "").upper()
    ident = data.get("identifier") or data.get("_id", "")

    if event in STATUS_EVENTS:
        with _db() as con:
            row = con.execute("SELECT ghl_message_id, location_id FROM messages WHERE sh_id=?", (ident,)).fetchone()
        if row and row["ghl_message_id"]:  # only our own outbound texts are in this table
            _set_status(row["location_id"], row["ghl_message_id"], STATUS_EVENTS[event],
                        data.get("errorCode") or data.get("error") if STATUS_EVENTS[event] == "failed" else None)
        return
    if event and event != "MESSAGE_RECEIVED":
        return

    # v2 envelope nests the message under metaData.Message (same parsing as avalon-crm)
    src = (data.get("metaData") or {}).get("Message")
    if not isinstance(src, dict):
        src = data
    to_raw = src.get("recipientPhoneNumber") or src.get("to") or data.get("phoneNumber") or ""
    if isinstance(to_raw, list):
        to_raw = to_raw[0] if to_raw else ""
    # The SignalHouse group is shared with Avalon: only texts sent to our 208 number belong in GHL
    if _digits11(to_raw) != _digits11(_env("GHL_SMS_FROM_NUMBER")):
        return
    sender = _digits11(src.get("senderPhoneNumber") or src.get("from"))
    body = (src.get("messageBody") or src.get("body") or src.get("text") or "").strip()
    key = ident or src.get("_id", "") or delivery_id
    if not sender or not body:
        return
    with _db() as con:
        if key and con.execute("SELECT 1 FROM messages WHERE sh_id=?", (key,)).fetchone():
            return  # SignalHouse retry, already posted

    loc = _env("GHL_SMS_LOCATION_ID", "6b4I6ILHBVcWQYlmPj3i")
    try:
        contact_id, name = _contact_for(loc, sender)
        conv_id = _conversation_for(loc, contact_id) if contact_id else None
        if not conv_id:
            log.error("Inbound SMS from %s: no GHL contact/conversation", sender)
            return
        msg = {"type": "SMS", "conversationId": conv_id, "message": body}
        if _env("GHL_SMS_PROVIDER_ID"):
            msg["conversationProviderId"] = _env("GHL_SMS_PROVIDER_ID")
        r = _ghl("POST", "/conversations/messages/inbound", loc, json=msg)
        ghl_id = (r.json() if r.ok else {}).get("messageId")
        if body.strip().upper().rstrip(".!") in OPT_OUT_WORDS:
            # Carrier opt-out keyword: mark the contact do-not-text in GHL so no workflow or bulk send texts them again
            _ghl("PUT", f"/contacts/{contact_id}", loc, version="2021-07-28",
                 json={"dndSettings": {"SMS": {"status": "active", "message": f"Replied {body.strip()}", "code": "STOP"}}})
            log.info("Contact %s replied %s, SMS DND set", contact_id, body.strip())
    except Exception as exc:
        log.exception("Inbound SMS to GHL failed: %s", exc)
        return
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)",
                    (key or f"in-{time.time()}", ghl_id, loc, "inbound", sender, body, "received", time.time()))
    log.info("Inbound SMS from %s posted to GHL conversation %s", sender, conv_id)
    if sender != _digits11(_env("OWNER_ALERT_NUMBER", "12085910132")):
        who = f"{name}, {_pretty(sender)}" if name else _pretty(sender)
        notify_owner(f"Reply from {who}: {body}")
