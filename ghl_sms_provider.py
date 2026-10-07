"""
GHL custom SMS provider backed by SignalHouse (Lumen's own 10DLC number).

The native SignalHouse GHL app kept binding to an empty SignalHouse account, so this
replaces it: a private GHL Marketplace app whose conversation provider (type SMS,
"custom provider" unticked, so it replaces the default SMS channel) points here.

  GHL user/workflow sends SMS  -> POST /ghl-sms/outbound?k=KEY  -> SignalHouse /message/sms
  SignalHouse delivery events  -> POST /ghl-sms/signalhouse?k=KEY -> GHL message status
  Lead replies by text         -> POST /ghl-sms/signalhouse?k=KEY -> GHL inbound message
  App install (OAuth)          -> GET  /ghl-sms/oauth/callback   -> tokens stored in ghl_sms.db

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
import time

import requests
from flask import Blueprint, abort, request

log = logging.getLogger(__name__)
ghl_sms_bp = Blueprint("ghl_sms", __name__, url_prefix="/ghl-sms")

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


def _save_tokens(location_id, data):
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO tokens VALUES (?,?,?,?)",
                    (location_id, data["access_token"], data["refresh_token"],
                     time.time() + int(data.get("expires_in", 86399)) - 300))


def _token(location_id):
    with _db() as con:
        row = con.execute("SELECT * FROM tokens WHERE location_id=?", (location_id,)).fetchone()
    if not row:
        raise RuntimeError(f"app not installed on location {location_id}")
    if time.time() < row["expires_at"]:
        return row["access_token"]
    r = requests.post(f"{GHL}/oauth/token", data={
        "client_id": _env("GHL_SMS_CLIENT_ID"), "client_secret": _env("GHL_SMS_CLIENT_SECRET"),
        "grant_type": "refresh_token", "refresh_token": row["refresh_token"], "user_type": "Location",
    }, timeout=15).json()
    if "access_token" not in r:
        raise RuntimeError(f"token refresh failed: {r}")
    _save_tokens(location_id, r)
    return r["access_token"]


def _ghl(method, path, location_id, version="2021-04-15", **kw):
    headers = {"Authorization": f"Bearer {_token(location_id)}", "Version": version, "Accept": "application/json"}
    r = requests.request(method, f"{GHL}{path}", headers=headers, timeout=15, **kw)
    if r.status_code >= 300:
        log.error("GHL %s %s -> %s %s", method, path, r.status_code, r.text[:300])
    return r


def _set_status(location_id, ghl_message_id, status, error=None):
    body = {"status": status}
    if error:
        body["error"] = {"code": "1", "type": "saas", "message": str(error)[:200]}
    _ghl("PUT", f"/conversations/messages/{ghl_message_id}/status", location_id, json=body)


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
    return None, data.get("message") or data.get("error") or f"HTTP {r.status_code}"


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
    if "access_token" not in r or not r.get("locationId"):
        log.error("GHL SMS install failed: %s", r)
        return f"Install failed: {r.get('error_description') or r.get('message') or r}", 400
    _save_tokens(r["locationId"], r)
    log.info("GHL SMS provider installed on location %s", r["locationId"])
    return f"Lumen SMS installed on location {r['locationId']}. You can close this tab."


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
STATUS_EVENTS = {"MESSAGE_DELIVERED": "delivered", "SMS_FAILED": "failed",
                 "MMS_FAILED": "failed", "MESSAGE_FAILED": "failed"}


def _contact_for(location_id, phone):
    e164 = "+" + phone
    r = _ghl("GET", "/contacts/search/duplicate", location_id, version="2021-07-28",
             params={"locationId": location_id, "number": e164})
    contact = (r.json() if r.ok else {}).get("contact")
    if contact:
        return contact["id"]
    r = _ghl("POST", "/contacts/", location_id, version="2021-07-28",
             json={"locationId": location_id, "phone": e164, "source": "Inbound SMS"})
    return (r.json().get("contact") or {}).get("id") if r.ok else None


def _conversation_for(location_id, contact_id):
    r = _ghl("GET", "/conversations/search", location_id, params={"locationId": location_id, "contactId": contact_id})
    convs = (r.json() if r.ok else {}).get("conversations") or []
    if convs:
        return convs[0]["id"]
    r = _ghl("POST", "/conversations/", location_id, json={"locationId": location_id, "contactId": contact_id})
    return (r.json().get("conversation") or {}).get("id") if r.ok else None


@ghl_sms_bp.route("/signalhouse", methods=["POST"])
def signalhouse_event():
    _check_key()
    data = request.get_json(silent=True) or {}
    event = (data.get("event") or data.get("type") or data.get("eventType") or "").upper()
    ident = data.get("identifier") or data.get("_id", "")

    if event in STATUS_EVENTS:
        with _db() as con:
            row = con.execute("SELECT ghl_message_id, location_id FROM messages WHERE sh_id=?", (ident,)).fetchone()
        if row and row["ghl_message_id"]:
            _set_status(row["location_id"], row["ghl_message_id"], STATUS_EVENTS[event],
                        data.get("errorCode") or data.get("error") if STATUS_EVENTS[event] == "failed" else None)
        return "", 200
    if event and event != "MESSAGE_RECEIVED":
        return "", 200

    # v2 envelope nests the message under metaData.Message (same parsing as avalon-crm)
    src = (data.get("metaData") or {}).get("Message")
    if not isinstance(src, dict):
        src = data
    sender = _digits11(src.get("senderPhoneNumber") or src.get("from"))
    body = (src.get("messageBody") or src.get("body") or src.get("text") or "").strip()
    ident = ident or src.get("_id", "")
    if not sender or not body:
        return "", 200
    with _db() as con:
        if ident and con.execute("SELECT 1 FROM messages WHERE sh_id=?", (ident,)).fetchone():
            return "", 200  # SignalHouse retry, already posted

    loc = _env("GHL_SMS_LOCATION_ID", "6b4I6ILHBVcWQYlmPj3i")
    try:
        contact_id = _contact_for(loc, sender)
        conv_id = _conversation_for(loc, contact_id) if contact_id else None
        if not conv_id:
            log.error("Inbound SMS from %s: no GHL contact/conversation", sender)
            return "", 200
        msg = {"type": "SMS", "conversationId": conv_id, "message": body}
        if _env("GHL_SMS_PROVIDER_ID"):
            msg["conversationProviderId"] = _env("GHL_SMS_PROVIDER_ID")
        r = _ghl("POST", "/conversations/messages/inbound", loc, json=msg)
        ghl_id = (r.json() if r.ok else {}).get("messageId")
    except Exception as exc:
        log.exception("Inbound SMS to GHL failed: %s", exc)
        return "", 500  # let SignalHouse retry
    with _db() as con:
        con.execute("INSERT OR REPLACE INTO messages VALUES (?,?,?,?,?,?,?,?)",
                    (ident or f"in-{time.time()}", ghl_id, loc, "inbound", sender, body, "received", time.time()))
    return "", 200
