"""
Contractor OS booking automation for the GHL calendar "Lumen | Meta Ads" (MK7 sub-account).

GHL workflows can only be built in the GHL UI, so this does the booking workflow server side.
Every 60s it reads upcoming events on the calendar and, once per event:
  - new booking: move the Contractor OS opportunity to Call Booked, text the lead a confirmation,
    and text Kendall the booking with the time in Beirut
  - about a day before: reminder text that asks for their website
  - about an hour before: "talking in an hour" text
  - cancelled: text Kendall
Texts go through GHL (POST /conversations/messages), so they show in the conversation and are
delivered by the Lumen SMS provider (SignalHouse 208 number).

Env: GHL_MK7_PIT (private integration token for MK7, calendars + opportunities + conversations).
"""

import datetime
import logging
import os
import threading
import time
from zoneinfo import ZoneInfo

import requests

from ghl_sms_provider import _db, _ghl, notify_owner

log = logging.getLogger(__name__)

GHL = "https://services.leadconnectorhq.com"
LOCATION = "6b4I6ILHBVcWQYlmPj3i"
CALENDAR_ID = "KAu6wku82FcqXDWSYTTP"
PIPELINE_ID = "mWQgum6OL4kJs8SSJExX"
STAGE_CALL_BOOKED = "3bdf427f-408b-442f-8cd3-c2fddbfdef6b"
OWNER_TZ = ZoneInfo("Asia/Beirut")
DEFAULT_LEAD_TZ = "America/Phoenix"

CONFIRM = "Hey {first}, you're booked with Kendall for {when}. The Google Meet link is in your calendar invite email. If anything comes up, just text me here."
DAY_BEFORE = "Hey {first}, quick reminder about our call tomorrow at {time}. If you have a website or Google page, send it over and I'll look at it before we talk."
HOUR_BEFORE = "Hey {first}, talking in an hour. The Google Meet link is in your calendar invite email, see you there."


def _h(version="2021-04-15"):
    return {"Authorization": f"Bearer {os.environ.get('GHL_MK7_PIT', '')}", "Version": version, "Accept": "application/json"}


def _init():
    with _db() as con:
        con.execute("""CREATE TABLE IF NOT EXISTS booking_reminders (
            event_id TEXT PRIMARY KEY, contact_id TEXT, start_ts REAL, booked_ts REAL,
            confirmed INTEGER DEFAULT 0, day_sent INTEGER DEFAULT 0, hour_sent INTEGER DEFAULT 0,
            cancelled INTEGER DEFAULT 0)""")


def _ts(v):
    if isinstance(v, (int, float)) or (isinstance(v, str) and v.isdigit()):
        return int(v) / (1000 if int(v) > 10**11 else 1)
    return datetime.datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()


def _contact(contact_id):
    r = requests.get(f"{GHL}/contacts/{contact_id}", headers=_h("2021-07-28"), timeout=15)
    return (r.json() if r.ok else {}).get("contact") or {}


def _send(contact_id, text):
    if os.environ.get("SERVER_BOOKING_TEXTS", "on").lower() == "off":
        return True  # GHL workflow owns the booking texts now; keep Call Booked + owner alerts only
    # Lumen SMS app token: proven path for sending through our SignalHouse provider
    r = _ghl("POST", "/conversations/messages", LOCATION, json={"type": "SMS", "contactId": contact_id, "message": text})
    if r.status_code >= 300:
        log.error("booking text to %s failed: %s %s", contact_id, r.status_code, r.text[:200])
    return r.status_code < 300


def _move_to_call_booked(contact_id):
    r = requests.get(f"{GHL}/opportunities/search", headers=_h("2021-07-28"), timeout=15,
                     params={"location_id": LOCATION, "pipeline_id": PIPELINE_ID, "contact_id": contact_id})
    for opp in (r.json() if r.ok else {}).get("opportunities", []):
        requests.put(f"{GHL}/opportunities/{opp['id']}", headers=_h("2021-07-28"), timeout=15,
                     json={"pipelineId": PIPELINE_ID, "pipelineStageId": STAGE_CALL_BOOKED})


def _fmt(ts, tz, with_day=True):
    t = datetime.datetime.fromtimestamp(ts, tz)
    hour = t.strftime("%I:%M%p").lstrip("0").lower()
    return f"{t.strftime('%A')} at {hour}" if with_day else hour


def _tick():
    now = time.time()
    r = requests.get(f"{GHL}/calendars/events", headers=_h(), timeout=20, params={
        "locationId": LOCATION, "calendarId": CALENDAR_ID,
        "startTime": int((now - 3600) * 1000), "endTime": int((now + 30 * 86400) * 1000)})
    if not r.ok:
        log.error("booking poll failed: %s %s", r.status_code, r.text[:200])
        return
    for ev in r.json().get("events", []):
        eid, cid = ev.get("id"), ev.get("contactId")
        if not eid or not cid:
            continue
        start = _ts(ev["startTime"])
        status = (ev.get("appointmentStatus") or "").lower()
        with _db() as con:
            row = con.execute("SELECT * FROM booking_reminders WHERE event_id=?", (eid,)).fetchone()
            if not row:
                con.execute("INSERT INTO booking_reminders (event_id, contact_id, start_ts, booked_ts) VALUES (?,?,?,?)",
                            (eid, cid, start, now))
                row = con.execute("SELECT * FROM booking_reminders WHERE event_id=?", (eid,)).fetchone()
            elif row["start_ts"] != start:  # rescheduled: reminders run again for the new time
                con.execute("UPDATE booking_reminders SET start_ts=?, day_sent=0, hour_sent=0, confirmed=0 WHERE event_id=?",
                            (start, eid))
                row = con.execute("SELECT * FROM booking_reminders WHERE event_id=?", (eid,)).fetchone()

        contact = None
        if status in ("cancelled", "invalid"):
            if not row["cancelled"]:
                contact = _contact(cid)
                notify_owner(f"Call cancelled: {(contact.get('contactName') or '').title()} "
                             f"({_fmt(start, OWNER_TZ)} your time)")
                with _db() as con:
                    con.execute("UPDATE booking_reminders SET cancelled=1 WHERE event_id=?", (eid,))
            continue
        if start < now:
            continue

        contact = _contact(cid)
        first = ((contact.get("firstName") or contact.get("contactName") or "there").split() or ["there"])[0].title()
        try:
            lead_tz = ZoneInfo(contact.get("timezone") or DEFAULT_LEAD_TZ)
        except Exception:
            lead_tz = ZoneInfo(DEFAULT_LEAD_TZ)
        until = start - now

        if not row["confirmed"]:
            _move_to_call_booked(cid)
            _send(cid, CONFIRM.format(first=first, when=_fmt(start, lead_tz)))
            notify_owner(f"New call booked: {(contact.get('contactName') or first).title()} {contact.get('phone', '')}, "
                         f"{_fmt(start, OWNER_TZ)} your time ({_fmt(start, lead_tz)} theirs)")
            with _db() as con:
                con.execute("UPDATE booking_reminders SET confirmed=1 WHERE event_id=?", (eid,))
        # Day-before reminder only when the booking was made well ahead of the call
        if not row["day_sent"] and 3 * 3600 < until <= 24 * 3600 and start - row["booked_ts"] > 26 * 3600:
            _send(cid, DAY_BEFORE.format(first=first, time=_fmt(start, lead_tz, with_day=False)))
            with _db() as con:
                con.execute("UPDATE booking_reminders SET day_sent=1 WHERE event_id=?", (eid,))
        if not row["hour_sent"] and 10 * 60 < until <= 65 * 60 and start - row["booked_ts"] > 2 * 3600:
            _send(cid, HOUR_BEFORE.format(first=first))
            with _db() as con:
                con.execute("UPDATE booking_reminders SET hour_sent=1 WHERE event_id=?", (eid,))


def start_booking_reminders():
    if not os.environ.get("GHL_MK7_PIT"):
        print("[Bookings] GHL_MK7_PIT not set, booking reminders off")
        return
    _init()

    def loop():
        while True:
            try:
                _tick()
            except Exception as e:
                log.exception("booking reminders tick failed: %s", e)
            time.sleep(60)

    threading.Thread(target=loop, daemon=True).start()
    print("[Bookings] booking reminders started")
