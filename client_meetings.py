"""Weekly client meetings: recurring Google Calendar invite + a confirm or reschedule email the day before.

Built for Avalon Laser (Fridays 9:00am Pacific), works for any client.

How it works, with as few moving parts as possible:
  - The meeting is ONE recurring event on kendall@lumenmarketing.co, created by scripts/create_meeting_series.py.
    Google sends the calendar invite and every update, from Kendall's own address, with the Meet link.
  - Google Calendar is the only state. The series is found by a private extended property (lumen_series=<key>),
    the recipients are the event's attendees, and "this week's email already went out" is a property on that
    week's instance. No database table, no env var per client.
  - A background thread checks every 5 minutes. At 9:00am (series time zone) the day before an instance,
    it emails the attendees two buttons: Confirm, or Choose another time.
  - Choose another time offers only slots inside a buffer after the original start (default 4 hours, 30 minute
    steps), and only ones that are free on Kendall's calendar. Picking one moves that single week's instance,
    so the series itself never changes. No reply means the meeting stands.

Env: GCAL_CLIENT_ID, GCAL_CLIENT_SECRET, GCAL_REFRESH_TOKEN (same OAuth client as the WhatsApp booking agent),
RESEND_API_KEY, MEETING_ADMIN_KEY (for the manual send endpoint), optional MEETINGS_ENABLED=false to stop sends.
"""
import os, json, hmac, time, base64, hashlib, threading, datetime as dt
from zoneinfo import ZoneInfo

import requests
from flask import Blueprint, request, abort

meetings_bp = Blueprint("meetings", __name__)

CAL_ID = "primary"
OWNER_EMAIL = "kendall@lumenmarketing.co"
BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://lumenmarketing.co")
SEND_HOUR = 9            # local hour, day before the meeting
BUFFER_HOURS = 4         # reschedule window after the original start
STEP_MIN = 30            # slot spacing
TICK_SECONDS = 300


# ── Google ────────────────────────────────────────────────────────────────────
def _svc():
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials(
        None, refresh_token=os.environ.get("GCAL_REFRESH_TOKEN", ""),
        client_id=os.environ.get("GCAL_CLIENT_ID", ""), client_secret=os.environ.get("GCAL_CLIENT_SECRET", ""),
        token_uri="https://oauth2.googleapis.com/token",
        scopes=["https://www.googleapis.com/auth/calendar.events",
                "https://www.googleapis.com/auth/calendar.readonly"])
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


def configured():
    return all(os.environ.get(k) for k in ("GCAL_CLIENT_ID", "GCAL_CLIENT_SECRET", "GCAL_REFRESH_TOKEN"))


def _props(ev):
    return (ev.get("extendedProperties") or {}).get("private") or {}


def all_series(svc=None):
    svc = svc or _svc()
    items = svc.events().list(calendarId=CAL_ID, privateExtendedProperty="lumen_meeting=1",
                              singleEvents=False, maxResults=50).execute().get("items", [])
    return [e for e in items if e.get("recurrence") and e.get("status") != "cancelled"]


def get_series(key, svc=None):
    svc = svc or _svc()
    items = svc.events().list(calendarId=CAL_ID, privateExtendedProperty=f"lumen_series={key}",
                              singleEvents=False, maxResults=5).execute().get("items", [])
    items = [e for e in items if e.get("recurrence") and e.get("status") != "cancelled"]
    return items[0] if items else None


def next_instance(series, svc=None, after=None):
    """Next occurrence of the series. Uses a singleEvents list, not events.instances(): Google's instances()
    leaves out an occurrence once it has been moved, which would lose track of a rescheduled week."""
    svc = svc or _svc()
    after = after or dt.datetime.now(dt.timezone.utc)
    key = _props(series).get("lumen_series")
    items = svc.events().list(calendarId=CAL_ID, singleEvents=True, orderBy="startTime", timeMin=after.isoformat(),
                              maxResults=6, privateExtendedProperty=f"lumen_series={key}").execute().get("items", [])
    for i in items:
        if i.get("recurringEventId") == series["id"] and i.get("status") != "cancelled":
            return i
    return None


def _start(ev):
    return dt.datetime.fromisoformat(ev["start"]["dateTime"].replace("Z", "+00:00"))


def _tz(series):
    return ZoneInfo(series["start"].get("timeZone") or "America/Los_Angeles")


def recipients(series):
    """Guests who get the confirm email. Kendall is the organizer, so he is skipped unless the series
    is flagged allow_owner=1 (used for testing on his own inbox)."""
    allow_owner = _props(series).get("allow_owner") == "1"
    return [a["email"] for a in series.get("attendees", [])
            if a.get("email") and not a.get("resource") and (allow_owner or a["email"].lower() != OWNER_EMAIL)]


def _meet_link(ev):
    for ep in (ev.get("conferenceData") or {}).get("entryPoints", []):
        if ep.get("entryPointType") == "video":
            return ep.get("uri")
    return ev.get("hangoutLink")


def _mark(svc, inst, **kv):
    p = dict(_props(inst)); p.update({k: str(v) for k, v in kv.items()})
    svc.events().patch(calendarId=CAL_ID, eventId=inst["id"], sendUpdates="none",
                       body={"extendedProperties": {"private": p}}).execute()


# ── Signed links ──────────────────────────────────────────────────────────────
def _key():
    s = os.environ.get("MEETING_SIGNING_KEY") or (os.environ.get("GCAL_CLIENT_SECRET", "") + os.environ.get("SECRET_KEY", ""))
    return hashlib.sha256(("lumen-meetings|" + s).encode()).digest()


def make_token(series_key, instance_id, exp_ts):
    body = base64.urlsafe_b64encode(json.dumps({"s": series_key, "i": instance_id, "e": int(exp_ts)}).encode()).decode().rstrip("=")
    sig = base64.urlsafe_b64encode(hmac.new(_key(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")[:32]
    return f"{body}.{sig}"


def read_token(tok):
    try:
        body, sig = tok.split(".", 1)
        good = base64.urlsafe_b64encode(hmac.new(_key(), body.encode(), hashlib.sha256).digest()).decode().rstrip("=")[:32]
        if not hmac.compare_digest(sig, good):
            return None
        d = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
        return d if d["e"] > time.time() else None
    except Exception:
        return None


# ── Slots ─────────────────────────────────────────────────────────────────────
def original_start(inst, series):
    """The series' normal time on this instance's day, even if this week was already moved."""
    ot = inst.get("originalStartTime", {}).get("dateTime")
    return dt.datetime.fromisoformat(ot.replace("Z", "+00:00")) if ot else _start(inst)


def candidate_slots(svc, series, inst):
    tz = _tz(series)
    base = original_start(inst, series).astimezone(tz)
    length = _start({"start": inst["end"]}) - _start(inst)
    cur = _start(inst)
    now = dt.datetime.now(dt.timezone.utc)
    slots, t = [], base
    while t + length <= base + dt.timedelta(hours=BUFFER_HOURS):
        if t > now:
            slots.append(t)
        t += dt.timedelta(minutes=STEP_MIN)
    if not slots:
        return [], cur, length
    fb = svc.freebusy().query(body={"timeMin": slots[0].isoformat(), "timeMax": (slots[-1] + length).isoformat(),
                                    "items": [{"id": CAL_ID}]}).execute()
    busy = [(dt.datetime.fromisoformat(b["start"].replace("Z", "+00:00")), dt.datetime.fromisoformat(b["end"].replace("Z", "+00:00")))
            for b in fb["calendars"][CAL_ID].get("busy", [])]
    # the meeting itself shows as busy; it is not a conflict with its own new time
    busy = [(b0, b1) for b0, b1 in busy if not (b0 == cur and b1 == cur + length)]
    free = [s for s in slots if not any(s < b1 and s + length > b0 for b0, b1 in busy)]
    return free, cur, length


# ── Email ─────────────────────────────────────────────────────────────────────
def _fmt(d, tz, with_day=True):
    d = d.astimezone(tz)
    h = d.strftime("%I:%M%p").lstrip("0").lower()
    return (d.strftime("%A, %B ") + str(d.day) + " at " + h) if with_day else h


def _btn(href, label, dark=True):
    bg, fg, bd = ("#1a1a1a", "#ffffff", "#1a1a1a") if dark else ("#ffffff", "#1a1a1a", "#1a1a1a")
    return (f'<a href="{href}" style="display:inline-block;padding:14px 26px;margin:0 10px 12px 0;border-radius:8px;'
            f'background:{bg};color:{fg};border:1.5px solid {bd};font-weight:600;font-size:15px;text-decoration:none">{label}</a>')


def _wrap(inner):
    return (f'<div style="font-family:Inter,Helvetica,Arial,sans-serif;color:#1a1a1a;max-width:560px;margin:0 auto;padding:36px 24px;line-height:1.7">'
            f'<div style="font-size:11px;letter-spacing:.14em;font-weight:700;color:#c5a44e;margin-bottom:22px">LUMEN MARKETING SOLUTIONS</div>'
            f'{inner}<p style="font-size:13px;color:#888;margin-top:34px">Kendall Davis · Lumen Marketing Solutions</p></div>')


def _resend(to, subject, html, reply_to=OWNER_EMAIL):
    r = requests.post("https://api.resend.com/emails", timeout=30,
                      headers={"Authorization": f"Bearer {os.environ.get('RESEND_API_KEY', '')}"},
                      json={"from": "Kendall Davis <notifications@lumenmarketing.co>", "to": to,
                            "reply_to": reply_to, "subject": subject, "html": html})
    return r.status_code, r.text[:200]


def send_confirmation(series_key, force=False):
    """Email this week's confirm/reschedule request. Idempotent per instance unless force."""
    svc = _svc()
    series = get_series(series_key, svc)
    if not series:
        return {"ok": False, "error": f"no series {series_key}"}
    inst = next_instance(series, svc)
    if not inst:
        return {"ok": False, "error": "no upcoming instance"}
    if _props(inst).get("confirm_sent") and not force:
        return {"ok": True, "skipped": "already sent", "instance": inst["id"]}
    tz = _tz(series); start = _start(inst)
    to = recipients(series)
    if not to:
        return {"ok": False, "error": "series has no attendees"}
    tok = make_token(series_key, inst["id"], start.timestamp())
    names = _props(series).get("greeting") or "there"
    when = _fmt(start, tz)
    html = _wrap(
        f'<p style="font-size:16px">Hi {names},</p>'
        f'<p style="font-size:16px">Just confirming our weekly call tomorrow, <b>{when} Pacific</b>.</p>'
        f'<div style="margin:26px 0 18px">{_btn(f"{BASE_URL}/meeting/confirm?t={tok}", "Confirm")}'
        f'{_btn(f"{BASE_URL}/meeting/reschedule?t={tok}", "Choose another time", dark=False)}</div>'
        f'<p style="font-size:14px;color:#555">If that time no longer works, choose another time and pick any open slot on the same day. '
        f'If I don\'t hear back, we\'ll meet as scheduled.</p>')
    code, body = _resend(to, f"Confirming our call tomorrow, {_fmt(start, tz, False)} Pacific", html)
    if code in (200, 201):
        _mark(svc, inst, confirm_sent=dt.datetime.utcnow().isoformat(timespec="seconds"))
    return {"ok": code in (200, 201), "to": to, "instance": inst["id"], "when": when, "resend": body}


# ── Scheduler ─────────────────────────────────────────────────────────────────
_started = False
_lock = threading.Lock()


def _due(series, inst, now_utc):
    tz = _tz(series)
    start = _start(inst).astimezone(tz)
    send_at = (start - dt.timedelta(days=1)).replace(hour=SEND_HOUR, minute=0, second=0, microsecond=0)
    return send_at <= now_utc.astimezone(tz) < start


def tick():
    if os.environ.get("MEETINGS_ENABLED", "true").lower() == "false" or not configured():
        return
    svc = _svc()
    now = dt.datetime.now(dt.timezone.utc)
    for series in all_series(svc):
        p = _props(series)
        if p.get("auto") != "1":
            continue
        inst = next_instance(series, svc, after=now)
        if inst and not _props(inst).get("confirm_sent") and _due(series, inst, now):
            print("[meetings] sending", p.get("lumen_series"), send_confirmation(p.get("lumen_series")), flush=True)


def _loop():
    time.sleep(20)
    while True:
        try:
            tick()
        except Exception as e:
            print(f"[meetings] tick error: {e}", flush=True)
        time.sleep(TICK_SECONDS)


def start_scheduler():
    global _started
    with _lock:
        if _started or not configured():
            return
        threading.Thread(target=_loop, daemon=True, name="client-meetings").start()
        _started = True


# ── Pages ─────────────────────────────────────────────────────────────────────
def _page(title, inner):
    return f'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
<style>*{{box-sizing:border-box}}body{{margin:0;background:#faf8f3;font-family:Inter,Helvetica,Arial,sans-serif;color:#1a1a1a}}
.c{{max-width:520px;margin:0 auto;padding:56px 22px}}.k{{font-size:11px;letter-spacing:.14em;font-weight:700;color:#c5a44e;margin-bottom:26px}}
h1{{font-size:28px;font-weight:800;letter-spacing:-.01em;line-height:1.2;margin:0 0 16px}}p{{font-size:16px;line-height:1.7;color:#444;margin:0 0 18px}}
.card{{background:#fff;border:1px solid #e6e2d6;border-radius:12px;padding:26px 24px;margin-top:26px}}
.slots{{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:12px;margin-top:10px}}
button{{font:inherit;font-size:16px;font-weight:600;padding:15px 10px;border-radius:9px;border:1.5px solid #1a1a1a;background:#fff;color:#1a1a1a;cursor:pointer;width:100%}}
button:hover{{background:#1a1a1a;color:#fff}}a{{color:#1a1a1a}}.m{{font-size:14px;color:#777}}</style></head>
<body><div class="c"><div class="k">LUMEN MARKETING SOLUTIONS</div>{inner}</div></body></html>'''


def _load(tok):
    d = read_token(tok or "")
    if not d:
        return None, None, None, None
    svc = _svc()
    series = get_series(d["s"], svc)
    if not series:
        return None, None, None, None
    inst = svc.events().get(calendarId=CAL_ID, eventId=d["i"]).execute()
    if inst.get("status") == "cancelled":
        return None, None, None, None
    return d, svc, series, inst


EXPIRED = ("Link expired", "<h1>This link has expired</h1><p>This week's meeting has already started or passed. "
           "Reply to the email and I'll sort out a time.</p>")


@meetings_bp.route("/meeting/confirm")
def confirm():
    d, svc, series, inst = _load(request.args.get("t"))
    if not d:
        return _page(*EXPIRED)
    tz = _tz(series); start = _start(inst)
    if not _props(inst).get("confirmed"):
        _mark(svc, inst, confirmed=dt.datetime.utcnow().isoformat(timespec="seconds"))
        _resend([OWNER_EMAIL], f"Confirmed: {series.get('summary')} {_fmt(start, tz)}",
                _wrap(f"<p><b>{series.get('summary')}</b> is confirmed for <b>{_fmt(start, tz)} Pacific</b>.</p>"))
    link = _meet_link(inst)
    return _page("Confirmed", f"<h1>You're confirmed</h1><p>See you <b>{_fmt(start, tz)} Pacific</b>.</p>"
                 + (f'<p><a href="{link}">Join link for the call</a></p>' if link else "")
                 + '<p class="m">The calendar invite already has everything you need.</p>')


@meetings_bp.route("/meeting/reschedule", methods=["GET", "POST"])
def reschedule():
    tok = request.values.get("t")
    d, svc, series, inst = _load(tok)
    if not d:
        return _page(*EXPIRED)
    tz = _tz(series)
    free, cur, length = candidate_slots(svc, series, inst)
    if request.method == "POST":
        try:
            pick = dt.datetime.fromtimestamp(int(request.form.get("slot", "0")), dt.timezone.utc)
        except Exception:
            abort(400)
        if pick == cur:
            return _page("Confirmed", f"<h1>You're all set</h1><p>We're meeting <b>{_fmt(cur, tz)} Pacific</b>.</p>")
        if pick not in free:
            return _page("Taken", "<h1>That time was just taken</h1><p>Go back and pick another open slot.</p>"
                         f'<p><a href="/meeting/reschedule?t={tok}">See open times</a></p>')
        stz = series["start"].get("timeZone") or "America/Los_Angeles"
        svc.events().patch(calendarId=CAL_ID, eventId=inst["id"], sendUpdates="all", body={
            "start": {"dateTime": pick.astimezone(tz).isoformat(), "timeZone": stz},
            "end": {"dateTime": (pick + length).astimezone(tz).isoformat(), "timeZone": stz},
            "extendedProperties": {"private": {**_props(inst), "rescheduled": pick.isoformat(), "confirmed": "1"}}}).execute()
        when = _fmt(pick, tz)
        _resend(recipients(series), f"Moved: our call is now {when} Pacific",
                _wrap(f"<p style='font-size:16px'>Done. This week's call is now <b>{when} Pacific</b>. "
                      f"The calendar invite has been updated, and we're back to the usual time next week.</p>"))
        _resend([OWNER_EMAIL], f"Rescheduled: {series.get('summary')} to {when}",
                _wrap(f"<p><b>{series.get('summary')}</b> moved from {_fmt(cur, tz)} to <b>{when} Pacific</b>.</p>"))
        return _page("Rescheduled", f"<h1>You're booked for {_fmt(pick, tz, False)}</h1>"
                     f"<p>This week's call is now <b>{when} Pacific</b>. The calendar invite has been updated.</p>"
                     "<p class='m'>We're back to the usual time next week.</p>")
    buttons = "".join(
        f'<form method="post"><input type="hidden" name="t" value="{tok}"><input type="hidden" name="slot" value="{int(s.timestamp())}">'
        f'<button>{_fmt(s, tz, False)}{" (current)" if s == cur else ""}</button></form>' for s in free)
    body = (f"<h1>Pick a time that works</h1><p>Our call is currently <b>{_fmt(cur, tz)} Pacific</b>. "
            f"Choose any open time below and the calendar invite updates automatically.</p>"
            f'<div class="card"><div class="m" style="margin-bottom:8px">{_fmt(original_start(inst, series), tz).split(" at ")[0]}, Pacific time</div>'
            f'<div class="slots">{buttons}</div></div>' if free else
            "<h1>No other times are open</h1><p>Reply to the email and I'll find a time that works.</p>")
    return _page("Choose a time", body)


@meetings_bp.route("/meeting/admin/send-now")
def admin_send_now():
    if not os.environ.get("MEETING_ADMIN_KEY") or request.args.get("key") != os.environ.get("MEETING_ADMIN_KEY"):
        abort(404)
    return send_confirmation(request.args.get("series", ""), force=request.args.get("force") == "1")
