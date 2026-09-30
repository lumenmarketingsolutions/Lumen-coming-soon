"""Create a weekly client meeting series on kendall@lumenmarketing.co. Google emails the invite.

  python3 scripts/create_meeting_series.py --key avalon_weekly --summary "Avalon Laser x Lumen, weekly call" \
      --attendees laleh@avalon-laser.com,avesta70@gmail.com --greeting "Laleh and Reza" \
      --first 2026-10-02 --time 09:00 --tz America/Los_Angeles --minutes 30 --auto 1

Needs GCAL_CLIENT_ID / GCAL_CLIENT_SECRET / GCAL_REFRESH_TOKEN in the environment.
Refuses to create a second series with the same key.
"""
import argparse, os, sys, uuid, datetime as dt
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from client_meetings import _svc, get_series, CAL_ID, OWNER_EMAIL

a = argparse.ArgumentParser()
for k in ("key", "summary", "attendees", "greeting", "first", "time"):
    a.add_argument(f"--{k}", required=True)
a.add_argument("--tz", default="America/Los_Angeles"); a.add_argument("--minutes", type=int, default=30)
a.add_argument("--auto", default="1"); a.add_argument("--byday", default="FR")
a.add_argument("--description", default="Our weekly check-in: last week's results, what changes next week, creative in progress, and your questions.\n\n"
               "The day before each call you'll get an email to confirm or pick another time.")
x = a.parse_args()
svc = _svc()
if get_series(x.key, svc):
    sys.exit(f"series {x.key} already exists, not creating another")
start = dt.datetime.fromisoformat(f"{x.first}T{x.time}:00")
end = start + dt.timedelta(minutes=x.minutes)
body = {
    "summary": x.summary, "description": x.description,
    "start": {"dateTime": start.isoformat(), "timeZone": x.tz},
    "end": {"dateTime": end.isoformat(), "timeZone": x.tz},
    "recurrence": [f"RRULE:FREQ=WEEKLY;BYDAY={x.byday}"],
    "attendees": [{"email": e.strip()} for e in x.attendees.split(",") if e.strip()] + [{"email": OWNER_EMAIL, "organizer": True, "responseStatus": "accepted"}],
    "conferenceData": {"createRequest": {"requestId": uuid.uuid4().hex, "conferenceSolutionKey": {"type": "hangoutsMeet"}}},
    "reminders": {"useDefault": False, "overrides": [{"method": "popup", "minutes": 30}]},
    "extendedProperties": {"private": {"lumen_meeting": "1", "lumen_series": x.key, "auto": x.auto, "greeting": x.greeting}},
}
ev = svc.events().insert(calendarId=CAL_ID, body=body, sendUpdates="all", conferenceDataVersion=1).execute()
print("created", ev["id"], ev.get("htmlLink"), "meet:", ev.get("hangoutLink"))
