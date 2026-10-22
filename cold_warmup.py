"""
Lumen Cold Email warmup — free, inbox to inbox.

Every active cold inbox with warmup on sends a few normal looking emails a day to the other cold inboxes,
and the receiving inbox replies in thread some of the time. Each tick it also pulls any warmup mail out of
Spam back into the Inbox, marks it read and stars some of it. That's the signal Google learns from: mail
from this domain gets opened, replied to and rescued from spam.

  Ramp:     2 sends on day one, +1 per day, up to warmup_max (default 8). Weekends at half.
  Window:   8am to 7pm America/Denver, sends spread out with random gaps.
  Marker:   header X-Lumen-W on every warmup message. cold_email._ingest skips these so they never show
            up in the reply inbox or count as leads, and they never count toward cold send caps.

Needs at least two inboxes to send anything. With one inbox it only does spam rescue.
Env: COLD_WARMUP=off stops it (cold sender/receiver keep running).
"""

import datetime, email.utils, imaplib, os, random, re, threading, time
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

from crm import db, now_iso

WARM_HEADER = "X-Lumen-W"
WARM_TZ = ZoneInfo("America/Denver")
WINDOW = (8, 19)
MAX_DEPTH = 3          # new message + up to 2 replies in a thread


def init_warmup_db():
    con = db()
    con.execute("""CREATE TABLE IF NOT EXISTS cold_warmup_msgs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        from_inbox_id INTEGER NOT NULL,
        to_inbox_id INTEGER NOT NULL,
        message_id TEXT NOT NULL,
        refs TEXT,
        depth INTEGER NOT NULL DEFAULT 1,
        subject TEXT,
        reply_due_at TEXT,
        replied INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL)""")
    con.execute("CREATE INDEX IF NOT EXISTS idx_cw_from_day ON cold_warmup_msgs(from_inbox_id, created_at)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_cw_due ON cold_warmup_msgs(to_inbox_id, replied, reply_due_at)")
    cols = {r[1] for r in con.execute("PRAGMA table_info(cold_inboxes)").fetchall()}
    if "warmup_on" not in cols:
        con.execute("ALTER TABLE cold_inboxes ADD COLUMN warmup_on INTEGER NOT NULL DEFAULT 1")
    if "warmup_max" not in cols:
        con.execute("ALTER TABLE cold_inboxes ADD COLUMN warmup_max INTEGER NOT NULL DEFAULT 8")
    if "warmup_next_at" not in cols:
        con.execute("ALTER TABLE cold_inboxes ADD COLUMN warmup_next_at TEXT")
    con.commit()
    con.close()


# ── Content ───────────────────────────────────────────────────────────────────
SUBJECTS = [
    "Quick question", "Thursday", "Following up", "Notes from today", "That doc", "Next week",
    "Checking in", "Calendar", "The plan", "A thought", "Got a minute?", "Updated numbers",
    "Before Friday", "Small change", "The list", "Lunch?", "Call tomorrow", "Draft attached in Drive",
    "Monday", "Question on the timeline", "Thanks for this", "One more thing", "Review", "Plan for the week",
    "Idea for the client", "Quick favor", "Recap", "Schedule", "Heads up", "When you get a chance",
]
OPENERS = [
    "Hey {first},", "Hi {first},", "{first},", "Morning {first},", "Hey {first}, hope your day is going well.",
    "Hi {first}, hope the week is treating you well.",
]
MIDDLES = [
    "Do you have time tomorrow afternoon to go over the plan for next week? Thirty minutes should be enough.",
    "I went through the notes from earlier and I think we can move the review to Thursday. Does that still work for you?",
    "Can you send me the latest version of the list when you get a chance? I want to make sure I'm working from the right one.",
    "I put the updated numbers in the shared folder. Let me know if anything looks off to you.",
    "Quick one. Are we still meeting at the same time on Monday or did that move?",
    "I had an idea about how we handle the follow ups and wanted to run it by you before I change anything.",
    "Thanks again for the help yesterday. It made the rest of the afternoon a lot easier.",
    "I'm blocking out some time Friday morning to clean up the calendar. Anything you want me to add?",
    "Can you take a look at the draft before I send it out? No rush, end of day is fine.",
    "I talked to the team and everyone is fine with the new schedule. I'll send the invite later today.",
    "Where did we land on the budget question from last week? I want to close it out.",
    "I moved our call to 2pm since the morning filled up. Hope that's ok.",
    "Just wanted to check that you got the file I sent over. Sometimes the big ones don't come through.",
    "I'm putting together a short recap for the week. Anything you want included?",
    "Do you remember the name of the tool we looked at last month for scheduling? I can't find my notes.",
    "Can we push the check in to next week? This one got busy fast.",
]
CLOSERS = [
    "Thanks,\n{me}", "Talk soon,\n{me}", "Best,\n{me}", "{me}", "Thanks!\n{me}", "Appreciate it,\n{me}",
    "Let me know,\n{me}", "Cheers,\n{me}",
]
REPLIES = [
    "Sounds good, that works for me.", "Yes, Thursday is fine. I'll put it on the calendar.",
    "Got it, thanks. I'll take a look this afternoon.", "Just sent it over. Let me know if it doesn't come through.",
    "Same time works. See you then.", "Good idea. Let's talk it through on the call.",
    "Happy to help. Anytime.", "Nothing to add from my side, thanks for organizing it.",
    "I'll review it tonight and send notes in the morning.", "Perfect, thanks for letting me know.",
    "We said we'd revisit it at the end of the month. I'll add it to the list.", "2pm works. Thanks for moving it.",
    "Yes, got it. Everything opened fine.", "Thanks. I'll send you a couple of points later today.",
    "I think it was in the doc from the 12th. I'll look and send it to you.", "No problem, next week is fine.",
]


def _first(name):
    return (name or "").split()[0] if (name or "").strip() else "there"


def _compose(sender, recipient):
    body = "\n\n".join([
        random.choice(OPENERS).format(first=_first(recipient["from_name"])),
        random.choice(MIDDLES),
        random.choice(CLOSERS).format(me=_first(sender["from_name"])),
    ])
    return random.choice(SUBJECTS), body


def _compose_reply(sender, recipient):
    return "\n\n".join([
        random.choice(["", "Hey {first},", "Hi {first},", "{first},"]).format(first=_first(recipient["from_name"])),
        random.choice(REPLIES),
        random.choice(["{me}", "Thanks,\n{me}", "Best,\n{me}", ""]).format(me=_first(sender["from_name"])),
    ]).strip()


# ── Plan ──────────────────────────────────────────────────────────────────────
def warmup_target(ibx, local_now):
    days = (local_now.date() - datetime.date.fromisoformat(ibx["created_at"][:10])).days
    n = min(ibx["warmup_max"], 2 + max(days, 0))
    return max(1, n // 2) if local_now.weekday() >= 5 else n


def warmup_sent_today(con, inbox_id):
    day = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return con.execute("SELECT COUNT(*) FROM cold_warmup_msgs WHERE from_inbox_id = ? AND created_at >= ?",
                       (inbox_id, day)).fetchone()[0]


def _next_gap(target):
    minutes = (WINDOW[1] - WINDOW[0]) * 60 / max(target, 1)
    return datetime.timedelta(minutes=random.uniform(0.6, 1.4) * minutes)


# ── Send ──────────────────────────────────────────────────────────────────────
def _send(sender, recipient, subject, body, in_reply_to=None, refs=None):
    import cold_email as ce
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = email.utils.formataddr((sender["from_name"], sender["email"]))
    msg["To"] = email.utils.formataddr((recipient["from_name"], recipient["email"]))
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=False)
    mid = email.utils.make_msgid(domain=sender["email"].split("@")[1])
    msg["Message-ID"] = mid
    msg[WARM_HEADER] = "1"
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = refs or in_reply_to
    s = ce._smtp(sender)
    try:
        s.sendmail(sender["email"], [recipient["email"]], msg.as_string())
    finally:
        try:
            s.quit()
        except Exception:
            pass
    return mid


def _record(con, sender, recipient, mid, refs, depth, subject):
    due = None
    if depth < MAX_DEPTH and random.random() < (0.7 if depth == 1 else 0.4):
        due = (datetime.datetime.now(datetime.timezone.utc)
               + datetime.timedelta(minutes=random.randint(20, 180))).isoformat()
    con.execute("INSERT INTO cold_warmup_msgs (from_inbox_id, to_inbox_id, message_id, refs, depth, subject, "
                "reply_due_at, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (sender["id"], recipient["id"], mid, refs, depth, subject, due, now_iso()))


def warmup_tick():
    """One pass: rescue spam, send due replies, start new threads. Returns number sent."""
    con = db()
    utc = datetime.datetime.now(datetime.timezone.utc)
    local = utc.astimezone(WARM_TZ)
    inboxes = [r for r in con.execute("SELECT * FROM cold_inboxes WHERE status = 'active' AND warmup_on = 1")
               .fetchall()]
    for ibx in inboxes:
        rescue_spam(ibx)
    if not (WINDOW[0] <= local.hour < WINDOW[1]) or len(inboxes) < 2:
        con.close()
        return 0
    by_id = {i["id"]: i for i in inboxes}
    sent = 0
    for ibx in inboxes:
        target = warmup_target(ibx, local)
        done = warmup_sent_today(con, ibx["id"])
        if done >= target + 2:
            continue
        # Replies owed by this inbox go first (replies may run 2 over the target).
        due = con.execute("SELECT * FROM cold_warmup_msgs WHERE to_inbox_id = ? AND replied = 0 AND "
                          "reply_due_at IS NOT NULL AND reply_due_at <= ? ORDER BY reply_due_at LIMIT 1",
                          (ibx["id"], utc.isoformat())).fetchone()
        try:
            if due and due["from_inbox_id"] in by_id:
                peer = by_id[due["from_inbox_id"]]
                subj = due["subject"] if (due["subject"] or "").lower().startswith("re:") else f"Re: {due['subject']}"
                refs = " ".join(x for x in [due["refs"], due["message_id"]] if x)
                mid = _send(ibx, peer, subj, _compose_reply(ibx, peer), in_reply_to=due["message_id"], refs=refs)
                con.execute("UPDATE cold_warmup_msgs SET replied = 1 WHERE id = ?", (due["id"],))
                _record(con, ibx, peer, mid, refs, due["depth"] + 1, subj)
                con.commit()
                sent += 1
                continue
            if due:
                con.execute("UPDATE cold_warmup_msgs SET replied = 1 WHERE id = ?", (due["id"],))
                con.commit()
            if done >= target:
                continue
            nxt = ibx["warmup_next_at"]
            if nxt and datetime.datetime.fromisoformat(nxt) > utc:
                continue
            peer = random.choice([p for p in inboxes if p["id"] != ibx["id"]])
            subj, body = _compose(ibx, peer)
            mid = _send(ibx, peer, subj, body)
            _record(con, ibx, peer, mid, None, 1, subj)
            con.execute("UPDATE cold_inboxes SET warmup_next_at = ? WHERE id = ?",
                        ((utc + _next_gap(target)).isoformat(), ibx["id"]))
            con.commit()
            sent += 1
        except Exception as e:
            print(f"[warmup] send from {ibx['email']} failed: {e}")
            con.execute("UPDATE cold_inboxes SET warmup_next_at = ? WHERE id = ?",
                        ((utc + datetime.timedelta(minutes=30)).isoformat(), ibx["id"]))
            con.commit()
    con.close()
    return sent


# ── Engagement: spam rescue, read, star ───────────────────────────────────────
def _spam_folder(m):
    typ, data = m.list()
    for line in data or []:
        s = line.decode(errors="ignore") if isinstance(line, bytes) else str(line)
        if "\\Junk" in s:
            name = re.search(r'"([^"]+)"\s*$', s) or re.search(r"(\S+)\s*$", s)
            if name:
                return name.group(1)
    return "[Gmail]/Spam"


def rescue_spam(ibx):
    """Move warmup mail out of Spam, mark warmup mail in the Inbox read, star about 1 in 5."""
    import cold_email as ce
    try:
        m = ce._imap(ibx)
    except Exception as e:
        print(f"[warmup] imap {ibx['email']} failed: {e}")
        return
    try:
        crit = f'(HEADER {WARM_HEADER} "1")'
        if m.select(f'"{_spam_folder(m)}"')[0] == "OK":
            typ, d = m.uid("search", None, crit)
            uids = (d[0] or b"").split()
            if uids:
                seq = b",".join(uids).decode()
                if m.uid("MOVE", seq, "INBOX")[0] != "OK":
                    m.uid("COPY", seq, "INBOX")
                    m.uid("STORE", seq, "+FLAGS", r"(\Deleted)")
                    m.expunge()
                print(f"[warmup] {ibx['email']}: rescued {len(uids)} from spam")
        if m.select("INBOX")[0] == "OK":
            typ, d = m.uid("search", None, f"UNSEEN {crit}")
            for uid in (d[0] or b"").split():
                m.uid("STORE", uid, "+FLAGS", r"(\Seen)")
                if random.random() < 0.2:
                    m.uid("STORE", uid, "+FLAGS", r"(\Flagged)")
    except imaplib.IMAP4.error as e:
        print(f"[warmup] {ibx['email']} engage failed: {e}")
    finally:
        try:
            m.logout()
        except Exception:
            pass


# ── Worker ────────────────────────────────────────────────────────────────────
_started = False


def start_warmup_worker():
    global _started
    if _started or os.environ.get("COLD_WARMUP", "on").lower() == "off" \
            or os.environ.get("COLD_EMAIL_WORKERS", "on").lower() == "off":
        return
    _started = True

    def loop():
        time.sleep(60)
        while True:
            try:
                warmup_tick()
            except Exception as e:
                print(f"[warmup] tick failed: {e}")
            time.sleep(300 + random.randint(-60, 60))

    threading.Thread(target=loop, daemon=True, name="cold-warmup").start()
    print("[warmup] worker started")
