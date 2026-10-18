"""
Lumen Cold Email — send, receive and reply from our own inboxes. Admin-only, mounted at /crm/email.

Separate from outreach.py on purpose: that tool sends through Resend on go.lumenmarketing.co and is
never used for cold. This one sends from Google Workspace inboxes on the cold domains
(golumenmarketing.com, withlumenmarketing.com, ...) over SMTP and reads replies back over IMAP.

  Inboxes    = mailbox + app password (encrypted at rest), daily cap with a warmup ramp.
  Campaigns  = audience (reuses outreach_audiences CSV upload) x steps (day 0, +3, +7 ...), sent as one
               thread from the same inbox, stops when the lead replies, bounces or opts out.
  Inbox      = every reply in one place; answer from the same mailbox so the thread stays intact.
  Interested = contact + opportunity in GHL MK7 "Contractor OS" at Conversation, thread as a note.

Workers (single gunicorn worker, daemon threads like booking_reminders.py):
  sender   every 30s: due steps, inside the campaign's days/hours, per-inbox daily cap + human gaps.
  receiver every 2 min: new mail per inbox -> reply / auto-reply / bounce / opt-out / other.

Env: COLD_EMAIL_KEY (encrypts app passwords; falls back to SECRET_KEY), GHL_MK7_PIT,
     OUTREACH_MAILING_ADDRESS (CAN-SPAM footer), COLD_EMAIL_ALERT_TO (default kendall@lumenmarketing.co),
     COLD_EMAIL_WORKERS=off to stop both threads.
"""

import base64, datetime, email, email.utils, hashlib, hmac, html as html_lib, imaplib, json, os, random, re
import smtplib, ssl, threading, time
from email.header import decode_header, make_header
from email.mime.text import MIMEText
from zoneinfo import ZoneInfo

import requests
from cryptography.fernet import Fernet, InvalidToken
from flask import Blueprint, render_template, request, redirect, abort, jsonify, flash, url_for

from crm import db, current_user, admin_required, now_iso, RESEND_API_KEY

cold_bp = Blueprint("cold", __name__, url_prefix="/crm/email")


@cold_bp.context_processor
def _inject_user():
    return {"u": current_user()}

GHL = "https://services.leadconnectorhq.com"
GHL_LOCATION = os.environ.get("GHL_SMS_LOCATION_ID", "6b4I6ILHBVcWQYlmPj3i")
GHL_PIPELINE = "mWQgum6OL4kJs8SSJExX"                      # Contractor OS
GHL_STAGE_CONVERSATION = "a84968bd-a3c6-4e5b-a715-5f7253cc0e22"
PUBLIC_BASE_URL = os.environ.get("CRM_BASE_URL", "https://lumenmarketing.co").rstrip("/")
MAILING_ADDRESS = os.environ.get("OUTREACH_MAILING_ADDRESS", "").strip()
ALERT_TO = os.environ.get("COLD_EMAIL_ALERT_TO", "kendall@lumenmarketing.co")

PROVIDERS = {
    "google":    {"smtp_host": "smtp.gmail.com", "smtp_port": 465, "imap_host": "imap.gmail.com", "imap_port": 993},
    "microsoft": {"smtp_host": "smtp.office365.com", "smtp_port": 587, "imap_host": "outlook.office365.com", "imap_port": 993},
}
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
TIMEZONES = ["America/New_York", "America/Chicago", "America/Denver", "America/Phoenix", "America/Los_Angeles",
             "Asia/Beirut", "Asia/Dubai", "Europe/London"]
OPT_OUT_RE = re.compile(r"^\s*(stop|unsubscribe|remove( me)?|take me off|not interested,? remove)\b", re.I)
AUTO_SUBJECT_RE = re.compile(r"(out of (the )?office|automatic reply|auto[- ]?reply|autoreply|away from|on vacation|"
                             r"thank you for (your )?(email|message|contacting))", re.I)


# ── Crypto for app passwords ──────────────────────────────────────────────────
def _seed():
    return (os.environ.get("COLD_EMAIL_KEY") or os.environ.get("SECRET_KEY", "lumen-wl-key-2026")).encode()


def _fernet():
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(_seed()).digest()))


def _enc(s):
    return _fernet().encrypt(s.encode()).decode()


def _dec(s):
    try:
        return _fernet().decrypt((s or "").encode()).decode()
    except InvalidToken:
        return ""


# ── Schema ────────────────────────────────────────────────────────────────────
def init_cold_db():
    con = db()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS cold_inboxes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL UNIQUE,
        from_name TEXT NOT NULL,
        provider TEXT NOT NULL DEFAULT 'google',
        smtp_host TEXT NOT NULL, smtp_port INTEGER NOT NULL,
        imap_host TEXT NOT NULL, imap_port INTEGER NOT NULL,
        password_enc TEXT NOT NULL,
        signature TEXT,
        daily_cap INTEGER NOT NULL DEFAULT 20,      -- cold sends per day once ramped
        ramp_start INTEGER NOT NULL DEFAULT 5,      -- cold sends on the first sending day
        ramp_step INTEGER NOT NULL DEFAULT 2,       -- +N per day until daily_cap
        cold_start_date TEXT,                       -- YYYY-MM-DD; no cold sends before (warmup period)
        min_gap_min INTEGER NOT NULL DEFAULT 6,     -- minutes between sends from this inbox (+ jitter)
        status TEXT NOT NULL DEFAULT 'active',      -- active | paused | error
        last_error TEXT,
        last_sent_at TEXT,
        imap_last_uid INTEGER,
        imap_uidvalidity TEXT,
        last_checked_at TEXT,
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cold_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        audience_id INTEGER,
        status TEXT NOT NULL DEFAULT 'draft',       -- draft | active | paused | completed
        inbox_ids TEXT NOT NULL DEFAULT '[]',
        send_days TEXT NOT NULL DEFAULT '0,1,2,3,4',-- Mon=0
        window_start INTEGER NOT NULL DEFAULT 7,
        window_end INTEGER NOT NULL DEFAULT 16,
        timezone TEXT NOT NULL DEFAULT 'America/Denver',
        daily_limit INTEGER NOT NULL DEFAULT 0,     -- 0 = only inbox caps apply
        ghl_tag TEXT,
        created_at TEXT NOT NULL,
        launched_at TEXT
    );

    CREATE TABLE IF NOT EXISTS cold_steps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL REFERENCES cold_campaigns(id) ON DELETE CASCADE,
        step_no INTEGER NOT NULL,
        delay_days INTEGER NOT NULL DEFAULT 0,      -- days after the previous step
        subject TEXT,                               -- blank on follow-ups = "Re: <first subject>"
        body TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS cold_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL REFERENCES cold_campaigns(id) ON DELETE CASCADE,
        email TEXT NOT NULL,
        first_name TEXT, last_name TEXT, company TEXT, extra_json TEXT,
        inbox_id INTEGER,
        status TEXT NOT NULL DEFAULT 'active',      -- active | replied | bounced | unsubscribed | completed | stopped
        last_step INTEGER NOT NULL DEFAULT 0,
        next_send_at TEXT,
        thread_subject TEXT,
        first_message_id TEXT,
        last_message_id TEXT,
        interest TEXT,                              -- interested | not_interested | later
        ghl_contact_id TEXT, ghl_opportunity_id TEXT,
        replied_at TEXT,
        unread INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        UNIQUE(campaign_id, email)
    );
    CREATE INDEX IF NOT EXISTS idx_cl_due ON cold_leads(status, next_send_at);
    CREATE INDEX IF NOT EXISTS idx_cl_email ON cold_leads(email);

    CREATE TABLE IF NOT EXISTS cold_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lead_id INTEGER,
        inbox_id INTEGER,
        direction TEXT NOT NULL,                    -- out | in
        kind TEXT NOT NULL,                         -- step | manual | reply | auto | bounce | optout | other
        step_no INTEGER,
        message_id TEXT, in_reply_to TEXT,
        from_addr TEXT, to_addr TEXT, subject TEXT,
        body_text TEXT,
        created_at TEXT NOT NULL,
        is_read INTEGER NOT NULL DEFAULT 1
    );
    CREATE INDEX IF NOT EXISTS idx_cm_lead ON cold_messages(lead_id);
    CREATE INDEX IF NOT EXISTS idx_cm_msgid ON cold_messages(message_id);
    CREATE INDEX IF NOT EXISTS idx_cm_inbox_day ON cold_messages(inbox_id, direction, created_at);
    """)
    # Reuse the outreach suppression list so an opt-out anywhere is an opt-out everywhere.
    con.execute("""CREATE TABLE IF NOT EXISTS outreach_suppressions (
        email TEXT PRIMARY KEY, reason TEXT NOT NULL, source_campaign_id INTEGER, created_at TEXT NOT NULL)""")
    con.commit()
    con.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _norm(e):
    return (e or "").strip().lower()


def _utcnow():
    return datetime.datetime.utcnow()


def _suppressed(con, e):
    return bool(con.execute("SELECT 1 FROM outreach_suppressions WHERE email = ?", (_norm(e),)).fetchone())


def _suppress(con, e, reason, campaign_id=None):
    con.execute("INSERT OR IGNORE INTO outreach_suppressions (email, reason, source_campaign_id, created_at) "
                "VALUES (?, ?, ?, ?)", (_norm(e), reason, campaign_id, now_iso()))
    con.execute("UPDATE cold_leads SET status = CASE WHEN status='active' THEN ? ELSE status END WHERE email = ?",
                ("bounced" if reason == "bounce" else "unsubscribed", _norm(e)))


def _render(text, lead):
    lead = dict(lead)
    vals = {
        "first_name": (lead.get("first_name") or "").strip() or "there",
        "last_name": (lead.get("last_name") or "").strip(),
        "company": (lead.get("company") or "").strip() or "your business",
        "email": lead.get("email") or "",
    }
    try:
        vals.update({k.lower(): str(v) for k, v in json.loads(lead.get("extra_json") or "{}").items() if v})
    except Exception:
        pass
    out = re.sub(r"\{\{\s*([a-zA-Z0-9_ ]+?)\s*\}\}", lambda m: vals.get(m.group(1).lower().replace(" ", "_"), ""), text or "")
    # Spintax {a|b|c} so 20 identical emails don't leave the same fingerprint.
    return re.sub(r"\{([^{}|]+(?:\|[^{}|]+)+)\}", lambda m: random.choice(m.group(1).split("|")), out)


def _unsub_token(lead_id, addr):
    sig = hmac.new(hashlib.sha256(b"unsub:" + _seed()).digest(), f"{lead_id}:{_norm(addr)}".encode(), hashlib.sha256).hexdigest()[:24]
    return f"{lead_id}.{sig}"


def _unsub_url(lead):
    return f"{PUBLIC_BASE_URL}/crm/email/u/{_unsub_token(lead['id'], lead['email'])}"


def _footer(lead):
    lines = []
    if MAILING_ADDRESS:
        lines.append(MAILING_ADDRESS)
    lines.append("If this isn't relevant, reply \"stop\" and I won't email again.")
    return "\n\n" + "\n".join(lines)


def inbox_cap_today(ibx, today=None):
    """Cold sends allowed today for this inbox: 0 before cold_start_date, then ramp_start + ramp_step/day up to cap."""
    today = today or _utcnow().date()
    start = ibx["cold_start_date"]
    if start:
        try:
            d0 = datetime.date.fromisoformat(start)
        except ValueError:
            d0 = today
        if today < d0:
            return 0
        days = (today - d0).days
    else:
        days = (today - datetime.date.fromisoformat(ibx["created_at"][:10])).days
    return max(0, min(ibx["daily_cap"], ibx["ramp_start"] + ibx["ramp_step"] * days))


def _sent_today(con, inbox_id):
    day = _utcnow().date().isoformat()
    return con.execute("SELECT COUNT(*) FROM cold_messages WHERE inbox_id = ? AND direction='out' AND kind='step' "
                       "AND created_at >= ?", (inbox_id, day)).fetchone()[0]


def _alert(subject, html_body, sms=None):
    def _bg():
        try:
            if RESEND_API_KEY:
                requests.post("https://api.resend.com/emails", timeout=10,
                              headers={"Authorization": f"Bearer {RESEND_API_KEY}"},
                              json={"from": "Lumen Cold Email <notifications@lumenmarketing.co>",
                                    "to": [ALERT_TO], "subject": subject, "html": html_body})
        except Exception as e:
            print(f"[cold] alert email failed: {e}")
        if sms:
            try:
                from ghl_sms_provider import notify_owner
                notify_owner(sms)
            except Exception as e:
                print(f"[cold] alert sms failed: {e}")
    threading.Thread(target=_bg, daemon=True).start()


# ── SMTP / IMAP ───────────────────────────────────────────────────────────────
def _smtp(ibx):
    pw = _dec(ibx["password_enc"])
    if ibx["smtp_port"] == 465:
        s = smtplib.SMTP_SSL(ibx["smtp_host"], 465, timeout=30, context=ssl.create_default_context())
    else:
        s = smtplib.SMTP(ibx["smtp_host"], ibx["smtp_port"], timeout=30)
        s.starttls(context=ssl.create_default_context())
    s.login(ibx["email"], pw)
    return s


def _imap(ibx):
    m = imaplib.IMAP4_SSL(ibx["imap_host"], ibx["imap_port"], ssl_context=ssl.create_default_context())
    m.login(ibx["email"], _dec(ibx["password_enc"]))
    return m


def test_inbox(ibx):
    """(ok, message). Logs into SMTP and IMAP without sending anything."""
    try:
        s = _smtp(ibx)
        s.quit()
    except Exception as e:
        return False, f"SMTP login failed: {e}"
    try:
        m = _imap(ibx)
        m.select("INBOX", readonly=True)
        m.logout()
    except Exception as e:
        return False, f"IMAP login failed: {e}"
    return True, "SMTP and IMAP both connected"


def send_mail(ibx, to_addr, subject, body, in_reply_to=None, references=None, unsub_url=None):
    """Plain-text send from the inbox. Returns the Message-ID. Raises on failure."""
    msg = MIMEText(body, "plain", "utf-8")
    msg["From"] = email.utils.formataddr((ibx["from_name"], ibx["email"]))
    msg["To"] = to_addr
    msg["Subject"] = subject
    msg["Date"] = email.utils.formatdate(localtime=False)
    mid = email.utils.make_msgid(domain=ibx["email"].split("@")[1])
    msg["Message-ID"] = mid
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = references or in_reply_to
    if unsub_url:
        msg["List-Unsubscribe"] = f"<mailto:{ibx['email']}?subject=unsubscribe>, <{unsub_url}>"
        msg["List-Unsubscribe-Post"] = "List-Unsubscribe=One-Click"
    s = _smtp(ibx)
    try:
        s.sendmail(ibx["email"], [to_addr], msg.as_string())
    finally:
        try:
            s.quit()
        except Exception:
            pass
    return mid


def _dh(v):
    try:
        return str(make_header(decode_header(v or "")))
    except Exception:
        return v or ""


def _body_text(msg):
    if msg.is_multipart():
        html_part = None
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                continue
            ct = part.get_content_type()
            if ct == "text/plain":
                return _decode_part(part)
            if ct == "text/html" and html_part is None:
                html_part = part
        if html_part is not None:
            return _strip_html(_decode_part(html_part))
        return ""
    t = _decode_part(msg)
    return _strip_html(t) if msg.get_content_type() == "text/html" else t


def _decode_part(part):
    payload = part.get_payload(decode=True) or b""
    return payload.decode(part.get_content_charset() or "utf-8", errors="replace")


def _strip_html(s):
    s = re.sub(r"(?is)<(script|style).*?</\1>", "", s)
    s = re.sub(r"(?i)<br\s*/?>|</p>|</div>", "\n", s)
    s = html_lib.unescape(re.sub(r"<[^>]+>", "", s))
    return re.sub(r"\n{3,}", "\n\n", s).strip()


def strip_quoted(text):
    """Keep only the new part of a reply (drop 'On ... wrote:' and > quoted lines)."""
    out = []
    for line in (text or "").splitlines():
        if re.match(r"^\s*On .+wrote:\s*$", line) or re.match(r"^-{2,}\s*Original Message", line, re.I) \
                or re.match(r"^\s*From:\s.+", line) and out:
            break
        if line.startswith(">"):
            continue
        out.append(line)
    return "\n".join(out).strip()


def _bounce_recipient(msg):
    """If this is a delivery failure, return the failed address."""
    frm = _norm(email.utils.parseaddr(msg.get("From", ""))[1])
    is_dsn = msg.get_content_type() == "multipart/report" or frm.startswith(("mailer-daemon", "postmaster")) \
        or re.search(r"(delivery status notification|undeliverable|delivery has failed|returned mail|failure notice)",
                     _dh(msg.get("Subject")), re.I)
    if not is_dsn:
        return None
    raw = msg.as_string()
    m = re.search(r"Final-Recipient:\s*rfc822;\s*<?([^\s>]+@[^\s>]+)", raw, re.I) \
        or re.search(r"Original-Recipient:\s*rfc822;\s*<?([^\s>]+@[^\s>]+)", raw, re.I) \
        or re.search(r"X-Failed-Recipients:\s*([^\s,]+@[^\s,]+)", raw, re.I)
    if m:
        return _norm(m.group(1))
    m = re.search(r"wasn't delivered to\s+<?([^\s>]+@[^\s>]+)", raw, re.I) \
        or re.search(r"couldn't be delivered to\s+<?([^\s>]+@[^\s>]+)", raw, re.I)
    return _norm(m.group(1)) if m else ""


def _is_auto_reply(msg):
    a = (msg.get("Auto-Submitted") or "").lower()
    return (a and a != "no") or msg.get("X-Autoreply") or msg.get("X-Autorespond") \
        or (msg.get("Precedence") or "").lower() in ("auto_reply", "bulk", "junk") \
        or bool(AUTO_SUBJECT_RE.search(_dh(msg.get("Subject"))))


# ── Receiver ──────────────────────────────────────────────────────────────────
def check_inbox(ibx_id):
    con = db()
    ibx = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (ibx_id,)).fetchone()
    if not ibx:
        con.close()
        return 0
    found = 0
    try:
        m = _imap(ibx)
        typ, data = m.select("INBOX", readonly=True)
        uidv = (m.response("UIDVALIDITY")[1] or [b""])[0]
        uidv = uidv.decode() if isinstance(uidv, bytes) else str(uidv or "")
        last = ibx["imap_last_uid"]
        if last is None or uidv != (ibx["imap_uidvalidity"] or ""):
            # First connect: start from now, don't import the mailbox history.
            typ, d = m.uid("search", None, "ALL")
            uids = [int(x) for x in (d[0] or b"").split()]
            con.execute("UPDATE cold_inboxes SET imap_last_uid=?, imap_uidvalidity=?, last_checked_at=? WHERE id=?",
                        (max(uids) if uids else 0, uidv, now_iso(), ibx_id))
            con.commit()
            m.logout()
            con.close()
            return 0
        typ, d = m.uid("search", None, f"UID {last + 1}:*")
        uids = sorted(int(x) for x in (d[0] or b"").split() if int(x) > last)
        for uid in uids[:200]:
            typ, parts = m.uid("fetch", str(uid), "(BODY.PEEK[])")
            raw = next((p[1] for p in parts if isinstance(p, tuple)), None)
            if raw:
                _ingest(con, ibx, email.message_from_bytes(raw))
                found += 1
            con.execute("UPDATE cold_inboxes SET imap_last_uid=? WHERE id=?", (uid, ibx_id))
            con.commit()
        m.logout()
        con.execute("UPDATE cold_inboxes SET last_checked_at=?, status=CASE WHEN status='error' THEN 'active' "
                    "ELSE status END, last_error=NULL WHERE id=?", (now_iso(), ibx_id))
        con.commit()
    except Exception as e:
        con.execute("UPDATE cold_inboxes SET last_error=?, last_checked_at=? WHERE id=?",
                    (f"IMAP: {e}"[:300], now_iso(), ibx_id))
        con.commit()
        print(f"[cold] imap {ibx['email']} failed: {e}")
    con.close()
    return found


def _ingest(con, ibx, msg):
    frm_name, frm = email.utils.parseaddr(msg.get("From", ""))
    frm = _norm(frm)
    if frm == _norm(ibx["email"]):
        return
    subject = _dh(msg.get("Subject"))
    mid = (msg.get("Message-ID") or "").strip()
    if mid and con.execute("SELECT 1 FROM cold_messages WHERE message_id = ?", (mid,)).fetchone():
        return
    irt = (msg.get("In-Reply-To") or "").strip()
    refs = (msg.get("References") or "").split()
    body = _body_text(msg)

    bounced = _bounce_recipient(msg)
    if bounced is not None:
        lead = con.execute("SELECT * FROM cold_leads WHERE email = ? AND inbox_id = ? ORDER BY id DESC",
                           (bounced, ibx["id"])).fetchone() if bounced else None
        if bounced:
            _suppress(con, bounced, "bounce", lead["campaign_id"] if lead else None)
        _store(con, lead["id"] if lead else None, ibx["id"], "in", "bounce", mid, irt, frm, ibx["email"], subject,
               body[:4000], read=1)
        return

    # Match: threading headers first, then sender address on this inbox.
    lead = None
    for ref in [irt] + refs[::-1]:
        if not ref:
            continue
        row = con.execute("SELECT lead_id FROM cold_messages WHERE message_id = ? AND lead_id IS NOT NULL",
                          (ref,)).fetchone()
        if row:
            lead = con.execute("SELECT * FROM cold_leads WHERE id = ?", (row["lead_id"],)).fetchone()
            break
    if not lead:
        lead = con.execute("SELECT * FROM cold_leads WHERE email = ? AND inbox_id = ? ORDER BY id DESC",
                           (frm, ibx["id"])).fetchone()

    if not lead:
        _store(con, None, ibx["id"], "in", "other", mid, irt, frm, ibx["email"], subject, body[:20000], read=0)
        return

    new_part = strip_quoted(body)
    if _is_auto_reply(msg):
        _store(con, lead["id"], ibx["id"], "in", "auto", mid, irt, frm, ibx["email"], subject, body[:20000], read=1)
        return
    if OPT_OUT_RE.match(new_part or "") or re.match(r"^\s*unsubscribe\b", subject or "", re.I):
        _suppress(con, frm, "unsubscribe", lead["campaign_id"])
        _store(con, lead["id"], ibx["id"], "in", "optout", mid, irt, frm, ibx["email"], subject, body[:20000], read=1)
        return

    _store(con, lead["id"], ibx["id"], "in", "reply", mid, irt, frm, ibx["email"], subject, body[:20000], read=0)
    con.execute("UPDATE cold_leads SET status = CASE WHEN status IN ('active','completed') THEN 'replied' ELSE status END, "
                "replied_at = COALESCE(replied_at, ?), unread = 1, last_message_id = ? WHERE id = ?",
                (now_iso(), mid or lead["last_message_id"], lead["id"]))
    who = (lead["company"] or frm_name or frm)
    preview = html_lib.escape((new_part or body)[:600]).replace("\n", "<br>")
    link = f"{PUBLIC_BASE_URL}/crm/email/inbox/{lead['id']}"
    _alert(f"Reply from {who}: {subject}",
           f"<p><b>{html_lib.escape(who)}</b> ({html_lib.escape(frm)}) replied to {html_lib.escape(ibx['email'])}:</p>"
           f"<blockquote style='border-left:3px solid #ddd;padding-left:12px;color:#333'>{preview}</blockquote>"
           f"<p><a href='{link}'>Open the thread and reply</a></p>",
           sms=f"Cold email reply from {who}. {link}")


def _store(con, lead_id, inbox_id, direction, kind, mid, irt, frm, to, subject, body, step_no=None, read=1):
    con.execute("INSERT INTO cold_messages (lead_id, inbox_id, direction, kind, step_no, message_id, in_reply_to, "
                "from_addr, to_addr, subject, body_text, created_at, is_read) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (lead_id, inbox_id, direction, kind, step_no, mid, irt, frm, to, subject, body, now_iso(), read))


# ── Sender ────────────────────────────────────────────────────────────────────
def _in_window(camp, now_utc):
    local = now_utc.replace(tzinfo=datetime.timezone.utc).astimezone(ZoneInfo(camp["timezone"]))
    days = {int(x) for x in (camp["send_days"] or "").split(",") if x.strip().isdigit()}
    return local.weekday() in days and camp["window_start"] <= local.hour < camp["window_end"]


def _steps(con, campaign_id):
    return con.execute("SELECT * FROM cold_steps WHERE campaign_id = ? ORDER BY step_no", (campaign_id,)).fetchall()


def _pick_inbox(con, camp, inboxes, now):
    """Active inbox in this campaign with room today and its gap elapsed, least used first."""
    ids = json.loads(camp["inbox_ids"] or "[]")
    best = None
    for i in ids:
        ibx = inboxes.get(i)
        if not ibx or ibx["status"] != "active":
            continue
        if ibx["last_sent_at"] and (now - datetime.datetime.fromisoformat(ibx["last_sent_at"])).total_seconds() \
                < ibx["min_gap_min"] * 60:
            continue
        room = inbox_cap_today(ibx) - _sent_today(con, i)
        if room <= 0:
            continue
        if not best or room > best[0]:
            best = (room, ibx)
    return best[1] if best else None


def _inbox_ready(con, ibx, now):
    if not ibx or ibx["status"] != "active":
        return False
    if ibx["last_sent_at"] and (now - datetime.datetime.fromisoformat(ibx["last_sent_at"])).total_seconds() \
            < ibx["min_gap_min"] * 60:
        return False
    return inbox_cap_today(ibx) - _sent_today(con, ibx["id"]) > 0


def sender_tick():
    """Send at most one email per inbox per tick. Returns number sent."""
    con = db()
    now = _utcnow()
    inboxes = {r["id"]: r for r in con.execute("SELECT * FROM cold_inboxes").fetchall()}
    sent = 0
    used = set()
    for camp in con.execute("SELECT * FROM cold_campaigns WHERE status = 'active'").fetchall():
        if not _in_window(camp, now):
            continue
        steps = _steps(con, camp["id"])
        if not steps:
            continue
        if camp["daily_limit"]:
            today = con.execute("SELECT COUNT(*) FROM cold_messages m JOIN cold_leads l ON l.id = m.lead_id "
                                "WHERE l.campaign_id = ? AND m.direction='out' AND m.kind='step' AND m.created_at >= ?",
                                (camp["id"], now.date().isoformat())).fetchone()[0]
            if today >= camp["daily_limit"]:
                continue
        # Follow-ups first (they're owed), then new leads.
        due = con.execute("SELECT * FROM cold_leads WHERE campaign_id = ? AND status = 'active' AND next_send_at <= ? "
                          "ORDER BY last_step DESC, next_send_at LIMIT 50", (camp["id"], now.isoformat())).fetchall()
        if not due:
            if not con.execute("SELECT 1 FROM cold_leads WHERE campaign_id = ? AND status = 'active'",
                               (camp["id"],)).fetchone():
                con.execute("UPDATE cold_campaigns SET status = 'completed' WHERE id = ?", (camp["id"],))
                con.commit()
            continue
        for lead in due:
            if _suppressed(con, lead["email"]):
                con.execute("UPDATE cold_leads SET status='unsubscribed' WHERE id=?", (lead["id"],))
                con.commit()
                continue
            ibx = inboxes.get(lead["inbox_id"]) if lead["inbox_id"] else None
            if ibx:
                if ibx["id"] in used or not _inbox_ready(con, ibx, now):
                    continue
            else:
                ibx = _pick_inbox(con, camp, {k: v for k, v in inboxes.items() if k not in used}, now)
                if not ibx:
                    break
            step = next((s for s in steps if s["step_no"] == lead["last_step"] + 1), None)
            if not step:
                con.execute("UPDATE cold_leads SET status='completed', next_send_at=NULL WHERE id=?", (lead["id"],))
                con.commit()
                continue
            ok = _send_step(con, camp, lead, step, steps, ibx)
            used.add(ibx["id"])
            inboxes[ibx["id"]] = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (ibx["id"],)).fetchone()
            sent += 1 if ok else 0
            if camp["daily_limit"]:
                break
    con.close()
    return sent


def _send_step(con, camp, lead, step, steps, ibx):
    first_subject = lead["thread_subject"] or _render(steps[0]["subject"] or "", lead)
    subject = _render(step["subject"], lead) if (step["subject"] or "").strip() else f"Re: {first_subject}"
    sig = f"\n\n{ibx['signature']}" if (ibx["signature"] or "").strip() else ""
    body = _render(step["body"], lead).strip() + sig + _footer(lead)
    refs = None
    if lead["first_message_id"]:
        refs = " ".join(x for x in [lead["first_message_id"], lead["last_message_id"]] if x)
    try:
        mid = send_mail(ibx, lead["email"], subject, body, in_reply_to=lead["last_message_id"], references=refs,
                        unsub_url=_unsub_url(lead))
    except smtplib.SMTPRecipientsRefused as e:
        _suppress(con, lead["email"], "bounce", camp["id"])
        con.commit()
        print(f"[cold] refused {lead['email']}: {e}")
        return False
    except (smtplib.SMTPAuthenticationError, smtplib.SMTPSenderRefused) as e:
        con.execute("UPDATE cold_inboxes SET status='error', last_error=? WHERE id=?", (f"SMTP: {e}"[:300], ibx["id"]))
        con.commit()
        _alert(f"Cold inbox stopped: {ibx['email']}", f"<p>{html_lib.escape(str(e))}</p><p>It's paused until you fix "
               f"it at <a href='{PUBLIC_BASE_URL}/crm/email/inboxes'>Inboxes</a>.</p>")
        return False
    except Exception as e:
        con.execute("UPDATE cold_inboxes SET last_error=?, last_sent_at=? WHERE id=?",
                    (f"SMTP: {e}"[:300], now_iso(), ibx["id"]))
        con.commit()
        print(f"[cold] send {lead['email']} via {ibx['email']} failed: {e}")
        return False
    nxt = next((s for s in steps if s["step_no"] == step["step_no"] + 1), None)
    next_at = None
    if nxt:
        next_at = (_utcnow() + datetime.timedelta(days=max(nxt["delay_days"], 1),
                                                  minutes=random.randint(-90, 90))).isoformat()
    _store(con, lead["id"], ibx["id"], "out", "step", mid, lead["last_message_id"], ibx["email"], lead["email"],
           subject, body, step_no=step["step_no"])
    con.execute("UPDATE cold_leads SET inbox_id=?, last_step=?, next_send_at=?, status=?, thread_subject=?, "
                "first_message_id=COALESCE(first_message_id, ?), last_message_id=? WHERE id=?",
                (ibx["id"], step["step_no"], next_at, "active" if nxt else "completed", first_subject, mid, mid,
                 lead["id"]))
    gap = ibx["min_gap_min"] * 60 + random.randint(0, ibx["min_gap_min"] * 60)
    con.execute("UPDATE cold_inboxes SET last_sent_at=?, last_error=NULL WHERE id=?",
                ((_utcnow() + datetime.timedelta(seconds=gap - ibx["min_gap_min"] * 60)).isoformat(), ibx["id"]))
    con.commit()
    return True


# ── Workers ───────────────────────────────────────────────────────────────────
_started = False


def start_cold_workers():
    global _started
    if _started or os.environ.get("COLD_EMAIL_WORKERS", "on").lower() == "off":
        return
    _started = True

    def send_loop():
        while True:
            try:
                sender_tick()
            except Exception as e:
                print(f"[cold] sender tick failed: {e}")
            time.sleep(30)

    def recv_loop():
        while True:
            try:
                con = db()
                ids = [r["id"] for r in con.execute("SELECT id FROM cold_inboxes WHERE status != 'paused'").fetchall()]
                con.close()
                for i in ids:
                    check_inbox(i)
            except Exception as e:
                print(f"[cold] receiver tick failed: {e}")
            time.sleep(120)

    threading.Thread(target=send_loop, daemon=True, name="cold-sender").start()
    threading.Thread(target=recv_loop, daemon=True, name="cold-receiver").start()
    print("[cold] sender + receiver started")


# ── GHL ───────────────────────────────────────────────────────────────────────
def _ghl(method, path, version="2021-07-28", **kw):
    h = {"Authorization": f"Bearer {os.environ.get('GHL_MK7_PIT', '')}", "Version": version,
         "Accept": "application/json"}
    return requests.request(method, f"{GHL}{path}", headers=h, timeout=20, **kw)


def push_to_ghl(con, lead):
    """Upsert the contact, open a Contractor OS opportunity at Conversation, add the thread as a note."""
    if not os.environ.get("GHL_MK7_PIT"):
        return False, "GHL_MK7_PIT not set"
    camp = con.execute("SELECT * FROM cold_campaigns WHERE id = ?", (lead["campaign_id"],)).fetchone()
    tags = ["cold-email"] + ([camp["ghl_tag"]] if camp and camp["ghl_tag"] else [])
    body = {"locationId": GHL_LOCATION, "email": lead["email"], "source": "Cold Email", "tags": tags}
    if lead["first_name"]:
        body["firstName"] = lead["first_name"]
    if lead["last_name"]:
        body["lastName"] = lead["last_name"]
    if lead["company"]:
        body["companyName"] = lead["company"]
    try:
        extra = json.loads(lead["extra_json"] or "{}")
    except Exception:
        extra = {}
    phone = next((v for k, v in extra.items() if "phone" in k.lower() and v), None)
    if phone:
        body["phone"] = phone
    r = _ghl("POST", "/contacts/upsert", json=body)
    if not r.ok:
        return False, f"contact upsert {r.status_code}: {r.text[:200]}"
    cid = r.json().get("contact", {}).get("id")
    oid = lead["ghl_opportunity_id"]
    if not oid:
        name = f"{lead['company'] or lead['email']} (cold email)"
        r = _ghl("POST", "/opportunities/", json={"locationId": GHL_LOCATION, "pipelineId": GHL_PIPELINE,
                                                  "pipelineStageId": GHL_STAGE_CONVERSATION, "contactId": cid,
                                                  "name": name, "status": "open", "source": "Cold Email"})
        if r.ok:
            oid = r.json().get("opportunity", {}).get("id")
        elif "duplicate" not in r.text.lower():
            return False, f"opportunity {r.status_code}: {r.text[:200]}"
    msgs = con.execute("SELECT * FROM cold_messages WHERE lead_id = ? AND kind IN ('step','manual','reply') "
                       "ORDER BY id", (lead["id"],)).fetchall()
    thread = "\n\n".join(f"{'US' if m['direction'] == 'out' else 'THEM'} ({m['created_at'][:16]} UTC) "
                         f"{m['subject']}\n{strip_quoted(m['body_text'])[:1500]}" for m in msgs)
    _ghl("POST", f"/contacts/{cid}/notes",
         json={"body": f"Cold email thread ({camp['name'] if camp else ''}). Full thread: "
                       f"{PUBLIC_BASE_URL}/crm/email/inbox/{lead['id']}\n\n{thread}"[:60000]})
    con.execute("UPDATE cold_leads SET ghl_contact_id=?, ghl_opportunity_id=? WHERE id=?", (cid, oid, lead["id"]))
    con.commit()
    return True, cid


# ── Stats ─────────────────────────────────────────────────────────────────────
def _stats(con, campaign_id=None, since=None):
    where, args = "", []
    if campaign_id:
        where += " AND l.campaign_id = ?"
        args.append(campaign_id)
    since_sql = ""
    if since:
        since_sql = " AND m.created_at >= ?"
    q = lambda extra, a: con.execute(
        f"SELECT COUNT(DISTINCT {extra}) FROM cold_messages m JOIN cold_leads l ON l.id = m.lead_id WHERE 1=1"
        f"{where}{since_sql} AND {a}", args + ([since] if since else [])).fetchone()[0]
    sent = q("m.id", "m.direction='out' AND m.kind='step'")
    contacted = q("m.lead_id", "m.direction='out' AND m.kind='step'")
    replied = q("m.lead_id", "m.direction='in' AND m.kind='reply'")
    bounced = q("m.lead_id", "m.kind='bounce'")
    optout = q("m.lead_id", "m.kind='optout'")
    wi = "WHERE interest='interested'" + (" AND campaign_id = ?" if campaign_id else "")
    interested = con.execute(f"SELECT COUNT(*) FROM cold_leads {wi}", [campaign_id] if campaign_id else []).fetchone()[0]
    pct = lambda n: round(100.0 * n / contacted, 1) if contacted else 0
    return {"sent": sent, "contacted": contacted, "replied": replied, "reply_rate": pct(replied),
            "bounced": bounced, "bounce_rate": pct(bounced), "optout": optout, "interested": interested}


# ── Routes: overview ──────────────────────────────────────────────────────────
@cold_bp.route("/")
@admin_required
def overview():
    con = db()
    inboxes = con.execute("SELECT * FROM cold_inboxes ORDER BY id").fetchall()
    rows = []
    for i in inboxes:
        rows.append({"i": i, "cap": inbox_cap_today(i), "sent": _sent_today(con, i["id"])})
    camps = con.execute("SELECT c.*, (SELECT COUNT(*) FROM cold_leads WHERE campaign_id=c.id) AS n_leads "
                        "FROM cold_campaigns c ORDER BY c.id DESC").fetchall()
    camp_stats = {c["id"]: _stats(con, c["id"]) for c in camps}
    week = (_utcnow() - datetime.timedelta(days=7)).isoformat()
    unread = con.execute("SELECT COUNT(*) FROM cold_leads WHERE unread = 1").fetchone()[0]
    ctx = dict(active="cold", inboxes=rows, camps=camps, camp_stats=camp_stats, total=_stats(con),
               week=_stats(con, since=week), unread=unread, today_cap=sum(r["cap"] for r in rows if r["i"]["status"] == "active"),
               today_sent=sum(r["sent"] for r in rows), mailing_address=MAILING_ADDRESS,
               ghl_ready=bool(os.environ.get("GHL_MK7_PIT")))
    con.close()
    return render_template("crm/cold_overview.html", **ctx)


# ── Routes: inboxes ───────────────────────────────────────────────────────────
@cold_bp.route("/inboxes")
@admin_required
def inboxes_page():
    con = db()
    rows = [{"i": i, "cap": inbox_cap_today(i), "sent": _sent_today(con, i["id"]),
             "tomorrow": inbox_cap_today(i, _utcnow().date() + datetime.timedelta(days=1))}
            for i in con.execute("SELECT * FROM cold_inboxes ORDER BY id").fetchall()]
    con.close()
    return render_template("crm/cold_inboxes.html", active="cold", rows=rows,
                           default_start=(_utcnow().date() + datetime.timedelta(days=14)).isoformat())


@cold_bp.route("/inboxes", methods=["POST"])
@admin_required
def inbox_create():
    f = request.form
    addr = _norm(f.get("email"))
    pw = (f.get("password") or "").replace(" ", "")
    if not addr or "@" not in addr or not pw:
        flash("Email and app password are required.", "error")
        return redirect(url_for("cold.inboxes_page"))
    prov = PROVIDERS.get(f.get("provider") or "google", PROVIDERS["google"])
    con = db()
    try:
        con.execute("INSERT INTO cold_inboxes (email, from_name, provider, smtp_host, smtp_port, imap_host, imap_port, "
                    "password_enc, signature, daily_cap, ramp_start, ramp_step, cold_start_date, min_gap_min, created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (addr, (f.get("from_name") or "Kendall Davis").strip(), f.get("provider") or "google",
                     prov["smtp_host"], prov["smtp_port"], prov["imap_host"], prov["imap_port"], _enc(pw),
                     (f.get("signature") or "").strip(), int(f.get("daily_cap") or 20), int(f.get("ramp_start") or 5),
                     int(f.get("ramp_step") or 2), f.get("cold_start_date") or None, int(f.get("min_gap_min") or 6),
                     now_iso()))
        con.commit()
        ibx = con.execute("SELECT * FROM cold_inboxes WHERE email = ?", (addr,)).fetchone()
        ok, msg = test_inbox(ibx)
        if not ok:
            con.execute("UPDATE cold_inboxes SET status='error', last_error=? WHERE id=?", (msg[:300], ibx["id"]))
            con.commit()
        flash(f"{addr} added. {msg}", "ok" if ok else "error")
    except Exception as e:
        flash(f"Could not add {addr}: {e}", "error")
    con.close()
    return redirect(url_for("cold.inboxes_page"))


@cold_bp.route("/inboxes/<int:iid>", methods=["POST"])
@admin_required
def inbox_update(iid):
    f = request.form
    action = f.get("action")
    con = db()
    ibx = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (iid,)).fetchone() or abort(404)
    if action == "test":
        ok, msg = test_inbox(ibx)
        con.execute("UPDATE cold_inboxes SET status=?, last_error=? WHERE id=?",
                    ("active" if ok and ibx["status"] == "error" else ibx["status"] if ok else "error",
                     None if ok else msg[:300], iid))
        flash(f"{ibx['email']}: {msg}", "ok" if ok else "error")
    elif action in ("pause", "resume"):
        con.execute("UPDATE cold_inboxes SET status=? WHERE id=?", ("paused" if action == "pause" else "active", iid))
    elif action == "delete":
        con.execute("DELETE FROM cold_inboxes WHERE id=?", (iid,))
        flash(f"{ibx['email']} removed.", "ok")
    elif action == "save":
        pw = (f.get("password") or "").replace(" ", "")
        con.execute("UPDATE cold_inboxes SET from_name=?, signature=?, daily_cap=?, ramp_start=?, ramp_step=?, "
                    "cold_start_date=?, min_gap_min=? WHERE id=?",
                    ((f.get("from_name") or ibx["from_name"]).strip(), (f.get("signature") or "").strip(),
                     int(f.get("daily_cap") or 20), int(f.get("ramp_start") or 5), int(f.get("ramp_step") or 2),
                     f.get("cold_start_date") or None, int(f.get("min_gap_min") or 6), iid))
        if pw:
            con.execute("UPDATE cold_inboxes SET password_enc=? WHERE id=?", (_enc(pw), iid))
        flash(f"{ibx['email']} saved.", "ok")
    con.commit()
    con.close()
    return redirect(url_for("cold.inboxes_page"))


# ── Routes: campaigns ─────────────────────────────────────────────────────────
DEFAULT_STEPS = [
    (0, "quick question about {{company}}", "Hi {{first_name}},\n\n"),
    (3, "", "Hi {{first_name}}, bumping this up in case it got buried.\n\n"),
    (4, "", "Hi {{first_name}},\n\n"),
    (5, "", "Hi {{first_name}}, I'll close this out.\n\n"),
]


@cold_bp.route("/campaigns/new", methods=["POST"])
@admin_required
def campaign_create():
    name = (request.form.get("name") or "").strip() or "New campaign"
    con = db()
    ids = [r["id"] for r in con.execute("SELECT id FROM cold_inboxes ORDER BY id").fetchall()]
    cur = con.execute("INSERT INTO cold_campaigns (name, inbox_ids, created_at) VALUES (?,?,?)",
                      (name, json.dumps(ids), now_iso()))
    cid = cur.lastrowid
    for n, (d, s, b) in enumerate(DEFAULT_STEPS, 1):
        con.execute("INSERT INTO cold_steps (campaign_id, step_no, delay_days, subject, body) VALUES (?,?,?,?,?)",
                    (cid, n, d, s, b))
    con.commit()
    con.close()
    return redirect(url_for("cold.campaign_page", cid=cid))


@cold_bp.route("/campaigns/<int:cid>")
@admin_required
def campaign_page(cid):
    con = db()
    camp = con.execute("SELECT * FROM cold_campaigns WHERE id = ?", (cid,)).fetchone() or abort(404)
    steps = _steps(con, cid)
    inboxes = con.execute("SELECT * FROM cold_inboxes ORDER BY id").fetchall()
    try:
        audiences = con.execute("SELECT id, name, member_count FROM outreach_audiences ORDER BY id DESC").fetchall()
    except Exception:
        audiences = []
    status_filter = request.args.get("s")
    q = "SELECT * FROM cold_leads WHERE campaign_id = ?" + (" AND status = ?" if status_filter else "") + \
        " ORDER BY COALESCE(replied_at, '') DESC, id LIMIT 300"
    leads = con.execute(q, (cid, status_filter) if status_filter else (cid,)).fetchall()
    counts = {r["status"]: r["n"] for r in con.execute(
        "SELECT status, COUNT(*) n FROM cold_leads WHERE campaign_id = ? GROUP BY status", (cid,)).fetchall()}
    sample = con.execute("SELECT * FROM cold_leads WHERE campaign_id = ? ORDER BY id LIMIT 1", (cid,)).fetchone()
    if not sample and camp["audience_id"]:
        sample = con.execute("SELECT 0 AS id, email, first_name, last_name, company, extra_json "
                             "FROM outreach_audience_members WHERE audience_id = ? LIMIT 1",
                             (camp["audience_id"],)).fetchone()
    sample = dict(sample) if sample else {"id": 0, "email": "owner@example.com", "first_name": "Mike",
                                          "last_name": "", "company": "Summit Hardscapes", "extra_json": "{}"}
    previews = []
    first = _render(steps[0]["subject"], sample) if steps else ""
    for s in steps:
        subj = _render(s["subject"], sample) if (s["subject"] or "").strip() else f"Re: {first}"
        previews.append({"subject": subj, "body": _render(s["body"], sample).strip()})
    # Projected days from launch to each step
    day, timeline = 0, []
    for s in steps:
        day += s["delay_days"] if s["step_no"] > 1 else 0
        timeline.append(day)
    ctx = dict(active="cold", camp=camp, steps=steps, inboxes=inboxes, audiences=audiences, leads=leads,
               counts=counts, stats=_stats(con, cid), previews=previews, timeline=timeline, sample=sample,
               sel_inboxes=set(json.loads(camp["inbox_ids"] or "[]")),
               sel_days={int(x) for x in camp["send_days"].split(",") if x.strip().isdigit()},
               days=DAYS, timezones=TIMEZONES, status_filter=status_filter,
               daily_capacity=sum(inbox_cap_today(i) for i in inboxes
                                  if i["id"] in set(json.loads(camp["inbox_ids"] or "[]")) and i["status"] == "active"))
    con.close()
    return render_template("crm/cold_campaign.html", **ctx)


@cold_bp.route("/campaigns/<int:cid>", methods=["POST"])
@admin_required
def campaign_save(cid):
    f = request.form
    action = f.get("action", "save")
    con = db()
    camp = con.execute("SELECT * FROM cold_campaigns WHERE id = ?", (cid,)).fetchone() or abort(404)
    if action in ("save", "launch", "test"):
        con.execute("UPDATE cold_campaigns SET name=?, audience_id=?, inbox_ids=?, send_days=?, window_start=?, "
                    "window_end=?, timezone=?, daily_limit=?, ghl_tag=? WHERE id=?",
                    ((f.get("name") or camp["name"]).strip(), int(f["audience_id"]) if f.get("audience_id") else None,
                     json.dumps([int(x) for x in f.getlist("inbox_ids")]),
                     ",".join(f.getlist("send_days")) or "0,1,2,3,4",
                     int(f.get("window_start") or 7), int(f.get("window_end") or 16),
                     f.get("timezone") or "America/Denver", int(f.get("daily_limit") or 0),
                     (f.get("ghl_tag") or "").strip() or None, cid))
        con.execute("DELETE FROM cold_steps WHERE campaign_id = ?", (cid,))
        n = 0
        for subj, body, delay in zip(f.getlist("step_subject"), f.getlist("step_body"), f.getlist("step_delay")):
            if not body.strip():
                continue
            n += 1
            con.execute("INSERT INTO cold_steps (campaign_id, step_no, delay_days, subject, body) VALUES (?,?,?,?,?)",
                        (cid, n, int(delay or (0 if n == 1 else 3)), subj.strip() if n == 1 or subj.strip() else "",
                         body.replace("\r\n", "\n")))
        if f.get("add_step"):
            con.execute("INSERT INTO cold_steps (campaign_id, step_no, delay_days, subject, body) VALUES (?,?,?,?,?)",
                        (cid, n + 1, 3, "", "Hi {{first_name}},\n\n"))
        con.commit()
        flash("Saved.", "ok")
    if action == "launch":
        camp = con.execute("SELECT * FROM cold_campaigns WHERE id = ?", (cid,)).fetchone()
        steps = _steps(con, cid)
        problems = []
        if not camp["audience_id"]:
            problems.append("pick an audience")
        if not json.loads(camp["inbox_ids"] or "[]"):
            problems.append("pick at least one inbox")
        if not steps or not (steps[0]["subject"] or "").strip():
            problems.append("step 1 needs a subject")
        known = {"first_name", "last_name", "company", "email"}
        sample = con.execute("SELECT extra_json FROM outreach_audience_members WHERE audience_id = ? LIMIT 1",
                             (camp["audience_id"],)).fetchone() if camp["audience_id"] else None
        try:
            known |= {k.lower().replace(" ", "_") for k in json.loads((sample and sample["extra_json"]) or "{}")}
        except Exception:
            pass
        used_tags = {t.lower().replace(" ", "_") for st in steps
                     for t in re.findall(r"\{\{\s*([a-zA-Z0-9_ ]+?)\s*\}\}", (st["subject"] or "") + st["body"])}
        if used_tags - known:
            problems.append("unknown merge tag " + ", ".join("{{%s}}" % t for t in sorted(used_tags - known)))
        if problems:
            flash("Can't launch yet: " + ", ".join(problems) + ".", "error")
        else:
            added = _enroll(con, camp)
            con.execute("UPDATE cold_campaigns SET status='active', launched_at=COALESCE(launched_at, ?) WHERE id=?",
                        (now_iso(), cid))
            con.commit()
            flash(f"Launched. {added} new leads queued. Sending runs inside your window, capped per inbox.", "ok")
    elif action == "pause":
        con.execute("UPDATE cold_campaigns SET status='paused' WHERE id=?", (cid,))
        con.commit()
    elif action == "resume":
        con.execute("UPDATE cold_campaigns SET status='active' WHERE id=?", (cid,))
        con.commit()
    elif action == "delete":
        con.execute("DELETE FROM cold_leads WHERE campaign_id=?", (cid,))
        con.execute("DELETE FROM cold_steps WHERE campaign_id=?", (cid,))
        con.execute("DELETE FROM cold_campaigns WHERE id=?", (cid,))
        con.commit()
        con.close()
        flash("Campaign deleted.", "ok")
        return redirect(url_for("cold.overview"))
    elif action == "test":
        to = _norm(f.get("test_to")) or _norm(current_user()["email"])
        steps = _steps(con, cid)
        ids = json.loads(camp["inbox_ids"] or "[]")
        ibx = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (ids[0],)).fetchone() if ids else None
        if not ibx or not steps:
            flash("Add an inbox and at least one step first.", "error")
        else:
            lead = {"id": 0, "email": to, "first_name": "Mike", "last_name": "", "company": "Summit Hardscapes",
                    "extra_json": "{}"}
            sample = con.execute("SELECT * FROM outreach_audience_members WHERE audience_id = ? LIMIT 1",
                                 (camp["audience_id"],)).fetchone() if camp["audience_id"] else None
            if sample:
                lead.update({k: sample[k] for k in ("first_name", "last_name", "company", "extra_json")})
            try:
                first = _render(steps[0]["subject"], lead)
                for s in steps:
                    subj = _render(s["subject"], lead) if (s["subject"] or "").strip() else f"Re: {first}"
                    sig = f"\n\n{ibx['signature']}" if (ibx["signature"] or "").strip() else ""
                    send_mail(ibx, to, f"[TEST step {s['step_no']}] {subj}", _render(s["body"], lead).strip() + sig +
                              _footer(lead))
                flash(f"Sent {len(steps)} test emails to {to} from {ibx['email']}.", "ok")
            except Exception as e:
                flash(f"Test failed: {e}", "error")
    con.close()
    return redirect(url_for("cold.campaign_page", cid=cid))


def _enroll(con, camp):
    """Copy audience members into cold_leads (skip suppressed + anyone already in another active cold campaign)."""
    members = con.execute("SELECT * FROM outreach_audience_members WHERE audience_id = ?", (camp["audience_id"],)).fetchall()
    now = _utcnow().isoformat()
    added = 0
    for m in members:
        e = _norm(m["email"])
        if not e or _suppressed(con, e):
            continue
        if con.execute("SELECT 1 FROM cold_leads WHERE email = ? AND campaign_id != ? AND status IN ('active','replied')",
                       (e, camp["id"])).fetchone():
            continue
        cur = con.execute("INSERT OR IGNORE INTO cold_leads (campaign_id, email, first_name, last_name, company, "
                          "extra_json, next_send_at, created_at) VALUES (?,?,?,?,?,?,?,?)",
                          (camp["id"], e, m["first_name"], m["last_name"], m["company"], m["extra_json"], now, now_iso()))
        added += cur.rowcount
    return added


# ── Routes: unified inbox ─────────────────────────────────────────────────────
@cold_bp.route("/inbox")
@cold_bp.route("/inbox/<int:lead_id>")
@admin_required
def inbox(lead_id=None):
    tab = request.args.get("tab", "replies")
    con = db()
    if tab == "other":
        threads = con.execute("SELECT m.*, i.email AS inbox_email FROM cold_messages m LEFT JOIN cold_inboxes i "
                              "ON i.id = m.inbox_id WHERE m.lead_id IS NULL AND m.kind = 'other' "
                              "ORDER BY m.id DESC LIMIT 200").fetchall()
    else:
        where = {"replies": "l.replied_at IS NOT NULL", "unread": "l.unread = 1",
                 "interested": "l.interest = 'interested'", "later": "l.interest = 'later'"}.get(tab, "l.replied_at IS NOT NULL")
        threads = con.execute(
            "SELECT l.*, c.name AS campaign_name, (SELECT body_text FROM cold_messages WHERE lead_id = l.id AND "
            "direction='in' AND kind='reply' ORDER BY id DESC LIMIT 1) AS last_in, (SELECT created_at FROM cold_messages "
            f"WHERE lead_id = l.id ORDER BY id DESC LIMIT 1) AS last_at FROM cold_leads l JOIN cold_campaigns c ON "
            f"c.id = l.campaign_id WHERE {where} ORDER BY last_at DESC LIMIT 200").fetchall()
    lead, msgs, ibx = None, [], None
    if lead_id:
        lead = con.execute("SELECT l.*, c.name AS campaign_name FROM cold_leads l JOIN cold_campaigns c "
                           "ON c.id = l.campaign_id WHERE l.id = ?", (lead_id,)).fetchone() or abort(404)
        msgs = con.execute("SELECT * FROM cold_messages WHERE lead_id = ? ORDER BY id", (lead_id,)).fetchall()
        ibx = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (lead["inbox_id"],)).fetchone()
        con.execute("UPDATE cold_leads SET unread = 0 WHERE id = ?", (lead_id,))
        con.execute("UPDATE cold_messages SET is_read = 1 WHERE lead_id = ?", (lead_id,))
        con.commit()
    other_id = request.args.get("m")
    other = con.execute("SELECT * FROM cold_messages WHERE id = ? AND lead_id IS NULL", (other_id,)).fetchone() \
        if other_id else None
    if other:
        con.execute("UPDATE cold_messages SET is_read = 1 WHERE id = ?", (other["id"],))
        con.commit()
    counts = {
        "unread": con.execute("SELECT COUNT(*) FROM cold_leads WHERE unread = 1").fetchone()[0],
        "replies": con.execute("SELECT COUNT(*) FROM cold_leads WHERE replied_at IS NOT NULL").fetchone()[0],
        "interested": con.execute("SELECT COUNT(*) FROM cold_leads WHERE interest = 'interested'").fetchone()[0],
        "later": con.execute("SELECT COUNT(*) FROM cold_leads WHERE interest = 'later'").fetchone()[0],
        "other": con.execute("SELECT COUNT(*) FROM cold_messages WHERE lead_id IS NULL AND kind='other' AND is_read=0").fetchone()[0],
    }
    con.close()
    return render_template("crm/cold_inbox.html", active="cold", tab=tab, threads=threads, lead=lead, msgs=msgs,
                           ibx=ibx, counts=counts, other=other, strip_quoted=strip_quoted)


@cold_bp.route("/inbox/<int:lead_id>", methods=["POST"])
@admin_required
def inbox_action(lead_id):
    f = request.form
    action = f.get("action")
    con = db()
    lead = con.execute("SELECT * FROM cold_leads WHERE id = ?", (lead_id,)).fetchone() or abort(404)
    if action == "reply":
        body = (f.get("body") or "").replace("\r\n", "\n").strip()
        ibx = con.execute("SELECT * FROM cold_inboxes WHERE id = ?", (lead["inbox_id"],)).fetchone()
        if not body or not ibx:
            flash("Write a reply first." if not body else "This lead has no inbox.", "error")
        else:
            last_in = con.execute("SELECT * FROM cold_messages WHERE lead_id = ? AND direction='in' ORDER BY id DESC "
                                  "LIMIT 1", (lead_id,)).fetchone()
            subj = (last_in["subject"] if last_in else None) or lead["thread_subject"] or ""
            if not subj.lower().startswith("re:"):
                subj = f"Re: {subj}"
            irt = (last_in["message_id"] if last_in else None) or lead["last_message_id"]
            refs = " ".join(x for x in [lead["first_message_id"], irt] if x)
            sig = f"\n\n{ibx['signature']}" if (ibx["signature"] or "").strip() else ""
            quoted = ""
            if last_in:
                when = last_in["created_at"][:16].replace("T", " ")
                quoted = f"\n\nOn {when} UTC, {last_in['from_addr']} wrote:\n" + "\n".join(
                    "> " + l for l in strip_quoted(last_in["body_text"]).splitlines())
            try:
                mid = send_mail(ibx, lead["email"], subj, body + sig + quoted, in_reply_to=irt, references=refs)
                _store(con, lead_id, ibx["id"], "out", "manual", mid, irt, ibx["email"], lead["email"], subj, body + sig)
                con.execute("UPDATE cold_leads SET last_message_id=?, unread=0 WHERE id=?", (mid, lead_id))
                con.commit()
                flash("Reply sent.", "ok")
            except Exception as e:
                flash(f"Reply failed: {e}", "error")
    elif action in ("interested", "not_interested", "later"):
        con.execute("UPDATE cold_leads SET interest=?, status=CASE WHEN status='active' THEN 'replied' ELSE status END "
                    "WHERE id=?", (action, lead_id))
        con.commit()
        if action == "interested":
            ok, info = push_to_ghl(con, con.execute("SELECT * FROM cold_leads WHERE id=?", (lead_id,)).fetchone())
            flash("Marked interested and sent to GHL Contractor OS (Conversation)." if ok else f"Marked interested, "
                  f"but GHL push failed: {info}", "ok" if ok else "error")
    elif action == "unsubscribe":
        _suppress(con, lead["email"], "manual", lead["campaign_id"])
        con.commit()
        flash(f"{lead['email']} won't be emailed again.", "ok")
    elif action == "stop":
        con.execute("UPDATE cold_leads SET status='stopped', next_send_at=NULL WHERE id=?", (lead_id,))
        con.commit()
    con.close()
    return redirect(url_for("cold.inbox", lead_id=lead_id, tab=f.get("tab") or "replies"))


@cold_bp.route("/check-now", methods=["POST"])
@admin_required
def check_now():
    con = db()
    ids = [r["id"] for r in con.execute("SELECT id FROM cold_inboxes WHERE status != 'paused'").fetchall()]
    con.close()
    n = sum(check_inbox(i) for i in ids)
    flash(f"Checked {len(ids)} inbox(es), {n} new message(s).", "ok")
    return redirect(request.referrer or url_for("cold.inbox"))


# ── Public unsubscribe ────────────────────────────────────────────────────────
@cold_bp.route("/u/<token>", methods=["GET", "POST"])
def unsubscribe(token):
    try:
        lid = int(token.split(".", 1)[0])
    except ValueError:
        abort(404)
    con = db()
    lead = con.execute("SELECT * FROM cold_leads WHERE id = ?", (lid,)).fetchone()
    if not lead or not hmac.compare_digest(_unsub_token(lid, lead["email"]), token):
        con.close()
        abort(404)
    page = ("<!doctype html><meta name=viewport content='width=device-width,initial-scale=1'>"
            "<body style='font-family:system-ui;background:#fff;color:#222;padding:48px 20px;max-width:520px;"
            "margin:auto;line-height:1.6'>%s</body>")
    if request.method == "GET":
        # Link scanners prefetch GET, so the opt-out itself only happens on POST (also RFC 8058 one-click).
        con.close()
        return page % ("<h2>Unsubscribe</h2><p>Stop all emails to %s?</p><form method=post><button style='padding:12px "
                       "20px;font-size:16px'>Unsubscribe</button></form>" % html_lib.escape(lead["email"]))
    _suppress(con, lead["email"], "unsubscribe", lead["campaign_id"])
    con.commit()
    con.close()
    return page % "<h2>You're unsubscribed</h2><p>You won't get any more emails from us.</p>"


init_cold_db()
start_cold_workers()
