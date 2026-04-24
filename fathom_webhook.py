"""
Fathom webhook handler — receives `new-meeting-content-ready` events,
verifies the HMAC signature, matches the call to a known Lumen client,
and dual-logs to ClickUp (Active Clients + Knowledge Base).

Env vars required:
  FATHOM_WEBHOOK_SECRET  raw Fathom webhook secret (starts with `whsec_`)
  CLICKUP_TOKEN          ClickUp API token (starts with `pk_`)
"""
import os
import hmac
import json
import time
import base64
import hashlib
import threading

import requests
from flask import Blueprint, request, jsonify

fathom_bp = Blueprint("fathom", __name__)

FATHOM_WEBHOOK_SECRET = os.environ.get("FATHOM_WEBHOOK_SECRET", "")
CLICKUP_TOKEN = os.environ.get("CLICKUP_TOKEN", "")
CLICKUP_API = "https://api.clickup.com/api/v2"

# Resend for the internal call-summary email. Uses existing RESEND_API_KEY.
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")

# Internal recipients for the per-call summary. Hardcoded so client attendees
# can never be cc'd by accident — Fathom's "share to attendees" sends to
# everyone including the client, which is exactly what we're avoiding.
TEAM_RECIPIENTS = [
    "kendall@lumenmarketing.co",
    "marykatezarehghazarian@gmail.com",
]

REPLAY_WINDOW_SECONDS = 5 * 60

# Client match rules. Order matters — first match wins.
CLIENTS = [
    {
        "slug": "avalon",
        "name": "Avalon Laser",
        "active_list": "901712259931",  # Active Clients → Avalon Laser → General All Location
        "kb_list":     "901712382740",  # KB → Client Intelligence → Avalon Laser
        "domains":     {"avalon-laser.com"},
        "emails":      {"avesta70@gmail.com", "bre@avalon-laser.com"},
        "keywords":    {"avalon", "reza", "avesta"},
    },
    {
        "slug": "berry-clean",
        "name": "Berry Clean",
        "active_list": "901710484704",  # Active Clients → Berry Clean → Berry Clean Projects
        "kb_list":     "901712382739",  # KB → Client Intelligence → Berry Clean
        "domains":     set(),
        "emails":      set(),
        "keywords":    {"berry clean", "spencer"},
    },
    {
        "slug": "jesse",
        "name": "Jesse McCusker",
        "active_list": "901712372406",  # Active Clients → Jesse McCusker → Referral Projects
        "kb_list":     "901712382741",  # KB → Client Intelligence → Jesse McCusker
        "domains":     set(),
        "emails":      set(),
        "keywords":    {"jesse", "mccusker", "cap hardware", "sublime"},
    },
    {
        "slug": "jared",
        "name": "Jared Casados",
        "active_list": "901712376747",  # Active Clients → Jared Casados → Bryan Hymas AI Build
        "kb_list":     "901712382742",  # KB → Client Intelligence → Jared Casados
        "domains":     set(),
        "emails":      set(),
        "keywords":    {"jared", "casados", "bryan hymas", "brian hymas"},
    },
    {
        "slug": "jeremiah",
        "name": "Jeremiah Newby",
        "active_list": "901712440792",  # Active Clients → Jeremiah Newby → Webinar Funnel Build
        "kb_list":     "901712440967",  # KB → Client Intelligence → Jeremiah Newby
        "domains":     set(),
        "emails":      set(),
        "keywords":    {"jeremiah", "newby", "private funding"},
    },
    # MK7 Media is MaryKate's own business. These calls get logged into a
    # dedicated MK7 Media Calls list so reports can filter independently from
    # Lumen work. This rule MUST come before the marykate rule because her
    # email is on every MK7 call.
    {
        "slug": "mk7-media",
        "name": "MK7 Media",
        "active_list": "901712965950",  # Active Clients → MK7 Media Calls
        "kb_list":     "901712382751",  # KB → Agency Operations → Lumen Internal (tagged mk7-media)
        "domains":     set(),
        "emails":      set(),
        "keywords":    {"mk7 media", "mk7media", "mk 7 media"},
    },
    # MaryKate is last. Her email catches internal Lumen syncs (e.g. weekly
    # 1:1s with Kendall where no client is on the invite) that don't match
    # any client rule above. Client calls she joins route to the client.
    {
        "slug": "marykate",
        "name": "Marykate G.",
        "active_list": "901712376573",  # Active Clients → Marykate G. → CRM & Outreach Agent
        "kb_list":     "901712382743",  # KB → Client Intelligence → Marykate G.
        "domains":     set(),
        "emails":      {"marykatezarehghazarian@gmail.com"},
        "keywords":    {"marykate", "mary kate"},
    },
]

# Fallback when no client matches — prospects / unknown calls
FALLBACK_ACTIVE_LIST = "901712964268"  # Active Clients → Fathom Calls
FALLBACK_KB_LIST     = "901712382751"  # KB → Agency Operations → Lumen Internal


def verify_signature(headers, raw_body: bytes) -> bool:
    """Verify Fathom webhook HMAC-SHA256 signature per the Fathom spec."""
    wid = headers.get("webhook-id", "")
    wts = headers.get("webhook-timestamp", "")
    wsig = headers.get("webhook-signature", "")
    if not (wid and wts and wsig and FATHOM_WEBHOOK_SECRET):
        return False
    try:
        ts = int(wts)
    except ValueError:
        return False
    if abs(time.time() - ts) > REPLAY_WINDOW_SECONDS:
        return False

    secret = FATHOM_WEBHOOK_SECRET
    if secret.startswith("whsec_"):
        secret = secret[len("whsec_"):]
    try:
        key = base64.b64decode(secret)
    except Exception:
        key = secret.encode()

    signed = f"{wid}.{wts}.".encode() + raw_body
    expected = base64.b64encode(
        hmac.new(key, signed, hashlib.sha256).digest()
    ).decode()

    for part in wsig.split():
        b64 = part.split(",", 1)[1] if "," in part else part
        if hmac.compare_digest(b64, expected):
            return True
    return False


def match_client(payload: dict):
    """Return the first matching CLIENTS entry, or None."""
    invitees = payload.get("calendar_invitees") or []
    emails = {(i.get("email") or "").lower() for i in invitees if i.get("email")}
    domains = {(i.get("email_domain") or "").lower() for i in invitees if i.get("email_domain")}
    title_blob = " ".join([
        (payload.get("meeting_title") or ""),
        (payload.get("title") or ""),
    ]).lower()

    for c in CLIENTS:
        if emails & {e.lower() for e in c["emails"]}:
            return c
        if domains & {d.lower() for d in c["domains"]}:
            return c
        if any(kw in title_blob for kw in c["keywords"]):
            return c
    return None


def fmt_transcript(transcript) -> str:
    if not transcript:
        return "_No transcript available._"
    lines = []
    for item in transcript:
        spk = (item.get("speaker") or {}).get("display_name") or "Unknown"
        ts = item.get("timestamp") or ""
        txt = item.get("text") or ""
        lines.append(f"**{spk}** [{ts}]: {txt}")
    return "\n\n".join(lines)


def fmt_attendees(invitees) -> str:
    if not invitees:
        return "_none_"
    lines = []
    for i in invitees:
        name = i.get("name") or ""
        email = i.get("email") or ""
        ext = " (external)" if i.get("is_external") else ""
        lines.append(f"- {name} <{email}>{ext}")
    return "\n".join(lines)


def cu_post(path: str, body: dict):
    try:
        r = requests.post(
            f"{CLICKUP_API}{path}",
            headers={"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"},
            json=body,
            timeout=20,
        )
    except Exception as e:
        print(f"[fathom] ClickUp request error: {e}")
        return None
    if r.status_code >= 300:
        print(f"[fathom] ClickUp {path} {r.status_code}: {r.text[:300]}")
        return None
    return r.json()


def create_task(list_id: str, name: str, description: str, tags: list, parent: str = None):
    body = {"name": name, "markdown_description": description, "tags": tags}
    if parent:
        body["parent"] = parent
    return cu_post(f"/list/{list_id}/task", body)


def log_to_clickup(payload: dict):
    """Dual-log a Fathom meeting payload to Active Clients + Knowledge Base."""
    title = payload.get("meeting_title") or payload.get("title") or "Untitled call"
    date = (payload.get("recording_start_time") or payload.get("created_at") or "")[:10]
    share_url = payload.get("share_url") or payload.get("url") or ""
    summary_md = ((payload.get("default_summary") or {}).get("markdown_formatted") or "").strip()
    transcript_md = fmt_transcript(payload.get("transcript"))
    attendees_md = fmt_attendees(payload.get("calendar_invitees"))
    action_items = payload.get("action_items") or []

    match = match_client(payload)
    if match:
        active_list = match["active_list"]
        kb_list = match["kb_list"]
        client_tag = match["slug"]
        client_name = match["name"]
        print(f"[fathom] matched client: {client_name}")
    else:
        active_list = FALLBACK_ACTIVE_LIST
        kb_list = FALLBACK_KB_LIST
        client_tag = "prospect"
        client_name = "Unmatched / Prospect"
        print("[fathom] no client match — routing to Fathom Calls")

    tags = [client_tag, "call-log", "fathom"]

    active_desc = (
        f"**Client:** {client_name}\n"
        f"**Date:** {date}\n"
        f"**Fathom:** {share_url}\n\n"
        f"## Summary\n{summary_md or '_none_'}\n\n"
        f"## Attendees\n{attendees_md}"
    )
    active_task = create_task(
        active_list,
        f"📞 Call: {title} — {date}",
        active_desc,
        tags,
    )

    if active_task and action_items:
        parent_id = active_task["id"]
        for a in action_items:
            desc = a.get("description") or "Action item"
            assignee_email = (a.get("assignee") or {}).get("email") or ""
            playback = a.get("recording_playback_url") or ""
            sub_desc = f"**Assignee:** {assignee_email}\n\n[Jump to moment in call]({playback})"
            create_task(active_list, desc, sub_desc, tags + ["action-item"], parent=parent_id)

    kb_desc = (
        f"**Client:** {client_name}\n"
        f"**Date:** {date}\n"
        f"**Fathom:** {share_url}\n\n"
        f"## Summary\n{summary_md or '_none_'}\n\n"
        f"## Attendees\n{attendees_md}\n\n"
        f"## Transcript\n{transcript_md}"
    )
    create_task(
        kb_list,
        f"Call Log: {title} — {date}",
        kb_desc,
        tags,
    )


def _html_escape(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _md_to_html(md: str) -> str:
    """Minimal markdown → HTML for Fathom summaries (headings, bullets, bold, line breaks).
    Fathom summaries are constrained enough that a heavy parser isn't worth the dep.
    """
    if not md:
        return "<p><em>No summary returned by Fathom.</em></p>"
    out = []
    in_list = False
    for raw_line in md.splitlines():
        line = raw_line.rstrip()
        if not line.strip():
            if in_list:
                out.append("</ul>"); in_list = False
            continue
        # Headings
        if line.startswith("### "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h4 style=\"margin:22px 0 8px;font-size:14px;color:#222;\">{_html_escape(line[4:])}</h4>")
            continue
        if line.startswith("## "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h3 style=\"margin:24px 0 10px;font-size:16px;color:#111;\">{_html_escape(line[3:])}</h3>")
            continue
        if line.startswith("# "):
            if in_list: out.append("</ul>"); in_list = False
            out.append(f"<h2 style=\"margin:24px 0 10px;font-size:18px;color:#111;\">{_html_escape(line[2:])}</h2>")
            continue
        # Bullets
        if line.lstrip().startswith(("- ", "* ")):
            if not in_list:
                out.append("<ul style=\"margin:8px 0;padding-left:20px;color:#333;line-height:1.55;\">"); in_list = True
            text = line.lstrip()[2:]
            # Bold
            text = _html_escape(text)
            while "**" in text:
                text = text.replace("**", "<strong>", 1)
                if "**" in text:
                    text = text.replace("**", "</strong>", 1)
            out.append(f"<li>{text}</li>")
            continue
        # Paragraph
        if in_list: out.append("</ul>"); in_list = False
        text = _html_escape(line)
        while "**" in text:
            text = text.replace("**", "<strong>", 1)
            if "**" in text:
                text = text.replace("**", "</strong>", 1)
        out.append(f"<p style=\"margin:10px 0;color:#333;line-height:1.6;\">{text}</p>")
    if in_list:
        out.append("</ul>")
    return "\n".join(out)


def _fmt_attendees_html(invitees) -> str:
    if not invitees:
        return '<p style="margin:0;color:#888;">No attendees listed.</p>'
    rows = []
    for i in invitees:
        name = _html_escape(i.get("name") or "")
        email = _html_escape(i.get("email") or "")
        ext = ' <span style="color:#b08258;font-size:12px;font-weight:500;">(external)</span>' if i.get("is_external") else ''
        rows.append(
            f'<tr><td style="padding:4px 0;color:#333;">{name}</td>'
            f'<td style="padding:4px 0 4px 12px;color:#666;font-size:13px;">{email}{ext}</td></tr>'
        )
    return f'<table style="width:100%;border-collapse:collapse;">{"".join(rows)}</table>'


def _fmt_action_items_html(items) -> str:
    if not items:
        return '<p style="margin:0;color:#888;">No action items captured.</p>'
    rows = []
    for a in items:
        desc = _html_escape(a.get("description") or "Action item")
        assignee = _html_escape((a.get("assignee") or {}).get("email") or "unassigned")
        playback = a.get("recording_playback_url") or ""
        link_html = f' <a href="{_html_escape(playback)}" style="color:#26CC7A;text-decoration:none;font-size:12px;">↗ jump to moment</a>' if playback else ''
        rows.append(
            f'<li style="margin-bottom:8px;color:#333;line-height:1.5;">'
            f'{desc}<div style="font-size:12px;color:#888;margin-top:2px;">'
            f'{assignee}{link_html}</div></li>'
        )
    return f'<ul style="margin:0;padding-left:20px;">{"".join(rows)}</ul>'


def send_team_summary_email(payload: dict):
    """Send an internal-only summary email to the Lumen team after each call.

    Fathom's own "send to attendees" feature cc's the client, which is not
    what we want for internal debriefs. This sends the same breakdown to a
    hardcoded internal list instead.
    """
    if not RESEND_API_KEY:
        print("[fathom] RESEND_API_KEY unset — skipping team summary email")
        return

    title = payload.get("meeting_title") or payload.get("title") or "Untitled call"
    started = payload.get("recording_start_time") or payload.get("created_at") or ""
    date_display = started[:16].replace("T", " ") + " UTC" if started else "Unknown time"
    share_url = payload.get("share_url") or payload.get("url") or ""
    summary_md = ((payload.get("default_summary") or {}).get("markdown_formatted") or "").strip()
    invitees = payload.get("calendar_invitees") or []
    action_items = payload.get("action_items") or []

    match = match_client(payload)
    client_badge = f'<span style="display:inline-block;padding:3px 10px;background:#e8f9f0;color:#1da563;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase;">{_html_escape(match["name"])}</span>' if match else '<span style="display:inline-block;padding:3px 10px;background:#f5f5f5;color:#888;border-radius:6px;font-size:11px;font-weight:600;letter-spacing:0.5px;text-transform:uppercase;">Internal</span>'

    watch_button = f'<a href="{_html_escape(share_url)}" style="display:inline-block;padding:12px 24px;background:#111;color:#fff;text-decoration:none;border-radius:8px;font-weight:600;font-size:14px;">Watch the recording →</a>' if share_url else ''

    html = f"""
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;max-width:640px;margin:0 auto;padding:32px 24px;background:#ffffff;color:#1a1a1a;">
  <div style="margin-bottom:20px;">{client_badge}</div>
  <h1 style="margin:0 0 6px;font-size:22px;color:#111;line-height:1.3;">{_html_escape(title)}</h1>
  <p style="margin:0 0 24px;color:#888;font-size:13px;">{_html_escape(date_display)}</p>

  {('<div style="margin-bottom:28px;">' + watch_button + '</div>') if watch_button else ''}

  <div style="padding:20px 0;border-top:1px solid #eee;">
    <h3 style="margin:0 0 12px;font-size:15px;color:#111;text-transform:uppercase;letter-spacing:0.8px;">Summary</h3>
    {_md_to_html(summary_md)}
  </div>

  <div style="padding:20px 0;border-top:1px solid #eee;">
    <h3 style="margin:0 0 12px;font-size:15px;color:#111;text-transform:uppercase;letter-spacing:0.8px;">Action items</h3>
    {_fmt_action_items_html(action_items)}
  </div>

  <div style="padding:20px 0;border-top:1px solid #eee;">
    <h3 style="margin:0 0 12px;font-size:15px;color:#111;text-transform:uppercase;letter-spacing:0.8px;">Attendees</h3>
    {_fmt_attendees_html(invitees)}
  </div>

  <p style="margin:28px 0 0;font-size:11px;color:#bbb;text-align:center;">Internal debrief — not sent to attendees. Automated by the Fathom webhook.</p>
</div>
"""

    subject = f"[Call debrief] {title}"
    body = {
        "from": "Lumen Calls <notifications@lumenmarketing.co>",
        "to": TEAM_RECIPIENTS,
        "subject": subject,
        "html": html,
    }
    try:
        r = requests.post(
            "https://api.resend.com/emails",
            headers={
                "Authorization": f"Bearer {RESEND_API_KEY}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=20,
        )
        if r.status_code >= 300:
            print(f"[fathom] Resend {r.status_code}: {r.text[:300]}")
        else:
            print(f"[fathom] team summary sent to {', '.join(TEAM_RECIPIENTS)}")
    except Exception as e:
        print(f"[fathom] team summary send error: {e}")


@fathom_bp.route("/webhooks/fathom", methods=["POST"])
def fathom_webhook():
    raw = request.get_data()
    if not verify_signature(request.headers, raw):
        print("[fathom] signature verification failed")
        return jsonify({"error": "invalid signature"}), 401
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as e:
        print(f"[fathom] bad JSON: {e}")
        return jsonify({"error": "bad json"}), 400

    threading.Thread(target=log_to_clickup, args=(payload,), daemon=True).start()
    threading.Thread(target=send_team_summary_email, args=(payload,), daemon=True).start()
    return jsonify({"ok": True}), 200
