"""
Meta Lead Gen webhook — fires when someone submits the Lumen lead form,
fetches the lead fields from the Graph API, and emails Kendall instantly.

Env vars required:
  META_LEADS_VERIFY_TOKEN   — must match the token set in Meta App Dashboard
  META_LEADS_ACCESS_TOKEN   — Lumen Marketing Co. page access token
  RESEND_API_KEY            — already set on Railway
"""

import os
import json
import threading
import requests
from flask import Blueprint, request, jsonify

meta_leads_bp = Blueprint("meta_leads", __name__)

VERIFY_TOKEN  = os.environ.get("META_LEADS_VERIFY_TOKEN", "lumen-meta-leads-verify")
ACCESS_TOKEN  = os.environ.get("META_LEADS_ACCESS_TOKEN", "")
RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL  = "kendall@lumenmarketing.co"
NOTIFY_FROM   = "Lumen Leads <kendall@lumenmarketing.co>"
GRAPH_BASE    = "https://graph.facebook.com/v21.0"


# ─── Webhook verification (Meta GET handshake) ───────────────────────────────

@meta_leads_bp.route("/webhooks/meta-leads", methods=["GET"])
def verify():
    mode      = request.args.get("hub.mode")
    token     = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")
    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Forbidden", 403


# ─── Lead event receiver ─────────────────────────────────────────────────────

@meta_leads_bp.route("/webhooks/meta-leads", methods=["POST"])
def receive():
    data = request.get_json(silent=True) or {}
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            if change.get("field") == "leadgen":
                val = change.get("value", {})
                leadgen_id = val.get("leadgen_id")
                if leadgen_id:
                    threading.Thread(
                        target=_process_lead, args=(leadgen_id,), daemon=True
                    ).start()
    return jsonify({"ok": True})


# ─── Lead processing ─────────────────────────────────────────────────────────

def _process_lead(leadgen_id):
    try:
        r = requests.get(
            f"{GRAPH_BASE}/{leadgen_id}",
            params={
                "access_token": ACCESS_TOKEN,
                "fields": "field_data,created_time,ad_name,adset_name,campaign_name,form_id",
            },
            timeout=8,
        )
        lead = r.json()
        if "error" in lead:
            print(f"[Meta Leads] Graph error for {leadgen_id}: {lead['error']}")
            return

        fields = {
            f["name"]: (f["values"][0] if f.get("values") else "")
            for f in lead.get("field_data", [])
        }

        name     = fields.get("full_name") or fields.get("name") or "(no name)"
        phone    = fields.get("phone_number") or fields.get("phone") or "(no phone)"
        business = fields.get("business_type") or "(not provided)"
        ad_name  = lead.get("ad_name") or lead.get("adset_name") or "Unknown ad"

        _send_email(name=name, phone=phone, business=business, ad_name=ad_name, leadgen_id=leadgen_id)

    except Exception as e:
        print(f"[Meta Leads] exception processing {leadgen_id}: {e}")


# ─── Email notification ───────────────────────────────────────────────────────

def _send_email(*, name, phone, business, ad_name, leadgen_id):
    if not RESEND_API_KEY:
        print("[Meta Leads] RESEND_API_KEY not set — skipping email")
        return

    digits = "".join(c for c in phone if c.isdigit())
    if len(digits) == 10:
        phone_tel = f"+1{digits}"
    elif len(digits) == 11 and digits.startswith("1"):
        phone_tel = f"+{digits}"
    else:
        phone_tel = digits or phone

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#f4f1ec;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1A1A1A;">
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#f4f1ec;padding:32px 16px;">
<tr><td align="center">
<table cellpadding="0" cellspacing="0" border="0" width="600" style="max-width:600px;width:100%;background:#ffffff;border-radius:16px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,0.06);">

<tr><td style="background:#1a1a1a;padding:22px 32px;color:#fff;">
  <div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;font-weight:600;opacity:0.7;">Lumen Marketing · Meta Lead Form</div>
  <div style="font-size:22px;font-weight:700;margin-top:6px;">New lead just came in</div>
</td></tr>

<tr><td style="padding:28px 32px 8px;">
  <div style="font-size:26px;font-weight:700;">{name}</div>
  <div style="color:#5C5C5C;font-size:14px;margin-top:6px;">{business}</div>
</td></tr>

<tr><td style="padding:24px 32px 0;">
  <div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#1a1a1a;font-weight:700;margin-bottom:10px;opacity:0.5;">Contact</div>
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:14px;">
    <tr><td style="padding:6px 0;color:#5C5C5C;width:80px;">Phone</td>
        <td><a href="tel:{phone_tel}" style="color:#1A1A1A;text-decoration:none;font-weight:600;">{phone}</a></td></tr>
  </table>
</td></tr>

<tr><td style="padding:20px 32px 0;">
  <a href="tel:{phone_tel}" style="display:inline-block;background:#1a1a1a;color:#fff;text-decoration:none;padding:14px 24px;border-radius:10px;font-weight:600;font-size:14px;margin-right:8px;margin-bottom:8px;">Call them</a>
  <a href="sms:{phone_tel}" style="display:inline-block;background:#fff;color:#1a1a1a;text-decoration:none;padding:12.5px 22.5px;border-radius:10px;font-weight:600;font-size:14px;border:1.5px solid #1a1a1a;margin-bottom:8px;">Text them</a>
</td></tr>

<tr><td style="padding:24px 32px 28px;">
  <div style="font-size:11px;letter-spacing:0.22em;text-transform:uppercase;color:#1a1a1a;font-weight:700;margin-bottom:10px;opacity:0.5;">Attribution</div>
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="font-size:12px;color:#5C5C5C;">
    <tr><td style="padding:4px 0;">Ad</td><td style="text-align:right;color:#1A1A1A;">{ad_name}</td></tr>
    <tr><td style="padding:4px 0;">Lead ID</td><td style="text-align:right;font-family:'SF Mono',Menlo,monospace;color:#1A1A1A;">{leadgen_id}</td></tr>
  </table>
</td></tr>

<tr><td style="background:#FAF7F2;padding:16px 32px;text-align:center;">
  <div style="font-size:11px;color:#5C5C5C;letter-spacing:0.06em;">Lumen Mainframe · Meta Lead Gen</div>
</td></tr>

</table></td></tr></table>
</body></html>"""

    subject = f"New Lead — {name} — {business}"
    payload = {
        "from": NOTIFY_FROM,
        "to": [NOTIFY_EMAIL],
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
            json=payload,
            timeout=8,
        )
        if r.status_code >= 300:
            print(f"[Meta Leads email] {r.status_code}: {r.text[:200]}")
        else:
            print(f"[Meta Leads email] sent for leadgen_id={leadgen_id}")
    except Exception as e:
        print(f"[Meta Leads email] exception: {e}")
