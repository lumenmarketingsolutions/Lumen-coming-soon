"""
Local test for the Fathom webhook handler.

Usage:
  1. In one terminal:
       export FATHOM_WEBHOOK_SECRET='whsec_gS2o0aTDniIRnyCH7RyiGwO6MUUHCI6j'
       export CLICKUP_TOKEN='pk_89293931_SIWYQSKKYV9PU20YODDKKGSHGK0EIF83'
       flask --app app run --port 5055
  2. In another terminal:
       python3 test_fathom_local.py [avalon|prospect]

Default scenario is `prospect` (routes to Fathom Calls fallback list).
"""
import os
import sys
import time
import json
import base64
import hmac
import hashlib
import uuid

import requests

SECRET = "whsec_gS2o0aTDniIRnyCH7RyiGwO6MUUHCI6j"
ENDPOINT = "http://127.0.0.1:5055/webhooks/fathom"


def payload_prospect():
    return {
        "title": "Intro call with Acme Widgets",
        "meeting_title": "Intro call with Acme Widgets",
        "recording_id": 999999001,
        "url": "https://fathom.video/test-prospect",
        "share_url": "https://fathom.video/share/test-prospect",
        "created_at": "2026-04-15T18:05:00Z",
        "scheduled_start_time": "2026-04-15T17:00:00Z",
        "scheduled_end_time": "2026-04-15T17:30:00Z",
        "recording_start_time": "2026-04-15T17:01:00Z",
        "recording_end_time": "2026-04-15T17:29:00Z",
        "calendar_invitees_domains_type": "one_or_more_external",
        "transcript_language": "en",
        "transcript": [
            {"speaker": {"display_name": "Kendall Davis", "matched_calendar_invitee_email": "kendall@lumenmarketing.co"},
             "text": "Thanks for hopping on. Walk me through what you're trying to solve.",
             "timestamp": "00:00:15"},
            {"speaker": {"display_name": "Sam Buyer", "matched_calendar_invitee_email": "sam@acmewidgets.com"},
             "text": "We need a funnel that actually converts cold traffic for our ecom store.",
             "timestamp": "00:00:22"},
        ],
        "default_summary": {
            "template_name": "general",
            "markdown_formatted": "## Summary\n\nProspect is an ecom brand looking for conversion-focused funnel work. Budget ~$5k/mo. Next step: send proposal."
        },
        "action_items": [
            {"description": "Send Lumen proposal with case studies",
             "user_generated": False, "completed": False,
             "recording_timestamp": "00:25:00",
             "recording_playback_url": "https://fathom.video/test-prospect#t=1500",
             "assignee": {"name": "Kendall Davis", "email": "kendall@lumenmarketing.co", "team": "Lumen"}},
            {"description": "Follow up in 48 hours if no response",
             "user_generated": False, "completed": False,
             "recording_timestamp": "00:26:30",
             "recording_playback_url": "https://fathom.video/test-prospect#t=1590",
             "assignee": {"name": "Kendall Davis", "email": "kendall@lumenmarketing.co", "team": "Lumen"}},
        ],
        "calendar_invitees": [
            {"name": "Kendall Davis", "matched_speaker_display_name": "Kendall Davis",
             "email": "kendall@lumenmarketing.co", "is_external": False, "email_domain": "lumenmarketing.co"},
            {"name": "Sam Buyer", "matched_speaker_display_name": "Sam Buyer",
             "email": "sam@acmewidgets.com", "is_external": True, "email_domain": "acmewidgets.com"},
        ],
        "recorded_by": {"name": "Kendall Davis", "email": "kendall@lumenmarketing.co",
                        "team": "Lumen", "email_domain": "lumenmarketing.co"},
        "crm_matches": None,
    }


def payload_avalon():
    p = payload_prospect()
    p["title"] = "Avalon Laser — Reza weekly sync"
    p["meeting_title"] = "Avalon Laser — Reza weekly sync"
    p["recording_id"] = 999999002
    p["url"] = "https://fathom.video/test-avalon"
    p["share_url"] = "https://fathom.video/share/test-avalon"
    p["default_summary"]["markdown_formatted"] = (
        "## Summary\n\nReviewed Encinitas CPL drop, agreed to push more budget to San Diego. Bre wants new creative by Friday."
    )
    p["action_items"] = [
        {"description": "Ship new San Diego creative by Friday",
         "user_generated": False, "completed": False,
         "recording_timestamp": "00:10:00",
         "recording_playback_url": "https://fathom.video/test-avalon#t=600",
         "assignee": {"name": "Kendall Davis", "email": "kendall@lumenmarketing.co", "team": "Lumen"}},
    ]
    p["calendar_invitees"] = [
        {"name": "Kendall Davis", "matched_speaker_display_name": "Kendall Davis",
         "email": "kendall@lumenmarketing.co", "is_external": False, "email_domain": "lumenmarketing.co"},
        {"name": "Reza Avalon", "matched_speaker_display_name": "Reza",
         "email": "avesta70@gmail.com", "is_external": True, "email_domain": "gmail.com"},
    ]
    return p


def sign(body_bytes: bytes):
    wid = f"msg_{uuid.uuid4().hex}"
    wts = str(int(time.time()))
    secret = SECRET[len("whsec_"):] if SECRET.startswith("whsec_") else SECRET
    try:
        key = base64.b64decode(secret)
    except Exception:
        key = secret.encode()
    signed = f"{wid}.{wts}.".encode() + body_bytes
    sig_b64 = base64.b64encode(hmac.new(key, signed, hashlib.sha256).digest()).decode()
    return {
        "webhook-id": wid,
        "webhook-timestamp": wts,
        "webhook-signature": f"v1,{sig_b64}",
        "Content-Type": "application/json",
    }


def main():
    scenario = (sys.argv[1] if len(sys.argv) > 1 else "prospect").lower()
    payload = payload_avalon() if scenario == "avalon" else payload_prospect()
    body = json.dumps(payload).encode("utf-8")
    headers = sign(body)
    print(f"Scenario: {scenario}")
    print(f"POST {ENDPOINT}")
    r = requests.post(ENDPOINT, data=body, headers=headers, timeout=15)
    print(f"Status: {r.status_code}")
    print(f"Body: {r.text}")


if __name__ == "__main__":
    main()
