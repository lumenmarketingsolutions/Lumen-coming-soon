"""Phorest API integration for Mane Styling Studio.

Two responsibilities:
  1. create_client(...)         — fires on funnel submit. Pushes the lead into
                                   Phorest as a new client, returns the Phorest
                                   client_id which we store against the lead row.
  2. list_appointments_since(...)— polled every ~15 min by a background worker.
                                   When an appointment shows up for a client_id
                                   we created from a funnel lead, the caller
                                   fires a Meta CAPI `Schedule` event back to
                                   Mane's pixel — closing the loop on Meta
                                   optimization.

Phorest API quirks worth remembering:
  - No webhooks. Polling only.
  - No sandbox. The live API hits real-time data, so test writes need clear
    "DELETE-ME" markers in the client name.
  - Username is prefixed `global/`. Basic Auth header is base64(global/user:pass).
  - Rate limit: 100 rps. We are nowhere near it.
  - US salons hit api-gateway-us; EU salons hit api-gateway-eu. Mane is US.

All credentials live in env vars. Module is a no-op when MANE_PHOREST_USERNAME
is unset, mirroring the Meta CAPI helper's "configured-or-silent" pattern.
"""
import os
import base64
import datetime as _dt

import requests


_BASE = os.environ.get(
    "MANE_PHOREST_BASE",
    "https://api-gateway-us.phorest.com/third-party-api-server",
)
_USERNAME           = os.environ.get("MANE_PHOREST_USERNAME", "")
_PASSWORD           = os.environ.get("MANE_PHOREST_PASSWORD", "")
_BUSINESS_ID        = os.environ.get("MANE_PHOREST_BUSINESS_ID", "")
_BRANCH_ID          = os.environ.get("MANE_PHOREST_BRANCH_ID", "")
# Optional: tag every funnel-sourced client with this Phorest category so
# Keanna can filter her Client list. Empty → no tag applied. Phorest doesn't
# expose a "list categories" API, so the ID is fished out of a client that
# already wears the category (see project_mane_phorest.md memory for steps).
_LEAD_CATEGORY_ID   = os.environ.get("MANE_PHOREST_LEAD_CATEGORY_ID", "")
_TIMEOUT            = 8  # seconds


def _configured() -> bool:
    return bool(_USERNAME and _PASSWORD and _BUSINESS_ID and _BRANCH_ID)


def _auth_header() -> dict:
    token = base64.b64encode(f"{_USERNAME}:{_PASSWORD}".encode()).decode()
    return {
        "Authorization": f"Basic {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _split_name(full_name: str) -> tuple[str, str]:
    """Phorest requires firstName + lastName. We collect a single name field
    on the funnel, so we split here. If only one word, use that as first name
    and a hyphen as last (Phorest rejects empty strings on required fields)."""
    parts = (full_name or "").strip().split()
    if not parts:
        return "Guest", "-"
    if len(parts) == 1:
        return parts[0], "-"
    return parts[0], " ".join(parts[1:])


def _normalize_phone(phone: str) -> str:
    digits = "".join(c for c in (phone or "") if c.isdigit())
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return digits


def create_client(
    *,
    name: str,
    email: str,
    phone: str,
    source: str,
    notes: str = "",
    external_id: str = "",
):
    """Push a funnel lead into Phorest. Fire-and-forget — returns the new
    Phorest client_id on success, None on any failure (logs the error).

    `source` is one of "color-funnel" / "extension-funnel" — surfaced in notes.
    `external_id` should be the lead's row id in our SQLite, so we can later
    look up the Phorest client_id from our side too.
    """
    if not _configured():
        return None

    first, last = _split_name(name)
    payload = {
        "firstName": first,
        "lastName": last,
        "email": (email or "").strip() or None,
        "mobile": _normalize_phone(phone) or None,
        "creatingBranchId": _BRANCH_ID,
        # Tag with the source so Keanna can filter inside Phorest
        "notes": (f"Source: Lumen {source} funnel\n{notes}").strip(),
        "externalId": external_id or None,
        # Consent flags — they submitted a form with consent legal text, so
        # we mark email/SMS marketing as opted-in. Reminders too (they want
        # the confirmation about their consult/color appt).
        "emailMarketingConsent": True,
        "smsMarketingConsent": True,
        "emailReminderConsent": True,
        "smsReminderConsent": True,
    }
    if _LEAD_CATEGORY_ID:
        payload["clientCategoryIds"] = [_LEAD_CATEGORY_ID]
    # Trim None values — Phorest dislikes nulls on optional fields
    payload = {k: v for k, v in payload.items() if v is not None}

    url = f"{_BASE}/api/business/{_BUSINESS_ID}/client"
    try:
        r = requests.post(url, json=payload, headers=_auth_header(), timeout=_TIMEOUT)
        if r.status_code in (200, 201):
            return (r.json() or {}).get("clientId")
        # 409 = client already exists (email/phone matched); not a failure for our
        # purposes, but Phorest doesn't return the existing client_id on 409 so
        # we just log and move on.
        print(f"[phorest] create_client → HTTP {r.status_code}: {r.text[:240]}")
    except Exception as e:
        print(f"[phorest] create_client → exception: {e}")
    return None


def list_appointments_since(updated_from: _dt.datetime, page_size: int = 100):
    """Yield AppointmentResponse dicts for appointments updated since
    `updated_from`. Paginates automatically. Stops on any error.

    Caller is responsible for tracking the high-water-mark timestamp.

    Phorest changed this endpoint (verified live 2026-07-02): the old single
    `updated_from` param now 400s. It requires from_date/to_date (appointment
    DATE, max 31-day span) and accepts updated_after/updated_before alongside
    as the updates filter. Bookings land weeks out, so we sweep three 31-day
    appointment-date windows (a week back through ~12 weeks ahead), each
    filtered to appointments updated since the cursor. Dedupe on
    appointmentId in case a window boundary double-yields."""
    if not _configured():
        return

    now = _dt.datetime.utcnow()
    after_iso = updated_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    before_iso = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    seen = set()
    win_start = now - _dt.timedelta(days=7)
    for _ in range(3):
        win_end = win_start + _dt.timedelta(days=30)
        page = 0
        while True:
            url = (
                f"{_BASE}/api/business/{_BUSINESS_ID}/branch/{_BRANCH_ID}/appointment"
                f"?from_date={win_start.strftime('%Y-%m-%d')}&to_date={win_end.strftime('%Y-%m-%d')}"
                f"&updated_after={after_iso}&updated_before={before_iso}"
                f"&size={page_size}&page={page}&fetch_canceled=false"
            )
            try:
                r = requests.get(url, headers=_auth_header(), timeout=_TIMEOUT)
            except Exception as e:
                print(f"[phorest] list_appointments → exception: {e}")
                return
            if r.status_code != 200:
                print(f"[phorest] list_appointments → HTTP {r.status_code}: {r.text[:240]}")
                return
            body = r.json() or {}
            for appt in (body.get("_embedded") or {}).get("appointments", []):
                aid = appt.get("appointmentId")
                if aid in seen:
                    continue
                seen.add(aid)
                yield appt
            info = body.get("page") or {}
            if page + 1 >= int(info.get("totalPages", 0) or 0):
                break
            page += 1
        win_start = win_end + _dt.timedelta(days=1)
