"""
New lumenmarketing.co website (Next.js static export in site_build/), plus its lead flow.

Serving:
  /                     -> site_build/index.html (app.py index() calls site_page_or_none)
  /_next/<path>         -> build assets, cached forever (file names are content hashed)
  /<path>               -> lowest priority catch-all: <path>, <path>.html, <path>/index.html, else 404.html.
                           Every existing Flask route wins over this one.

Lead flow (contact page, 3 steps):
  POST /api/lumen-inquiry  step 1: name, email, phone, business, website, SMS consent (+ proof)
                           -> sqlite row, GHL contact upsert (Mk7 Media location, tag "lumen website"),
                              opportunity in the "Lumen Website" pipeline, email alert, Meta CAPI Lead.
                           step 2 (is_enrichment): industry, need, budget -> GHL tags + note, details email.
  Step 3 is the GHL calendar widget. booking_reminders.py handles confirmation/reminder texts,
  owner alerts and moving the opportunity to the booked stage.
"""
import datetime
import html as _h
import os
import sqlite3
import threading

import requests
from flask import Blueprint, jsonify, make_response, request, send_from_directory

SITE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "site_build")

GHL = "https://services.leadconnectorhq.com"
GHL_LOCATION = "6b4I6ILHBVcWQYlmPj3i"  # Mk7 Media sub-account (holds the Lumen calendar)
PIPELINE_NAME = "Lumen Website"
TAG = "lumen website"

lumen_site_bp = Blueprint("lumen_site", __name__)

# Until launch the new site is only visible with a preview cookie: visit /?preview=<key> once.
# Launch = set LUMEN_NEW_SITE=1 on Railway. Preview responses are noindex.
PREVIEW_KEY = os.environ.get("LUMEN_PREVIEW_KEY", "WJehxYu37g3M")


def site_live():
    return os.environ.get("LUMEN_NEW_SITE") == "1"


def _enabled():
    return site_live() or request.cookies.get("lumen_preview") == PREVIEW_KEY or request.args.get("preview") == PREVIEW_KEY


@lumen_site_bp.after_app_request
def _preview_cookie(resp):
    if request.args.get("preview") == PREVIEW_KEY:
        resp.set_cookie("lumen_preview", PREVIEW_KEY, max_age=60 * 86400, httponly=True, samesite="Lax", secure=request.is_secure)
    if not site_live() and (request.cookies.get("lumen_preview") == PREVIEW_KEY or request.args.get("preview") == PREVIEW_KEY):
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    # Private client pages (WhatsApp onboarding links we send by hand): never indexed.
    if request.path.startswith(("/connect/", "/whatsapp-connect")):
        resp.headers["X-Robots-Tag"] = "noindex, nofollow"
    return resp
_cfg = {}  # filled by init_lumen_site(): send_email, notify, db_path, capi


def _site_file(rel):
    full = os.path.join(SITE_DIR, rel)
    if rel and os.path.isfile(full) and os.path.realpath(full).startswith(os.path.realpath(SITE_DIR)):
        return send_from_directory(SITE_DIR, rel)
    return None


def site_page_or_none(path=""):
    """Serve a page of the new site if it exists (and is live or previewed), else None."""
    if not os.path.isdir(SITE_DIR) or not _enabled():
        return None
    p = path.strip("/")
    for c in ([p, p + ".html", p + "/index.html"] if p else ["index.html"]):
        r = _site_file(c)
        if r is not None:
            if c.endswith(".html") or c.endswith(".txt") or c.endswith(".xml"):
                # pages must always be revalidated, or phones keep an old version (and the old/new site
                # switch by cookie would be cached). Hashed assets under /_next stay cached forever.
                r.headers["Cache-Control"] = "no-cache, must-revalidate"
                r.headers["Vary"] = "Cookie"
            return r
    return None


@lumen_site_bp.route("/_next/<path:p>")
def site_next_assets(p):
    r = _site_file(os.path.join("_next", p))
    if r is None:
        return "Not found", 404
    r.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return r


@lumen_site_bp.route("/<path:p>")
def site_catch_all(p):
    r = site_page_or_none(p)
    if r is not None:
        return r
    if not _enabled():
        return "Not found", 404
    nf = _site_file("404.html")
    if nf is not None:
        resp = make_response(nf)
        resp.status_code = 404
        return resp
    return "Not found", 404


# ───────────────────────── GHL ─────────────────────────

def _h_ghl(version="2021-07-28"):
    return {"Authorization": f"Bearer {os.environ.get('GHL_MK7_PIT', '')}", "Version": version,
            "Accept": "application/json", "Content-Type": "application/json"}


_pipeline = {}


def lumen_pipeline():
    """Find the 'Lumen Website' pipeline by name (Kendall creates it in GHL). Cached once found."""
    if _pipeline.get("id"):
        return _pipeline
    try:
        r = requests.get(f"{GHL}/opportunities/pipelines", headers=_h_ghl(), timeout=15, params={"locationId": GHL_LOCATION})
        for p in (r.json() if r.ok else {}).get("pipelines", []):
            if p.get("name", "").strip().lower() == PIPELINE_NAME.lower():
                stages = p.get("stages", [])
                booked = next((s["id"] for s in stages if "book" in s.get("name", "").lower()), None)
                _pipeline.update(id=p["id"], first=stages[0]["id"] if stages else None, booked=booked)
                break
    except Exception as e:
        print("[lumen-site] pipeline lookup failed:", e)
    return _pipeline


def _ghl_upsert(name, email, phone, business, website, tags):
    first, _, last = name.strip().partition(" ")
    body = {"locationId": GHL_LOCATION, "firstName": first, "lastName": last.strip(), "email": email,
            "phone": phone, "companyName": business or None, "website": website or None,
            "source": "lumenmarketing.co", "tags": tags}
    body = {k: v for k, v in body.items() if v not in (None, "")}
    r = requests.post(f"{GHL}/contacts/upsert", headers=_h_ghl(), json=body, timeout=15)
    if r.status_code >= 300:
        print("[lumen-site] GHL upsert failed", r.status_code, r.text[:300])
        return None
    return (r.json().get("contact") or {}).get("id")


def _ghl_note(contact_id, text):
    requests.post(f"{GHL}/contacts/{contact_id}/notes", headers=_h_ghl(), json={"body": text}, timeout=15)


def _ghl_opportunity(contact_id, name, business):
    p = lumen_pipeline()
    if not p.get("id"):
        print("[lumen-site] no 'Lumen Website' pipeline yet, skipping opportunity")
        return
    s = requests.get(f"{GHL}/opportunities/search", headers=_h_ghl(), timeout=15,
                     params={"location_id": GHL_LOCATION, "pipeline_id": p["id"], "contact_id": contact_id})
    if s.ok and s.json().get("opportunities"):
        return
    r = requests.post(f"{GHL}/opportunities/", headers=_h_ghl(), timeout=15, json={
        "locationId": GHL_LOCATION, "pipelineId": p["id"], "pipelineStageId": p["first"], "contactId": contact_id,
        "name": f"{name} · {business}" if business else name, "status": "open", "source": "lumenmarketing.co"})
    if r.status_code >= 300:
        print("[lumen-site] opportunity failed", r.status_code, r.text[:300])


# ───────────────────────── emails ─────────────────────────

def _email(title, rows, phone):
    cells = "".join(
        f"<tr><td style='padding:9px 0;color:#8d8d96;font-size:13px;width:150px;vertical-align:top'>{_h.escape(k)}</td>"
        f"<td style='padding:9px 0;color:#f2f2f3;font-size:15px'>{_h.escape(str(v or '-'))}</td></tr>"
        for k, v in rows)
    digits = "".join(c for c in (phone or "") if c.isdigit())
    btn = (f"<a href='sms:+{digits if len(digits) > 10 else '1' + digits}' style='display:inline-block;margin-top:26px;"
           f"background:#fff;color:#0a0a0f;text-decoration:none;font-weight:600;font-size:14px;padding:12px 22px;border-radius:999px'>"
           f"Text them</a>") if digits else ""
    return (f"<div style='background:#0a0a0f;padding:36px 16px;font-family:Inter,Helvetica,Arial,sans-serif'>"
            f"<div style='max-width:560px;margin:0 auto;background:#111118;border:1px solid #1f1f2b;border-radius:14px;padding:32px'>"
            f"<p style='margin:0;font-size:12px;letter-spacing:.08em;color:#9b7bff'>LUMENMARKETING.CO</p>"
            f"<p style='margin:10px 0 22px;font-size:24px;font-weight:700;color:#f2f2f3;letter-spacing:-.02em'>{_h.escape(title)}</p>"
            f"<table style='width:100%;border-collapse:collapse'>{cells}</table>{btn}"
            f"<p style='margin:28px 0 0;font-size:12px;color:#5c5c66'>The booking link is on their screen now. "
            f"You'll get a text if they book.</p></div></div>")


# ───────────────────────── endpoint ─────────────────────────

def _db():
    con = sqlite3.connect(_cfg["db_path"])
    con.execute("""CREATE TABLE IF NOT EXISTS lumen_site_inquiries (id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT, name TEXT, email TEXT, phone TEXT, business TEXT, website TEXT,
        consent_service INTEGER, consent_marketing INTEGER, consent_service_text TEXT, consent_marketing_text TEXT,
        industry TEXT, need TEXT, budget TEXT, ghl_contact_id TEXT, ip TEXT, user_agent TEXT, page TEXT,
        created_at TEXT, updated_at TEXT)""")
    return con


@lumen_site_bp.route("/api/lumen-inquiry", methods=["POST"])
def lumen_inquiry():
    d = request.get_json(silent=True) or {}
    g = lambda k, n=300: str(d.get(k) or "").strip()[:n]
    name, email, phone = g("name"), g("email"), g("phone", 40)
    if not name or "@" not in email:
        return jsonify({"ok": False, "error": "missing"}), 400
    ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    ua = (request.headers.get("User-Agent") or "")[:300]
    now = datetime.datetime.utcnow().isoformat()
    business, website = g("business"), g("website")

    if d.get("is_enrichment"):
        industry, need, budget = g("industry", 80), g("need", 80), g("budget", 80)
        with _db() as con:
            row = con.execute("SELECT id, ghl_contact_id FROM lumen_site_inquiries WHERE email=? ORDER BY id DESC LIMIT 1",
                              (email,)).fetchone()
            if row:
                con.execute("UPDATE lumen_site_inquiries SET industry=?, need=?, budget=?, updated_at=? WHERE id=?",
                            (industry, need, budget, now, row[0]))
        cid = row[1] if row else None

        def work():
            try:
                c = cid or _ghl_upsert(name, email, phone, business, website, [TAG])
                if c:
                    _ghl_upsert(name, email, phone, business, website,
                                [TAG, f"industry: {industry}".lower(), f"need: {need}".lower(), f"budget: {budget}".lower()])
                    _ghl_note(c, f"Website answers\nIndustry: {industry}\nNeeds most: {need}\nMonthly ad budget: {budget}")
            except Exception as e:
                print("[lumen-site] enrichment GHL error:", e)
        threading.Thread(target=work, daemon=True).start()
        _cfg["send_email"](_cfg["notify"], f"Lead details: {name}", _email(f"{name} answered the questions", [
            ("Business", business), ("Industry", industry), ("Needs most", need), ("Monthly ad budget", budget),
            ("Phone", phone), ("Email", email)], phone))
        return jsonify({"ok": True})

    row = dict(event_id=g("event_id", 80), name=name, email=email, phone=phone, business=business, website=website,
               consent_service=1 if d.get("consent_service") else 0, consent_marketing=1 if d.get("consent_marketing") else 0,
               consent_service_text=g("consent_service_text", 1000), consent_marketing_text=g("consent_marketing_text", 1000),
               ip=ip, user_agent=ua, page=(request.headers.get("Referer") or "")[:300], created_at=now, updated_at=now)
    with _db() as con:
        cur = con.execute(f"INSERT INTO lumen_site_inquiries ({','.join(row)}) VALUES ({','.join('?' * len(row))})",
                          tuple(row.values()))
        rid = cur.lastrowid

    def work():
        try:
            tags = [TAG] + (["sms consent: service"] if row["consent_service"] else []) + \
                   (["sms consent: marketing"] if row["consent_marketing"] else [])
            cid = _ghl_upsert(name, email, phone, business, website, tags)
            if cid:
                with _db() as con:
                    con.execute("UPDATE lumen_site_inquiries SET ghl_contact_id=? WHERE id=?", (cid, rid))
                _ghl_opportunity(cid, name, business)
                consent = (f"SMS consent (service): {'yes' if row['consent_service'] else 'no'}\n"
                           f"SMS consent (marketing): {'yes' if row['consent_marketing'] else 'no'}\n"
                           f"Captured {now} UTC from {ip}")
                _ghl_note(cid, f"New lead from lumenmarketing.co\n{consent}")
        except Exception as e:
            print("[lumen-site] GHL error:", e)
        try:
            _cfg["capi"](row["event_id"], name, phone, g("fbp", 200), g("fbc", 300), ip, ua, row["page"])
        except Exception as e:
            print("[lumen-site] capi error:", e)
    threading.Thread(target=work, daemon=True).start()

    _cfg["send_email"](_cfg["notify"], f"New lead: {name}", _email(f"New lead: {name}", [
        ("Business", business), ("Website", website), ("Phone", phone), ("Email", email),
        ("SMS consent", ", ".join(x for x, on in [("service", row["consent_service"]), ("marketing", row["consent_marketing"])] if on) or "none")],
        phone))
    return jsonify({"ok": True})


def init_lumen_site(app, send_email, notify, db_path, capi):
    _cfg.update(send_email=send_email, notify=notify, db_path=db_path, capi=capi)
    app.register_blueprint(lumen_site_bp)
