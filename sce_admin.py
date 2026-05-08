"""
Internal admin portal for SCE offers (Kendall + Nate only).

Routes (all behind /admin):
  GET  /admin                              → login form
  POST /admin/login                        → handle credentials
  GET  /admin/logout                       → clear session
  GET  /admin/sce/mothersday               → metrics dashboard
  GET  /admin/sce/mothersday/contacts      → lead list
  GET  /admin/sce/mothersday/abandoned     → sessions with no lead

Auth: hardcoded allowlist of emails + a password env var. Constant-time
compare. Flask session for state. No public registration.
"""

import os
import json
import sqlite3
import secrets
import datetime
from functools import wraps
from collections import OrderedDict, Counter
from flask import Blueprint, render_template, request, redirect, url_for, session, abort, jsonify

sce_admin_bp = Blueprint("sce_admin", __name__)

ADMIN_EMAILS = {"kendall@lumenmarketing.co", "n.wilkinson@launchpoint.dev"}
ADMIN_PASSWORD = os.environ.get("SCE_ADMIN_PASSWORD", "Sce123$$")

DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "waitlist.db")

# Funnel page identifiers in the order users encounter them.
FUNNEL_PAGES = [
    ("landing",     "Landing"),
    ("tier",        "Tier select"),
    ("preferences", "Preferences"),
    ("reserve",     "Opt-in"),
    ("booked",      "Confirmed"),
]


def login_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("sce_admin"):
            return redirect(url_for("sce_admin.login"))
        return f(*args, **kwargs)
    return wrapper


# ─────────────────────────── Auth ───────────────────────────

@sce_admin_bp.route("/admin")
def login():
    if session.get("sce_admin"):
        return redirect(url_for("sce_admin.md_metrics"))
    return render_template("admin_login.html", error=None)


@sce_admin_bp.route("/admin/login", methods=["POST"])
def login_handler():
    email = (request.form.get("email") or "").strip().lower()
    password = (request.form.get("password") or "").strip()
    if email in ADMIN_EMAILS and secrets.compare_digest(password, ADMIN_PASSWORD):
        session["sce_admin"] = email
        session.permanent = True
        return redirect(url_for("sce_admin.md_metrics"))
    return render_template("admin_login.html", error="Invalid credentials"), 401


@sce_admin_bp.route("/admin/logout")
def logout():
    session.pop("sce_admin", None)
    return redirect(url_for("sce_admin.login"))


# ────────────────────────── Helpers ──────────────────────────

def _con():
    return sqlite3.connect(DB_PATH)


def _parse_range(args):
    """Return (start_iso, end_iso, key) for range filter. key in {7d, 30d, all}."""
    key = (args.get("range") or "30d").strip()
    end = datetime.datetime.utcnow()
    if key == "7d":
        start = end - datetime.timedelta(days=7)
    elif key == "all":
        start = datetime.datetime(2000, 1, 1)
    else:
        key = "30d"
        start = end - datetime.timedelta(days=30)
    return start.isoformat(), end.isoformat(), key


def _md_metrics(start_iso, end_iso):
    """Aggregate Mother's Day funnel metrics for the date range."""
    con = _con()
    try:
        # Visits by page (deduped by session+page)
        page_counts = OrderedDict((pid, 0) for pid, _ in FUNNEL_PAGES)
        rows = con.execute("""
            SELECT page, COUNT(DISTINCT session_id) FROM mothersday_visits
            WHERE entered_at >= ? AND entered_at <= ?
            GROUP BY page
        """, (start_iso, end_iso)).fetchall()
        for page, count in rows:
            if page in page_counts:
                page_counts[page] = count

        # Total unique visitors = sessions that hit landing OR any page.
        total_visitors = con.execute("""
            SELECT COUNT(DISTINCT session_id) FROM mothersday_visits
            WHERE entered_at >= ? AND entered_at <= ?
        """, (start_iso, end_iso)).fetchone()[0] or 0

        # Lead count (total, includes pre-tracking leads)
        leads = con.execute("""
            SELECT COUNT(*) FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ?
        """, (start_iso, end_iso)).fetchone()[0] or 0

        # Tracked leads (have a session_id we can join to visits) — used for
        # conversion rate so the math stays consistent with total_visitors.
        leads_tracked = con.execute("""
            SELECT COUNT(*) FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ? AND session_id != ''
        """, (start_iso, end_iso)).fetchone()[0] or 0

        # Paid count
        paid = con.execute("""
            SELECT COUNT(*) FROM mothersday_leads
            WHERE status='paid' AND created_at >= ? AND created_at <= ?
        """, (start_iso, end_iso)).fetchone()[0] or 0

        # Avg time on page (across all pages in range, only counting >0)
        avg_time_ms = con.execute("""
            SELECT AVG(time_on_page_ms) FROM mothersday_visits
            WHERE entered_at >= ? AND entered_at <= ? AND time_on_page_ms > 0
        """, (start_iso, end_iso)).fetchone()[0] or 0
        avg_time_s = round(avg_time_ms / 1000, 1) if avg_time_ms else 0

        # Time on page per page (median-ish via avg)
        time_per_page = OrderedDict((pid, 0) for pid, _ in FUNNEL_PAGES)
        rows = con.execute("""
            SELECT page, AVG(time_on_page_ms) FROM mothersday_visits
            WHERE entered_at >= ? AND entered_at <= ? AND time_on_page_ms > 0
            GROUP BY page
        """, (start_iso, end_iso)).fetchall()
        for page, ms in rows:
            if page in time_per_page:
                time_per_page[page] = round((ms or 0) / 1000, 1)

        # Device split
        device_rows = con.execute("""
            SELECT device, COUNT(DISTINCT session_id) FROM mothersday_visits
            WHERE entered_at >= ? AND entered_at <= ?
            GROUP BY device
        """, (start_iso, end_iso)).fetchall()
        devices = {"Desktop": 0, "Mobile": 0}
        for d, c in device_rows:
            if d in devices:
                devices[d] = c

        # Tier split (from leads)
        tier_rows = con.execute("""
            SELECT tier, COUNT(*) FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ?
            GROUP BY tier
        """, (start_iso, end_iso)).fetchall()
        tier_split = {"full-day": 0, "day-out": 0}
        for t, c in tier_rows:
            if t in tier_split:
                tier_split[t] = c

        # Car split (from leads)
        car_rows = con.execute("""
            SELECT car, COUNT(*) FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ?
            GROUP BY car
        """, (start_iso, end_iso)).fetchall()
        car_split = {}
        for c, n in car_rows:
            car_split[c] = n

        # Restaurant prefs (parse comma-separated, count occurrences)
        pref_rows = con.execute("""
            SELECT restaurant_prefs FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ? AND restaurant_prefs != ''
        """, (start_iso, end_iso)).fetchall()
        prefs_counter = Counter()
        for (raw,) in pref_rows:
            for p in [x.strip() for x in (raw or "").split(",") if x.strip()]:
                prefs_counter[p] += 1
        restaurant_prefs = sorted(prefs_counter.items(), key=lambda kv: -kv[1])

        # Conversion rate = tracked_leads / total_visitors. Filtering by
        # session_id excludes leads created before tracking was wired up so
        # the rate stays internally consistent with visit data. Hard cap at
        # 100 % as a defensive safeguard against any future data drift.
        if total_visitors:
            conv_rate = min((leads_tracked / total_visitors) * 100, 100)
        else:
            conv_rate = 0
    finally:
        con.close()

    return {
        "total_visitors": total_visitors,
        "leads": leads,
        "leads_tracked": leads_tracked,
        "paid": paid,
        "conv_rate": round(conv_rate, 2),
        "avg_time_s": avg_time_s,
        "page_counts": page_counts,
        "time_per_page": time_per_page,
        "devices": devices,
        "tier_split": tier_split,
        "car_split": car_split,
        "restaurant_prefs": restaurant_prefs,
    }


# ────────────────────────── Pages ──────────────────────────

@sce_admin_bp.route("/admin/sce/mothersday")
@login_required
def md_metrics():
    start, end, range_key = _parse_range(request.args)
    metrics = _md_metrics(start, end)
    return render_template(
        "admin_sce_metrics.html",
        active="metrics",
        offer_id="mothersday",
        offer_name="Mother's Day",
        metrics=metrics,
        funnel_pages=FUNNEL_PAGES,
        range_key=range_key,
    )


@sce_admin_bp.route("/admin/sce/mothersday/contacts")
@login_required
def md_contacts():
    start, end, range_key = _parse_range(request.args)
    q = (request.args.get("q") or "").strip().lower()

    con = _con()
    try:
        rows = con.execute("""
            SELECT id, created_at, name, email, phone, tier, car, sku,
                   price_cents, status, restaurant_prefs, special_notes,
                   utm_source, utm_campaign, ip
            FROM mothersday_leads
            WHERE created_at >= ? AND created_at <= ?
            ORDER BY id DESC
        """, (start, end)).fetchall()
    finally:
        con.close()

    leads = []
    for r in rows:
        rec = {
            "id": r[0], "created_at": r[1], "name": r[2], "email": r[3],
            "phone": r[4], "tier": r[5], "car": r[6], "sku": r[7],
            "price": (r[8] or 0) / 100, "status": r[9],
            "restaurant_prefs": r[10] or "", "special_notes": r[11] or "",
            "utm_source": r[12] or "", "utm_campaign": r[13] or "",
            "ip": r[14] or "",
        }
        if q:
            haystack = " ".join([
                rec["name"], rec["email"], rec["phone"],
                rec["tier"], rec["car"], rec["restaurant_prefs"], rec["special_notes"],
            ]).lower()
            if q not in haystack:
                continue
        leads.append(rec)

    return render_template(
        "admin_sce_contacts.html",
        active="contacts",
        offer_id="mothersday",
        offer_name="Mother's Day",
        leads=leads,
        range_key=range_key,
        q=q,
    )


@sce_admin_bp.route("/admin/sce/mothersday/abandoned")
@login_required
def md_abandoned():
    start, end, range_key = _parse_range(request.args)
    con = _con()
    try:
        # Sessions that have visits in range but no matching lead.
        rows = con.execute("""
            SELECT v.session_id,
                   MAX(v.entered_at) AS last_seen,
                   COUNT(*) AS visit_count,
                   GROUP_CONCAT(DISTINCT v.page) AS pages,
                   MAX(v.ip) AS ip,
                   MAX(v.user_agent) AS ua,
                   MAX(v.device) AS device,
                   MAX(v.utm_source) AS utm_source,
                   MAX(v.utm_campaign) AS utm_campaign
            FROM mothersday_visits v
            LEFT JOIN mothersday_leads l ON l.session_id = v.session_id
            WHERE v.entered_at >= ? AND v.entered_at <= ?
              AND l.id IS NULL
            GROUP BY v.session_id
            ORDER BY last_seen DESC
        """, (start, end)).fetchall()
    finally:
        con.close()

    sessions = []
    page_order = {p: i for i, (p, _) in enumerate(FUNNEL_PAGES)}
    for r in rows:
        pages = (r[3] or "").split(",")
        furthest = max([page_order.get(p, -1) for p in pages], default=-1)
        furthest_page = FUNNEL_PAGES[furthest][1] if furthest >= 0 else "—"
        sessions.append({
            "session_id": r[0],
            "last_seen": r[1],
            "visits": r[2],
            "pages": pages,
            "furthest_page": furthest_page,
            "ip": r[4] or "—",
            "ua": (r[5] or "")[:80] + ("…" if r[5] and len(r[5]) > 80 else ""),
            "device": r[6] or "—",
            "utm_source": r[7] or "—",
            "utm_campaign": r[8] or "—",
        })

    return render_template(
        "admin_sce_abandoned.html",
        active="abandoned",
        offer_id="mothersday",
        offer_name="Mother's Day",
        sessions=sessions,
        range_key=range_key,
    )
