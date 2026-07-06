"""
Lumen — Contractor Marketing Efficiency Score (MES) quiz funnel.

Lead-gen quiz for US home-service contractors (Midwest focus):
  GET  /score                → quiz landing (single-page funnel, JS-driven steps)
  GET  /marketing-score      → alias for ad links
  POST /score/submit         → validate answers, compute score server-side,
                               save lead (own table + main CRM `leads` table),
                               email scorecard to the lead + notify Kendall,
                               return the full results payload as JSON

Scoring lives here (not in JS) so the on-page results and the emailed
scorecard always agree, and so the score can't be spoofed client-side.

Benchmark percentiles are modeled — a normal curve per trade calibrated
against published home-services industry data (speed-to-lead, review-count
and follow-up studies). They're framed on-page as benchmarks, not as a live
leaderboard.
"""

import os
import math
import sqlite3
import datetime
import threading
import requests
from flask import Blueprint, render_template, request, jsonify

lumen_quiz_bp = Blueprint("lumen_quiz", __name__)

# ─────────────────── Configuration ───────────────────

RESEND_API_KEY = os.environ.get("RESEND_API_KEY", "")
NOTIFY_EMAIL = "kendall@lumenmarketing.co"
FROM_EMAIL = "Lumen <kendall@lumenmarketing.co>"
CALENDLY_URL = "https://calendly.com/lumenmarketingco/lumen"

DATA_DIR = "/data" if os.path.isdir("/data") else os.path.dirname(__file__)
DB_PATH = os.path.join(DATA_DIR, "waitlist.db")


# ─────────────────── Quiz definition ───────────────────
# 5 pillars × 2 questions × 10 pts = 100 max.
# Option ids are what the frontend posts; points stay server-side.

PILLARS = {
    "leadgen":    "Lead Generation",
    "speed":      "Speed to Lead",
    "followup":   "Follow-Up",
    "reputation": "Reputation",
    "tracking":   "Tracking & Numbers",
}

QUESTIONS = {
    "source": {
        "pillar": "leadgen",
        "label": "Where do most of your new customers come from?",
        "options": {
            "wom":      {"label": "Word of mouth / referrals only",        "pts": 2},
            "leadsite": {"label": "Angi, HomeAdvisor, Thumbtack",           "pts": 4},
            "google":   {"label": "Google search & Maps",                   "pts": 7},
            "ads":      {"label": "Ads we run ourselves",                   "pts": 9},
            "multi":    {"label": "Several channels, consistently",         "pts": 10},
        },
    },
    "pipeline": {
        "pillar": "leadgen",
        "label": "How would you describe your job pipeline?",
        "options": {
            "famine":  {"label": "Feast or famine — big swings",                    "pts": 1},
            "seasonal":{"label": "Mostly steady, but slow seasons hurt",            "pts": 5},
            "booked":  {"label": "Booked out 2–4 weeks, consistently",              "pts": 8},
            "control": {"label": "Booked out, and I can turn up volume on demand",  "pts": 10},
        },
    },
    "response": {
        "pillar": "speed",
        "label": "A new lead calls or fills out a form. How fast does someone respond?",
        "options": {
            "5min":   {"label": "Under 5 minutes, every time",  "pts": 10},
            "hour":   {"label": "Within the hour",              "pts": 7},
            "sameday":{"label": "Same day, usually",            "pts": 4},
            "later":  {"label": "When I get a chance",          "pts": 1},
        },
    },
    "afterhours": {
        "pillar": "speed",
        "label": "What happens when someone calls after hours or while you're on a roof / under a sink?",
        "options": {
            "vm":      {"label": "Goes to voicemail",                          "pts": 2},
            "callback":{"label": "Voicemail, I call back when I can",          "pts": 4},
            "team":    {"label": "A team member or answering service picks up","pts": 7},
            "auto":    {"label": "Automatic text-back / online booking",       "pts": 10},
        },
    },
    "quotes": {
        "pillar": "followup",
        "label": "You send a quote and hear nothing back. What happens next?",
        "options": {
            "moveon": {"label": "I move on to the next job",             "pts": 1},
            "once":   {"label": "I follow up once or twice",             "pts": 5},
            "until":  {"label": "I follow up until I get a yes or no",   "pts": 8},
            "auto":   {"label": "Automated follow-up does it for me",    "pts": 10},
        },
    },
    "pastcust": {
        "pillar": "followup",
        "label": "Your past customers — what do you do with them?",
        "options": {
            "nothing": {"label": "Nothing, honestly",                                  "pts": 1},
            "phone":   {"label": "Their numbers are in my phone somewhere",            "pts": 4},
            "list":    {"label": "I keep a list / CRM but rarely use it",              "pts": 6},
            "market":  {"label": "I actively work the list — repeat, referral, reviews","pts": 10},
        },
    },
    "reviews": {
        "pillar": "reputation",
        "label": "How many Google reviews does your business have?",
        "options": {
            "u10":  {"label": "Under 10",   "pts": 2},
            "u50":  {"label": "10 – 49",    "pts": 5},
            "u100": {"label": "50 – 99",    "pts": 8},
            "o100": {"label": "100+",       "pts": 10},
        },
    },
    "askreview": {
        "pillar": "reputation",
        "label": "How do reviews actually get asked for?",
        "options": {
            "never":  {"label": "We don't really ask",                       "pts": 1},
            "somet":  {"label": "When I remember to",                        "pts": 4},
            "always": {"label": "We ask every happy customer",               "pts": 7},
            "auto":   {"label": "Automated review requests after every job", "pts": 10},
        },
    },
    "cac": {
        "pillar": "tracking",
        "label": "Do you know what it costs you to land a new customer?",
        "options": {
            "noidea": {"label": "No idea",                        "pts": 1},
            "rough":  {"label": "A rough guess",                  "pts": 5},
            "channel":{"label": "Roughly, per channel",           "pts": 8},
            "exact":  {"label": "Yes — I track it exactly",       "pts": 10},
        },
    },
    "attribution": {
        "pillar": "tracking",
        "label": "Do you know where every job came from?",
        "options": {
            "no":   {"label": "Not really",                       "pts": 2},
            "ask":  {"label": "I ask customers sometimes",        "pts": 5},
            "most": {"label": "I track most of them",             "pts": 8},
            "all":  {"label": "Every job is tagged to a source",  "pts": 10},
        },
    },
}

# Profile questions (not scored — personalization + lead qualification)

# "noun" is the in-sentence form: "…of {noun} contractors in Ohio"
TRADES = {
    "hvac":       {"label": "HVAC",                  "noun": "HVAC",         "avg": 52, "cust": "a homeowner with a dead furnace"},
    "roofing":    {"label": "Roofing",               "noun": "roofing",      "avg": 53, "cust": "a homeowner with storm damage"},
    "plumbing":   {"label": "Plumbing",              "noun": "plumbing",     "avg": 51, "cust": "a homeowner with a burst pipe"},
    "electrical": {"label": "Electrical",            "noun": "electrical",   "avg": 50, "cust": "a homeowner who needs an electrician"},
    "remodel":    {"label": "Remodeling",            "noun": "remodeling",   "avg": 49, "cust": "a homeowner planning a remodel"},
    "landscape":  {"label": "Landscaping / Lawn",    "noun": "landscaping",  "avg": 46, "cust": "a homeowner comparing lawn quotes"},
    "concrete":   {"label": "Concrete / Paving",     "noun": "concrete",     "avg": 47, "cust": "a homeowner pricing a new driveway"},
    "painting":   {"label": "Painting",              "noun": "painting",     "avg": 47, "cust": "a homeowner collecting paint bids"},
    "garage":     {"label": "Garage Doors",          "noun": "garage door",  "avg": 50, "cust": "a homeowner with a stuck garage door"},
    "cleaning":   {"label": "Cleaning / Restoration","noun": "restoration",  "avg": 48, "cust": "a homeowner with water damage"},
    "other":      {"label": "Other Home Services",   "noun": "home-service", "avg": 49, "cust": "a homeowner who needs your service"},
}
BENCH_SD = 14.0  # spread of the modeled benchmark curve

STATES = {
    "OH": "Ohio", "MI": "Michigan", "IN": "Indiana", "IL": "Illinois",
    "WI": "Wisconsin", "MN": "Minnesota", "IA": "Iowa", "MO": "Missouri",
    "KS": "Kansas", "NE": "Nebraska", "SD": "South Dakota", "ND": "North Dakota",
    "OTHER": "your area",
}

REVENUE_BANDS = {
    "u20":   {"label": "Under $20K / mo",   "mid": 12000},
    "20_50": {"label": "$20K – $50K / mo",  "mid": 35000},
    "50_100":{"label": "$50K – $100K / mo", "mid": 75000},
    "100_250":{"label": "$100K – $250K / mo","mid": 175000},
    "o250":  {"label": "$250K+ / mo",       "mid": 350000},
}

# Grade tiers by total score
GRADES = [
    (85, "A", "Elite",      "Your marketing runs like a machine. The gap between you and everyone else is your moat."),
    (70, "B", "Strong",     "You're ahead of most contractors in your market — but you're still leaving jobs on the table."),
    (55, "C", "Average",    "You're running with the pack. Average is where good contractors stay stuck for years."),
    (40, "D", "Leaking",    "Work is coming in, but your system is leaking jobs at almost every stage."),
    (0,  "F", "Critical",   "Right now, your growth depends on luck and referrals. That ceiling is lower than your skill level."),
]

# Diagnosis copy for the weakest pillar. {cust} = trade customer phrase.
PILLAR_DIAGNOSIS = {
    "leadgen": {
        "title": "Your biggest leak: Lead Generation",
        "body": ("Your work is good enough to grow on referrals — but referrals don't scale, and they dry up "
                 "exactly when you need them most. When {cust} searches for help, your competitors show up "
                 "and you don't. You're not losing jobs because of your work. You're losing them because "
                 "nobody sees you first."),
        "stat": "Contractors who own a predictable lead channel grow 2–3x faster than referral-only shops.",
    },
    "speed": {
        "title": "Your biggest leak: Speed to Lead",
        "body": ("When {cust} reaches out, they're calling your competitors too — and studies consistently show "
                 "the majority of these jobs go to whoever responds first. Every voicemail, every 'I'll call "
                 "them back tonight' is a job that quietly went to the other guy. This is the most expensive "
                 "leak a contractor can have, and the fastest one to fix."),
        "stat": "Responding within 5 minutes makes you up to 21x more likely to win the job than responding in an hour.",
    },
    "followup": {
        "title": "Your biggest leak: Follow-Up",
        "body": ("Most of the money you're losing isn't in leads you never got — it's in quotes you already sent. "
                 "When {cust} goes quiet, it's rarely a no. They got busy, got overwhelmed, or are waiting to see "
                 "who cares enough to check in. Right now, nobody does. Your quoted-but-unclosed pipe is almost "
                 "certainly your cheapest source of new revenue."),
        "stat": "Roughly 8 in 10 sales take 5+ follow-ups. Most contractors stop after one.",
    },
    "reputation": {
        "title": "Your biggest leak: Reputation",
        "body": ("Before {cust} calls anyone, they compare Google profiles — review count, recency, and stars. "
                 "If a competitor has 150 reviews and you have 20, you lose the job before the phone ever rings, "
                 "no matter how good your work is. Reviews are the Midwest handshake at internet scale, and "
                 "right now yours is costing you jobs silently."),
        "stat": "9 in 10 homeowners read reviews before contacting a contractor — most won't call past the top 3 profiles.",
    },
    "tracking": {
        "title": "Your biggest leak: Tracking & Numbers",
        "body": ("You can't fix what you can't see. Without knowing what a customer costs you and where each job "
                 "comes from, every marketing dollar is a guess — you'll keep feeding channels that don't work "
                 "and starving the ones that do. The contractors pulling away in your market aren't smarter. "
                 "They just know their numbers, so every decision compounds."),
        "stat": "Contractors who track cost-per-job typically cut acquisition costs 20–30% without spending more.",
    },
}


# ─────────────────── DB ───────────────────

def init_quiz_db():
    """Idempotent DB setup for quiz leads."""
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS quiz_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT DEFAULT '',
            company TEXT DEFAULT '',
            email TEXT NOT NULL,
            phone TEXT DEFAULT '',
            trade TEXT DEFAULT '',
            state TEXT DEFAULT '',
            revenue TEXT DEFAULT '',
            answers_json TEXT DEFAULT '',
            score INTEGER DEFAULT 0,
            grade TEXT DEFAULT '',
            percentile INTEGER DEFAULT 0,
            weakest_pillar TEXT DEFAULT '',
            ip TEXT DEFAULT '',
            user_agent TEXT DEFAULT '',
            referrer TEXT DEFAULT '',
            utm_source TEXT DEFAULT '',
            utm_medium TEXT DEFAULT '',
            utm_campaign TEXT DEFAULT '',
            utm_content TEXT DEFAULT '',
            utm_term TEXT DEFAULT '',
            created_at TEXT NOT NULL
        )
    """)
    con.execute("CREATE INDEX IF NOT EXISTS idx_quiz_leads_email ON quiz_leads(email)")
    con.commit()
    con.close()


# ─────────────────── Scoring ───────────────────

def compute_results(answers, trade_id, state_id, revenue_id):
    """Score the answers and build the full results payload."""
    trade = TRADES.get(trade_id, TRADES["other"])
    state_name = STATES.get(state_id, "your area")

    pillar_pts = {p: 0 for p in PILLARS}
    pillar_max = {p: 0 for p in PILLARS}
    for qid, q in QUESTIONS.items():
        qmax = max(o["pts"] for o in q["options"].values())
        pillar_max[q["pillar"]] += qmax
        opt = q["options"].get(answers.get(qid, ""))
        if opt:
            pillar_pts[q["pillar"]] += opt["pts"]

    score = sum(pillar_pts.values())  # max 100

    for cutoff, letter, tier, blurb in GRADES:
        if score >= cutoff:
            grade_letter, grade_tier, grade_blurb = letter, tier, blurb
            break

    # Percentile vs modeled trade benchmark (normal curve), clamped so the
    # result page never claims 0th or 100th percentile.
    z = (score - trade["avg"]) / (BENCH_SD * math.sqrt(2))
    percentile = round(50 * (1 + math.erf(z)))
    percentile = max(3, min(97, percentile))

    pillars = []
    for pid, pname in PILLARS.items():
        pct = round(100 * pillar_pts[pid] / pillar_max[pid]) if pillar_max[pid] else 0
        pillars.append({"id": pid, "name": pname, "pts": pillar_pts[pid],
                        "max": pillar_max[pid], "pct": pct})

    weakest = min(pillars, key=lambda p: p["pct"])
    diag = PILLAR_DIAGNOSIS[weakest["id"]]
    diagnosis = {
        "pillar": weakest["id"],
        "title": diag["title"],
        "body": diag["body"].format(cust=trade["cust"]),
        "stat": diag["stat"],
    }

    # Estimated annual revenue left on the table. Deliberately conservative:
    # inefficiency share of annual revenue, scaled by how low the score is.
    leak = None
    band = REVENUE_BANDS.get(revenue_id)
    if band and score < 90:
        annual = band["mid"] * 12
        center = annual * (100 - score) / 100 * 0.18
        low = int(round(center * 0.75 / 1000)) * 1000
        high = int(round(center * 1.35 / 1000)) * 1000
        if low >= 5000:
            leak = {"low": low, "high": high}

    return {
        "score": score,
        "grade": grade_letter,
        "tier": grade_tier,
        "blurb": grade_blurb,
        "percentile": percentile,
        "benchmark_avg": trade["avg"],
        "top25": min(97, trade["avg"] + 10),
        "pillars": pillars,
        "diagnosis": diagnosis,
        "leak": leak,
        "trade_label": trade["label"],
        "trade_noun": trade["noun"],
        "state_name": state_name,
    }


# ─────────────────── Email ───────────────────

def _send_resend(to, subject, html, reply_to=None):
    if not RESEND_API_KEY:
        print("[Lumen quiz email] RESEND_API_KEY not set, skipping")
        return
    payload = {"from": FROM_EMAIL, "to": to if isinstance(to, list) else [to],
               "subject": subject, "html": html}
    if reply_to:
        payload["reply_to"] = reply_to

    def _send():
        try:
            r = requests.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {RESEND_API_KEY}",
                         "Content-Type": "application/json"},
                json=payload, timeout=8,
            )
            if r.status_code >= 300:
                print(f"[Lumen quiz email] {r.status_code}: {r.text[:300]}")
        except Exception as e:
            print(f"[Lumen quiz email] exception: {e}")
    threading.Thread(target=_send, daemon=True).start()


def _bar_color(pct):
    if pct >= 75:
        return "#22c55e"
    if pct >= 50:
        return "#eab308"
    return "#ef4444"


def _scorecard_email_html(first_name, results):
    """Branded scorecard email sent to the lead."""
    pillar_rows = ""
    for p in results["pillars"]:
        color = _bar_color(p["pct"])
        pillar_rows += f"""
        <tr>
          <td style="padding:10px 0 4px 0;font-size:13px;font-weight:600;color:#e8e8f0;">{p['name']}
            <span style="float:right;color:#8b8ba0;font-weight:500;">{p['pts']}/{p['max']}</span></td>
        </tr>
        <tr>
          <td style="padding:0 0 8px 0;">
            <div style="background:#1a1a25;border-radius:99px;height:8px;overflow:hidden;">
              <div style="background:{color};height:8px;width:{max(4, p['pct'])}%;border-radius:99px;"></div>
            </div>
          </td>
        </tr>"""

    leak_block = ""
    if results["leak"]:
        leak_block = f"""
        <div style="background:rgba(239,68,68,0.07);border:1px solid rgba(239,68,68,0.25);border-radius:12px;padding:20px 22px;margin:0 0 24px 0;">
          <div style="font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#f87171;margin-bottom:8px;">Estimated Annual Leak</div>
          <div style="font-size:26px;font-weight:800;color:#fff;letter-spacing:-0.5px;">${results['leak']['low']:,} – ${results['leak']['high']:,}</div>
          <p style="font-size:13px;line-height:1.6;color:#b8b8c8;margin:8px 0 0 0;">Revenue contractors at your level typically leave on the table each year with a score of {results['score']}. Estimate based on your revenue range and score.</p>
        </div>"""

    return f"""
    <div style="font-family:Inter,-apple-system,sans-serif;background:#0a0a0f;padding:40px 20px;color:#e8e8f0;">
      <div style="max-width:580px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:40px 32px;">
        <div style="display:flex;align-items:center;gap:10px;margin-bottom:28px;">
          <div style="width:10px;height:10px;background:#6128DB;border-radius:50%;"></div>
          <div style="font-size:12px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#44445a;">Lumen &middot; Marketing Efficiency Score</div>
        </div>
        <h1 style="font-size:26px;font-weight:800;letter-spacing:-1px;line-height:1.25;margin:0 0 8px 0;color:#f0f0f5;">Your score: {results['score']}/100 &middot; Grade {results['grade']}</h1>
        <p style="font-size:15px;line-height:1.65;color:#b8b8c8;margin:0 0 24px 0;">{first_name}, you scored higher than <strong style="color:#fff;">{results['percentile']}%</strong> of {results['trade_noun']} contractors benchmarked in {results['state_name']}. The average is {results['benchmark_avg']} — the top 25% start around {results['top25']}.</p>

        <div style="background:rgba(97,40,219,0.08);border:1px solid rgba(97,40,219,0.3);border-radius:12px;padding:20px 22px;margin:0 0 24px 0;">
          <div style="font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#7c4dff;margin-bottom:8px;">{results['diagnosis']['title']}</div>
          <p style="font-size:14px;line-height:1.65;color:#e8e8f0;margin:0 0 10px 0;">{results['diagnosis']['body']}</p>
          <p style="font-size:12px;line-height:1.5;color:#8b8ba0;margin:0;font-style:italic;">{results['diagnosis']['stat']}</p>
        </div>

        {leak_block}

        <div style="font-size:11px;font-weight:700;letter-spacing:2px;text-transform:uppercase;color:#44445a;margin:0 0 4px 0;">Your Breakdown</div>
        <table style="width:100%;border-collapse:collapse;margin-bottom:28px;">{pillar_rows}</table>

        <p style="font-size:15px;line-height:1.65;color:#b8b8c8;margin:0 0 22px 0;">Every leak in this scorecard is a system problem, not an effort problem — and systems can be built. That's what we do: ads that bring the leads, and the follow-up machine that closes them, installed in 10 days.</p>
        <a href="{CALENDLY_URL}" style="display:inline-block;background:#6128DB;color:#fff;font-size:15px;font-weight:700;padding:16px 30px;border-radius:10px;text-decoration:none;letter-spacing:0.3px;">Book your free strategy call &rarr;</a>
        <p style="margin-top:36px;font-size:13px;color:#6b6b80;line-height:1.55;">Reply to this email if you have questions about your score. I read everything.<br>Kendall &middot; Lumen</p>
      </div>
      <p style="max-width:580px;margin:16px auto 0;font-size:11px;color:#44445a;line-height:1.5;text-align:center;">Benchmarks modeled from published home-services industry data. Lumen Marketing &middot; lumenmarketing.co</p>
    </div>
    """


def _notify_email_html(lead, results, answer_labels):
    rows = ""
    fields = [
        ("Company", lead["company"]),
        ("Trade", results["trade_label"]),
        ("State", results["state_name"]),
        ("Revenue", lead["revenue_label"]),
        ("Score", f"{results['score']}/100 · Grade {results['grade']} ({results['tier']})"),
        ("Percentile", f"Top {100 - results['percentile']}%"),
        ("Weakest Pillar", PILLARS[results["diagnosis"]["pillar"]]),
    ] + answer_labels
    for k, v in fields:
        rows += f"""<tr><td style="padding:10px 0;border-bottom:1px solid #1a1a25;font-size:11px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:#7c4dff;width:38%;vertical-align:top;">{k}</td><td style="padding:10px 0 10px 16px;border-bottom:1px solid #1a1a25;color:#e8e8f0;font-size:13px;line-height:1.5;">{v}</td></tr>"""
    return f"""
    <div style="font-family:Inter,-apple-system,sans-serif;background:#0a0a0f;padding:32px 20px;color:#e8e8f0;">
      <div style="max-width:620px;margin:0 auto;background:#111118;border:1px solid #1a1a25;border-radius:14px;padding:32px;">
        <div style="font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;color:#7c4dff;margin-bottom:10px;">New Quiz Lead &middot; Contractor MES</div>
        <h2 style="font-size:22px;font-weight:700;margin:0 0 6px 0;color:#fff;">{lead['name'] or 'Anonymous'} scored {results['score']}/100</h2>
        <p style="font-size:13px;color:#8b8ba0;margin:0 0 24px 0;">{lead['email']} &middot; {lead['phone']}</p>
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
      </div>
    </div>
    """


# ─────────────────── Routes ───────────────────

@lumen_quiz_bp.route("/score")
@lumen_quiz_bp.route("/marketing-score")
def quiz_page():
    return render_template(
        "lumen_quiz.html",
        questions=QUESTIONS,
        trades=TRADES,
        states=STATES,
        revenue_bands=REVENUE_BANDS,
        calendly_url=CALENDLY_URL,
    )


@lumen_quiz_bp.route("/score/submit", methods=["POST"])
def quiz_submit():
    data = request.get_json(silent=True) or {}

    name = (data.get("name") or "").strip()[:120]
    company = (data.get("company") or "").strip()[:160]
    email = (data.get("email") or "").strip().lower()[:200]
    phone = (data.get("phone") or "").strip()[:40]
    trade_id = (data.get("trade") or "other").strip()
    state_id = (data.get("state") or "OTHER").strip()
    revenue_id = (data.get("revenue") or "").strip()
    answers = data.get("answers") or {}

    if not name or not company or not email or not phone:
        return jsonify({"ok": False, "error": "Missing contact info"}), 400
    if "@" not in email or "." not in email.split("@")[-1]:
        return jsonify({"ok": False, "error": "Invalid email"}), 400
    # Every scored question must have a valid option id
    for qid, q in QUESTIONS.items():
        if answers.get(qid) not in q["options"]:
            return jsonify({"ok": False, "error": "Incomplete quiz"}), 400

    results = compute_results(answers, trade_id, state_id, revenue_id)
    revenue_label = REVENUE_BANDS.get(revenue_id, {}).get("label", "Not shared")
    now = datetime.datetime.utcnow().isoformat()

    import json as _json
    answers_json = _json.dumps(answers)

    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("""
            INSERT INTO quiz_leads (
                name, company, email, phone, trade, state, revenue,
                answers_json, score, grade, percentile, weakest_pillar,
                ip, user_agent, referrer,
                utm_source, utm_medium, utm_campaign, utm_content, utm_term,
                created_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            name, company, email, phone, trade_id, state_id, revenue_label,
            answers_json, results["score"], results["grade"],
            results["percentile"], results["diagnosis"]["pillar"],
            request.headers.get("X-Forwarded-For", request.remote_addr or "")[:100],
            (request.headers.get("User-Agent") or "")[:300],
            (data.get("referrer") or "")[:300],
            (data.get("utm_source") or "")[:100],
            (data.get("utm_medium") or "")[:100],
            (data.get("utm_campaign") or "")[:150],
            (data.get("utm_content") or "")[:150],
            (data.get("utm_term") or "")[:150],
            now,
        ))
        # Mirror into the main CRM leads table so it shows up in /admin/crm
        con.execute("""
            INSERT INTO leads (
                name, email, phone, business, revenue, marketing, challenge,
                stage, source, tags, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            name, email, phone, company, revenue_label,
            QUESTIONS["source"]["options"].get(answers.get("source"), {}).get("label", ""),
            PILLARS[results["diagnosis"]["pillar"]],
            "new", "contractor-quiz",
            f"quiz,score:{results['score']},trade:{trade_id},state:{state_id}",
            now, now,
        ))
        con.commit()
        con.close()
    except Exception as e:
        print(f"[Lumen quiz] DB error: {e}")

    first_name = name.split(" ")[0] if name else "there"

    # Scorecard to the lead
    _send_resend(
        email,
        f"Your Marketing Efficiency Score: {results['score']}/100",
        _scorecard_email_html(first_name, results),
    )

    # Internal notification
    answer_labels = [
        (q["label"][:60], q["options"].get(answers.get(qid), {}).get("label", "—"))
        for qid, q in QUESTIONS.items()
    ]
    _send_resend(
        NOTIFY_EMAIL,
        f"Quiz Lead: {name} — {company[:50]} — {results['score']}/100",
        _notify_email_html(
            {"name": name, "company": company, "email": email,
             "phone": phone, "revenue_label": revenue_label},
            results, answer_labels,
        ),
        reply_to=email,
    )

    return jsonify({"ok": True, "results": results})
