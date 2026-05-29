"""
MK7 Outreach Agent — Daily Scrapers (Lead Generation Module).

Each active scraper wakes once per UTC day, rotates through configured
country × industry × region combos, calls Claude Sonnet 4.6 with the
web_search tool to find verified business leads with public emails,
filters them (MX-check, dedup against past audiences + global suppressions),
creates a new audience, and auto-launches a campaign with the scraper's
chosen template.

Routes are added to the existing outreach_bp blueprint (mounted at /crm/outreach).
The scheduler thread starts on module import (alongside the send worker in
outreach.py); each scraper run blocks the scheduler briefly, then it sleeps
5 minutes between checks. Single gunicorn worker assumed (same constraint
as outreach.py — see project memory).

Cost (per 100-lead run, Sonnet 4.6 + web_search):
  input ~5k tokens   = $0.015
  output ~30k tokens = $0.45
  cached input read  = effectively free after first run (~3k tokens at 0.30/M)
  web searches ~30   = $0.30
  TOTAL              ≈ $0.75 / run
  10 runs/day        ≈ $7.50/day = $225/month at 1k/day target
"""

from flask import render_template, request, jsonify, abort
import os, datetime, json, re, threading, time, random
import sqlite3

from crm import db, current_user, admin_required, now_iso, get_setting, set_setting
from outreach import (
    outreach_bp,
    valid_email, norm_email,
    add_suppression, is_suppressed,
    refresh_audience_count,
    _queue_campaign_sends,
)


# ── Config ────────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = os.environ.get("OUTREACH_SCRAPER_MODEL", "claude-sonnet-4-6")
# Cap web searches per call — keeps cost predictable on a misbehaving run.
WEB_SEARCH_MAX_USES = int(os.environ.get("OUTREACH_SCRAPER_WEB_SEARCH_MAX", "40"))
# Server-side tool loop hits 10 iterations → pause_turn; we let it resume N times.
PAUSE_TURN_MAX_RESUMES = 4

# Defaults the user can override per-scraper. Keep these tight — the scraper
# rotates one entry per day so big lists mean longer cycles before repeat.
DEFAULT_COUNTRIES = [
    "USA", "Canada", "UK", "Australia",
    "Lebanon", "UAE", "Saudi Arabia",
]
DEFAULT_INDUSTRIES = [
    "salons and med spas",
    "home services (HVAC, plumbing, roofing)",
    "fitness studios and pilates",
    "auto detailers and mobile car washes",
    "boutique retail and specialty stores",
    "restaurants and cafes",
    "real estate brokerages",
]
DEFAULT_REGIONS = {
    "USA": ["California", "Texas", "Florida", "New York", "Illinois", "Arizona", "Georgia", "Pennsylvania", "North Carolina", "Washington"],
    "Canada": ["Ontario", "British Columbia", "Quebec", "Alberta"],
    "UK": ["England", "Scotland", "Wales"],
    "Australia": ["New South Wales", "Victoria", "Queensland", "Western Australia"],
    "Lebanon": [],
    "UAE": ["Dubai", "Abu Dhabi"],
    "Saudi Arabia": ["Riyadh", "Jeddah"],
}

# Domain-warmup curve for the brand-new go.lumenmarketing.co subdomain.
# Index = days since first send; value = max sends queueable that UTC day.
# After the curve ends (~day 10), per-scraper daily_target rules.
WARMUP_CURVE = [50, 100, 200, 350, 500, 650, 800, 1000, 1000, 1000]


# ── The prompt ────────────────────────────────────────────────────────────────
# Adapted from Kendall's MK7 Setter Lead List Generator prompt. Three changes:
#   1. Email is REQUIRED (was optional). Drop leads without a verified email.
#   2. No setter-name input — system runs autonomously, no interactive Qs.
#   3. Output is JSON, not CSV (easier for code to parse + validate).
SCRAPER_SYSTEM_PROMPT = """You are the MK7 Outreach Agent — Lead Generation Module.

Your job is to produce verified cold-EMAIL outreach leads for MK7 Media. Every lead must be a real business with a real, publicly-listed email address.

## What counts as a verified lead (STRICT — do not bend this)

Every lead row must satisfy ALL of these:

1. **Publicly-listed email — REQUIRED.** Found on the business's own website (contact page, about page, footer), Google Business listing, or directly visible in search result snippets. Do not fabricate, infer, or guess email addresses. Do not invent patterns like firstname@company.com. If you cannot find a verified, publicly-listed email, DROP the lead. **Better to ship 60 verified leads than 100 with guessed addresses.**
2. **Real Instagram @handle** — confirmed via web search returning the instagram.com/handle page, or linked from the business's own website.
3. **Real phone number** — listed on the business's own website, Google Business, Yelp, or BBB. Do not guess area codes.
4. **Working website** — must resolve. Booking pages (Booksy, Vagaro, GlossGenius, StyleSeat) count for service businesses if that's how they operate.
5. **Active business** — not closed, not "coming soon," not a defunct account, no dormant IG (no posts in 6+ months).
6. **Small/independent.** Exclude national chains, large franchises, and anything with 10+ locations. Independent owner-operators only.

## Chain exclusion list (varies by country)

- **USA/Canada:** Drybar, Blo, Salon Lofts, European Wax Center, Sport Clips, Great Clips, F45, OrangeTheory, Club Pilates, Pure Barre, HOTWORX, Ideal Image, LaserAway, Merry Maids, Cleaning Authority, Stanley Steemer, Orkin, Terminix, Roto-Rooter, CertaPro.
- **UK:** Toni & Guy, Rush, Regis, Treatwell-owned chains, Sk:n, The Gym Group, PureGym franchise locations.
- **Australia:** Just Cuts, Endota Spa multi-location, Anytime Fitness franchise locations.
- **UAE/GCC:** Tips & Toes large chain locations, Nstyle franchise locations.
- **General rule:** if the brand has 10+ locations or operates as a franchise, exclude it.

## Instagram follower sweet spot

Aim for **500–3,000 followers**. Acceptable range: 300–5,000. Drop accounts with 10K+ (too big, won't engage with cold outreach) or under 100 (likely inactive). When you can't verify exact count, prioritize signals of "small independent operator": single location, owner-named, boutique branding, neighborhood focus, posted in the last few months.

## Research process

For each lead, use web search to:

1. Find candidate businesses by category + city (e.g., "best independent hair salons Pasadena CA contact email").
2. Visit the business's website (contact / about pages) and extract a verified email.
3. Verify the Instagram handle resolves and the account is active.
4. Cross-check phone on Yelp or Google Business.

**Search for emails first.** Many small businesses don't publish them on their website — those are dead ends for email outreach, don't waste time. Move on to candidates that DO have public emails.

Work in geographic clusters — pick 4-6 cities/metros within the requested region and pull 15-25 verified leads per cluster until you hit the batch target.

## Output

When the user asks for N leads in {country} ({industry}, optionally in {region}), use web search to gather them, then return **ONLY a JSON array** — no prose, no markdown code fences, no preamble, no closing remarks. The next system parses your output directly with `json.loads()`.

Schema (exact field names, in this order):

```
[
  {
    "country": "<country>",
    "region": "<full state/province/region name, no abbreviations>",
    "city": "<primary city>",
    "category": "<specific business type, capitalized: 'Hair Salon', 'Med Spa', 'HVAC', 'Pilates Studio', etc.>",
    "business_name": "<as the business writes it on its own site/IG>",
    "first_name": "<owner first name if found, else null>",
    "last_name": "<owner last name if found, else null>",
    "email": "<verified, publicly-listed email>",
    "phone": "<formatted per country: USA/Canada '(XXX) XXX-XXXX'; international '+CC X XXXX XXXX'>",
    "website": "<full URL with https://>",
    "instagram": "@<handle, lowercase, no URL>",
    "address": "<full street address if listed; city-only acceptable>",
    "notes": "<factual context only: '~1,200 IG followers; family-owned since 2014; downtown'. NO outreach angles, no pitch language.>"
  }
]
```

Rules:
- If a non-email field cannot be verified, use `null` (not empty string, not "N/A").
- NEVER fabricate any field. Blank > guess.
- NEVER include leads without verified emails. Drop them silently and ship a shorter list.
- Phone format depends on country: USA/Canada `(XXX) XXX-XXXX`; UK `+44 XX XXXX XXXX`; UAE `+971 X XXX XXXX`; Lebanon `+961 X XXX XXX`; Australia `+61 X XXXX XXXX`.
- Region values: USA = full state name ("California", not "CA"). Canada = full province ("Ontario"). UK = country within UK ("England") or county. Other countries = state/emirate/region.
- For Lebanon, Singapore, or other single-region requests, use the country name as the region.
- Your entire output must be ONLY the JSON array — no text before `[`, no text after `]`.

## Tone

Direct, factual. Skip pleasantries. Get the research done, return the JSON."""


# ── DB schema ─────────────────────────────────────────────────────────────────
def init_scraper_db():
    """Idempotent. Called on import."""
    con = db()
    con.executescript("""
    CREATE TABLE IF NOT EXISTS outreach_scrapers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        description TEXT,
        -- JSON arrays / dict. NULL means "use defaults".
        countries_json TEXT,
        industries_json TEXT,
        regions_json TEXT,           -- {country: [regions]}; missing key → no region
        daily_target INTEGER NOT NULL DEFAULT 100,
        send_window_hours REAL NOT NULL DEFAULT 4.0,
        schedule_hour_utc INTEGER NOT NULL DEFAULT 13,  -- 13 UTC ≈ 9am ET
        template_id INTEGER REFERENCES outreach_templates(id),
        status TEXT NOT NULL DEFAULT 'paused',  -- active | paused
        run_counter INTEGER NOT NULL DEFAULT 0,
        last_run_at TEXT,
        last_run_date TEXT,           -- YYYY-MM-DD UTC — for once-per-day gating
        total_leads_found INTEGER NOT NULL DEFAULT 0,
        total_leads_used INTEGER NOT NULL DEFAULT 0,    -- after dedup/MX/suppress
        total_cost_usd REAL NOT NULL DEFAULT 0,
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS outreach_scraper_runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scraper_id INTEGER NOT NULL REFERENCES outreach_scrapers(id) ON DELETE CASCADE,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        status TEXT NOT NULL,         -- running | completed | failed
        country TEXT,
        industry TEXT,
        region TEXT,
        leads_returned INTEGER DEFAULT 0,     -- raw count from Claude
        leads_valid INTEGER DEFAULT 0,        -- after email+MX validation
        leads_added INTEGER DEFAULT 0,        -- after dedup + suppression
        audience_id INTEGER,
        campaign_id INTEGER,
        input_tokens INTEGER DEFAULT 0,
        output_tokens INTEGER DEFAULT 0,
        cache_read_tokens INTEGER DEFAULT 0,
        cache_write_tokens INTEGER DEFAULT 0,
        web_searches INTEGER DEFAULT 0,
        cost_estimate_usd REAL DEFAULT 0,
        error TEXT,
        triggered_by TEXT NOT NULL DEFAULT 'scheduler'  -- scheduler | manual
    );
    CREATE INDEX IF NOT EXISTS idx_runs_scraper ON outreach_scraper_runs(scraper_id);
    CREATE INDEX IF NOT EXISTS idx_runs_started ON outreach_scraper_runs(started_at);
    """)
    con.commit()
    con.close()


# ── Helpers ───────────────────────────────────────────────────────────────────
def _json_load(s, default):
    if not s:
        return default
    try:
        return json.loads(s)
    except Exception:
        return default


def _today_utc():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d")


def warmup_cap_for_today(con):
    """Returns the max sends queueable across all campaigns today based on the
    domain warmup curve. None = no cap (past day 10 of the curve)."""
    row = con.execute(
        "SELECT MIN(sent_at) FROM outreach_sends WHERE sent_at IS NOT NULL"
    ).fetchone()
    if not row or not row[0]:
        return WARMUP_CURVE[0]
    try:
        first = datetime.datetime.fromisoformat(row[0])
    except Exception:
        return WARMUP_CURVE[0]
    days = (datetime.datetime.utcnow() - first).days
    if days >= len(WARMUP_CURVE):
        return None
    return WARMUP_CURVE[max(days, 0)]


def sends_queued_or_sent_today(con):
    """How many outreach_sends are scheduled to fire today (UTC)? Used to
    enforce the warmup cap across all scrapers."""
    start = _today_utc() + "T00:00:00"
    end = _today_utc() + "T23:59:59"
    row = con.execute(
        "SELECT COUNT(*) FROM outreach_sends WHERE scheduled_at >= ? AND scheduled_at <= ? "
        "AND status IN ('queued', 'sending', 'sent')",
        (start, end)
    ).fetchone()
    return row[0] or 0


def has_mx_record(domain, timeout=2.5):
    """Cheap deliverability sniff. Drop emails on domains with no MX record —
    they'd just bounce, hurt our sender reputation, and waste Resend quota.
    Falls open (returns True) on lookup failure so transient DNS issues don't
    nuke an entire batch."""
    if not domain:
        return False
    try:
        import dns.resolver
        dns.resolver.resolve(domain, "MX", lifetime=timeout)
        return True
    except ImportError:
        # dnspython not installed locally — skip the check rather than fail.
        return True
    except Exception:
        return False


def extract_domain(email):
    if not email or "@" not in email:
        return ""
    return email.split("@", 1)[1].strip().lower()


def email_already_in_history(con, email):
    """Has this email been a member of any past outreach audience? Prevents
    re-emailing the same lead from a future scraper run."""
    e = norm_email(email)
    row = con.execute(
        "SELECT 1 FROM outreach_audience_members WHERE email = ? LIMIT 1",
        (e,)
    ).fetchone()
    return bool(row)


def pick_combo(scraper):
    """Round-robin (country, industry, region) selector. Independent rotation
    on country and industry means we cycle through every combination over
    ~len(countries) × len(industries) days before repeating exactly."""
    countries = _json_load(scraper["countries_json"], DEFAULT_COUNTRIES)
    industries = _json_load(scraper["industries_json"], DEFAULT_INDUSTRIES)
    regions_dict = _json_load(scraper["regions_json"], DEFAULT_REGIONS)
    if not countries or not industries:
        return None, None, None
    n = scraper["run_counter"] or 0
    country = countries[n % len(countries)]
    industry = industries[n % len(industries)]
    regions_for_country = regions_dict.get(country, [])
    region = random.choice(regions_for_country) if regions_for_country else ""
    return country, industry, region


# ── Claude call ───────────────────────────────────────────────────────────────
def _anthropic_client():
    """Lazy import + cached singleton so missing-key envs don't crash boot."""
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY not set — scraper disabled")
    import anthropic
    return anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)


def _estimate_cost(usage):
    """Sonnet 4.6 pricing as of 2026: $3/M input, $15/M output, $0.30/M cache
    read, $3.75/M cache write (1.25x base). Web search ~$10 per 1000 searches.
    Returns USD."""
    in_tok = getattr(usage, "input_tokens", 0) or 0
    out_tok = getattr(usage, "output_tokens", 0) or 0
    cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    stu = getattr(usage, "server_tool_use", None)
    searches = getattr(stu, "web_search_requests", 0) if stu else 0
    cost = (
        in_tok * 3.0 / 1_000_000
        + out_tok * 15.0 / 1_000_000
        + cache_read * 0.30 / 1_000_000
        + cache_write * 3.75 / 1_000_000
        + searches * 0.01
    )
    return round(cost, 4), searches


def _parse_lead_json(raw_text):
    """Claude may wrap output in ```json fences or add stray prose despite
    instructions. Extract the first JSON array and parse it. Returns ([], err)
    on failure."""
    if not raw_text:
        return [], "empty response"
    # Strip markdown fences if present.
    s = raw_text.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```\s*$", "", s)
    # Try direct parse first.
    try:
        data = json.loads(s)
        if isinstance(data, list):
            return data, None
    except Exception:
        pass
    # Fallback: find the outermost [...] block.
    start = s.find("[")
    end = s.rfind("]")
    if start == -1 or end == -1 or end <= start:
        return [], "no JSON array found in response"
    try:
        data = json.loads(s[start:end + 1])
        if isinstance(data, list):
            return data, None
        return [], "JSON parsed but root is not a list"
    except json.JSONDecodeError as e:
        return [], f"JSON decode: {e}"


def call_claude_for_leads(country, industry, region, target):
    """Single Claude call with cached system prompt + web_search tool.
    Handles pause_turn resume (server-side tool loop hit 10 iterations).
    Returns (raw_leads, usage, error)."""
    client = _anthropic_client()

    region_clause = f" in {region}" if region else ""
    user_msg = (
        f"Find {target} verified cold-email outreach leads in {country}{region_clause}, "
        f"in the {industry} category. Follow your system instructions exactly. "
        f"Return ONLY the JSON array — no prose, no markdown fences."
    )
    messages = [{"role": "user", "content": user_msg}]

    # Use streaming to dodge the SDK's non-streaming timeout guard for large
    # max_tokens. For a single response with web_search the call typically
    # finishes in 1-3 minutes; streaming costs us nothing extra and lets us
    # use a generous max_tokens for the JSON output.
    last_response = None
    for attempt in range(PAUSE_TURN_MAX_RESUMES + 1):
        with client.messages.stream(
            model=CLAUDE_MODEL,
            max_tokens=48000,
            system=[{
                "type": "text",
                "text": SCRAPER_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }],
            tools=[{
                "type": "web_search_20260209",
                "name": "web_search",
                "max_uses": WEB_SEARCH_MAX_USES,
            }],
            messages=messages,
        ) as stream:
            last_response = stream.get_final_message()

        if last_response.stop_reason != "pause_turn":
            break
        # Server-side loop hit 10 iterations; resume by echoing the assistant
        # turn back. Do NOT add a new user message — the API detects the
        # trailing server_tool_use block and resumes automatically.
        messages = messages + [{"role": "assistant", "content": last_response.content}]
    else:
        return [], (last_response.usage if last_response else None), \
               f"hit pause_turn cap ({PAUSE_TURN_MAX_RESUMES})"

    # Concatenate every text block in the final message — the JSON might be
    # split across multiple blocks if the model emitted text + tool_use mixed.
    full_text = ""
    for block in last_response.content:
        if getattr(block, "type", "") == "text":
            full_text += block.text

    raw_leads, err = _parse_lead_json(full_text)
    if err:
        return [], last_response.usage, err
    return raw_leads, last_response.usage, None


# ── Lead validation ───────────────────────────────────────────────────────────
def validate_and_dedupe(con, raw_leads):
    """Apply our own validation on top of Claude's: regex email, MX check,
    suppression list, in-DB history dedupe. Returns (kept_members, stats)."""
    kept = []
    seen_in_batch = set()
    stats = {"invalid_email": 0, "no_mx": 0, "suppressed": 0, "duped": 0}
    # MX cache so we only DNS-lookup each domain once per batch.
    mx_cache = {}
    for lead in raw_leads:
        if not isinstance(lead, dict):
            stats["invalid_email"] += 1
            continue
        email = norm_email(lead.get("email") or "")
        if not valid_email(email):
            stats["invalid_email"] += 1
            continue
        if email in seen_in_batch:
            stats["duped"] += 1
            continue
        if is_suppressed(con, email):
            stats["suppressed"] += 1
            continue
        if email_already_in_history(con, email):
            stats["duped"] += 1
            continue
        domain = extract_domain(email)
        if domain not in mx_cache:
            mx_cache[domain] = has_mx_record(domain)
        if not mx_cache[domain]:
            stats["no_mx"] += 1
            continue
        seen_in_batch.add(email)
        kept.append({
            "email": email,
            "first_name": (lead.get("first_name") or "").strip() or None,
            "last_name": (lead.get("last_name") or "").strip() or None,
            "company": (lead.get("business_name") or "").strip() or None,
            # Stash everything else (country/region/city/category/phone/website/IG/etc.)
            # as JSON so merge tags can reach it later if templates want.
            "extras": {k: v for k, v in lead.items()
                       if k not in ("email", "first_name", "last_name", "business_name")
                       and v not in (None, "", "null")},
        })
    return kept, stats


# ── The actual scraper run ────────────────────────────────────────────────────
def execute_scraper_run(scraper_id, triggered_by="scheduler"):
    """One end-to-end scrape: pick combo → call Claude → validate → audience →
    campaign. Always logs an outreach_scraper_runs row (success or failure)."""
    con = db()
    scraper = con.execute("SELECT * FROM outreach_scrapers WHERE id = ?", (scraper_id,)).fetchone()
    if not scraper:
        con.close()
        return None

    # Warmup-aware target: lower of scraper.daily_target and remaining headroom
    # under today's curve cap.
    cap = warmup_cap_for_today(con)
    used_today = sends_queued_or_sent_today(con)
    target = scraper["daily_target"]
    if cap is not None:
        headroom = max(cap - used_today, 0)
        target = min(target, headroom)
    if target <= 0:
        # Warmup cap already hit by other scrapers / campaigns today.
        cur = con.cursor()
        cur.execute("""
            INSERT INTO outreach_scraper_runs
                (scraper_id, started_at, completed_at, status, error, triggered_by)
            VALUES (?, ?, ?, 'failed', ?, ?)
        """, (scraper_id, now_iso(), now_iso(),
              f"warmup cap reached (used={used_today}, cap={cap})", triggered_by))
        con.commit()
        con.close()
        return cur.lastrowid

    country, industry, region = pick_combo(scraper)
    if not country:
        cur = con.cursor()
        cur.execute("""INSERT INTO outreach_scraper_runs
            (scraper_id, started_at, completed_at, status, error, triggered_by)
            VALUES (?, ?, ?, 'failed', 'no countries/industries configured', ?)
        """, (scraper_id, now_iso(), now_iso(), triggered_by))
        con.commit()
        con.close()
        return cur.lastrowid

    # Log the run as 'running' first so it shows up in the UI immediately.
    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_scraper_runs
            (scraper_id, started_at, status, country, industry, region, triggered_by)
        VALUES (?, ?, 'running', ?, ?, ?, ?)
    """, (scraper_id, now_iso(), country, industry, region, triggered_by))
    run_id = cur.lastrowid
    con.commit()
    con.close()

    error_msg = None
    audience_id = None
    campaign_id = None
    leads_returned = 0
    valid_members = []
    val_stats = {}
    usage = None
    cost = 0
    web_searches = 0

    try:
        raw_leads, usage, err = call_claude_for_leads(country, industry, region, target)
        leads_returned = len(raw_leads)
        if err:
            error_msg = f"Claude: {err}"
        elif raw_leads:
            con = db()
            valid_members, val_stats = validate_and_dedupe(con, raw_leads)
            con.close()

        if usage:
            cost, web_searches = _estimate_cost(usage)

        # Create audience + campaign even if 0 valid leads — we want a record.
        if valid_members:
            con = db()
            cur = con.cursor()
            audience_name = f"Auto · {country} · {industry[:30]} · {_today_utc()}"
            cur.execute("""
                INSERT INTO outreach_audiences
                    (name, source, created_at)
                VALUES (?, 'derived', ?)
            """, (audience_name, now_iso()))
            audience_id = cur.lastrowid
            rows = []
            for m in valid_members:
                rows.append((
                    audience_id, m["email"], m["first_name"], m["last_name"], m["company"],
                    json.dumps(m["extras"]) if m["extras"] else None, now_iso(),
                ))
            cur.executemany("""
                INSERT OR IGNORE INTO outreach_audience_members
                    (audience_id, email, first_name, last_name, company, extra_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, rows)
            refresh_audience_count(con, audience_id)
            con.commit()

            # Auto-launch campaign with the scraper's chosen template.
            scraper_row = con.execute(
                "SELECT * FROM outreach_scrapers WHERE id = ?", (scraper_id,)
            ).fetchone()
            template_id = scraper_row["template_id"]
            if template_id:
                tmpl = con.execute("SELECT 1 FROM outreach_templates WHERE id = ?",
                                   (template_id,)).fetchone()
                if tmpl:
                    campaign_name = f"Auto · {scraper_row['name']} · {_today_utc()}"
                    cur.execute("""
                        INSERT INTO outreach_campaigns
                            (name, template_id, audience_id, status, send_window_hours,
                             started_at, created_at)
                        VALUES (?, ?, ?, 'sending', ?, ?, ?)
                    """, (campaign_name, template_id, audience_id,
                          scraper_row["send_window_hours"], now_iso(), now_iso()))
                    campaign_id = cur.lastrowid
                    queued = _queue_campaign_sends(
                        con, campaign_id, audience_id, scraper_row["send_window_hours"]
                    )
                    con.execute("UPDATE outreach_campaigns SET total_queued = ? WHERE id = ?",
                                (queued, campaign_id))
                    con.commit()
                else:
                    error_msg = "Scraper template was deleted; audience created but no campaign launched"
            else:
                error_msg = "No template assigned to scraper; audience created but no campaign launched"
            con.close()
        elif not error_msg:
            error_msg = "0 leads passed validation"

    except Exception as e:
        error_msg = f"run error: {e}"

    # Finalize the run row + scraper totals.
    con = db()
    con.execute("""
        UPDATE outreach_scraper_runs SET
            completed_at = ?,
            status = ?,
            leads_returned = ?,
            leads_valid = ?,
            leads_added = ?,
            audience_id = ?,
            campaign_id = ?,
            input_tokens = ?,
            output_tokens = ?,
            cache_read_tokens = ?,
            cache_write_tokens = ?,
            web_searches = ?,
            cost_estimate_usd = ?,
            error = ?
        WHERE id = ?
    """, (
        now_iso(),
        "completed" if (valid_members and not error_msg) else ("failed" if error_msg and not valid_members else "completed"),
        leads_returned,
        len(valid_members),
        len(valid_members) if audience_id else 0,
        audience_id,
        campaign_id,
        getattr(usage, "input_tokens", 0) if usage else 0,
        getattr(usage, "output_tokens", 0) if usage else 0,
        getattr(usage, "cache_read_input_tokens", 0) if usage else 0,
        getattr(usage, "cache_creation_input_tokens", 0) if usage else 0,
        web_searches,
        cost,
        error_msg,
        run_id,
    ))
    # Bump scraper run_counter + totals + last_run_at.
    con.execute("""
        UPDATE outreach_scrapers SET
            run_counter = run_counter + 1,
            last_run_at = ?,
            last_run_date = ?,
            total_leads_found = total_leads_found + ?,
            total_leads_used = total_leads_used + ?,
            total_cost_usd = total_cost_usd + ?
        WHERE id = ?
    """, (now_iso(), _today_utc(), leads_returned, len(valid_members), cost, scraper_id))
    con.commit()
    con.close()
    return run_id


# ── Scheduler thread ──────────────────────────────────────────────────────────
_scheduler_started = False
_scheduler_lock = threading.Lock()


def _scheduler_tick():
    """Once-per-5-min check: are any active scrapers due to run today? A scraper
    is due if status='active', last_run_date != today, and current UTC hour
    >= schedule_hour_utc."""
    con = db()
    now = datetime.datetime.utcnow()
    today = _today_utc()
    due = con.execute("""
        SELECT id, schedule_hour_utc, last_run_date FROM outreach_scrapers
        WHERE status = 'active'
    """).fetchall()
    con.close()
    for r in due:
        if r["last_run_date"] == today:
            continue
        if now.hour < r["schedule_hour_utc"]:
            continue
        try:
            execute_scraper_run(r["id"], triggered_by="scheduler")
        except Exception as e:
            print(f"[outreach-scrapers] scheduler error on scraper {r['id']}: {e}")


def _scheduler_loop():
    print(f"[outreach-scrapers] scheduler started (model={CLAUDE_MODEL})")
    # Quick stagger so we don't race the send worker on first boot.
    time.sleep(15)
    while True:
        try:
            _scheduler_tick()
        except Exception as e:
            print(f"[outreach-scrapers] scheduler tick error: {e}")
        time.sleep(300)  # 5 min between checks


def start_scheduler():
    global _scheduler_started
    with _scheduler_lock:
        if _scheduler_started:
            return
        if not ANTHROPIC_API_KEY:
            print("[outreach-scrapers] WARN: ANTHROPIC_API_KEY unset, scheduler not started")
            return
        t = threading.Thread(target=_scheduler_loop, daemon=True, name="outreach-scrapers")
        t.start()
        _scheduler_started = True


# ── Routes (attached to existing outreach_bp) ─────────────────────────────────
@outreach_bp.route("/scrapers")
@admin_required
def scrapers_list():
    u = current_user()
    con = db()
    rows = con.execute("""
        SELECT s.*, t.name AS template_name
        FROM outreach_scrapers s
        LEFT JOIN outreach_templates t ON t.id = s.template_id
        ORDER BY s.created_at DESC
    """).fetchall()
    last_runs = {}
    for r in rows:
        last = con.execute("""
            SELECT status, started_at, completed_at, error, country, industry,
                   leads_added, cost_estimate_usd
            FROM outreach_scraper_runs
            WHERE scraper_id = ?
            ORDER BY started_at DESC LIMIT 1
        """, (r["id"],)).fetchone()
        last_runs[r["id"]] = dict(last) if last else None
    cap = warmup_cap_for_today(con)
    used_today = sends_queued_or_sent_today(con)
    con.close()
    return render_template(
        "crm/outreach_scrapers.html",
        u=u, scrapers=rows, last_runs=last_runs,
        warmup_cap=cap, used_today=used_today,
        anthropic_configured=bool(ANTHROPIC_API_KEY),
    )


@outreach_bp.route("/scrapers/new")
@admin_required
def scraper_new():
    u = current_user()
    con = db()
    templates = con.execute(
        "SELECT id, name, subject FROM outreach_templates ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return render_template(
        "crm/outreach_scraper_edit.html",
        u=u, s=None, templates=templates,
        default_countries=", ".join(DEFAULT_COUNTRIES),
        default_industries="\n".join(DEFAULT_INDUSTRIES),
        default_regions_json=json.dumps(DEFAULT_REGIONS, indent=2),
    )


@outreach_bp.route("/scrapers/<int:scraper_id>/edit")
@admin_required
def scraper_edit(scraper_id):
    u = current_user()
    con = db()
    s = con.execute("SELECT * FROM outreach_scrapers WHERE id = ?", (scraper_id,)).fetchone()
    if not s:
        con.close(); abort(404)
    templates = con.execute(
        "SELECT id, name, subject FROM outreach_templates ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return render_template(
        "crm/outreach_scraper_edit.html",
        u=u, s=s, templates=templates,
        default_countries=", ".join(DEFAULT_COUNTRIES),
        default_industries="\n".join(DEFAULT_INDUSTRIES),
        default_regions_json=json.dumps(DEFAULT_REGIONS, indent=2),
    )


@outreach_bp.route("/scrapers/<int:scraper_id>")
@admin_required
def scraper_detail(scraper_id):
    u = current_user()
    con = db()
    s = con.execute("""
        SELECT s.*, t.name AS template_name
        FROM outreach_scrapers s
        LEFT JOIN outreach_templates t ON t.id = s.template_id
        WHERE s.id = ?
    """, (scraper_id,)).fetchone()
    if not s:
        con.close(); abort(404)
    runs = con.execute("""
        SELECT * FROM outreach_scraper_runs
        WHERE scraper_id = ?
        ORDER BY started_at DESC LIMIT 30
    """, (scraper_id,)).fetchall()
    templates = con.execute(
        "SELECT id, name FROM outreach_templates ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return render_template(
        "crm/outreach_scraper_detail.html",
        u=u, s=s, runs=runs, templates=templates,
        countries=_json_load(s["countries_json"], DEFAULT_COUNTRIES),
        industries=_json_load(s["industries_json"], DEFAULT_INDUSTRIES),
        regions=_json_load(s["regions_json"], DEFAULT_REGIONS),
    )


@outreach_bp.route("/api/scrapers", methods=["POST"])
@admin_required
def api_create_scraper():
    u = current_user()
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"ok": False, "error": "Name required"}), 400
    # Countries / industries: accept arrays OR comma/newline-separated strings.
    countries = data.get("countries")
    industries = data.get("industries")
    if isinstance(countries, str):
        countries = [c.strip() for c in re.split(r"[,\n]+", countries) if c.strip()]
    if isinstance(industries, str):
        industries = [i.strip() for i in re.split(r"[\n]+", industries) if i.strip()]
    regions = data.get("regions")
    if isinstance(regions, str):
        try:
            regions = json.loads(regions) if regions.strip() else {}
        except Exception:
            return jsonify({"ok": False, "error": "Regions JSON is malformed"}), 400
    con = db()
    cur = con.cursor()
    cur.execute("""
        INSERT INTO outreach_scrapers
            (name, description, countries_json, industries_json, regions_json,
             daily_target, send_window_hours, schedule_hour_utc, template_id, status,
             created_by_user_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        name, (data.get("description") or "").strip() or None,
        json.dumps(countries) if countries else None,
        json.dumps(industries) if industries else None,
        json.dumps(regions) if regions else None,
        int(data.get("daily_target") or 100),
        float(data.get("send_window_hours") or 4),
        int(data.get("schedule_hour_utc") or 13),
        data.get("template_id") or None,
        (data.get("status") or "paused").strip(),
        u["id"], now_iso(),
    ))
    sid = cur.lastrowid
    con.commit()
    con.close()
    return jsonify({"ok": True, "id": sid})


@outreach_bp.route("/api/scrapers/<int:scraper_id>", methods=["PATCH"])
@admin_required
def api_update_scraper(scraper_id):
    data = request.get_json(silent=True) or {}
    fields = {}
    for k in ("name", "description", "status"):
        if k in data:
            v = data[k]
            if isinstance(v, str):
                v = v.strip() or None
            fields[k] = v
    if "daily_target" in data:
        fields["daily_target"] = int(data["daily_target"])
    if "send_window_hours" in data:
        fields["send_window_hours"] = float(data["send_window_hours"])
    if "schedule_hour_utc" in data:
        fields["schedule_hour_utc"] = int(data["schedule_hour_utc"])
    if "template_id" in data:
        fields["template_id"] = data["template_id"] or None
    if "countries" in data:
        c = data["countries"]
        if isinstance(c, str):
            c = [x.strip() for x in re.split(r"[,\n]+", c) if x.strip()]
        fields["countries_json"] = json.dumps(c) if c else None
    if "industries" in data:
        i = data["industries"]
        if isinstance(i, str):
            i = [x.strip() for x in re.split(r"[\n]+", i) if x.strip()]
        fields["industries_json"] = json.dumps(i) if i else None
    if "regions" in data:
        r = data["regions"]
        if isinstance(r, str):
            try:
                r = json.loads(r) if r.strip() else {}
            except Exception:
                return jsonify({"ok": False, "error": "Regions JSON is malformed"}), 400
        fields["regions_json"] = json.dumps(r) if r else None
    if not fields:
        return jsonify({"ok": True, "noop": True})
    con = db()
    sets = ", ".join(f"{k} = ?" for k in fields)
    con.execute(f"UPDATE outreach_scrapers SET {sets} WHERE id = ?",
                list(fields.values()) + [scraper_id])
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/scrapers/<int:scraper_id>", methods=["DELETE"])
@admin_required
def api_delete_scraper(scraper_id):
    con = db()
    con.execute("DELETE FROM outreach_scrapers WHERE id = ?", (scraper_id,))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@outreach_bp.route("/api/scrapers/<int:scraper_id>/run", methods=["POST"])
@admin_required
def api_run_scraper_now(scraper_id):
    """Fire a run immediately, off the daily schedule. Useful for testing.
    Runs in a background thread so the request returns quickly — the run
    itself takes 1-3 minutes."""
    con = db()
    s = con.execute("SELECT id, status FROM outreach_scrapers WHERE id = ?",
                    (scraper_id,)).fetchone()
    con.close()
    if not s:
        abort(404)
    def _bg():
        try:
            execute_scraper_run(scraper_id, triggered_by="manual")
        except Exception as e:
            print(f"[outreach-scrapers] manual run error: {e}")
    threading.Thread(target=_bg, daemon=True).start()
    return jsonify({"ok": True, "queued": True})


# Init on import
init_scraper_db()
start_scheduler()
