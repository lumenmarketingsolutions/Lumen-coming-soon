"""
Primed Loans autonomous ad team.

Five roles run one "morning meeting" a day (08:00 Beirut): Analyst (Meta + GHL scorecard), Finance (budget and
status proposals), Creative Director (new variations, rendered from templates, uploaded PAUSED), CEO (challenges
the three, decides, writes Kendall's brief). Kendall runs the dashboard at /primed/admin.

Guardrails: Ezio's main account is READ ONLY. GHL is READ ONLY. Writes only go to the backup account and only to
the ad set / campaigns listed in rules. Money actions default to "approve" (queue for Kendall); creating paused ads
is automatic. Every write is logged.
"""
import os, json, re, time, sqlite3, threading, datetime, base64, io, traceback, secrets
import requests
from flask import Blueprint, request, session, redirect, url_for, render_template_string, jsonify, send_from_directory

primed_bp = Blueprint("primed_team", __name__)

# ---------------------------------------------------------------- config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("PRIMED_TEAM_DB", os.path.join(BASE_DIR, "primed_team.db"))
CREATIVE_DIR = os.path.join(BASE_DIR, "static", "primed_creatives")
FONT_DIR = os.path.join(BASE_DIR, "static", "fonts", "primed")
os.makedirs(CREATIVE_DIR, exist_ok=True)

META_TOKEN = os.environ.get("LUMEN_META_CAPI_TOKEN", "")
GHL_TOKEN = os.environ.get("PRIMED_GHL_TOKEN", "")
GHL_LOC = os.environ.get("PRIMED_GHL_LOCATION", "O3NExmJB4fQ5iKCFGMZj")
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
RESEND_KEY = os.environ.get("RESEND_API_KEY", "")
ADMIN_PASSWORD = os.environ.get("PRIMED_ADMIN_PASSWORD", "primed2026")
MODEL = os.environ.get("PRIMED_TEAM_MODEL", "claude-opus-5")
MEETING_HOUR = int(os.environ.get("PRIMED_TEAM_HOUR", "8"))  # Beirut
BRIEF_TO = [e.strip() for e in os.environ.get("PRIMED_BRIEF_TO", "kendallwdavis11@gmail.com,kendall@lumenmarketing.co").split(",")]

ACT_MAIN = "act_5422851404498967"      # Ezio's, read only
ACT_BACKUP = "act_1692097901857740"    # ours, guarded writes
PAGE_ID = os.environ.get("PRIMED_TEAM_PAGE_ID", "109574424155558")
GRAPH = "https://graph.facebook.com/v21.0"
GHL = "https://services.leadconnectorhq.com"
LAUNCH_DATE = "2026-09-01"
QUAL_TAGS = ("sent agreement", "awaiting offer", "offer presented", "funded", "funded new")
QUAL_RE = re.compile(r"sent agreement|awaiting offer|offer presented|offer accepted|offer received|funded", re.I)
LOST_FIELD = "ehzd7CHVdeQfLd8Zi3rK"

DEFAULT_RULES = {
    "goal": "Find the highest quality clients for Primed: judge every ad on cost per qualified lead and the revenue mix it attracts, never on CPL alone.",
    "write_adset_ids": "",              # comma separated ad set ids the team may create ads in / change budgets on
    "form_id": "",                       # the revenue-filtered lead form new ads use
    "daily_cap_backup": 300,             # CA$/day total across writable ad sets
    "max_budget_move_pct": 20,           # per ad set per day
    "min_spend_before_judgement": 400,   # CA$ per ad before kill
    "min_new_contacts": 30,              # GHL new contacts before a qualified-rate verdict
    "kill_qualified_pct": 20,
    "kill_cpl": 75,
    "scale_qualified_pct": 35,
    "scale_cpl": 60,
    "max_new_creatives_per_day": 3,
    "autopilot_budget": "off",           # off = queue for Kendall, on = execute
    "autopilot_status": "off",           # pause / activate ads
    "autopilot_create_ads": "on",        # new ads are always created PAUSED
    "kendall_notes": "RATE SHEET (from the funded book, 19.09.2026): repay factor 1.45 (client repays 1.45x), typical term 5 to 6 months, up to 12 to 18 months on larger files. Creatives show MONTHLY example figures on a 12 month term only, always labelled Example figures on a 12 month term, terms vary by file: $50,000 about $6,000 a month, $100,000 about $12,100 a month, $250,000 about $29,200 a month. The factor, the rate, any percentage, the total repaid and lender names are INTERNAL ONLY and never appear in any ad, image, headline or primary text. On the ad, only the monthly example figure with the example label. Never $800,000, never a weekly figure, never a payment without the example label. Kendall decides all money moves until autopilot is on.",  # standing instructions from Kendall to the team
}

# ---------------------------------------------------------------- db
def db():
    c = sqlite3.connect(DB_PATH, timeout=30)
    c.row_factory = sqlite3.Row
    return c

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, status TEXT, brief TEXT, transcript TEXT, error TEXT);
        CREATE TABLE IF NOT EXISTS scorecard(run_id INTEGER, ad_id TEXT, account TEXT, data TEXT, PRIMARY KEY(run_id, ad_id));
        CREATE TABLE IF NOT EXISTS proposals(id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, ts TEXT, kind TEXT, target_type TEXT, target_id TEXT, target_name TEXT, payload TEXT, reason TEXT, expected TEXT, status TEXT, decided_by TEXT, decided_at TEXT, result TEXT);
        CREATE TABLE IF NOT EXISTS ads_registry(ad_id TEXT PRIMARY KEY, name TEXT, account TEXT, adset_id TEXT, attributes TEXT, image TEXT, created_by TEXT, created_at TEXT);
        CREATE TABLE IF NOT EXISTS creatives(id INTEGER PRIMARY KEY AUTOINCREMENT, run_id INTEGER, code TEXT, name TEXT, spec TEXT, png TEXT, ad_id TEXT, status TEXT, ts TEXT);
        CREATE TABLE IF NOT EXISTS rules(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS ghl_cache(phone TEXT PRIMARY KEY, contact_id TEXT, data TEXT, ts TEXT);
        CREATE TABLE IF NOT EXISTS log(ts TEXT, level TEXT, msg TEXT);
        """)
        for k, v in DEFAULT_RULES.items():
            c.execute("INSERT OR IGNORE INTO rules VALUES(?,?)", (k, json.dumps(v)))

def rules():
    with db() as c:
        r = dict(DEFAULT_RULES)
        for row in c.execute("SELECT key,value FROM rules"):
            try: r[row["key"]] = json.loads(row["value"])
            except Exception: r[row["key"]] = row["value"]
        return r

def set_rule(k, v):
    with db() as c: c.execute("INSERT OR REPLACE INTO rules VALUES(?,?)", (k, json.dumps(v)))

def log(msg, level="info"):
    print(f"[primed-team] {msg}")
    try:
        with db() as c: c.execute("INSERT INTO log VALUES(?,?,?)", (now_iso(), level, str(msg)[:2000]))
    except Exception: pass

def now_iso(): return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

# ---------------------------------------------------------------- meta
def mget(path, **p):
    p["access_token"] = META_TOKEN
    for i in range(5):
        r = requests.get(f"{GRAPH}/{path}", params=p, timeout=120)
        if r.status_code in (429, 500, 502, 503): time.sleep(3 * 2 ** i); continue
        j = r.json()
        if "error" in j:
            code = j["error"].get("code")
            if code in (4, 17, 32, 613): time.sleep(20 * (i + 1)); continue
            raise RuntimeError(f"Meta GET {path}: {j['error'].get('message')}")
        return j
    raise RuntimeError(f"Meta GET {path}: retries exhausted")

def mpaged(path, **p):
    out, j = [], mget(path, **p)
    while True:
        out += j.get("data", [])
        nxt = (j.get("paging") or {}).get("next")
        if not nxt: return out
        j = requests.get(nxt, timeout=120).json()
        if "error" in j: return out

def mpost(path, data=None, files=None):
    """Guarded write. Only the backup account and only ids inside the writable scope."""
    if not path.startswith(ACT_BACKUP) and not _writable_object(path):
        raise PermissionError(f"write refused outside writable scope: {path}")
    data = dict(data or {}); data["access_token"] = META_TOKEN
    r = requests.post(f"{GRAPH}/{path}", data=data, files=files, timeout=180)
    j = r.json()
    if "error" in j: raise RuntimeError(f"Meta POST {path}: {j['error'].get('message')}")
    log(f"META WRITE {path} {json.dumps({k: v for k, v in data.items() if k != 'access_token'})[:300]} -> {j}")
    return j

def _writable_object(obj_id):
    """Ad set ids in rules, and ads inside them, are writable."""
    R = rules(); ids = {x.strip() for x in str(R.get("write_adset_ids", "")).split(",") if x.strip()}
    if obj_id in ids: return True
    with db() as c:
        row = c.execute("SELECT adset_id FROM ads_registry WHERE ad_id=?", (obj_id,)).fetchone()
    return bool(row and row["adset_id"] in ids)

def act(r, t="lead"):
    for a in r.get("actions", []) or []:
        if a["action_type"] == t: return float(a["value"])
    return 0.0

INS = "ad_id,ad_name,adset_id,adset_name,campaign_id,campaign_name,spend,impressions,clicks,reach,frequency,ctr,cpm,actions"

def pull_meta():
    out = {}
    for label, actid in (("MAIN", ACT_MAIN), ("BACKUP", ACT_BACKUP)):
        d = {}
        d["adsets"] = mpaged(f"{actid}/adsets", fields="id,name,status,effective_status,campaign_id,daily_budget,learning_stage_info", limit=200)
        d["ads"] = mpaged(f"{actid}/ads", fields="id,name,status,effective_status,adset_id,campaign_id,created_time,creative{title,body,image_url,thumbnail_url}", limit=300)
        for lab, pr in (("d1", {"date_preset": "yesterday"}), ("d7", {"date_preset": "last_7d"}), ("d14", {"date_preset": "last_14d"}),
                        ("launch", {"time_range": json.dumps({"since": LAUNCH_DATE, "until": datetime.date.today().isoformat()})})):
            d[lab] = mpaged(f"{actid}/insights", level="ad", fields=INS, limit=500, **pr)
        out[label] = d
    # form leads since launch (both forms + rules form) for the phone join
    pt = mget(PAGE_ID, fields="access_token").get("access_token")
    forms = {"1506542010608184", "1548218153233288"}
    if rules().get("form_id"): forms.add(str(rules()["form_id"]))
    leads = []
    for f in forms:
        url = f"{GRAPH}/{f}/leads"; params = {"fields": "id,created_time,ad_id,adset_id,campaign_id,form_id,field_data", "limit": 500, "access_token": pt}
        while True:
            j = requests.get(url, params=params, timeout=120).json()
            if "error" in j: break
            rows = j.get("data", []); leads += [r for r in rows if r["created_time"] >= LAUNCH_DATE]
            if rows and rows[-1]["created_time"] < LAUNCH_DATE: break
            nxt = (j.get("paging") or {}).get("next")
            if not nxt: break
            url, params = nxt, None
    out["form_leads"] = leads
    return out

# ---------------------------------------------------------------- ghl (read only)
GH = lambda: {"Authorization": f"Bearer {GHL_TOKEN}", "Version": "2021-07-28", "Content-Type": "application/json",
              "User-Agent": "Mozilla/5.0 (Macintosh) AppleWebKit/537.36 Chrome/120 Safari/537.36"}

def norm_phone(p):
    d = re.sub(r"\D", "", p or ""); return d[-10:] if len(d) >= 10 else d

def ghl_contact_by_phone(phone):
    for i in range(4):
        r = requests.post(f"{GHL}/contacts/search", headers=GH(), json={"locationId": GHL_LOC, "pageLimit": 3,
                          "filters": [{"field": "phone", "operator": "contains", "value": phone}]}, timeout=60)
        if r.status_code == 200:
            cs = r.json().get("contacts", [])
            return cs[0] if cs else None
        time.sleep(3)
    return None

def ghl_opps(contact_id):
    for i in range(4):
        r = requests.get(f"{GHL}/opportunities/search", headers=GH(), params={"location_id": GHL_LOC, "contact_id": contact_id, "limit": 20}, timeout=60)
        if r.status_code == 200: return r.json().get("opportunities", [])
        time.sleep(3)
    return []

STAGE_NAMES = {}
def stage_names():
    global STAGE_NAMES
    if STAGE_NAMES: return STAGE_NAMES
    r = requests.get(f"{GHL}/opportunities/pipelines", headers=GH(), params={"locationId": GHL_LOC}, timeout=60)
    if r.status_code == 200:
        for p in r.json().get("pipelines", []):
            for s in p["stages"]: STAGE_NAMES[s["id"]] = s["name"]
    return STAGE_NAMES

def score_contact(c, ops):
    sn = stage_names()
    stages = [sn.get(o["pipelineStageId"], "?") for o in ops]
    tags = [t.lower() for t in (c.get("tags") or [])]
    qual = any(QUAL_RE.search(s) for s in stages) or any(t in QUAL_TAGS for t in tags)
    worked = any(re.search(r"1st|2nd|3rd|call|agreement|awaiting|offer|funded|nurtur|not qualified", s, re.I) for s in stages) or any(t.startswith("wavv-") or t in ("1st call", "2nd call", "3rd call") for t in tags)
    lost = None
    for o in ops:
        for cf in o.get("customFields", []) or []:
            if cf.get("id") == LOST_FIELD: lost = cf.get("fieldValueString") or cf.get("fieldValue") or cf.get("value")
    nq = any("not qualified" in s.lower() for s in stages)
    terminal = qual or nq
    return {"contact_id": c["id"], "created": c.get("dateAdded"), "source": c.get("source"), "assigned": bool(c.get("assignedTo")),
            "stages": stages, "qualified": qual, "worked": worked, "nq": nq, "lost": lost, "terminal": terminal}

def join_ghl(form_leads):
    """Phone join of every form lead since launch to GHL, cached. Re-checks non terminal contacts each run."""
    rows = []
    with db() as c:
        cache = {r["phone"]: (r["contact_id"], json.loads(r["data"]) if r["data"] else None, r["ts"]) for r in c.execute("SELECT * FROM ghl_cache")}
    calls = 0
    for L in form_leads:
        fd = {x["name"]: (x.get("values") or [None])[0] for x in L.get("field_data", [])}
        ph = norm_phone(fd.get("phone_number"))
        if len(ph) < 10: continue
        rev = fd.get("what_was_your_business’s_total_revenue_last_year?") or fd.get("what_was_your_business’s_total_revenue_last_year?")
        row = {"lead_id": L["id"], "t": L["created_time"], "ad_id": L.get("ad_id"), "form": L.get("form_id"), "phone": ph, "rev": rev,
               "amt": fd.get("how_much_funding_are_you_looking_for?"), "soon": fd.get("how_soon_do_you_need_funding?")}
        cid, data, ts = cache.get(ph, (None, None, None))
        need = cid is None or data is None or (not data.get("terminal") and calls < 400)
        if need and calls < 450:
            calls += 1
            c_ = ghl_contact_by_phone(ph); time.sleep(0.15)
            if c_:
                created_before = (c_.get("dateAdded") or "") < L["created_time"][:10]
                ops = ghl_opps(c_["id"]); calls += 1; time.sleep(0.15)
                data = score_contact(c_, ops); data["existing_contact"] = created_before; cid = c_["id"]
            else:
                data = {"missing": True, "terminal": False}; cid = ""
            with db() as c:
                c.execute("INSERT OR REPLACE INTO ghl_cache VALUES(?,?,?,?)", (ph, cid, json.dumps(data), now_iso()))
        row["ghl"] = data or {"missing": True}
        rows.append(row)
    log(f"ghl join: {len(rows)} leads, {calls} api calls")
    return rows

# ---------------------------------------------------------------- scorecard
LOW_REV = ("under_$100k", "$100k–$250k")

def build_scorecard(meta, joined):
    ads = {}
    for acct in ("MAIN", "BACKUP"):
        d = meta[acct]
        adsets = {a["id"]: a for a in d["adsets"]}
        for a in d["ads"]:
            cr = a.get("creative") or {}
            ads[a["id"]] = {"ad_id": a["id"], "name": a["name"], "account": acct, "status": a["effective_status"], "adset_id": a["adset_id"],
                            "adset": adsets.get(a["adset_id"], {}).get("name"), "adset_budget": (float(adsets.get(a["adset_id"], {}).get("daily_budget") or 0)) / 100,
                            "campaign_id": a["campaign_id"], "created": a["created_time"][:10], "headline": cr.get("title"), "body": (cr.get("body") or "")[:200],
                            "thumb": cr.get("thumbnail_url")}
        for lab in ("d1", "d7", "d14", "launch"):
            for r in d[lab]:
                x = ads.setdefault(r["ad_id"], {"ad_id": r["ad_id"], "name": r["ad_name"], "account": acct, "status": "?", "adset_id": r["adset_id"], "adset": r["adset_name"]})
                x[lab] = {"spend": round(float(r["spend"]), 2), "leads": act(r), "ctr": float(r.get("ctr") or 0), "cpm": float(r.get("cpm") or 0),
                          "freq": float(r.get("frequency") or 0), "reach": int(r.get("reach") or 0), "impr": int(r.get("impressions") or 0)}
                x[lab]["cpl"] = round(x[lab]["spend"] / x[lab]["leads"], 2) if x[lab]["leads"] else None
    # GHL join per ad since launch
    for ad in ads.values(): ad["ghl"] = {"form_leads": 0, "existing": 0, "new": 0, "qualified": 0, "worked": 0, "nq": 0, "missing": 0, "low_rev": 0, "lost": {}, "rev": {}}
    for r in joined:
        ad = ads.get(r["ad_id"])
        if not ad: continue
        g = ad["ghl"]; g["form_leads"] += 1
        if r["rev"] in LOW_REV: g["low_rev"] += 1
        g["rev"][r["rev"] or "?"] = g["rev"].get(r["rev"] or "?", 0) + 1
        gh = r["ghl"]
        if gh.get("missing"): g["missing"] += 1; continue
        if gh.get("existing_contact"): g["existing"] += 1; continue
        g["new"] += 1; g["qualified"] += int(bool(gh.get("qualified"))); g["worked"] += int(bool(gh.get("worked"))); g["nq"] += int(bool(gh.get("nq")))
        if gh.get("lost"): g["lost"][gh["lost"]] = g["lost"].get(gh["lost"], 0) + 1
    for ad in ads.values():
        g = ad["ghl"]; sp = (ad.get("launch") or {}).get("spend", 0)
        g["qualified_pct"] = round(100 * g["qualified"] / g["new"], 1) if g["new"] else None
        g["cost_per_qualified"] = round(sp / g["qualified"], 0) if g["qualified"] else None
        g["existing_pct"] = round(100 * g["existing"] / g["form_leads"], 0) if g["form_leads"] else None
        g["low_rev_pct"] = round(100 * g["low_rev"] / g["form_leads"], 0) if g["form_leads"] else None
        g["unreachable"] = g["lost"].get("Unreachable", 0)
    # attach registry attributes
    with db() as c:
        for row in c.execute("SELECT ad_id, attributes FROM ads_registry"):
            if row["ad_id"] in ads:
                try: ads[row["ad_id"]]["attributes"] = json.loads(row["attributes"] or "{}")
                except Exception: pass
    # keep only ads with any spend since launch or active
    return {k: v for k, v in ads.items() if (v.get("launch") or {}).get("spend", 0) > 0 or v.get("status") == "ACTIVE"}

def adset_summary(meta):
    out = []
    for acct in ("MAIN", "BACKUP"):
        d = meta[acct]
        sp7 = {}; ld7 = {}; sp1 = {}; ld1 = {}
        for r in d["d7"]: sp7[r["adset_id"]] = sp7.get(r["adset_id"], 0) + float(r["spend"]); ld7[r["adset_id"]] = ld7.get(r["adset_id"], 0) + act(r)
        for r in d["d1"]: sp1[r["adset_id"]] = sp1.get(r["adset_id"], 0) + float(r["spend"]); ld1[r["adset_id"]] = ld1.get(r["adset_id"], 0) + act(r)
        for a in d["adsets"]:
            if a["effective_status"] != "ACTIVE" and a["id"] not in sp7: continue
            out.append({"account": acct, "adset_id": a["id"], "name": a["name"], "status": a["effective_status"], "daily_budget": (float(a.get("daily_budget") or 0)) / 100,
                        "learning": (a.get("learning_stage_info") or {}).get("status"), "spend7": round(sp7.get(a["id"], 0)), "leads7": ld7.get(a["id"], 0),
                        "spend1": round(sp1.get(a["id"], 0)), "leads1": ld1.get(a["id"], 0)})
    return out

# ---------------------------------------------------------------- llm
def llm(system, user, max_tokens=4000, json_mode=False):
    import anthropic
    client = anthropic.Anthropic(api_key=ANTHROPIC_KEY)
    if json_mode: system += "\n\nRespond with a single JSON object and nothing else. No markdown fences."
    for i in range(3):
        try:
            m = client.messages.create(model=MODEL, max_tokens=max_tokens, system=system, messages=[{"role": "user", "content": user}])
            txt = "".join(b.text for b in m.content if getattr(b, "type", "") == "text")
            if json_mode:
                txt = txt.strip()
                txt = re.sub(r"^```(?:json)?|```$", "", txt, flags=re.M).strip()
                return json.loads(txt)
            return txt
        except json.JSONDecodeError as e:
            if i == 2: raise
            user += "\n\nYour last answer was not valid JSON. Return only the JSON object."
        except Exception as e:
            if i == 2: raise
            time.sleep(5)

HOUSE_RULES = """House rules for every role:
- Primed Loans: Laval QC private lender, funds $10K to $300K (largest ever $300K, median about $20K), 24 to 72h, no collateral, weekly repayments 3 to 15 months, needs about $350K+ annual revenue. Meta lead-form ads only, Canada.
- The goal is the highest quality clients for Ezio. Rank by qualified rate and cost per qualified lead (a lead that reached Sent Agreement, Awaiting Offer, Offer Presented or Funded), never by CPL alone. The revenue answer on the form predicts everything: under $250K revenue almost never qualifies.
- Two ad accounts. MAIN is Ezio's and is READ ONLY: never propose a write to it, only observations. BACKUP is ours: writes are allowed only inside the writable ad sets in the rules.
- Never put the repayment factor, a rate, a percentage, a total repayment amount or a lender name in any ad. Payment appears only as a monthly example figure with the label Example figures on a 12 month term, terms vary by file.
- Never propose an ad that claims guaranteed approval, quotes a return, uses "no credit check" as the headline, says "cash advance", imitates a phone notification or checkbox UI, or carries $800,000. Revenue gates are phrased as eligibility about the business, not about the viewer.
- Copy voice: direct, human, specific numbers, no em dashes or hyphens, no staccato fragments, no "not X, it's Y" flips, no emojis, no AI words.
- Small samples: do not call a verdict on an ad with fewer than the minimum new contacts or minimum spend in the rules. Say "not enough data" instead.
- Write for Kendall (the human CEO). He is sharp and busy. Numbers, then the decision, then the why. Short paragraphs."""

def agent_analyst(scorecard, adsets, joined, R):
    sc = sorted(scorecard.values(), key=lambda a: -((a.get("launch") or {}).get("spend", 0)))[:40]
    compact = [{k: a.get(k) for k in ("ad_id", "name", "account", "status", "adset", "adset_budget", "d1", "d7", "d14", "launch", "ghl", "attributes")} for a in sc]
    # attribute rollup
    attr = {}
    for a in sc:
        for k, v in (a.get("attributes") or {}).items():
            key = f"{k}={v}"; t = attr.setdefault(key, {"ads": 0, "spend": 0, "new": 0, "qualified": 0, "form_leads": 0, "low_rev": 0})
            t["ads"] += 1; t["spend"] += (a.get("launch") or {}).get("spend", 0); g = a["ghl"]
            t["new"] += g["new"]; t["qualified"] += g["qualified"]; t["form_leads"] += g["form_leads"]; t["low_rev"] += g["low_rev"]
    sysm = HOUSE_RULES + "\n\nYou are the ANALYST. You read the numbers and say what is true, with confidence levels. You do not propose budgets or creatives; you tell the team what worked, what did not, why, and what is still unknown. Sample sizes matter."
    user = f"""Rules: {json.dumps({k: R[k] for k in ('min_spend_before_judgement','min_new_contacts','kill_qualified_pct','kill_cpl','scale_qualified_pct','scale_cpl')})}
Date: {datetime.date.today().isoformat()}. Leads since launch {LAUNCH_DATE}: {len(joined)} form submissions joined to GHL by phone.

AD SETS: {json.dumps(adsets)}

ADS (spend, leads, ctr, freq for yesterday d1 / 7d / 14d / since launch; ghl = GHL outcomes of the leads this ad produced since launch: form_leads, existing (already in GHL before the ad, i.e. re-submitters), new, qualified, worked, nq, low_rev (revenue answer under $250K), lost reasons, qualified_pct, cost_per_qualified): {json.dumps(compact)}

ATTRIBUTE ROLLUP (creative attributes across ads): {json.dumps(attr)}

Return JSON: {{"headline": one sentence, "winners": [{{"ad_id","name","why","confidence":"high|medium|low"}}], "losers": [...same...], "not_enough_data": [ad names], "attribute_learnings": [{{"attribute","finding","confidence"}}], "risks": [strings: fatigue, unreachable spikes, re-submitters, low revenue mix, learning phase], "questions_for_team": [max 3], "narrative": 6 to 10 short paragraphs for Kendall}}"""
    return llm(sysm, user, 5000, json_mode=True)

def agent_finance(analysis, adsets, scorecard, R):
    sysm = HOUSE_RULES + "\n\nYou are FINANCE, the media buyer. You turn the analyst's read into concrete budget and status proposals inside the rules. Every proposal names the exact object, the exact change, the reason and the expected effect. You never touch the MAIN account. You never move an ad set budget by more than max_budget_move_pct in a day and never push the backup total above daily_cap_backup. Ads below the minimums get 'hold', not 'kill'."
    writable = [x.strip() for x in str(R.get("write_adset_ids", "")).split(",") if x.strip()]
    ads_w = [{k: a.get(k) for k in ("ad_id", "name", "status", "adset_id", "d7", "launch", "ghl")} for a in scorecard.values() if a.get("adset_id") in writable]
    user = f"""Rules: {json.dumps({k: R[k] for k in ('daily_cap_backup','max_budget_move_pct','min_spend_before_judgement','min_new_contacts','kill_qualified_pct','kill_cpl','scale_qualified_pct','scale_cpl','kendall_notes')})}
Writable ad set ids: {writable}
AD SETS: {json.dumps(adsets)}
ADS IN WRITABLE AD SETS: {json.dumps(ads_w)}
ANALYST: {json.dumps(analysis)}

Return JSON: {{"summary": 2 sentences, "proposals": [{{"kind":"budget|pause_ad|activate_ad|pause_adset|hold|observation","target_type":"adset|ad|none","target_id":"","target_name":"","change":{{"daily_budget": number}} or {{"status":"PAUSED|ACTIVE"}} or {{}},"reason":"","expected":"","priority":1-3}}], "budget_plan": [{{"adset_id","name","current","proposed","why"}}], "main_account_observations": [strings, read only]}}"""
    return llm(sysm, user, 4000, json_mode=True)

def agent_creative(analysis, scorecard, R, registry):
    sysm = HOUSE_RULES + """

You are the CREATIVE DIRECTOR. You design the next static variations from the evidence, one variable changed per variation, so every result is readable. Two proven skins:
- PURPLE: field #7B4F9A, dark band #4B2A63 down the left, white Poppins type, yellow #F7C948 pill button, wordmark PRIMED LOANS bottom right. Lines: small regular line, huge bold number, regular line(s), hairline, small supporting lines, button label.
- BLACK: field #0B0B0B, cream #F3EBD8 condensed uppercase (Bebas Neue), 3 huge statement lines, hairline, 3 smaller lines, no button.
Also allowed: GREEN (#0F3D2E field, #082419 band, same layout as purple), NAVY (#0D1B2E, same layout as purple), CREAM (#F4EFE4 field, near black type, black button).
Every variation must be honest: real numbers only ($25,000 to $250,000, up to $300,000, $350K floor, 24 hours, same week), no payment figures unless Kendall's notes give a rate sheet. Attributes to tag on each: skin, anchor_number, filter (none|positive|negative), hook (amount|filter|bank_friction|scenario|time|objection|table|plain), language (en|fr), cta.
"""
    winners = [{"name": w["name"], "why": w["why"]} for w in analysis.get("winners", [])][:5]
    losers = [{"name": w["name"], "why": w["why"]} for w in analysis.get("losers", [])][:5]
    existing = [{"name": r["name"], "attributes": json.loads(r["attributes"] or "{}")} for r in registry][-40:]
    n = int(R.get("max_new_creatives_per_day", 3))
    user = f"""Kendall's notes: {R.get('kendall_notes','')}
Winners: {json.dumps(winners)}
Losers: {json.dumps(losers)}
Attribute learnings: {json.dumps(analysis.get('attribute_learnings', []))}
Ads already in market (do not duplicate): {json.dumps(existing)}

Design up to {n} new variations for today. For each, give the exact on-image lines for the template.
Return JSON: {{"rationale": 3 sentences, "variations": [{{"code":"V{datetime.date.today().strftime('%m%d')}-1","name":"short name","skin":"purple|black|green|navy|cream","hypothesis":"what one variable this tests and against which ad","attributes":{{"skin","anchor_number","filter","hook","language","cta"}},"lines":[{{"text":"","size":"small|medium|large|huge","weight":"regular|bold"}}],"button":"label or empty","primary_text":"","headline":""}}]}}"""
    return llm(sysm, user, 5000, json_mode=True)

def agent_ceo(analysis, finance, creative, R, adsets):
    sysm = HOUSE_RULES + "\n\nYou are the CEO, Kendall's proxy in the morning meeting. You have read the analyst, finance and the creative director. Challenge weak reasoning, resolve conflicts against the goal, and decide. You do not execute anything yourself: you route each finance proposal to 'queue' (Kendall approves), 'auto' (only if the rules' autopilot for that kind is on), or 'reject', and you approve or cut creative variations. Then you write Kendall's morning brief: what happened yesterday, what the team decided, what needs his hand, in under 350 words, plain and specific."
    user = f"""Rules: {json.dumps({k: R[k] for k in ('goal','autopilot_budget','autopilot_status','autopilot_create_ads','kendall_notes','daily_cap_backup')})}
AD SETS: {json.dumps(adsets)}
ANALYST: {json.dumps(analysis)}
FINANCE: {json.dumps(finance)}
CREATIVE: {json.dumps(creative)}

Return JSON: {{"challenges": [{{"to":"analyst|finance|creative","point":""}}], "decisions": [{{"proposal_index": int, "route":"queue|auto|reject", "note":""}}], "creative_decisions": [{{"code":"", "approve": true|false, "note":""}}], "brief_markdown": "", "needs_kendall": [strings], "tomorrow_focus": [strings]}}"""
    return llm(sysm, user, 4500, json_mode=True)

# ---------------------------------------------------------------- renderer (PIL)
SKINS = {
    "purple": {"bg": (123, 79, 154), "band": (75, 42, 99), "fg": (255, 255, 255), "btn": (247, 201, 72), "btn_fg": (20, 17, 8), "font": "Poppins", "upper": False},
    "green": {"bg": (15, 61, 46), "band": (8, 36, 25), "fg": (255, 255, 255), "btn": (247, 201, 72), "btn_fg": (20, 17, 8), "font": "Poppins", "upper": False},
    "navy": {"bg": (13, 27, 46), "band": None, "fg": (255, 255, 255), "btn": (247, 201, 72), "btn_fg": (20, 17, 8), "font": "Inter", "upper": False},
    "cream": {"bg": (244, 239, 228), "band": None, "fg": (22, 24, 29), "btn": (22, 24, 29), "btn_fg": (255, 255, 255), "font": "Inter", "upper": False},
    "black": {"bg": (11, 11, 11), "band": None, "fg": (243, 235, 216), "btn": None, "btn_fg": None, "font": "Bebas", "upper": True},
}
SIZES = {"small": 0.034, "medium": 0.048, "large": 0.072, "huge": 0.13}

def font(name, weight, px):
    from PIL import ImageFont
    files = {("Poppins", "regular"): "Poppins_wght_400.ttf", ("Poppins", "bold"): "Poppins_wght_800.ttf",
             ("Inter", "regular"): "Inter_wght_400.ttf", ("Inter", "bold"): "Inter_wght_700.ttf",
             ("Bebas", "regular"): "Bebas_Neue.ttf", ("Bebas", "bold"): "Bebas_Neue.ttf"}
    return ImageFont.truetype(os.path.join(FONT_DIR, files[(name, weight)]), int(px))

def render_card(spec, out_path, W=1080, H=1350):
    from PIL import Image, ImageDraw
    sk = SKINS.get(spec.get("skin", "purple"), SKINS["purple"])
    im = Image.new("RGB", (W, H), sk["bg"]); d = ImageDraw.Draw(im)
    if sk["band"]: d.rectangle([0, 0, int(W * 0.065), H], fill=sk["band"])
    x0 = int(W * 0.13) if sk["band"] else int(W * 0.09); maxw = W - x0 - int(W * 0.08)
    top = int(H * 0.10); bottom_reserved = int(H * 0.11) + (int(H * 0.11) if (spec.get("button") and sk["btn"]) else 0)
    avail = H - top - bottom_reserved
    lines = spec.get("lines", [])
    bebas = sk["font"] == "Bebas"
    def wrap(txt, f):
        words, out, cur = txt.split(), [], ""
        for w in words:
            t = (cur + " " + w).strip()
            if d.textlength(t, font=f) <= maxw: cur = t
            else:
                if cur: out.append(cur)
                cur = w
        if cur: out.append(cur)
        return out
    def layout(scale):
        blocks, y = [], 0
        for ln in lines:
            if ln.get("rule"): blocks.append(("rule", y)); y += int(36 * scale); continue
            txt = ln.get("text", "")
            if sk["upper"]: txt = txt.upper()
            px = H * SIZES.get(ln.get("size", "medium"), 0.048) * (1.25 if bebas else 1) * scale
            f = font(sk["font"], ln.get("weight", "regular"), px)
            rows = wrap(txt, f)
            # huge/large lines must not wrap: shrink this line until it fits on one row
            if ln.get("size") in ("huge", "large"):
                while len(rows) > 1 and px > 20:
                    px *= 0.94; f = font(sk["font"], ln.get("weight", "regular"), px); rows = wrap(txt, f)
            lh = int(px * (1.02 if bebas else 1.24))
            for r_ in rows: blocks.append(("text", y, r_, f)); y += lh
            y += int(px * 0.3)
        return blocks, y
    scale = 1.0
    blocks, total = layout(scale)
    while total > avail and scale > 0.45:
        scale -= 0.04; blocks, total = layout(scale)
    y0 = top
    for b in blocks:
        if b[0] == "rule": d.line([x0, y0 + b[1] + 8, x0 + maxw, y0 + b[1] + 8], fill=sk["fg"], width=2)
        else: d.text((x0, y0 + b[1]), b[2], font=b[3], fill=sk["fg"])
    y = y0 + total
    if spec.get("button") and sk["btn"]:
        f = font(sk["font"] if not bebas else "Inter", "bold", H * 0.034); tw = d.textlength(spec["button"], font=f)
        bx0, by0 = x0, y + 10; bx1, by1 = int(bx0 + tw + 80), int(by0 + H * 0.072)
        d.rounded_rectangle([bx0, by0, bx1, by1], radius=int(H * 0.04), fill=sk["btn"])
        d.text((bx0 + 40, by0 + (by1 - by0) / 2 - f.size * 0.62), spec["button"], font=f, fill=sk["btn_fg"])
    f = font("Inter", "bold", H * 0.02)
    wm = "PRIMED LOANS"; d.text((W - int(W * 0.08) - d.textlength(wm, font=f), H - int(H * 0.06)), wm, font=f, fill=sk["fg"])
    im.save(out_path, "PNG"); return out_path

# ---------------------------------------------------------------- meta creation
def create_paused_ad(png_path, name, primary_text, headline, adset_id, form_id):
    with open(png_path, "rb") as fh:
        img = mpost(f"{ACT_BACKUP}/adimages", files={"filename": (os.path.basename(png_path), fh, "image/png")})
    h = list(img["images"].values())[0]["hash"]
    cr = mpost(f"{ACT_BACKUP}/adcreatives", {"name": name, "object_story_spec": json.dumps({"page_id": PAGE_ID, "link_data": {
        "image_hash": h, "message": primary_text, "name": headline, "link": "http://fb.me/",
        "call_to_action": {"type": "APPLY_NOW", "value": {"lead_gen_form_id": form_id}}}})})
    ad = mpost(f"{ACT_BACKUP}/ads", {"name": name, "adset_id": adset_id, "creative": json.dumps({"creative_id": cr["id"]}), "status": "PAUSED"})
    return ad["id"]

def execute_proposal(pid, by="kendall"):
    with db() as c: p = c.execute("SELECT * FROM proposals WHERE id=?", (pid,)).fetchone()
    if not p: return "not found"
    payload = json.loads(p["payload"] or "{}"); res = ""
    try:
        if p["kind"] == "budget":
            R = rules(); cur = payload.get("current"); new = float(payload["daily_budget"])
            if cur and abs(new - cur) / max(cur, 1) > R["max_budget_move_pct"] / 100 + 1e-9:
                raise PermissionError(f"move exceeds {R['max_budget_move_pct']}% rule")
            mpost(p["target_id"], {"daily_budget": int(round(new * 100))}); res = f"budget set to {new}"
        elif p["kind"] in ("pause_ad", "activate_ad", "pause_adset"):
            mpost(p["target_id"], {"status": payload.get("status", "PAUSED")}); res = f"status {payload.get('status')}"
        else:
            res = "no-op"
        with db() as c: c.execute("UPDATE proposals SET status='executed', decided_by=?, decided_at=?, result=? WHERE id=?", (by, now_iso(), res, pid))
    except Exception as e:
        res = f"failed: {e}"
        with db() as c: c.execute("UPDATE proposals SET status='failed', decided_by=?, decided_at=?, result=? WHERE id=?", (by, now_iso(), res, pid))
    log(f"proposal {pid} -> {res}")
    return res

# ---------------------------------------------------------------- the meeting
_running = {"busy": False}

def run_meeting(trigger="scheduled"):
    if _running["busy"]: return "busy"
    _running["busy"] = True
    with db() as c:
        cur = c.execute("INSERT INTO runs(ts,status) VALUES(?,?)", (now_iso(), "running")); run_id = cur.lastrowid
    T = {"trigger": trigger}
    try:
        R = rules()
        meta = pull_meta(); T["pulled"] = {k: len(meta[k]["ads"]) for k in ("MAIN", "BACKUP")}; T["form_leads"] = len(meta["form_leads"])
        joined = join_ghl(meta["form_leads"])
        scorecard = build_scorecard(meta, joined); adsets = adset_summary(meta)
        with db() as c:
            for ad_id, a in scorecard.items():
                c.execute("INSERT OR REPLACE INTO scorecard VALUES(?,?,?,?)", (run_id, ad_id, a["account"], json.dumps(a)))
            registry = [dict(r) for r in c.execute("SELECT * FROM ads_registry")]
        analysis = agent_analyst(scorecard, adsets, joined, R); T["analyst"] = analysis
        finance = agent_finance(analysis, adsets, scorecard, R); T["finance"] = finance
        creative = agent_creative(analysis, scorecard, R, registry); T["creative"] = creative
        ceo = agent_ceo(analysis, finance, creative, R, adsets); T["ceo"] = ceo
        # proposals
        routes = {d.get("proposal_index"): d for d in ceo.get("decisions", [])}
        for i, pr in enumerate(finance.get("proposals", [])):
            dec = routes.get(i, {}); route = dec.get("route", "queue")
            if pr.get("kind") in ("hold", "observation") or pr.get("target_type") == "none": continue
            payload = dict(pr.get("change") or {})
            if pr["kind"] == "budget":
                cur = next((s["daily_budget"] for s in adsets if s["adset_id"] == pr.get("target_id")), None); payload["current"] = cur
            status = "rejected" if route == "reject" else "pending"
            with db() as c:
                cur_ = c.execute("INSERT INTO proposals(run_id,ts,kind,target_type,target_id,target_name,payload,reason,expected,status,decided_by,decided_at,result) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                 (run_id, now_iso(), pr["kind"], pr.get("target_type"), str(pr.get("target_id") or ""), pr.get("target_name"), json.dumps(payload), pr.get("reason"), pr.get("expected"), status, "ceo" if route == "reject" else None, now_iso() if route == "reject" else None, dec.get("note")))
                pid = cur_.lastrowid
            auto_ok = (pr["kind"] == "budget" and R.get("autopilot_budget") == "on") or (pr["kind"] in ("pause_ad", "activate_ad", "pause_adset") and R.get("autopilot_status") == "on")
            if route == "auto" and auto_ok: execute_proposal(pid, by="autopilot")
        # creatives
        approved = {d["code"] for d in ceo.get("creative_decisions", []) if d.get("approve")}
        writable = [x.strip() for x in str(R.get("write_adset_ids", "")).split(",") if x.strip()]
        for v in creative.get("variations", []):
            if v.get("code") not in approved: continue
            code = v["code"]; png = os.path.join(CREATIVE_DIR, f"{code}.png")
            try:
                render_card(v, png)
            except Exception as e:
                log(f"render failed {code}: {e}", "error"); continue
            ad_id, status = None, "rendered"
            if R.get("autopilot_create_ads") == "on" and writable and R.get("form_id"):
                try:
                    ad_id = create_paused_ad(png, f"PL-TEAM | {code} | {v.get('name','')}", v.get("primary_text", ""), v.get("headline", ""), writable[0], str(R["form_id"]))
                    status = "paused_in_meta"
                    with db() as c:
                        c.execute("INSERT OR REPLACE INTO ads_registry VALUES(?,?,?,?,?,?,?,?)", (ad_id, f"PL-TEAM | {code} | {v.get('name','')}", "BACKUP", writable[0], json.dumps(v.get("attributes", {})), f"/static/primed_creatives/{code}.png", "creative_director", now_iso()))
                except Exception as e:
                    status = f"upload failed: {e}"; log(status, "error")
            with db() as c:
                c.execute("INSERT INTO creatives(run_id,code,name,spec,png,ad_id,status,ts) VALUES(?,?,?,?,?,?,?,?)", (run_id, code, v.get("name"), json.dumps(v), f"/static/primed_creatives/{code}.png", ad_id, status, now_iso()))
        brief = ceo.get("brief_markdown", "")
        with db() as c:
            c.execute("UPDATE runs SET status='done', brief=?, transcript=? WHERE id=?", (brief, json.dumps(T), run_id))
        send_brief(run_id, brief, ceo, finance, creative)
        log(f"meeting {run_id} done ({trigger})")
        return "done"
    except Exception as e:
        err = traceback.format_exc()
        with db() as c: c.execute("UPDATE runs SET status='failed', error=?, transcript=? WHERE id=?", (err[-3000:], json.dumps(T, default=str), run_id))
        log(f"meeting failed: {e}", "error"); return f"failed: {e}"
    finally:
        _running["busy"] = False

def send_brief(run_id, brief, ceo, finance, creative):
    if not RESEND_KEY: return
    md = brief.replace("\n", "<br>")
    pend = [p for p in finance.get("proposals", []) if p.get("kind") not in ("hold", "observation")]
    items = "".join(f"<li><b>{p.get('kind')}</b> {p.get('target_name')}: {json.dumps(p.get('change'))}. {p.get('reason')}</li>" for p in pend)
    cre = "".join(f"<li><b>{v.get('code')}</b> {v.get('name')}: {v.get('hypothesis')}</li>" for v in creative.get("variations", []))
    need = "".join(f"<li>{x}</li>" for x in ceo.get("needs_kendall", []))
    html = f"""<div style="font-family:Inter,Helvetica,Arial,sans-serif;font-size:14px;line-height:1.6;color:#16181d;max-width:640px">
<p><b>Primed Loans · morning meeting #{run_id}</b></p><p>{md}</p>
<p><b>Proposals waiting for you</b></p><ul>{items or '<li>None</li>'}</ul>
<p><b>New creatives (paused, ready to review)</b></p><ul>{cre or '<li>None today</li>'}</ul>
<p><b>Needs your hand</b></p><ul>{need or '<li>Nothing</li>'}</ul>
<p><a href="https://lumenmarketing.co/primed/admin">Open the dashboard</a></p></div>"""
    try:
        requests.post("https://api.resend.com/emails", headers={"Authorization": f"Bearer {RESEND_KEY}", "Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                      json={"from": "Primed Ad Team <reports@lumenmarketing.co>", "to": BRIEF_TO, "subject": f"Primed morning meeting #{run_id}: {ceo.get('brief_markdown','')[:60].splitlines()[0] if ceo.get('brief_markdown') else 'brief'}", "html": html}, timeout=60)
    except Exception as e: log(f"brief email failed: {e}", "error")

# ---------------------------------------------------------------- scheduler
def _beirut_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=3)

def _loop():
    last_day = None
    while True:
        try:
            n = _beirut_now()
            if n.hour == MEETING_HOUR and last_day != n.date():
                with db() as c:
                    done_today = c.execute("SELECT 1 FROM runs WHERE ts >= ? AND status IN ('done','running')", ((datetime.datetime.utcnow() - datetime.timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ"),)).fetchone()
                if not done_today:
                    threading.Thread(target=run_meeting, args=("scheduled",), daemon=True).start()
                last_day = n.date()
        except Exception as e: print(f"[primed-team] loop error {e}")
        time.sleep(60)

def start_scheduler():
    init_db()
    if os.environ.get("PRIMED_TEAM_ENABLED", "false").lower() not in ("true", "1", "yes"):
        print("[primed-team] scheduler dormant (PRIMED_TEAM_ENABLED not set)"); return
    threading.Thread(target=_loop, daemon=True, name="primed-team").start()
    print(f"[primed-team] scheduler started, meeting at {MEETING_HOUR:02d}:00 Beirut")

# ---------------------------------------------------------------- dashboard
PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Primed Ad Team</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;800&display=swap" rel="stylesheet">
<style>
:root{--bg:#0f1012;--card:#17181b;--line:#26282d;--gold:#c9a227;--text:#f2f1ee;--muted:#9a9ea6;--green:#3ec37a;--red:#e5534b}
*{box-sizing:border-box;margin:0;padding:0}body{font-family:Inter,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--text);line-height:1.55;padding-bottom:60px}
a{color:var(--gold);text-decoration:none}.wrap{max-width:1180px;margin:0 auto;padding:0 18px}
header{border-bottom:1px solid var(--line);padding:16px 0;margin-bottom:24px}header .wrap{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}
header b{font-size:18px}header nav a{margin-left:16px;font-size:14px;color:var(--muted)}header nav a.on{color:var(--gold)}
h2{font-size:14px;letter-spacing:.1em;text-transform:uppercase;color:var(--gold);margin:28px 0 12px}
.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:18px 20px;margin-bottom:14px}
.brief{white-space:pre-wrap;font-size:15px;line-height:1.65}
table{width:100%;border-collapse:collapse;font-size:13px}th{font-size:11px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);text-align:left;padding:8px 8px 8px 0;border-bottom:1px solid var(--line)}
td{padding:9px 8px 9px 0;border-bottom:1px solid var(--line);vertical-align:top}td.r,th.r{text-align:right}tr:hover td{background:#1c1d21}
.btn{display:inline-block;padding:9px 16px;border-radius:8px;font-weight:600;font-size:13px;border:0;cursor:pointer;font-family:inherit}
.b-gold{background:var(--gold);color:#141108}.b-green{background:var(--green);color:#052b14}.b-red{background:var(--red);color:#fff}.b-ghost{background:transparent;border:1px solid var(--line);color:var(--text)}
.pill{display:inline-block;padding:2px 9px;border-radius:999px;font-size:11px;font-weight:600;border:1px solid var(--line);color:var(--muted)}
.pill.pending{color:var(--gold);border-color:var(--gold)}.pill.executed{color:var(--green);border-color:var(--green)}.pill.rejected,.pill.failed{color:var(--red);border-color:var(--red)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px}.grid img{width:100%;border-radius:8px;border:1px solid var(--line)}
.grid .c{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:12px}.grid small{color:var(--muted);font-size:12px;display:block;margin-top:6px}
input,textarea,select{width:100%;background:#0f1012;border:1px solid var(--line);border-radius:8px;color:var(--text);padding:10px;font:inherit;font-size:14px}
label{font-size:12px;color:var(--muted);display:block;margin:12px 0 4px}form.inline{display:inline}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}.kpi{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px}.kpi b{font-size:22px;display:block}.kpi span{font-size:12px;color:var(--muted)}
.muted{color:var(--muted)}.small{font-size:12px}
@media(max-width:700px){table{font-size:12px}th,td{padding:7px 6px 7px 0}}
</style></head><body>
<header><div class="wrap"><b>Primed Loans · Ad Team</b><nav><a href="/primed/admin" class="{{'on' if tab=='home' else ''}}">Today</a><a href="/primed/admin/ads" class="{{'on' if tab=='ads' else ''}}">Scorecard</a><a href="/primed/admin/creatives" class="{{'on' if tab=='creatives' else ''}}">Creatives</a><a href="/primed/admin/rules" class="{{'on' if tab=='rules' else ''}}">Rules</a><a href="/primed/admin/runs" class="{{'on' if tab=='runs' else ''}}">History</a><a href="/primed/admin/logout">Sign out</a></nav></div></header>
<div class="wrap">{{ body|safe }}</div></body></html>"""

LOGIN = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Primed Ad Team</title>
<style>body{font-family:Inter,Helvetica,Arial,sans-serif;background:#0f1012;color:#f2f1ee;display:flex;align-items:center;justify-content:center;min-height:100vh;margin:0}form{background:#17181b;border:1px solid #26282d;border-radius:14px;padding:28px;width:320px}input{width:100%;padding:12px;border-radius:8px;border:1px solid #26282d;background:#0f1012;color:#fff;font-size:16px;margin:10px 0 16px;box-sizing:border-box}button{width:100%;padding:12px;border:0;border-radius:8px;background:#c9a227;font-weight:700;font-size:15px}p{color:#e5534b;font-size:13px}</style></head>
<body><form method="post"><b>Primed Loans · Ad Team</b><input type="password" name="password" placeholder="Password" autofocus>{% if error %}<p>Wrong password.</p>{% endif %}<button>Sign in</button></form></body></html>"""

def _auth():
    return session.get("primed_admin_auth") is True

def _page(tab, body):
    return render_template_string(PAGE, tab=tab, body=body)

@primed_bp.route("/primed/admin/login", methods=["GET", "POST"])
def primed_login():
    if request.method == "POST":
        if (request.form.get("password") or "") == ADMIN_PASSWORD:
            session.permanent = True; session["primed_admin_auth"] = True
            return redirect("/primed/admin")
        return render_template_string(LOGIN, error=True)
    return render_template_string(LOGIN, error=False)

@primed_bp.route("/primed/admin/logout")
def primed_logout():
    session.pop("primed_admin_auth", None); return redirect("/primed/admin/login")

def _latest_run():
    with db() as c: return c.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1").fetchone()

def _fmt(v, d=0):
    if v is None: return "·"
    return f"{v:,.{d}f}"

@primed_bp.route("/primed/admin")
def primed_home():
    if not _auth(): return redirect("/primed/admin/login")
    init_db()
    run = _latest_run(); R = rules()
    with db() as c:
        pend = [dict(r) for r in c.execute("SELECT * FROM proposals WHERE status='pending' ORDER BY id DESC")]
        recent = [dict(r) for r in c.execute("SELECT * FROM proposals WHERE status!='pending' ORDER BY id DESC LIMIT 8")]
        cre = [dict(r) for r in c.execute("SELECT * FROM creatives ORDER BY id DESC LIMIT 6")]
        sc = [json.loads(r["data"]) for r in c.execute("SELECT data FROM scorecard WHERE run_id=?", (run["id"],))] if run else []
    T = json.loads(run["transcript"]) if run and run["transcript"] else {}
    an = T.get("analyst", {}); ce = T.get("ceo", {})
    b7 = sum((a.get("d7") or {}).get("spend", 0) for a in sc if a["account"] == "BACKUP"); l7 = sum((a.get("d7") or {}).get("leads", 0) for a in sc if a["account"] == "BACKUP")
    q = sum(a["ghl"]["qualified"] for a in sc if a["account"] == "BACKUP"); n = sum(a["ghl"]["new"] for a in sc if a["account"] == "BACKUP")
    body = f"""
<div class="kpis"><div class="kpi"><b>{'#'+str(run['id']) if run else '—'}</b><span>last meeting {run['ts'][:16].replace('T',' ') if run else 'none yet'} UTC · {run['status'] if run else ''}</span></div>
<div class="kpi"><b>CA${_fmt(b7)}</b><span>backup spend, last 7 days</span></div><div class="kpi"><b>{_fmt(l7)}</b><span>backup leads, last 7 days</span></div>
<div class="kpi"><b>{_fmt(100*q/n if n else 0)}%</b><span>qualified rate, backup, since launch ({q} of {n} new contacts)</span></div>
<div class="kpi"><b>{len(pend)}</b><span>proposals waiting for you</span></div></div>
<div style="margin:16px 0"><form class="inline" method="post" action="/primed/admin/run"><button class="btn b-gold">Run the morning meeting now</button></form> <span class="small muted">Takes 3 to 6 minutes. The brief lands here and in your inbox.</span></div>
<h2>Morning brief</h2><div class="card brief">{(run['brief'] if run and run['brief'] else (run['error'] if run and run['error'] else 'No meeting has run yet. Press the button above.'))}</div>
"""
    if an:
        body += "<h2>Analyst</h2><div class=card><p><b>" + an.get("headline", "") + "</b></p>"
        body += "<p class=small><b>Winners:</b> " + "; ".join(f"{w.get('name')} ({w.get('confidence')}): {w.get('why')}" for w in an.get("winners", [])) + "</p>"
        body += "<p class=small><b>Losers:</b> " + "; ".join(f"{w.get('name')} ({w.get('confidence')}): {w.get('why')}" for w in an.get("losers", [])) + "</p>"
        body += "<p class=small><b>Risks:</b> " + "; ".join(an.get("risks", [])) + "</p></div>"
    if ce.get("challenges"):
        body += "<h2>Meeting, round two</h2><div class=card>" + "".join(f"<p class=small><b>CEO to {c.get('to')}:</b> {c.get('point')}</p>" for c in ce["challenges"]) + "</div>"
    body += "<h2>Proposals waiting for you</h2>"
    if not pend: body += "<div class=card muted>Nothing pending.</div>"
    for p in pend:
        body += f"""<div class=card><div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap"><div><span class="pill pending">{p['kind']}</span> <b>{p['target_name'] or p['target_id']}</b> <span class=muted small>{p['payload']}</span><p class=small style="margin-top:6px">{p['reason']}</p><p class="small muted">Expected: {p['expected']} {('· CEO: '+p['result']) if p['result'] else ''}</p></div>
<div><form class=inline method=post action="/primed/admin/decide/{p['id']}/approve"><button class="btn b-green">Approve and execute</button></form> <form class=inline method=post action="/primed/admin/decide/{p['id']}/reject"><button class="btn b-ghost">Reject</button></form></div></div></div>"""
    if recent:
        body += "<h2>Recent decisions</h2><div class=card><table><tr><th>When</th><th>Kind</th><th>Target</th><th>Change</th><th>Status</th><th>Result</th></tr>"
        body += "".join(f"<tr><td>{p['decided_at'] or p['ts']}</td><td>{p['kind']}</td><td>{p['target_name']}</td><td class=small>{p['payload']}</td><td><span class='pill {p['status']}'>{p['status']}</span></td><td class=small>{p['result'] or ''}</td></tr>" for p in recent) + "</table></div>"
    if cre:
        body += "<h2>Latest creatives</h2><div class=grid>" + "".join(f"<div class=c><img src='{v['png']}'><b>{v['code']}</b> {v['name']}<small>{v['status']}{(' · ad '+v['ad_id']) if v['ad_id'] else ''}</small></div>" for v in cre) + "</div>"
    if ce.get("needs_kendall"):
        body += "<h2>Needs your hand</h2><div class=card><ul style='margin-left:18px'>" + "".join(f"<li>{x}</li>" for x in ce["needs_kendall"]) + "</ul></div>"
    return _page("home", body)

@primed_bp.route("/primed/admin/run", methods=["POST"])
def primed_run():
    if not _auth(): return redirect("/primed/admin/login")
    threading.Thread(target=run_meeting, args=("manual",), daemon=True).start()
    return redirect("/primed/admin/runs")

@primed_bp.route("/primed/admin/decide/<int:pid>/<action>", methods=["POST"])
def primed_decide(pid, action):
    if not _auth(): return redirect("/primed/admin/login")
    if action == "approve": execute_proposal(pid, by="kendall")
    else:
        with db() as c: c.execute("UPDATE proposals SET status='rejected', decided_by='kendall', decided_at=? WHERE id=?", (now_iso(), pid))
    return redirect("/primed/admin")

@primed_bp.route("/primed/admin/ads")
def primed_ads():
    if not _auth(): return redirect("/primed/admin/login")
    run = _latest_run()
    with db() as c: sc = [json.loads(r["data"]) for r in c.execute("SELECT data FROM scorecard WHERE run_id=?", (run["id"],))] if run else []
    sc.sort(key=lambda a: (a["account"] != "BACKUP", -((a.get("launch") or {}).get("spend", 0))))
    rows = ""
    for a in sc:
        L = a.get("launch") or {}; d7 = a.get("d7") or {}; g = a["ghl"]
        rows += f"<tr><td>{a['account'][0]}</td><td><b>{a['name'][:44]}</b><br><span class='small muted'>{a.get('adset','')} · {a.get('status','')}</span></td><td class=r>{_fmt(L.get('spend'))}</td><td class=r>{_fmt(L.get('leads'))}</td><td class=r>{_fmt(L.get('cpl'))}</td><td class=r>{_fmt(d7.get('ctr'),2)}</td><td class=r>{_fmt(d7.get('freq'),2)}</td><td class=r>{g['form_leads']}</td><td class=r>{g['existing_pct'] if g['existing_pct'] is not None else '·'}%</td><td class=r>{g['low_rev_pct'] if g['low_rev_pct'] is not None else '·'}%</td><td class=r>{g['new']}</td><td class=r><b>{g['qualified']}</b></td><td class=r><b>{_fmt(g['qualified_pct'],1)}%</b></td><td class=r>{_fmt(g['cost_per_qualified'])}</td><td class=r>{g['unreachable']}</td></tr>"
    body = f"<h2>Scorecard, since {LAUNCH_DATE} (meeting #{run['id'] if run else '—'})</h2><div class=card style='overflow:auto'><table><tr><th>Acct</th><th>Ad</th><th class=r>Spend</th><th class=r>Leads</th><th class=r>CPL</th><th class=r>CTR 7d</th><th class=r>Freq 7d</th><th class=r>Form</th><th class=r>Existing</th><th class=r>Low rev</th><th class=r>New</th><th class=r>Qual</th><th class=r>Qual %</th><th class=r>CPQ</th><th class=r>Unreach</th></tr>{rows}</table><p class='small muted' style='margin-top:10px'>Qualified = reached Sent Agreement, Awaiting Offer, Offer Presented or Funded (stage or tag). Existing = form lead who was already a GHL contact. Low rev = revenue answer under $250K. CPQ = spend since launch per qualified new contact.</p></div>"
    return _page("ads", body)

@primed_bp.route("/primed/admin/creatives")
def primed_creatives():
    if not _auth(): return redirect("/primed/admin/login")
    with db() as c:
        cre = [dict(r) for r in c.execute("SELECT * FROM creatives ORDER BY id DESC LIMIT 60")]
        reg = [dict(r) for r in c.execute("SELECT * FROM ads_registry ORDER BY created_at DESC")]
    body = "<h2>Team creatives</h2>"
    body += ("<div class=grid>" + "".join(f"<div class=c><img src='{v['png']}'><b>{v['code']}</b> {v['name']}<small>{json.loads(v['spec']).get('hypothesis','')}</small><small>{v['status']}{(' · ad '+v['ad_id']) if v['ad_id'] else ''}</small></div>" for v in cre) + "</div>") if cre else "<div class=card muted>None yet.</div>"
    body += "<h2>Ad registry (attributes the analyst learns from)</h2><div class=card><table><tr><th>Ad</th><th>Ad set</th><th>Attributes</th><th>By</th></tr>" + "".join(f"<tr><td>{r['name']}</td><td class=small>{r['adset_id']}</td><td class=small>{r['attributes']}</td><td class=small>{r['created_by']}</td></tr>" for r in reg) + "</table></div>"
    body += """<h2>Register an existing ad</h2><div class=card><form method=post action="/primed/admin/registry"><label>Ad id</label><input name=ad_id required><label>Name</label><input name=name><label>Ad set id</label><input name=adset_id><label>Attributes (JSON, e.g. {"skin":"purple","anchor_number":"250000","filter":"positive","hook":"filter","language":"en"})</label><input name=attributes value='{}'><div style="margin-top:12px"><button class="btn b-gold">Save</button></div></form></div>"""
    return _page("creatives", body)

@primed_bp.route("/primed/admin/registry", methods=["POST"])
def primed_registry():
    if not _auth(): return redirect("/primed/admin/login")
    f = request.form
    with db() as c:
        c.execute("INSERT OR REPLACE INTO ads_registry VALUES(?,?,?,?,?,?,?,?)", (f.get("ad_id","").strip(), f.get("name",""), "BACKUP", f.get("adset_id","").strip(), f.get("attributes","{}"), "", "kendall", now_iso()))
    return redirect("/primed/admin/creatives")

@primed_bp.route("/primed/admin/rules", methods=["GET", "POST"])
def primed_rules():
    if not _auth(): return redirect("/primed/admin/login")
    if request.method == "POST":
        for k in DEFAULT_RULES:
            if k in request.form:
                v = request.form.get(k)
                if isinstance(DEFAULT_RULES[k], (int, float)) and not isinstance(DEFAULT_RULES[k], bool):
                    try: v = float(v) if "." in v else int(v)
                    except Exception: pass
                set_rule(k, v)
        return redirect("/primed/admin/rules")
    R = rules(); fields = ""
    for k, v in R.items():
        if k in ("autopilot_budget", "autopilot_status", "autopilot_create_ads"):
            fields += f"<label>{k}</label><select name={k}><option {'selected' if v=='off' else ''}>off</option><option {'selected' if v=='on' else ''}>on</option></select>"
        elif k in ("goal", "kendall_notes"):
            fields += f"<label>{k}</label><textarea name={k} rows=4>{v}</textarea>"
        else:
            fields += f"<label>{k}</label><input name={k} value='{v}'>"
    body = f"""<h2>Rules the team works inside</h2><div class=card><p class="small muted">Autopilot off means the proposal waits for you on the Today page. write_adset_ids is the only place the team may create ads or change budgets. form_id must be the revenue filtered form. Kendall's notes are standing instructions every role reads each morning (rate sheet, things to avoid, priorities).</p><form method=post>{fields}<div style="margin-top:16px"><button class="btn b-gold">Save rules</button></div></form></div>"""
    return _page("rules", body)

@primed_bp.route("/primed/admin/runs")
def primed_runs():
    if not _auth(): return redirect("/primed/admin/login")
    with db() as c:
        runs = [dict(r) for r in c.execute("SELECT id,ts,status,substr(brief,1,160) b,error FROM runs ORDER BY id DESC LIMIT 40")]
        logs = [dict(r) for r in c.execute("SELECT * FROM log ORDER BY rowid DESC LIMIT 40")]
    body = "<h2>Meetings</h2><div class=card><table><tr><th>#</th><th>When (UTC)</th><th>Status</th><th>Brief</th></tr>" + "".join(f"<tr><td><a href='/primed/admin/runs/{r['id']}'>{r['id']}</a></td><td>{r['ts']}</td><td><span class='pill {'executed' if r['status']=='done' else ('failed' if r['status']=='failed' else 'pending')}'>{r['status']}</span></td><td class=small>{(r['b'] or r['error'] or '')[:160]}</td></tr>" for r in runs) + "</table></div>"
    body += "<h2>Log</h2><div class=card>" + "".join(f"<p class='small {'muted' if l['level']=='info' else ''}'>{l['ts']} · {l['msg'][:300]}</p>" for l in logs) + "</div>"
    return _page("runs", body)

@primed_bp.route("/primed/admin/runs/<int:rid>")
def primed_run_detail(rid):
    if not _auth(): return redirect("/primed/admin/login")
    with db() as c: r = c.execute("SELECT * FROM runs WHERE id=?", (rid,)).fetchone()
    if not r: return redirect("/primed/admin/runs")
    T = json.loads(r["transcript"]) if r["transcript"] else {}
    def sec(title, obj): return f"<h2>{title}</h2><div class=card><pre style='white-space:pre-wrap;font-family:inherit;font-size:13px'>{json.dumps(obj, indent=1, ensure_ascii=False)[:20000]}</pre></div>"
    body = f"<h2>Meeting #{rid} · {r['status']}</h2><div class='card brief'>{r['brief'] or r['error'] or ''}</div>" + sec("Analyst", T.get("analyst")) + sec("Finance", T.get("finance")) + sec("Creative director", T.get("creative")) + sec("CEO", T.get("ceo"))
    return _page("runs", body)
