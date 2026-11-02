# ── /FGCagenttraining — Mary trains the FGC Syria WhatsApp agent by being interviewed.
#    One continuous conversation, stored forever: every message she sends and every reply
#    is kept in SQLite and the FULL history goes to the model on every turn, so nothing is
#    ever lost between sessions. Facts she confirms are also saved to a structured "Syria
#    brief" (record_fact tool) that Kendall/Jarvis read via the export endpoint.
import os, sqlite3, datetime, json, base64, threading, time
from flask import Blueprint, render_template, request, jsonify, session, redirect

fgc_training_bp = Blueprint("fgc_training", __name__)

DB_PATH = os.path.join("/data" if os.path.isdir("/data") else os.path.dirname(__file__), "fgc_training.db")
PASSWORD = os.environ.get("FGC_TRAINING_PASSWORD", "Ghazarian123$$")
EXPORT_KEY = os.environ.get("FGC_TRAINING_EXPORT_KEY", "fgc-syria-export-7f3k")
MODEL = os.environ.get("FGC_TRAINING_MODEL", "claude-opus-5-5")
CONTEXT = open(os.path.join(os.path.dirname(__file__), "fgc_training", "fgc_context.md")).read()

SECTIONS = ["business", "products", "prices", "shipping", "payment", "customers",
            "dialect", "rules", "open_questions", "settings"]

INSTRUCTIONS = """You are training a new WhatsApp sales agent for Feels Good Club (FGC) in SYRIA, \
by interviewing Mary (Marykate), who runs FGC. You are talking with her in English. She is Lebanese, \
speaks Lebanese Arabic, and knows the business inside out.

THE GOAL
FGC already has a working WhatsApp agent in Lebanon. Its full instructions, product facts and real \
Lebanese customer chats are below. The Syria agent starts from exactly that and changes only what is \
different for Syria: the products and prices on offer, shipping, payment, the customers, and above all \
the Arabic. Syrian Arabic (Shami, Damascus-style as the default unless Mary says otherwise) is close to \
Lebanese but different in words, spelling and tone, and the agent has to sound like a real Syrian shop.

HOW TO INTERVIEW
- Guided, not rigid. Ask ONE question at a time, short and plain. Wait for her answer.
- Follow her lead when she volunteers something useful, then bring it back to the plan.
- When an answer is vague, ask one follow-up. Never assume a price, a number or a policy.
- Offer your best guess as a draft she can correct ("I'd guess X, is that right?") where it saves her \
time, especially for the Arabic.
- KEEP IT SHORT. Most of your messages are one or two lines: a quick acknowledgement ("Got it." / "Perfect.") and the next question. Never more than about 40 words unless you are showing Arabic lines for her to check. No lectures, no recaps, no lists of upcoming topics. Make it feel effortless for her.
- Questions must be short and easy to answer from a phone, ideally with a yes/no or a single number.
- Every time she confirms a concrete fact, call record_fact before you reply (one call per fact, \
several in a turn is fine). Record what she actually said, in her words where it matters. If she \
corrects an earlier fact, record it again under the same key; the newest one wins.
- THIS PAGE IS CONNECTED TO THE LIVE AGENT. Whatever you save reaches it within 5 minutes, on its own. \
Never say someone else has to set something up. Mary is the owner: when she tells you something is true \
or must change, it is an instruction, not a proposal. Save it in that same turn and tell her in one line \
what changed, "live in about 5 minutes". Only ask first if you truly cannot tell what she means, and \
then ask one short question.
- Apply a change everywhere it touches: call record_fact again under the EXISTING key of every item that \
now says something false (newest value per key wins, so the old value disappears). Never create a new \
key that contradicts an old one. Open questions never reach the live agent, so a confirmed change never \
goes there. A change Mary makes while you are in test mode counts too.
- Section "settings" drives things that are code in the live agent. Keep it right whenever she changes them: \
price_table is JSON of the total a customer pays with delivery by quantity, e.g. {"1":20,"2":30,"3":37}; \
add "4" or "each_4_plus" only if she gives one. greeting_en / greeting_ar / greeting_arabizi / greeting_fr: \
only if she gives exact opener wording, otherwise the opener follows the 1-item price by itself. \
followup_en / followup_ar (Syrian) / followup_lb (Lebanese) / followup_arabizi: exact 15-hour follow-up wording. \
One price for all products is all price_table can hold; if she sets a different price per product, save it \
in prices and say Kendall will see it, because order totals may need checking.
- She may stop at any time and come back days later. When she returns, greet her briefly, say in one \
line where you left off, and carry on. Never restart.

THE PLAN (in this order, but stay flexible)
1. Business basics: is it the same FGC brand in Syria, which cities, who handles orders and delivery, \
the new WhatsApp number's working hours.
2. Products: which products will be sold in Syria (go through the Lebanon list one by one: yes/no), any \
new products, pack sizes if different.
3. Prices: currency (USD or Syrian pounds, or both), price per product, bundle or multi-buy offers.
4. Shipping: which areas, delivery cost per area, delivery time, the courier, what happens if they're \
not home.
5. Payment: cash on delivery or other, any deposit, returns or exchanges.
6. Customers: who buys, what Syrians usually ask, what a serious buyer looks like vs a time waster, \
what details the agent needs to take an order (name, city, area, address, phone...).
7. The Arabic. This is the most important part. Take real lines from the Lebanon agent and the real \
chats below, show her the Lebanese version, propose your Syrian version, and ask her to confirm or fix \
it. Cover: greetings, the opening message, prices ("12$", "سعر"), delivery, asking for name and \
location, confirming an order, "ok/done", thanks, handling "too expensive", and common customer \
phrasings. Ask which words a Syrian would NEVER say, words that sound too Lebanese, and spelling habits \
(Arabic script vs Latin letters). Record every confirmed pair under section "dialect" with the \
Lebanese line, the Syrian line and a note.
8. Rules: anything the Syria agent must always or never do, when to hand a chat to a human.
9. Testing: when she wants to (or once the basics are covered, offer it), switch to test mode. In test \
mode Mary plays a Syrian customer and you reply AS the Syria agent: same rules as the Lebanon agent \
(extremely short, WhatsApp style) but in Syrian Arabic with the Syria facts. Mark test replies clearly \
by starting them with "🧪". After each test reply she can say what a real Syrian shop would have \
written; record that as a dialect or rule fact. She types "stop test" to go back to the interview.

YOUR ANSWERS GO LIVE
Every fact you record_fact is picked up by the LIVE WhatsApp agent within about 5 minutes,
automatically. So when Mary tells you a rule or a line, record it clearly and tell her it will
be live in a few minutes. You still cannot see the real customer chats; if she mentions one,
ask her to paste it. When she changes a rule, record it under the SAME key so the old one is
replaced, and mark anything no longer true as "SUPERSEDED: ...".

TOOL
record_fact(section, key, value): save one confirmed fact. section is one of: """ + ", ".join(SECTIONS) + """. \
key is a short stable label (e.g. "price_whitening_strips", "delivery_damascus", "greeting"). Use \
"open_questions" for things she said she'll check later.

=====================================================================
EVERYTHING THE LEBANON AGENT KNOWS TODAY (the starting point)
=====================================================================
""" + CONTEXT


def _db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def init_fgc_training_db():
    con = _db()
    con.execute("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, "
                "role TEXT, content TEXT)")
    con.execute("CREATE TABLE IF NOT EXISTS facts (id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT, "
                "section TEXT, key TEXT, value TEXT)")
    con.commit()
    con.close()


def _now():
    return datetime.datetime.utcnow().isoformat(timespec="seconds")


def _brief():
    """Latest value per (section, key)."""
    con = _db()
    rows = con.execute("SELECT section, key, value, ts FROM facts WHERE id IN "
                       "(SELECT MAX(id) FROM facts GROUP BY section, key) ORDER BY section, id").fetchall()
    con.close()
    return [dict(r) for r in rows]


def _history():
    con = _db()
    rows = con.execute("SELECT role, content, ts FROM messages ORDER BY id").fetchall()
    con.close()
    return [{"role": r["role"], "content": json.loads(r["content"]), "ts": r["ts"]} for r in rows]


def _save(role, content):
    con = _db()
    con.execute("INSERT INTO messages (ts, role, content) VALUES (?,?,?)", (_now(), role, json.dumps(content, ensure_ascii=False)))
    con.commit()
    con.close()


def _visible_text(content):
    """What the chat shows for a stored message: text blocks only."""
    if isinstance(content, str):
        return content
    out = []
    for b in content:
        if b.get("type") == "text":
            out.append(b["text"])
        elif b.get("type") == "image":
            out.append("[image]")
    return "\n".join(t for t in out if t)


TOOLS = [{
    "name": "record_fact",
    "description": "Save one fact Mary confirmed about the Syria business or the Syrian Arabic the agent must use.",
    "strict": True,
    "input_schema": {
        "type": "object",
        "properties": {
            "section": {"type": "string", "enum": SECTIONS},
            "key": {"type": "string", "description": "Short stable label, e.g. price_nasal_strips"},
            "value": {"type": "string", "description": "The fact, in Mary's words where it matters"},
        },
        "required": ["section", "key", "value"],
        "additionalProperties": False,
    },
}]


def _api_messages(history):
    """The whole conversation, every turn. Images older than the last user turn are replaced by a
    note (the assistant already described them in its reply); everything else is sent verbatim."""
    msgs = [{"role": h["role"], "content": h["content"]} for h in history]
    last_user = max((i for i, m in enumerate(msgs) if m["role"] == "user"), default=-1)
    for i, m in enumerate(msgs):
        if i != last_user and isinstance(m["content"], list):
            m["content"] = [b if b.get("type") != "image" else {"type": "text", "text": "[Mary shared an image here]"}
                            for b in m["content"]]
    # A restart between saving a tool call and saving its result would otherwise make every
    # later request fail, so a call with no result gets one here.
    fixed = []
    for i, m in enumerate(msgs):
        fixed.append(m)
        if m["role"] == "assistant" and isinstance(m["content"], list):
            ids = [b["id"] for b in m["content"] if b.get("type") == "tool_use"]
            nxt = msgs[i + 1]["content"] if i + 1 < len(msgs) else None
            have = {b.get("tool_use_id") for b in nxt if isinstance(b, dict)} if isinstance(nxt, list) else set()
            missing = [t for t in ids if t not in have]
            if missing:
                fixed.append({"role": "user", "content": [
                    {"type": "tool_result", "tool_use_id": t, "content": "saved"} for t in missing]})
    return fixed


def _system():
    brief = _brief()
    brief_txt = "\n".join(f"- [{f['section']}] {f['key']}: {f['value']}" for f in brief) or "(nothing recorded yet)"
    return [
        {"type": "text", "text": INSTRUCTIONS, "cache_control": {"type": "ephemeral", "ttl": "1h"}},
        {"type": "text", "text": "SYRIA BRIEF SO FAR (facts already confirmed, newest value per key):\n" + brief_txt
         + f"\n\nToday is {datetime.date.today().isoformat()}."},
    ]


def _run_turn():
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""),
                                 timeout=150, max_retries=1)
    for _ in range(8):  # tool loop
        resp = client.beta.messages.create(
            model=MODEL, max_tokens=16000,
            betas=["server-side-fallback-2026-07-01"],
            # Newer request fields go through extra_body so an older installed SDK can't reject them.
            extra_body={"fallbacks": "default", "thinking": {"type": "adaptive"},
                        "output_config": {"effort": "medium"},
                        "cache_control": {"type": "ephemeral", "ttl": "1h"}},
            system=_system(), tools=TOOLS, tool_choice={"type": "auto"},
            messages=_api_messages(_history()),
        )
        if resp.stop_reason == "refusal":
            msg = "Sorry, I couldn't answer that one. Could you put it another way?"
            _save("assistant", [{"type": "text", "text": msg}])   # or it would count as unanswered forever
            return msg
        content = [b.model_dump(exclude_none=True) for b in resp.content]
        _save("assistant", content)
        uses = [b for b in resp.content if b.type == "tool_use"]
        if not uses:
            return _visible_text(content)
        results = []
        con = _db()
        for u in uses:
            inp = u.input if isinstance(u.input, dict) else {}
            if inp.get("section") in SECTIONS and inp.get("key") and inp.get("value"):
                con.execute("INSERT INTO facts (ts, section, key, value) VALUES (?,?,?,?)",
                            (_now(), inp["section"], inp["key"][:120], inp["value"][:4000]))
                results.append({"type": "tool_result", "tool_use_id": u.id, "content": "saved"})
            else:
                results.append({"type": "tool_result", "tool_use_id": u.id, "is_error": True,
                                "content": "invalid fact, needs section, key and value"})
        con.commit()
        con.close()
        _save("user", results)
    msg = "I saved what you told me. Let's keep going."
    _save("assistant", [{"type": "text", "text": msg}])
    return msg


OPENER = ("(Mary just opened the training page for the first time. The greeting below was shown "
          "to her automatically. Continue the interview from her answer.)")


# ── Nothing Mary says gets missed ───────────────────────────────────────────
# One background worker does two jobs until both are clear: answer her, then CHECK her
# messages against the saved brief and fix anything the conversation missed. The state is
# in the database, so a crash, a deploy or a model outage only delays it. The live agent
# fetches the brief every 5 minutes, and each fetch also restarts this if it stopped.
_work = {"thread": None, "error": None, "failed_at": 0.0}
_work_lock = threading.Lock()
_health = {"agent_fetch": None, "audit_at": None, "audit_error": None}
AUDIT_SCHEMA = {
    "type": "object",
    "properties": {
        "writes": {"type": "array", "items": {
            "type": "object",
            "properties": {"section": {"type": "string", "enum": SECTIONS}, "key": {"type": "string"},
                           "value": {"type": "string"}},
            "required": ["section", "key", "value"], "additionalProperties": False}},
        "applied": {"type": "array", "items": {"type": "string"},
                    "description": "One short line per change Mary asked for"},
        "unsure": {"type": "array", "items": {"type": "string"},
                   "description": "Things she said that could not be applied, one short line each"},
    },
    "required": ["writes", "applied", "unsure"], "additionalProperties": False,
}


def _meta(key, default=None, value=None):
    con = _db()
    con.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    if value is not None:
        con.execute("INSERT OR REPLACE INTO meta (key, value) VALUES (?,?)", (key, str(value)))
        con.commit()
    row = con.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
    con.close()
    return row["value"] if row else default


def _rows():
    con = _db()
    rows = con.execute("SELECT id, role, content, ts FROM messages ORDER BY id").fetchall()
    con.close()
    return [{"id": r["id"], "role": r["role"], "content": json.loads(r["content"]), "ts": r["ts"]} for r in rows]


def _is_mary(row):
    c = row["content"]
    if row["role"] != "user":
        return False
    if isinstance(c, list) and c and all(b.get("type") == "tool_result" for b in c):
        return False
    return not _visible_text(c).startswith(OPENER[:20])


def _unanswered(rows=None):
    rows = rows if rows is not None else _rows()
    return bool(rows) and rows[-1]["role"] == "user"


def _unchecked(rows=None):
    rows = rows if rows is not None else _rows()
    mark = _meta("audit_mark")
    if mark is None:          # first run: everything before today's fix was already corrected by hand
        _meta("audit_mark", value=rows[-1]["id"] if rows else 0)
        return []
    return [r for r in rows if r["id"] > int(mark) and _is_mary(r)]


def _email_kendall(subject, lines):
    key = os.environ.get("RESEND_API_KEY", "")
    if not key:
        return
    try:
        import requests
        body = "<p>" + "</p><p>".join(str(x).replace("<", "&lt;") for x in lines) + "</p>"
        requests.post("https://api.resend.com/emails", timeout=15,
                      headers={"Authorization": f"Bearer {key}"},
                      json={"from": "Lumen <kendall@lumenmarketing.co>",
                            "to": [os.environ.get("FGC_TRAINING_NOTIFY", "kendall@lumenmarketing.co")],
                            "subject": subject, "html": body})
    except Exception as e:
        print(f"[fgc-training] email failed: {e}")


def _audit():
    """Read what Mary said since the last check and make the brief match it."""
    rows = _rows()
    new = _unchecked(rows)
    if not new:
        return
    upto = rows[-1]["id"]
    first = new[0]["id"]
    before = [r for r in rows if r["id"] < first and _visible_text(r["content"]).strip()][-10:]
    def fmt(r):
        return ("MARY" if r["role"] == "user" else "TRAINER") + ": " + _visible_text(r["content"])[:1500]
    convo = "\n".join(fmt(r) for r in rows if r["id"] >= first and _visible_text(r["content"]).strip())
    brief = "\n".join(f"- [{f['section']}] {f['key']}: {f['value']}" for f in _brief())
    prompt = ("You check that nothing Mary (the owner of Feels Good Club) said was missed. A live WhatsApp "
              "sales agent runs on the BRIEF below, refreshed every 5 minutes. Items in open_questions never "
              "reach it.\n\nFind every instruction or fact Mary gave in the NEW MESSAGES, and make the brief "
              "match. For each, output the writes needed: rewrite under the EXISTING section and key every item "
              "that is now false or contradicts her (newest value per key wins), and add a new key only if "
              "nothing covers it. Check prices, delivery, offers, Syrian pound amounts, wording lines and rules, "
              "not just the obvious item. If a confirmed change is sitting in open_questions, apply it and mark "
              "that item resolved.\n\nSection settings: price_table is JSON of the total a customer pays with "
              "delivery by quantity, like {\"1\":20,\"2\":30,\"3\":37}, with \"4\" or \"each_4_plus\" only if she "
              "gave one. greeting_en, greeting_ar, greeting_arabizi, greeting_fr are exact opener wording, only "
              "if she gave it. followup_en, followup_ar, followup_lb, followup_arabizi are exact follow-up wording.\n\n"
              "Do NOT write: her lines while she is role-playing a customer in test mode (trainer replies start "
              "with a test emoji), small talk, questions, or anything already correct in the brief. Keep her "
              "words where they matter. If everything is already applied, return empty writes. A message that "
              "just repeats something already applied needs no write.\n\nEARLIER MESSAGES FOR CONTEXT:\n"
              + "\n".join(fmt(r) for r in before) + "\n\nNEW MESSAGES:\n" + convo + "\n\nBRIEF:\n" + brief)
    import anthropic
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""), timeout=150, max_retries=1)
    resp = client.messages.create(
        model=MODEL, max_tokens=8000, messages=[{"role": "user", "content": prompt}],
        extra_body={"output_config": {"effort": "medium", "format": {"type": "json_schema", "schema": AUDIT_SCHEMA}}})
    d = json.loads("".join(b.text for b in resp.content if getattr(b, "type", "") == "text"))
    con = _db()
    n = 0
    for w in d.get("writes") or []:
        if w.get("section") in SECTIONS and w.get("key") and w.get("value"):
            con.execute("INSERT INTO facts (ts, section, key, value) VALUES (?,?,?,?)",
                        (_now(), w["section"], w["key"][:120], w["value"][:4000]))
            n += 1
    con.commit()
    con.close()
    _meta("audit_mark", value=upto)     # only after the writes are safely in
    _health.update(audit_at=_now(), audit_error=None)
    print(f"[fgc-training] checked {len(new)} message(s): {n} write(s)")
    if d.get("applied") or d.get("unsure"):
        lines = [f"Mary said: {_visible_text(r['content'])[:300]}" for r in new]
        lines += [f"Applied: {x}" for x in d.get("applied") or []]
        lines += [f"Could not apply, needs you: {x}" for x in d.get("unsure") or []]
        lines += [f"Check found and saved {n} item(s) the conversation had missed." if n else "Nothing was missed."]
        _email_kendall("FGC training: Mary changed something", lines)


def _ensure_work():
    """Start the worker if there is anything left to do. Safe to call from anywhere."""
    with _work_lock:
        if _work["thread"] is not None and _work["thread"].is_alive():
            return True
        rows = _rows()
        if not (_unanswered(rows) or _unchecked(rows)):
            _work["error"] = None
            return False
        if time.time() - _work["failed_at"] < 45:   # failed moments ago, don't hammer the API
            return False

        def work():
            try:
                for _ in range(5):
                    if not _unanswered():
                        break
                    _run_turn()
                _work["error"] = None
            except Exception as e:
                print(f"[fgc-training] model error: {e}")
                _work.update(error=str(e)[:200], failed_at=time.time())
            try:
                _audit()               # even if the reply failed, her instruction still gets saved
            except Exception as e:
                print(f"[fgc-training] check failed, will retry: {e}")
                _health["audit_error"] = str(e)[:200]
                _work["failed_at"] = time.time()

        _work["thread"] = threading.Thread(target=work, daemon=True)
        _work["thread"].start()
        return True


def _authed():
    return session.get("fgc_training_ok") is True


@fgc_training_bp.route("/FGCagenttraining", methods=["GET", "POST"])
@fgc_training_bp.route("/fgcagenttraining", methods=["GET", "POST"])
def page():
    if request.method == "POST":
        if (request.form.get("password") or "").strip() == PASSWORD:
            session["fgc_training_ok"] = True
            session.permanent = True
        return redirect("/FGCagenttraining")
    return render_template("fgc_training.html", authed=_authed())


GREETING = ("Hi MK 👋 I'm here to help you train the Syrian FGC agent.\n\n"
            "I'll ask short questions, one at a time. Everything is saved, so you can stop and come back whenever you like.\n\n"
            "First one: will the shop be called Feels Good Club in Syria too?")


def _seed():
    """The conversation always opens with the same greeting, written once."""
    if not _history():
        _save("user", [{"type": "text", "text": OPENER}])
        _save("assistant", [{"type": "text", "text": GREETING}])


@fgc_training_bp.route("/FGCagenttraining/api/history")
def history():
    if not _authed():
        return jsonify({"error": "auth"}), 401
    _seed()
    out = []
    for h in _history():
        c = h["content"]
        if h["role"] == "user" and isinstance(c, list) and all(b.get("type") == "tool_result" for b in c):
            continue
        t = _visible_text(c)
        if t.strip() and not t.startswith(OPENER[:20]):
            out.append({"role": h["role"], "text": t, "ts": h["ts"]})
    return jsonify({"messages": out})


@fgc_training_bp.route("/FGCagenttraining/api/chat", methods=["POST"])
def chat():
    if not _authed():
        return jsonify({"error": "auth"}), 401
    text = (request.form.get("text") or "").strip()
    blocks = []
    for f in request.files.getlist("images")[:4]:
        data = f.read()
        if data and len(data) < 5 * 1024 * 1024 and (f.mimetype or "").startswith("image/"):
            blocks.append({"type": "image", "source": {"type": "base64", "media_type": f.mimetype,
                                                       "data": base64.b64encode(data).decode()}})
    if text:
        blocks.append({"type": "text", "text": text})
    if not blocks:
        return jsonify({"error": "That image was too big to send and nothing was saved. Try a screenshot, or type it."}), 400
    _save("user", blocks)
    # The model can take longer than gunicorn's 30s request limit, which killed the request
    # and lost the reply (02.11.2026), so the turn runs in the background and the page polls.
    # "reply" is only for a page still open from before this change.
    _ensure_work()
    return jsonify({"pending": True, "saved": True,
                    "reply": "Saved. Please refresh this page to see the answer."})


@fgc_training_bp.route("/FGCagenttraining/api/status")
def status():
    if not _authed():
        return jsonify({"error": "auth"}), 401
    busy = _ensure_work()
    return jsonify({"pending": busy or _unanswered(), "delayed": bool(_work["error"])})


@fgc_training_bp.route("/FGCagenttraining/facts", methods=["POST"])
def facts():
    """For Kendall/Jarvis: correct saved facts directly. Newest value per key wins, so this
    replaces them. Body: {"facts": [{section, key, value}], "note": "shown in Mary's chat"}."""
    if request.args.get("k") != EXPORT_KEY:
        return "no", 403
    body = request.get_json(force=True) or {}
    con = _db()
    n = 0
    for f in body.get("facts") or []:
        if f.get("section") in SECTIONS and f.get("key") and f.get("value"):
            con.execute("INSERT INTO facts (ts, section, key, value) VALUES (?,?,?,?)",
                        (_now(), f["section"], f["key"][:120], f["value"][:4000]))
            n += 1
    con.commit()
    con.close()
    if body.get("note"):
        _save("assistant", [{"type": "text", "text": body["note"]}])
    return jsonify({"ok": True, "saved": n})


@fgc_training_bp.route("/FGCagenttraining/export")
def export():
    """For Kendall/Jarvis: the brief plus the full readable transcript."""
    if request.args.get("k") != EXPORT_KEY:
        return "no", 403
    if request.args.get("brief") == "1":
        # What the live WhatsApp agent pulls every few minutes. Doubles as the heartbeat.
        _health["agent_fetch"] = _now()
        try:
            _ensure_work()
        except Exception as e:
            print(f"[fgc-training] heartbeat: {e}")
        return jsonify({"brief": _brief()})
    msgs = [{"role": h["role"], "ts": h["ts"], "text": _visible_text(h["content"])} for h in _history()]
    return jsonify({"brief": _brief(), "messages": [m for m in msgs if m["text"].strip()]})


@fgc_training_bp.route("/FGCagenttraining/health")
def health():
    """For Kendall/Jarvis: is everything she said answered, saved and reaching the agent?"""
    if request.args.get("k") != EXPORT_KEY:
        return "no", 403
    rows = _rows()
    return jsonify({"unanswered": _unanswered(rows), "unchecked_messages": len(_unchecked(rows)),
                    "worker_running": bool(_work["thread"] and _work["thread"].is_alive()),
                    "last_error": _work["error"], "last_check": _health["audit_at"],
                    "last_check_error": _health["audit_error"],
                    "agent_last_fetched": _health["agent_fetch"], "facts": len(_brief())})


@fgc_training_bp.route("/FGCagenttraining/reset", methods=["POST"])
def reset():
    """Wipe the conversation and the brief so Mary starts fresh (used after Kendall's test)."""
    if request.args.get("k") != EXPORT_KEY:
        return "no", 403
    con = _db()
    con.execute("DELETE FROM messages")
    con.execute("DELETE FROM facts")
    con.commit()
    con.close()
    return jsonify({"ok": True})
