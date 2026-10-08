# ── /FGCagenttraining — Mary trains the FGC Syria WhatsApp agent by being interviewed.
#    One continuous conversation, stored forever: every message she sends and every reply
#    is kept in SQLite and the FULL history goes to the model on every turn, so nothing is
#    ever lost between sessions. Facts she confirms are also saved to a structured "Syria
#    brief" (record_fact tool) that Kendall/Jarvis read via the export endpoint.
import os, sqlite3, datetime, json, base64
from flask import Blueprint, render_template, request, jsonify, session, redirect

fgc_training_bp = Blueprint("fgc_training", __name__)

DB_PATH = os.path.join("/data" if os.path.isdir("/data") else os.path.dirname(__file__), "fgc_training.db")
PASSWORD = os.environ.get("FGC_TRAINING_PASSWORD", "Ghazarian123$$")
EXPORT_KEY = os.environ.get("FGC_TRAINING_EXPORT_KEY", "fgc-syria-export-7f3k")
MODEL = os.environ.get("FGC_TRAINING_MODEL", "claude-opus-5-5")
CONTEXT = open(os.path.join(os.path.dirname(__file__), "fgc_training", "fgc_context.md")).read()

SECTIONS = ["business", "products", "prices", "shipping", "payment", "customers",
            "dialect", "rules", "open_questions"]

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
    return msgs


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
    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY", ""))
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
            return "Sorry, I couldn't answer that one. Could you put it another way?"
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
    return "I saved what you told me. Let's keep going."


OPENER = ("(Mary just opened the training page for the first time. The greeting below was shown "
          "to her automatically. Continue the interview from her answer.)")


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
        return jsonify({"error": "empty"}), 400
    _save("user", blocks)
    try:
        reply = _run_turn()
    except Exception as e:
        print(f"[fgc-training] model error: {e}")
        return jsonify({"error": "Something went wrong on my side. Your message is saved, try sending again in a minute."}), 502
    return jsonify({"reply": reply})


@fgc_training_bp.route("/FGCagenttraining/export")
def export():
    """For Kendall/Jarvis: the brief plus the full readable transcript."""
    if request.args.get("k") != EXPORT_KEY:
        return "no", 403
    msgs = [{"role": h["role"], "ts": h["ts"], "text": _visible_text(h["content"])} for h in _history()]
    return jsonify({"brief": _brief(), "messages": [m for m in msgs if m["text"].strip()]})
