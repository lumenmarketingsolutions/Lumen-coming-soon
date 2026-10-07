# ── /ecom-DFY — custom rebuild of the client's DFY ecom store funnel (off Funnelish).
#    Layout + mechanics match the original. CONTENT below is placeholder until the
#    client sends their copy, images, Wistia ID and Stripe payment links.
#    Payment is never collected here: step 2 hands off to the client's Stripe links.
import os, sqlite3, datetime
from flask import Blueprint, render_template, request, jsonify, redirect

ecom_dfy_bp = Blueprint("ecom_dfy", __name__)

# Railway persistent volume (/data), local dir for dev — same rule as app.py
DB_PATH = os.path.join("/data" if os.path.isdir("/data") else os.path.dirname(__file__), "ecom_dfy.db")

# Stripe Payment Links from the client's account (set in Railway).
STRIPE_FE = os.environ.get("ECOM_DFY_STRIPE_FE", "")        # $20 front end
STRIPE_FE_BUMP = os.environ.get("ECOM_DFY_STRIPE_BUMP", "")  # $20 + $49 order bump
WISTIA_ID = os.environ.get("ECOM_DFY_WISTIA_ID", "")

# Every string/image the page shows. Swap these when the client's files arrive.
# img values are paths under /static/ecom-dfy/ ; empty = labelled placeholder box.
CONTENT = {
    "title": "[Page title]",
    "countdown_label": "[Countdown label]",
    "headline": "[Main headline]",
    "price_line": "[Price line]",
    "subhead": "[Subheadline]",
    "subhead_link": "[Underlined subheadline part]",
    "video_bar": "[Video bar text]",
    "social_proof_img": "",
    "form_title": "[Order form title]",
    "consent": "[SMS/email consent text naming the client's legal entity]",
    "step1_btn": "[Step 1 button]",
    "privacy_note": "[Privacy note]",
    "bump_title": "[Order bump checkbox label]",
    "bump_badge": "[Bump badge]",
    "bump_text": "[Order bump description]",
    "complete_btn": "[Complete order button]",
    "secure_note": "[Secure payment note]",
    "terms_url": "#", "privacy_url": "#",
    "works_title_a": "[How It Works heading,", "works_title_b": "highlight]",
    "steps": [
        {"img": "", "title": "[Step 1 title]", "body": ["[Step 1 paragraph]", "[Step 1 paragraph]"]},
        {"img": "", "title": "[Step 2 title]", "body": ["[Step 2 paragraph]"]},
        {"img": "", "title": "[Step 3 title]", "body": ["[Step 3 paragraph]"]},
        {"img": "", "title": "[Step 4 title]", "body": ["[Step 4 paragraph]"]},
    ],
    "cta": "[CTA button text]",
    "cta_sub": "[CTA guarantee line]",
    "bio_title_a": "[Bio heading,", "bio_title_b": "highlight]",
    "bio_img": "",
    "bio_intro": "[Bio intro, bold part]",
    "bio_body": ["[Bio paragraph]", "[Bio lead-in to list]"],
    "bio_points": ["[Credential 1]", "[Credential 2]", "[Credential 3]", "[Credential 4]"],
    "bio_close": "[Bio closing paragraph]",
    "model_title": "[Objection heading]",
    "model_sub": "[Objection subheading]",
    "model_body": ["[Paragraph]", "[Paragraph]", "[Bold paragraph]", "[Paragraph]", "[Bold closing line]"],
    "model_bold": [2, 4],
    "gallery_title_a": "[Gallery heading,", "gallery_title_b": "highlight]",
    "gallery": ["", "", "", "", "", ""],
    "stack_kicker": "[Stack kicker]",
    "stack_title": "[Stack heading]",
    "stack_img": "",
    "stack": [
        {"b": "[Item 1]", "t": "[description]"}, {"b": "[Item 2]", "t": "[description]"},
        {"b": "[Item 3]", "t": "[description]"}, {"b": "[Item 4]", "t": "[description]"},
        {"b": "[Item 5]", "t": "[description]"}, {"b": "[Bonus]", "t": "[description]"},
    ],
    "today_price": "[Today price]",
    "assurances": [
        {"icon": "", "title": "[Guarantee title]", "body": ["[Guarantee text]"]},
        {"icon": "", "title": "[Security title]", "body": ["[Security text]"]},
        {"icon": "", "title": "[Help title]", "body": ["[Support contact text]"]},
    ],
    "faq_title_a": "[FAQ heading,", "faq_title_b": "highlight]",
    "faq": [{"q": f"[Question {i}]", "a": ["[Answer]"]} for i in range(1, 12)],
    "final_title_a": "[Final CTA heading,", "final_title_b": "highlight]",
    "footer_logo": "",
    "copyright": "[Copyright line]",
    "footer_links": [("Data Protection", "#"), ("Earnings Disclaimer", "#"), ("Privacy Policy", "#"),
                     ("Terms & Conditions", "#"), ("GDPR", "#")],
    "disclaimers": ["[Disclaimer paragraph]", "[Disclaimer paragraph]", "[Disclaimer paragraph]"],
    "contact_lines": ["[Support email]", "[Support phone + hours]", "[Mailing address]"],
    "exit_title": "[Exit pop-up heading]",
    "exit_proof": "[Exit pop-up social proof line]",
}


def init_ecom_dfy_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS ecom_dfy_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, phone TEXT,
        bump INTEGER DEFAULT 0, source TEXT, page_url TEXT, created_at TEXT)""")
    con.commit()
    con.close()


@ecom_dfy_bp.route("/ecom-DFY")
@ecom_dfy_bp.route("/ecom-dfy")
def ecom_dfy_page():
    if request.path == "/ecom-dfy":
        return redirect("/ecom-DFY" + (("?" + request.query_string.decode()) if request.query_string else ""), 301)
    return render_template("ecom_dfy.html", c=CONTENT, wistia_id=WISTIA_ID,
                           checkout_ready=bool(STRIPE_FE))


@ecom_dfy_bp.route("/ecom-DFY/lead", methods=["POST"])
def ecom_dfy_lead():
    d = request.get_json(silent=True) or {}
    name, email, phone = (d.get(k, "").strip() for k in ("name", "email", "phone"))
    if not name or "@" not in email or sum(ch.isdigit() for ch in phone) < 7:
        return jsonify({"ok": False, "error": "Please fill in your name, email and phone."}), 400
    if not d.get("consent"):
        return jsonify({"ok": False, "error": "Please tick the consent box."}), 400
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO ecom_dfy_leads (name,email,phone,source,page_url,created_at) VALUES (?,?,?,?,?,?)",
                (name, email, phone, d.get("source", "step1"), d.get("page_url", ""),
                 datetime.datetime.utcnow().isoformat()))
    con.commit()
    con.close()
    return jsonify({"ok": True})


@ecom_dfy_bp.route("/ecom-DFY/checkout", methods=["POST"])
def ecom_dfy_checkout():
    d = request.get_json(silent=True) or {}
    link = STRIPE_FE_BUMP if (d.get("bump") and STRIPE_FE_BUMP) else STRIPE_FE
    if not link:
        return jsonify({"ok": False, "error": "Checkout isn't connected yet."}), 503
    email = (d.get("email") or "").strip()
    sep = "&" if "?" in link else "?"
    return jsonify({"ok": True, "url": link + (f"{sep}prefilled_email={email}" if "@" in email else "")})
