# ── /ecom-DFY — custom rebuild of the client's DFY ecom store funnel (off Funnelish).
#    Layout + mechanics match the original. CONTENT below is placeholder until the
#    client sends their copy, images, Wistia ID and Stripe payment links.
#    Payment is never collected here: step 2 hands off to the client's Stripe links.
import os, sqlite3, datetime, threading, html as html_lib
import requests
from flask import Blueprint, render_template, request, jsonify, redirect

ecom_dfy_bp = Blueprint("ecom_dfy", __name__)

# Railway persistent volume (/data), local dir for dev — same rule as app.py
DB_PATH = os.path.join("/data" if os.path.isdir("/data") else os.path.dirname(__file__), "ecom_dfy.db")

# Stripe Payment Links from the client's account (set in Railway).
STRIPE_FE = os.environ.get("ECOM_DFY_STRIPE_FE", "")        # $20 front end
STRIPE_FE_BUMP = os.environ.get("ECOM_DFY_STRIPE_BUMP", "")  # $20 + $49 order bump
WISTIA_ID = os.environ.get("ECOM_DFY_WISTIA_ID", "iz6rkfk6a3")  # client VSL (from their original page)

# Every string/image the page shows. Swap these when the client's files arrive.
# img values are paths under /static/ecom-dfy/ ; empty = labelled placeholder box.
FAQ_ANSWERS = [['Just one. The only account you’ll need is with an app called Shopify. Shopify is where we build your store and how you can edit it, control your products, and see your sales. It’s very user friendly, and we’ll help you figure things out if you have any questions.'], ['Because your store is built on Shopify, you’ll have to pay a monthly subscription fee. But don’t worry, it’s only $1 per month for the first three months. That means you have 90 days to test everything out and decide if this is something you want to keep investing in. After the first 90 days, you’ll be charged for their paid plan, which starts at $39 per month.', 'You can cancel at any time.'], ['Your success depends on a lot of factors, like how much work you put in, how much time and money you invest, and even how lucky you are. But we’re giving you everything we can to help.', 'With your digital franchise, you get a near-exact replica of my business, products that are proven to sell, and a free masterclass to help you get your first sales.', 'Even if you’ve never owned a business or done marketing before, this is a perfect foundation to get started.'], ['There are two paths you can take: free and paid. If you want to advertise your store for free, you can promote your products on social media, blogs, or online marketplaces. If you want to try paid advertising, you can get started with just $10 per day. We’ll cover everything in the bonus First Sale Masterclass that you get when you invest in your digital franchise.', 'You can also check out my YouTube channel for tons of free content that’ll help you grow your new business.'], ['Yes! Your digital franchise uses a model that my team and I call “Branded Dropshipping.” With Branded Dropshipping, you can sell to people around the world no matter where you live. That means you can live in Latin America and sell to the U.S. Or you can live in the Philippines and sell to Canada. Or you can live in Nigeria and sell to the U.K. As long as our suppliers can ship there, you can sell there.', '(That also means you earn in dollars, pounds, or euros!)'], ['Shopify makes payment processing easy! You just need to connect your bank account to your storefront, and you’ll receive payouts for all of the money you make.'], ['You own everything. After we deliver your store, you have full control and we’ll never ask you for any additional fees or payments. You become a real business owner, with a real asset.'], ['I genuinely believe that I could grow my own business to $100,000,000 if I wanted to. But I don’t. Think about all of the stress that comes with owning something so massive. I just want to relax, spend time with my wife and sons, and enjoy my life.'], ['It’s true that US$20 barely covers the costs I have for my team to build the stores, for us to advertise them, and for the other business expenses we have. But I’m okay with that. This stuff changed my life and helped me achieve the American dream. And I want to give you the same kind of opportunity.'], ['If you already have an online store and you’re not getting enough sales, it’s usually better to start over again from scratch. Let us build a new storefront for you based on my proven designs, with proven products, and proven suppliers.'], ['In 2026, online sales are predicted to reach $3.88T. By 2030, that number could reach $5T. I could sell one million digital franchises, and there would still be enough opportunity for all of us to earn our fair share.']]

CONTENT = {
    # Client copy (transcribed from their live page 08.10). Still placeholder: order bump,
    # step 2 buttons, exit pop-up, FAQ answers and all images (not visible on the source screenshot).
    "title": "Claim Your Digital Franchise | Only US$20",
    "countdown_label": "Your Digital Franchise Will Be Ready In:",
    "headline": "Claim Your Digital Franchise And Receive It In Less Than 24 Hours",
    "price_line": "Only US$20",
    "subhead": "Includes 20 Proven Products, A Professional Online Storefront, And",
    "subhead_link": "A Free Masterclass To Show You How To Get Your First Sale",
    "video_bar": "Hit Play On The Video",
    "social_proof_img": "social-proof.png",
    "form_title": "Enter Your Information Below To Claim Your Digital Franchise",
    "consent": "I consent to receiving email and SMS messages from AF Media LLC related to building and scaling my ecommerce business.",
    "step1_btn": "Go To Step #2",
    "privacy_note": "We Respect Your Privacy & Information",
    "bump_title": "[Order bump checkbox label]",
    "bump_badge": "[Bump badge]",
    "bump_text": "[Order bump description]",
    "complete_btn": "Complete My Order",
    "secure_note": "Secure checkout powered by Stripe. Your card details never touch this page.",
    "terms_url": "https://www.alexfedotoff.com/terms-and-conditions/", "privacy_url": "https://www.ecommercescalingsecrets.com/privacy-policy",
    "works_title_a": "How Your", "works_title_b": "Digital Franchise", "works_title_c": "Works",
    "steps": [
        {"img": "step-1.png", "title": "1. We Build Your Store", "body": [
            "You don\u2019t have to do any website design, product research, or tech setup. Instead, you get a near-exact copy of my successful Branded Dropshipping business.",
            "This includes the online storefront, proven products, and the back-end systems. Plus a bonus masterclass to show you how to get your first sales.",
            "No need to figure anything out yourself."]},
        {"img": "step-2.png", "title": "2. Your Suppliers Ship Directly To Your Customers", "body": [
            "When someone buys from your store, your suppliers handle fulfillment. That means they receive the order, package it, and ship it.",
            "You don\u2019t have to do a thing."]},
        {"img": "step-3.png", "title": "3. You Keep The Profits", "body": [
            "You set your own prices. Your suppliers charge you wholesale costs. You keep the difference.",
            "So if you sell a product for US$30 and your supplier charges you US$10, you keep US$20 from that sale!"]},
        {"img": "step-4.png", "title": "4. Grow At Your Pace, On Your Schedule", "body": [
            "Whether you work on this for an hour every night or a few hours on weekends, you decide how fast you grow. There\u2019s no boss. No clock to punch. No one telling you what to do.",
            "It\u2019s your business, and you make the rules."]},
    ],
    "cta": "Claim My Digital Franchise Now!",
    "cta_sub": "You\u2019re Protected By A 100% Money-Back Guarantee",
    "bio_title_a": "Why Trust Me To Build Your", "bio_title_b": "Digital Franchise", "bio_title_c": "?",
    "bio_img": "founder.png",
    "bio_intro": "Hi, I\u2019m Alex Fedotoff, founder of Brand Builders Academy and eCommerce Scaling Secrets.",
    "bio_body": ["I used to be a factory worker in Ukraine. No connections. No savings. No business background. Then, I discovered ecommerce and my life changed forever.",
                 "Since 2014, I\u2019ve\u2026"],
    "bio_points": ["Generated US$100,000,000+ in sales with my own brands",
                   "Been featured as a Forbes Magazine council member",
                   "Spoken on stage to share the latest digital marketing strategies (the same kind of content you get for free with your bonus First Sale Masterclass)",
                   "Helped 77,254+ people from all over the world get started with Branded Dropshipping"],
    "bio_close": "I\u2019m not a guru who makes a living selling courses. I\u2019m a practitioner who\u2019s still running his own brands. And the digital franchise you\u2019re about to get is built as a near-exact replica of my own business.",
    "model_title": "This Isn\u2019t Another Scammy Business Opportunity",
    "model_sub": "It\u2019s A Proven Model Responsible For Helping Thousands Of People Succeed Online",
    "model_body": ["You\u2019re not buying another course, coaching program, or mastermind subscription.",
                   "What you\u2019re investing in today is a proven business model called Branded Dropshipping. This model has already helped thousands of people succeed online.",
                   "It\u2019s a near-exact replica of my own business, built by my team, and owned entirely by you.",
                   "Think of it like buying a McDonald\u2019s franchise instead of trying to start your own restaurant from scratch. You get the systems, the products, and the guidance of what already works. But unlike a McDonald\u2019s franchise, you don\u2019t owe me anything after you take ownership of your store.",
                   "Every dollar you make is yours to keep, forever."],
    "model_bold": [2, 4],
    "model_img": "model.png",
    "gallery_title_a": "What Your", "gallery_title_b": "Digital Franchise", "gallery_title_c": "Could Look Like",
    "gallery": [f"gallery-{i}.png" for i in range(1, 9)],
    "stack_kicker": "Ready To Claim Your Digital Franchise?",
    "stack_title": "Here\u2019s Everything You Get For Only US$20",
    "stack_img": "stack.png",
    "stack": [
        {"b": "A Beautiful Online Storefront", "t": "professionally designed by my team and optimized to convert website visitors into buyers"},
        {"b": "20 Proven Products", "t": "hand-picked, pre-loaded into your store, and ready to sell"},
        {"b": "Our Custom Theme", "t": "that instantly positions your brand as premium, so your visitors will be happy to pay higher prices"},
        {"b": "Connections To Trusted Suppliers", "t": "who will take orders, package, and ship directly to your customers"},
        {"b": "Backend Automations", "t": "so you don\u2019t have to worry about any of the complicated tech stuff \u2013 it\u2019s all done for you!"},
        {"b": "BONUS: First Sale Masterclass", "t": "to teach you how to get your first sales, even if you\u2019ve never owned a business or marketed anything in your life"},
    ],
    "today_price": "Today: Only $20!",
    "assurances": [
        {"icon": "icon-guarantee.png", "title": "You\u2019re Protected By A 100% Money-Back Guarantee", "body": [
            "If you aren\u2019t completely blown away by your digital franchise and the new opportunities it opens up for you to create an online income, you\u2019re protected by a 100% money-back guarantee.",
            "Just reach out to my team, and we\u2019ll refund your purchase. No surveys. No questions. No hidden conditions."]},
        {"icon": "icon-secure.png", "title": "Secure Payment Processing", "body": [
            "This page is encrypted with the latest digital security technology so your information is 100% safe and secure."]},
        {"icon": "icon-help.png", "title": "Need Help With Your Order?", "body": [
            "If you need help with your order or if you have any questions before you invest, contact us at dfy@ecommercescalingsecrets.com or +1 (786) 464-5483."]},
    ],
    "faq_title_a": "Questions Others Asked Before Investing In Their", "faq_title_b": "Digital Franchise",
    "faq": [{"q": q, "a": a} for q, a in zip([
        "Do I need any special accounts before getting started?",
        "Are there any other costs after I invest US$20?",
        "Can I make this work even if I\u2019ve never owned a business or done marketing before?",
        "How much does it cost to advertise my store?",
        "Do these digital franchises work outside of the U.S.?",
        "How do I collect my money after I make sales?",
        "Do you still own a part of my store after I invest in the digital franchise?",
        "If these digital franchises are so good, why don\u2019t you just keep them for yourself?",
        "So if you\u2019re not making any money selling these digital franchises, why are you doing it?",
        "What if I already have an online store?",
        "Isn\u2019t the market too saturated by now?"], FAQ_ANSWERS)],
    "final_title_a": "Ready To Claim Your", "final_title_b": "Digital Franchise", "final_title_c": "?",
    "footer_logo": "final.png",
    "copyright": "This product is brought to you and copyrighted by Alex Fedotoff & Ecommerce Scaling Secrets, Copyright 2026",
    "footer_links": [("Data Protection", "https://www.alexfedotoff.com/data-protection/"),
                     ("Earnings Disclaimer", "https://www.alexfedotoff.com/earnings-disclaimer/"),
                     ("Privacy Policy", "https://www.alexfedotoff.com/privacy-policy/"),
                     ("Terms & Conditions", "https://www.alexfedotoff.com/terms-and-conditions/"),
                     ("GDPR", "https://www.alexfedotoff.com/gdpr/")],
    "disclaimers": [
        "We can not and do not make any guarantees about your ability to get results or earn any money with our ideas, information, tools, or strategies. What we can guarantee is your satisfaction with our training. We give you a 30-day 100% satisfaction guarantee on the products we sell, so if you are not happy for any reason with the quality of our training after going through at least 50% of the material, just ask for your money back. You should know that all products and services by our company are for educational and informational purposes only. Nothing on this page, any of our websites, or any of our content or curriculum is a promise or guarantee of results or future earnings, and we do not offer any legal, medical, tax or other professional advice. Any financial numbers referenced here, or on any of our sites, are illustrative of concepts only and should not be considered average earnings, exact earnings, or promises for actual or future performance. Use caution and always consult your accountant, lawyer or professional advisor before acting on this or any information related to a lifestyle change or your business or finances. You alone are responsible and accountable for your decisions, actions and results in life, and by your registration here you agree not to attempt to hold us liable for your decisions, actions or results, at any time, under any circumstance.",
        "The success of our students in the Done For You Ecom Stores varies significantly. While we provide all the tools and strategies that have worked for others, your individual success depends on various factors, including your background, dedication, desire, and motivation. We do not guarantee that you will achieve similar results to any examples shown.",
        "The testimonials on this page represent the experiences of individual users of our Done For You Ecom Stores. They are anecdotal only and may not represent the typical experience of all our users. The testimonials are not necessarily indicative of future performance or success of any other individuals. Your individual results may vary significantly.",
        "We make every effort to ensure that we accurately represent these products and services and their potential for income. Earning and Income statements made by our company and its customers are estimates of what we think you can possibly earn. There is no guarantee that you will make these levels of income, and you accept the risk that the earnings and income statements differ by individual. As with any business, your results may vary, and will be based on your individual capacity, business experience, expertise, and level of desire.",
        "The price listed for the Done For You Ecom Stores is subject to change without notice. This offer may contain additional products and services, including subscription-based services, which may incur additional charges. Please read the details of each offer carefully.",
    ],
    "contact_lines": ["Email: support@ecommercescalingsecrets.com",
                      "Phone: +1 (786) 464-5483 (Available Monday through Friday, 9 AM - 5 PM ET)",
                      "Mailing Address: 60 SW 13th St, Brickell, Miami, Florida",
                      "We strive to ensure all communication channels are open and readily available to you. Whether it\u2019s a question, comment, or concern, our team is ready to assist."],
    "exit_title": "Wait! Your Digital Franchise Is Still Reserved",
    "exit_proof": "Join 77,254+ people who got started with Branded Dropshipping.",
}


# ── Tracking: email to Kendall + GHL pipeline "Ecom DFY" (MK7 sub-account) ──
NOTIFY_TO = os.environ.get("ECOM_DFY_NOTIFY", "kendall@lumenmarketing.co")
GHL = "https://services.leadconnectorhq.com"
GHL_LOC = "6b4I6ILHBVcWQYlmPj3i"
GHL_PIPELINE = "QJ1glf6i0cGDOxHchLLN"
GHL_STAGES = [  # pipeline order; leads only ever move forward
    ("lead", "6295df55-a1e6-4832-b291-f2c9c5e668eb"),        # Form Filled (not paid)
    ("paid", "c6d4db86-eab6-4812-9742-d6620dcbf79e"),        # Paid
    ("onboarding", "e00912d2-46a9-47c1-add2-7428e96c685b"),  # Onboarding Received
    ("building", "5ea32b23-35f1-419c-a20f-8aacdf469002"),
    ("delivered", "1a7bd67e-a039-4c49-9ccb-9054203bee43"),
    ("claimed", "50a7005f-9f60-47d2-9cf9-d62c3c22b26c"),
]
STAGE_ID = dict(GHL_STAGES)
STAGE_RANK = {sid: i for i, (_, sid) in enumerate(GHL_STAGES)}


def _bg(fn, *args):
    threading.Thread(target=fn, args=args, daemon=True).start()


def _email(subject, title, rows):
    key = os.environ.get("RESEND_API_KEY", "")
    if not key:
        return
    body = "".join(f'<tr><td style="padding:7px 0;color:#777;width:150px;vertical-align:top">{html_lib.escape(k)}</td>'
                   f'<td style="padding:7px 0;font-weight:600">{html_lib.escape(v or "(left blank, we pick)")}</td></tr>'
                   for k, v in rows)
    html = (f'<div style="font-family:Arial,sans-serif;max-width:560px;margin:0 auto;padding:24px;color:#1a1a1a">'
            f'<div style="font-size:11px;letter-spacing:.2em;text-transform:uppercase;color:#999">Ecom DFY funnel</div>'
            f'<h2 style="margin:6px 0 18px">{html_lib.escape(title)}</h2><table style="width:100%;font-size:14px">{body}</table></div>')
    try:
        requests.post("https://api.resend.com/emails", timeout=15, headers={"Authorization": f"Bearer {key}"},
                      json={"from": "Ecom DFY <notifications@lumenmarketing.co>", "to": [NOTIFY_TO],
                            "subject": subject, "html": html})
    except Exception as e:
        print(f"[ecom-dfy] email failed: {e}")


def _ghl_track(name, email, phone, stage, note=None):
    pit = os.environ.get("GHL_MK7_PIT", "")
    if not pit or not email:
        return
    h = {"Authorization": f"Bearer {pit}", "Version": "2021-07-28", "Accept": "application/json"}
    try:
        c = {"locationId": GHL_LOC, "email": email, "source": "Ecom DFY funnel"}
        if name:
            c["name"] = name
        if phone:
            c["phone"] = phone
        r = requests.post(f"{GHL}/contacts/upsert", headers=h, json=c, timeout=15)
        cid = (r.json().get("contact") or {}).get("id") if r.ok else None
        if not cid:
            print(f"[ecom-dfy] GHL upsert failed: {r.status_code} {r.text[:200]}")
            return
        # tags are added (never replaced); green "paid" tag from the Paid stage on, so it shows on the card
        tags = ["ecom-dfy"] + (["paid"] if STAGE_RANK[STAGE_ID[stage]] >= STAGE_RANK[STAGE_ID["paid"]] else [])
        requests.post(f"{GHL}/contacts/{cid}/tags", headers=h, json={"tags": tags}, timeout=15)
        r = requests.get(f"{GHL}/opportunities/search", headers=h, timeout=15,
                         params={"location_id": GHL_LOC, "pipeline_id": GHL_PIPELINE, "contact_id": cid})
        opps = r.json().get("opportunities", []) if r.ok else []
        target = STAGE_ID[stage]
        if not opps:
            requests.post(f"{GHL}/opportunities/", headers=h, timeout=15, json={
                "pipelineId": GHL_PIPELINE, "locationId": GHL_LOC, "pipelineStageId": target, "status": "open",
                "contactId": cid, "name": f"{name or email} | DFY Store", "monetaryValue": 20})
        elif STAGE_RANK[target] > STAGE_RANK.get(opps[0]["pipelineStageId"], -1):
            requests.put(f"{GHL}/opportunities/{opps[0]['id']}", headers=h, timeout=15,
                         json={"pipelineId": GHL_PIPELINE, "pipelineStageId": target})
        if note:
            requests.post(f"{GHL}/contacts/{cid}/notes", headers=h, timeout=15, json={"body": note})
    except Exception as e:
        print(f"[ecom-dfy] GHL sync failed: {e}")


def _lead_tracking(name, email, phone):
    _email(f"New DFY lead: {name}", "Someone filled out the form", [("Name", name), ("Email", email), ("Phone", phone),
           ("Status", "Form filled, not paid yet")])
    _ghl_track(name, email, phone, "lead")


def _paid_tracking(name, email, phone):
    _email(f"DFY sale: {name or email} paid $20", "New $20 order", [("Name", name), ("Email", email), ("Phone", phone)])
    _ghl_track(name, email, phone, "paid")


def _onboarding_tracking(a):
    rows = [("Name", a["name"]), ("Email", a["email"]), ("Store name", a["store_name"]), ("Niche", a["niche"]),
            ("Style", a["style"]), ("Colors", a["colors"]), ("Anything else", a["notes"] or "(none)")]
    _email(f"DFY onboarding: {a['name'] or a['email']}", "Store preferences submitted", rows)
    note = "DFY store preferences\n" + "\n".join(f"{k}: {v or '(we pick)'}" for k, v in rows[2:])
    _ghl_track(a["name"], a["email"], a.get("phone", ""), "onboarding", note)


def init_ecom_dfy_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS ecom_dfy_leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, phone TEXT,
        bump INTEGER DEFAULT 0, source TEXT, page_url TEXT, created_at TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS ecom_dfy_onboarding (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, store_name TEXT, niche TEXT,
        style TEXT, colors TEXT, notes TEXT, created_at TEXT)""")
    con.commit()
    con.close()


@ecom_dfy_bp.route("/ecom-DFY")
@ecom_dfy_bp.route("/ecom-dfy")
def ecom_dfy_page():
    if request.path == "/ecom-dfy":
        return redirect("/ecom-DFY" + (("?" + request.query_string.decode()) if request.query_string else ""), 301)
    return render_template("ecom_dfy.html", c=CONTENT, wistia_id=WISTIA_ID,
                           checkout_ready=bool(STRIPE_FE),
                           bump_ready=bool(STRIPE_FE_BUMP))  # bump hidden until its own Payment Link exists


@ecom_dfy_bp.route("/ecom-DFY/thank-you")
def ecom_dfy_thanks():
    # Stripe Payment Link redirects here after a successful payment
    return render_template("ecom_dfy_thanks.html")


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
    _bg(_lead_tracking, name, email, phone)
    return jsonify({"ok": True})


@ecom_dfy_bp.route("/ecom-DFY/paid", methods=["POST"])
def ecom_dfy_paid():
    # Called once by the thank-you page (Stripe only sends buyers there after a successful payment)
    d = request.get_json(silent=True) or {}
    email = (d.get("email") or "").strip()
    if "@" in email:
        _bg(_paid_tracking, (d.get("name") or "").strip(), email, (d.get("phone") or "").strip())
    return jsonify({"ok": True})


@ecom_dfy_bp.route("/ecom-DFY/onboarding", methods=["POST"])
def ecom_dfy_onboarding():
    d = request.get_json(silent=True) or {}
    a = {k: (d.get(k) or "").strip()[:500] for k in ("name", "email", "phone", "store_name", "niche", "style", "colors", "notes")}
    if "@" not in a["email"]:
        return jsonify({"ok": False, "error": "Please enter the email you used at checkout."}), 400
    con = sqlite3.connect(DB_PATH)
    con.execute("INSERT INTO ecom_dfy_onboarding (name,email,store_name,niche,style,colors,notes,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (a["name"], a["email"], a["store_name"], a["niche"], a["style"], a["colors"], a["notes"],
                 datetime.datetime.utcnow().isoformat()))
    con.commit()
    con.close()
    _bg(_onboarding_tracking, a)
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
