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
    # Client copy (transcribed from their live page 08.10). Still placeholder: order bump,
    # step 2 buttons, exit pop-up, FAQ answers and all images (not visible on the source screenshot).
    "title": "Claim Your Digital Franchise | Only US$20",
    "countdown_label": "Your Digital Franchise Will Be Ready In:",
    "headline": "Claim Your Digital Franchise And Receive It In Less Than 24 Hours",
    "price_line": "Only US$20",
    "subhead": "Includes 20 Proven Products, A Professional Online Storefront, And",
    "subhead_link": "A Free Masterclass To Show You How To Get Your First Sale",
    "video_bar": "Hit Play On The Video",
    "social_proof_img": "",
    "form_title": "Enter Your Information Below To Claim Your Digital Franchise",
    "consent": "I consent to receiving email and SMS messages from AF Media LLC related to building and scaling my ecommerce business.",
    "step1_btn": "Go To Step #2",
    "privacy_note": "We Respect Your Privacy & Information",
    "bump_title": "[Order bump checkbox label]",
    "bump_badge": "[Bump badge]",
    "bump_text": "[Order bump description]",
    "complete_btn": "Complete My Order",
    "secure_note": "Secure checkout powered by Stripe. Your card details never touch this page.",
    "terms_url": "#", "privacy_url": "#",
    "works_title_a": "How Your", "works_title_b": "Digital Franchise", "works_title_c": "Works",
    "steps": [
        {"img": "", "title": "1. We Build Your Store", "body": [
            "You don\u2019t have to do any website design, product research, or tech setup. Instead, you get a near-exact copy of my successful Branded Dropshipping business.",
            "This includes the online storefront, proven products, and the back-end systems. Plus a bonus masterclass to show you how to get your first sales.",
            "No need to figure anything out yourself."]},
        {"img": "", "title": "2. Your Suppliers Ship Directly To Your Customers", "body": [
            "When someone buys from your store, your suppliers handle fulfillment. That means they receive the order, package it, and ship it.",
            "You don\u2019t have to do a thing."]},
        {"img": "", "title": "3. You Keep The Profits", "body": [
            "You set your own prices. Your suppliers charge you wholesale costs. You keep the difference.",
            "So if you sell a product for US$30 and your supplier charges you US$10, you keep US$20 from that sale!"]},
        {"img": "", "title": "4. Grow At Your Pace, On Your Schedule", "body": [
            "Whether you work on this for an hour every night or a few hours on weekends, you decide how fast you grow. There\u2019s no boss. No clock to punch. No one telling you what to do.",
            "It\u2019s your business, and you make the rules."]},
    ],
    "cta": "Claim My Digital Franchise Now!",
    "cta_sub": "You\u2019re Protected By A 100% Money-Back Guarantee",
    "bio_title_a": "Why Trust Me To Build Your", "bio_title_b": "Digital Franchise", "bio_title_c": "?",
    "bio_img": "",
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
    "gallery_title_a": "What Your", "gallery_title_b": "Digital Franchise", "gallery_title_c": "Could Look Like",
    "gallery": ["", "", "", "", "", ""],
    "stack_kicker": "Ready To Claim Your Digital Franchise?",
    "stack_title": "Here\u2019s Everything You Get For Only US$20",
    "stack_img": "",
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
        {"icon": "", "title": "You\u2019re Protected By A 100% Money-Back Guarantee", "body": [
            "If you aren\u2019t completely blown away by your digital franchise and the new opportunities it opens up for you to create an online income, you\u2019re protected by a 100% money-back guarantee.",
            "Just reach out to my team, and we\u2019ll refund your purchase. No surveys. No questions. No hidden conditions."]},
        {"icon": "", "title": "Secure Payment Processing", "body": [
            "This page is encrypted with the latest digital security technology so your information is 100% safe and secure."]},
        {"icon": "", "title": "Need Help With Your Order?", "body": [
            "If you need help with your order or if you have any questions before you invest, contact us at dfy@ecommercescalingsecrets.com or +1 (786) 464-5483."]},
    ],
    "faq_title_a": "Questions Others Asked Before Investing In Their", "faq_title_b": "Digital Franchise",
    "faq": [{"q": q, "a": ["[Answer]"]} for q in [
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
        "Isn\u2019t the market too saturated by now?"]],
    "final_title_a": "Ready To Claim Your", "final_title_b": "Digital Franchise", "final_title_c": "?",
    "footer_logo": "",
    "copyright": "This product is brought to you and copyrighted by Alex Fedotoff & Ecommerce Scaling Secrets, Copyright 2026",
    "footer_links": [("Data Protection", "#"), ("Earnings Disclaimer", "#"), ("Privacy Policy", "#"),
                     ("Terms & Conditions", "#"), ("GDPR", "#")],
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
