# SCE Boise — Father's Day Funnel · Deploy Guide

Build-your-own funnel: pick a car, duration, Anderson Reserve bourbon gift card, Modern BBQ Supply gift card → lead capture → Stripe Checkout → email + Meta CAPI tracking.

---

## What lives where

| File | Purpose |
|---|---|
| `sce_fathersday.py` | Flask blueprint. Pricing config (edit here), routes, DB, Stripe Checkout Session, lead email, Meta CAPI |
| `templates/sce_fd_landing.html` | Single-page builder + lead capture. SCE orange branding, mobile-first |
| `templates/sce_fd_booked.html` | Success page after Stripe redirect |
| `static/sce-fathersday/*.jpg` | Car photos (C8 Z06, GT3RS, Urus S, G63) |
| `app.py` | Blueprint registered on line ~86. `supercarexp.*` host redirect now points to `/fathersday` |

---

## Required environment variables

Add this to **Railway → SCE project → Variables** if not already present:

```
STRIPE_SECRET_KEY=sk_live_xxxx  (or STRIPE_SECRET_KEY_SCE)
```

Already configured from Mother's Day (verify they're still set):
```
RESEND_API_KEY=re_xxxx
META_PIXEL_ID_SCE=1514374663034732
META_CAPI_TOKEN_SCE=EAAxxxx
FD_BASE_URL=https://supercarexp.lumenmarketing.co   # optional, defaults to this
```

---

## Routes (after deploy)

| URL | What it does |
|---|---|
| `https://supercarexp.lumenmarketing.co/` | Auto-redirects to `/fathersday` |
| `https://supercarexp.lumenmarketing.co/fathersday` | Landing + builder (the customer-facing funnel) |
| `https://supercarexp.lumenmarketing.co/fathersday/optin` | Form POST → creates Stripe session → redirects to Stripe |
| `https://supercarexp.lumenmarketing.co/fathersday/booked` | Success page Stripe redirects to |
| `https://supercarexp.lumenmarketing.co/fathersday/stripe-webhook` | Webhook endpoint (register this with Stripe) |
| `https://supercarexp.lumenmarketing.co/mothersday` | Mother's Day funnel still accessible (in case of refund / lookup) |

---

## Stripe setup (one-time, ~5 min)

1. **Stripe Dashboard → Developers → Webhooks → Add endpoint**
   - URL: `https://supercarexp.lumenmarketing.co/fathersday/stripe-webhook`
   - Events to send: **`checkout.session.completed`**
   - (Optional, harden later: copy signing secret into `STRIPE_WEBHOOK_SECRET_FD` and verify in code)

2. **Verify Stripe key is live mode** (`sk_live_...`) not test mode in production.

3. **Test the flow:** open the landing page, build a package with your own email, submit → should land on Stripe Checkout. Cancel out (don't actually pay) to verify the redirect back works.

---

## Pricing — where to edit

All pricing lives in `sce_fathersday.py` near the top:

```python
RENTAL_PRICING = {
    ("c8z06", "4h"):  399,
    ("c8z06", "8h"):  599,
    ("c8z06", "24h"): 799,
    # ... etc
}

ANDERSON_RESERVE_VALUES  = [100, 150, 200]
MODERN_BBQ_SUPPLY_VALUES = [25, 50, 100]

BUNDLE_PREMIUM = 0   # Set to e.g. 50 to add a $50 bundle markup
```

Change a number, redeploy, the JS in the landing page auto-picks up the new prices (passed via Jinja context). **Server is the source of truth on the total** — the JS calc is for display only.

---

## Deploy to Railway

This repo deploys on push to `main` (assuming Railway is wired to GitHub).

```bash
cd /Users/kendalldavis/lumen-coming-soon
git add sce_fathersday.py \
        templates/sce_fd_landing.html \
        templates/sce_fd_booked.html \
        static/sce-fathersday/ \
        app.py \
        SCE_FATHERSDAY_DEPLOY.md
git commit -m "SCE Father's Day funnel: build-your-own + Stripe Checkout"
git push origin main
```

Railway picks up the push and redeploys (~60-90 sec).

---

## After deploy — smoke test (3 min)

1. Visit `https://supercarexp.lumenmarketing.co/` → should redirect to `/fathersday`
2. Build a package (pick car, duration, AR amount, MBS amount)
3. Watch the sticky bottom CTA update with the live total
4. Enter your real email + phone, hit **Reserve**
5. You should land on Stripe Checkout with the line items itemized
6. Check your inbox for the lead notification email (sent to `kendall@lumenmarketing.co` + `n.wilkinson@launchpoint.dev`)
7. (Optional) Complete the payment in test mode to verify the webhook fires + Purchase event lands in Meta Events Manager

---

## Mobile checklist (the user explicitly called this out)

Tested in the design:
- All text wraps cleanly on iPhone 12/13/14/15 widths (375px, 390px, 393px, 430px)
- Car cards are 2x2 grid on mobile, 1x4 on tablet+
- Duration + gift card chips are 3-column grid (fits at 320px+)
- Sticky bottom CTA stays out of the way; respects iOS safe area inset
- Tap targets ≥ 44px (Apple's HIG minimum)
- Inputs use system font 16px to avoid iOS auto-zoom

If you find a wrap issue or layout glitch on real device, the CSS is at the top of `templates/sce_fd_landing.html` (inline `<style>` block).

---

## Dashboards / where to see leads

Leads land in `fathersday_leads` table in `waitlist.db`. You can query via the existing SCE admin tool (`sce_admin.py`) — if it needs to show FD leads, add a new view there.

Resend email notification fires on every lead.

Meta CAPI fires both Lead (on form submit) and Purchase (on Stripe webhook) with `event_id` dedup against the browser-side pixel.

---

## Reverting back to Mother's Day (if needed)

In `app.py`, find:

```python
if host.startswith("supercarexp."):
    return redirect(url_for("sce_fd.landing"))
```

Change to:

```python
return redirect(url_for("sce_md.landing"))
```

Both funnels coexist at their respective paths regardless.

---

## What to do when Nate finalizes pricing

1. Edit `RENTAL_PRICING` and (optionally) `BUNDLE_PREMIUM` in `sce_fathersday.py`
2. Commit + push
3. Refresh the live funnel — JS picks up the new constants automatically
