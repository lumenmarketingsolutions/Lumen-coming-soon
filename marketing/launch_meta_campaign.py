#!/usr/bin/env python3
"""
Launch the Lumen MES Quiz lead-gen campaign on Meta via the Marketing API.

Creates (everything PAUSED so you can review in Ads Manager before spending):
  Campaign "MES Quiz — Contractors"  (Outcome: Leads, CBO $20/day default)
    └─ Ad set "ID+UT — Broad"        (Website Lead event on the Lumen pixel,
                                      Idaho + Utah, 25-60, Advantage+ audience
                                      & placements)
        ├─ Ad — Secret   (4:5 feed / 1:1 square / 9:16 story via placement rules)
        ├─ Ad — Rival
        └─ Ad — Numbers

Requirements:
  * Python 3.8+ with `requests`  (pip install requests)
  * Env vars:
      META_ADS_TOKEN    access token with ads_management (+ pages/business access
                        to the Lumen page and ad account). NOTE: the Events
                        Manager CAPI token usually does NOT have ads_management —
                        generate one from Business Settings → System users, or
                        Graph API Explorer with ads_management scope.
      META_AD_ACCOUNT   ad account id, with or without the act_ prefix
      META_PAGE_ID      the Lumen Marketing Facebook page id
    Optional:
      DAILY_BUDGET_CENTS  campaign daily budget in cents (default 2000 = $20/day)
      META_PIXEL_ID       defaults to the Lumen pixel 1119566303064711

Run from the repo root (the script finds the creatives relative to itself):
  python3 marketing/launch_meta_campaign.py
"""

import os
import sys
import json
import base64

try:
    import requests
except ImportError:
    sys.exit("pip install requests, then re-run")

API = "https://graph.facebook.com/v19.0"
TOKEN = os.environ.get("META_ADS_TOKEN", "")
ACCOUNT = os.environ.get("META_AD_ACCOUNT", "").removeprefix("act_")
PAGE_ID = os.environ.get("META_PAGE_ID", "")
PIXEL_ID = os.environ.get("META_PIXEL_ID", "1119566303064711")
DAILY_BUDGET = int(os.environ.get("DAILY_BUDGET_CENTS", "2000"))

CREATIVE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "creatives")
BASE_URL = "https://lumenmarketing.co/score?utm_source=meta&utm_medium=paid&utm_campaign=mes-quiz&utm_content="

ADS = [
    {
        "key": "secret",
        "name": "Ad — Secret",
        "body": "Best work in town shouldn't mean best-kept secret in town. Score your marketing in 60 seconds — free.",
    },
    {
        "key": "rival",
        "name": "Ad — Rival",
        "body": "8 questions. Instant score. See exactly how you stack up against other home-service contractors near you.",
    },
    {
        "key": "numbers",
        "name": "Ad — Numbers",
        "body": "Lead flow. Speed. Follow-up. Reviews. Tracking. Score all 5 free and find out where you're leaking jobs.",
    },
]
HEADLINE = "What's Your Marketing Efficiency Score?"


def die(msg):
    sys.exit(f"\n✗ {msg}")


def call(method, path, **params):
    params["access_token"] = TOKEN
    r = requests.request(method, f"{API}/{path}", data=params, timeout=60)
    out = r.json()
    if r.status_code >= 300 or "error" in out:
        err = out.get("error", {})
        die(f"{path}: {err.get('message', r.text[:300])} "
            f"(code {err.get('code')}, subcode {err.get('error_subcode')})")
    return out


def check_env():
    missing = [n for n, v in [("META_ADS_TOKEN", TOKEN), ("META_AD_ACCOUNT", ACCOUNT), ("META_PAGE_ID", PAGE_ID)] if not v]
    if missing:
        die("Missing env vars: " + ", ".join(missing) + "  (see header of this script)")
    me = call("GET", "me?fields=name")
    print(f"✓ Token OK (acting as: {me.get('name', '?')})")
    acct = call("GET", f"act_{ACCOUNT}?fields=name,account_status,currency")
    if acct.get("account_status") != 1:
        die(f"Ad account {acct.get('name')} status={acct.get('account_status')} — not active")
    print(f"✓ Ad account: {acct['name']} ({acct['currency']})")
    page = call("GET", f"{PAGE_ID}?fields=name")
    print(f"✓ Page: {page['name']}")


def region_keys():
    keys = {}
    for state in ("Idaho", "Utah"):
        res = requests.get(f"{API}/search", params={
            "type": "adgeolocation", "location_types": '["region"]',
            "q": state, "country_code": "US", "access_token": TOKEN}, timeout=30).json()
        match = next((d for d in res.get("data", []) if d["name"] == state), None)
        if not match:
            die(f"Could not resolve region key for {state}")
        keys[state] = match["key"]
    print(f"✓ Region keys: {keys}")
    return list(keys.values())


def upload_images():
    hashes = {}
    for ad in ADS:
        for size in ("1080x1350", "1080x1080", "1080x1920"):
            fname = f"lumen-mes-ad-{ad['key']}-{size}.png"
            path = os.path.join(CREATIVE_DIR, fname)
            if not os.path.exists(path):
                die(f"Missing creative: {path} — run from the repo, creatives ship in marketing/creatives/")
            with open(path, "rb") as f:
                res = call("POST", f"act_{ACCOUNT}/adimages", bytes=base64.b64encode(f.read()).decode())
            img = list(res["images"].values())[0]
            hashes[(ad["key"], size)] = img["hash"]
            print(f"✓ Uploaded {fname}")
    return hashes


def create_campaign():
    res = call("POST", f"act_{ACCOUNT}/campaigns",
               name="MES Quiz — Contractors",
               objective="OUTCOME_LEADS",
               status="PAUSED",
               special_ad_categories="[]",
               daily_budget=DAILY_BUDGET,
               bid_strategy="LOWEST_COST_WITHOUT_CAP")
    print(f"✓ Campaign created: {res['id']} (PAUSED, ${DAILY_BUDGET/100:.0f}/day)")
    return res["id"]


def create_adset(campaign_id, regions):
    targeting = {
        "geo_locations": {"regions": [{"key": k} for k in regions]},
        "age_min": 25, "age_max": 60,
        "targeting_automation": {"advantage_audience": 1},
    }
    res = call("POST", f"act_{ACCOUNT}/adsets",
               name="ID+UT — Broad",
               campaign_id=campaign_id,
               status="PAUSED",
               billing_event="IMPRESSIONS",
               optimization_goal="OFFSITE_CONVERSIONS",
               promoted_object=json.dumps({"pixel_id": PIXEL_ID, "custom_event_type": "LEAD"}),
               targeting=json.dumps(targeting))
    print(f"✓ Ad set created: {res['id']}")
    return res["id"]


def create_creative(ad, hashes):
    """Placement-customized creative: 4:5 default, 1:1 right column, 9:16 stories/reels."""
    url = BASE_URL + ad["key"]
    labels = {s: f"{ad['key']}-{s}" for s in ("1080x1350", "1080x1080", "1080x1920")}
    asset_feed_spec = {
        "images": [
            {"hash": hashes[(ad["key"], s)], "adlabels": [{"name": labels[s]}]}
            for s in ("1080x1350", "1080x1080", "1080x1920")
        ],
        "bodies": [{"text": ad["body"]}],
        "titles": [{"text": HEADLINE}],
        "link_urls": [{"website_url": url}],
        "ad_formats": ["SINGLE_IMAGE"],
        "call_to_action_types": ["LEARN_MORE"],
        "asset_customization_rules": [
            {   # stories + reels get 9:16
                "customization_spec": {
                    "publisher_platforms": ["facebook", "instagram", "messenger"],
                    "facebook_positions": ["story", "facebook_reels"],
                    "instagram_positions": ["story", "reels"],
                    "messenger_positions": ["story"],
                },
                "image_label": {"name": labels["1080x1920"]},
                "priority": 1,
            },
            {   # right column / search get 1:1
                "customization_spec": {
                    "publisher_platforms": ["facebook"],
                    "facebook_positions": ["right_hand_column", "search"],
                },
                "image_label": {"name": labels["1080x1080"]},
                "priority": 2,
            },
            {   # everything else gets 4:5
                "customization_spec": {"age_min": 25, "age_max": 60},
                "image_label": {"name": labels["1080x1350"]},
                "priority": 3,
            },
        ],
    }
    try:
        res = call("POST", f"act_{ACCOUNT}/adcreatives",
                   name=f"MES {ad['name']}",
                   object_story_spec=json.dumps({"page_id": PAGE_ID}),
                   asset_feed_spec=json.dumps(asset_feed_spec))
    except SystemExit:
        # Fallback: plain single-image creative with the 4:5 (Meta auto-crops others)
        print(f"  ! placement customization rejected for {ad['key']}, falling back to single 4:5 image")
        res = call("POST", f"act_{ACCOUNT}/adcreatives",
                   name=f"MES {ad['name']} (single)",
                   object_story_spec=json.dumps({
                       "page_id": PAGE_ID,
                       "link_data": {
                           "image_hash": hashes[(ad["key"], "1080x1350")],
                           "link": url,
                           "message": ad["body"],
                           "name": HEADLINE,
                           "call_to_action": {"type": "LEARN_MORE"},
                       },
                   }))
    print(f"✓ Creative: {ad['name']} → {res['id']}")
    return res["id"]


def create_ad(adset_id, ad, creative_id):
    res = call("POST", f"act_{ACCOUNT}/ads",
               name=ad["name"],
               adset_id=adset_id,
               creative=json.dumps({"creative_id": creative_id}),
               status="PAUSED")
    print(f"✓ Ad created: {ad['name']} → {res['id']}")
    return res["id"]


if __name__ == "__main__":
    print("── Lumen MES Quiz — Meta campaign launcher ──")
    check_env()
    regions = region_keys()
    hashes = upload_images()
    campaign_id = create_campaign()
    adset_id = create_adset(campaign_id, regions)
    for ad in ADS:
        creative_id = create_creative(ad, hashes)
        create_ad(adset_id, ad, creative_id)
    print("\n✅ Done. Everything is created PAUSED.")
    print("   Review it in Ads Manager (check previews on all placements),")
    print("   then flip the campaign to Active. Track results at:")
    print("   https://lumenmarketing.co/admin/quiz")
