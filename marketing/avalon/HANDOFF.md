# Avalon Laser — July Meta Ad Creatives (HANDOFF)

Session handoff. A previous Claude Code session set up this pipeline and validated
the layout/type on a placeholder background (see `out/PREVIEW-placeholder-lhr-sandiego.png`).
Pick up from here.

## The task

Static ad creatives for Meta (FB/IG) for client **Avalon Laser** (med spa; locations:
San Diego, Carlsbad, Encinitas). Three new July offers, replicating the existing
Botox/Dysport ad style. **One creative per location** (that's how the campaign
launches), so the location name is on the creative.

**Workflow agreed with Kendall:** build ONE creative first — Laser Hair Removal,
San Diego, 1080×1080 — get his approval, iterate, THEN batch the full set.
Do not batch before sign-off. Quality bar: indistinguishable in polish from the
Botox/Dysport originals — this is a real paid campaign.

## Source request (email from Laleh, verbatim)

> Hi Kendall,
>
> I would like to use this image for our Laser Hair Removal Offer.
>
> The offer will be:
> 50% off your first Laser Hair Removal package.
> Offer good through the end of July
> Available at Avalon Laser San Diego, Carlsbad, and Encinitas.
>
> I want to use this image for our Tirzepitide offer
>
> The offer is: New Client Intro offer: 1st month $280- $70 per weekly shot
> (Not sure how you want to word this)
>
> Information to include:
> - GIP and GLP-1 receptor agonist
> - Can lose up to 15%-22% of total body weight
> - FDA approved
> - Physician supervised medical weight loss
> - Personlized treatment plans
>
> Offer good through the end of July
> Available at Avalon Laser San Diego, Carlsbad, and Encinitas.
>
> The last image is the creative I shared with you on canva.
>
> The offer is for:
> Clear & Brilliant Photofractional Laser
> New Client Intro Offer: $380 for the first session
>
> Information to include:
> - Impactful results with minimal to no downtime
> - Work on texture by softening lines and wrinkles, stimulating new collagen,
>   resurfacing scarring, tightening pores and adding moisture.
> - Work on hyperpigmentation, photo damage, and treat melasma.
>
> Offer good through the end of July
> Available at Avalon Laser San Diego, Avalon Laser Carlsbad, and Avalon Laser Encinitas.

## Style system (from existing Botox/Dysport creatives — match exactly)

Existing ads are a 2-card carousel:

**Card 1 (photo card):**
- Full-bleed editorial beach stock photo (sun-drenched, airy, premium)
- Treatment name in huge heavy condensed all-caps ("BOTOX" / "DYSPORT") —
  white over water/dark areas, forest green over light sky/sand
- Price line in same heavy type: "$10 /PER UNIT" — green if headline white,
  dark charcoal if headline green; slash before unit is italic
- Qualifier in clean grotesque: "*New Patients | Any Location" → replace
  "Any Location" with the specific location per variant
- Small line: "Offer good through July 31"
- AVALON LASER wordmark: "AVALON" bold + "LASER" light, condensed caps,
  top-left or bottom-left wherever the photo is clean; white or green to suit
- Text block left-aligned over the calmest photo area

**Card 2 (green detail card):**
- Solid sage/forest green bg (~#5d9367), stacked all-caps chunky white display type
- White rounded pill at bottom: "Comment or DM to lock in your special pricing"
- Carries each offer's bullet list (photo card stays minimal)

**Type/colors (validated in build.js):** Anton (headline/price), Oswald 600/300
(AVALON|LASER wordmark), Inter (qualifiers). Accent green #4E8A50, wordmark
dark green #2E6B3A, charcoal #2b2b2b.

## Photo mapping (photos go in ./photos/)

- `photos/lhr.jpg` — Laser Hair Removal: beach photo, woman in straw hat sitting
  facing the ocean (clean equivalent of the old BOTOX creative photo)
- `photos/tirz.jpg` — Tirzepatide: beach photo, woman on white sand, hair in bun,
  looking over her shoulder (from the old DYSPORT creative)
- `photos/cnb.jpg` — Clear & Brilliant: TBD, best remaining provided photo.
  NOTE: Laleh's Canva reference for this offer is a cream/olive spa style — we are
  NOT copying that; bring this offer into the beach system for consistency.
  Kendall has 3 clean candidate photos (straw hat covering face / seated sunset
  from behind / headscarf portrait) — he will provide them.

## Offer copy per creative

1. LASER HAIR REMOVAL — `50% OFF /FIRST PACKAGE`
2. TIRZEPATIDE — `$280 /FIRST MONTH` + subline `$70 per weekly shot`
   (Laleh unsure on wording — improve if cleaner, flag changes to Kendall)
3. CLEAR & BRILLIANT — `$380 /FIRST SESSION` + subline `Photofractional Laser`

All: `*New Patients | {Location}` + `Offer good through July 31`.
Bullets from the email go on each offer's green card 2, not the photo card.

## Pipeline (working — see build.js)

HTML/CSS at exact pixel size → headless Chromium (Playwright) screenshot.

- `npm install playwright-core @fontsource/anton @fontsource/oswald @fontsource/inter`
- Gotcha (solved): fonts don't load via `page.setContent` — write HTML to disk,
  `page.goto(file://...)`, `await page.evaluate(() => document.fonts.ready)`
- Chromium executablePath in build.js points at the remote container's path
  (`/opt/pw-browsers/...`) — adjust locally (or use full `playwright` package)
- Usage: `node build.js lhr-sandiego 1080x1080 [photoPath]`
  IDs: `{lhr|tirz|cnb}-{sandiego|carlsbad|encinitas}`
- Without a photo arg/file it renders a placeholder gradient
- Card 2 (green detail card) is NOT built yet — add it

## Deliverables

1. FIRST: `lhr-sandiego` 1080×1080 with real photo → Kendall approves → iterate
2. Then: 9 photo cards (3 offers × 3 locations) in 1080×1080 + 1080×1350,
   plus 3 green detail cards (one per offer, location-agnostic).
   1080×1920 story versions only if asked.
3. Naming: `avalon-{offer}-{location}-{WxH}.png` → `marketing/avalon/out/`

## Open items

- Kendall to provide the 3 clean photos (chat-paste works in a local session)
- Clear & Brilliant photo choice needs Kendall's confirmation
- Tirzepatide price wording needs a final OK
- Old LHR creative (razor/gradient) is below the Botox/Dysport standard — the plan
  restyles LHR into the beach system; flag to Laleh if she wants her old look
