# WhatsApp Outreach Agent — v1

Claude-powered WhatsApp Cloud API agent for MK7 Media. It sends an approved
message template to a lead, then — once the lead replies — runs the conversation
as Kendall, qualifies them, and pushes toward a quick call. It flags `[[HANDOFF]]`
when a human should take over (real fit, asks for a person, upset/confused).

Implementation: `agent.py` in this folder. The HTTP routes live in the app
itself (`app.py` at the repo root): `GET/POST /webhooks/whatsapp`,
`GET /admin/whatsapp` (viewer + outreach form, admin-login protected),
`POST /admin/whatsapp/send`, `POST /admin/whatsapp/handoff`. The viewer template
is `templates/admin_whatsapp.html`.

## The number / Meta setup
- Phone: **+1 623 512 6504** — Phone Number ID `1082296231636502`,
  WABA `1457517218983357`, app "MK7 messaging" (App ID `2107067100091646`).
- Webhook callback URL: `https://mk7media.com/webhooks/whatsapp` — subscribe the
  app to the `messages` field in App Dashboard → WhatsApp → Configuration.
- Outreach template: **`lumen_inbound_followup`** (language `en`), one named body
  variable `{{customer_name}}` — auto-filled from the lead's first name when you
  send via `/admin/whatsapp/send` and leave the variables field blank.

## Behaviour
The persona and rules are the `SYSTEM_PROMPT` constant near the top of `agent.py`.
The template opens with "this is Kendall from MK7 Media", so the agent continues
the conversation in the first person as Kendall.

## Env vars (Railway → mk7media service → Variables)
| var | meaning |
|---|---|
| `WHATSAPP_ACCESS_TOKEN` | permanent **System User** token, scopes `whatsapp_business_messaging` + `whatsapp_business_management`, assigned to the WABA. (Graph API Explorer tokens expire — don't use one here.) |
| `WHATSAPP_APP_SECRET` | app secret — verifies the `X-Hub-Signature-256` header on incoming webhooks. Optional: if unset, the check is skipped (with a warning) so the webhook still works. |
| `WHATSAPP_VERIFY_TOKEN` | must match the "Verify token" you enter in the Meta webhook config. Defaults to `mk7-whatsapp-verify`. |
| `ANTHROPIC_API_KEY` | powers the agent's replies. |
| `WHATSAPP_AGENT_MODEL` | model id. Defaults to `claude-opus-4-7`; set `claude-sonnet-4-6` for a cheaper high-volume bot. |
| `WHATSAPP_AUTO_REPLY` | set to `0` to disable auto-replies — the agent then only logs inbound messages and emails the team. |
| `WHATSAPP_DEFAULT_TEMPLATE` / `WHATSAPP_DEFAULT_TEMPLATE_LANG` | override the kickoff template name/language (defaults `lumen_inbound_followup` / `en`). |
| `WHATSAPP_DB_PATH` | SQLite path. Defaults to `whatsapp.db` (on the Railway disk). |
| `WHATSAPP_NOTIFY_EMAILS` | comma-separated team emails for handoff / failure notices. Uses the app's `RESEND_API_KEY`. |
| `WHATSAPP_PHONE_NUMBER_ID` / `WHATSAPP_WABA_ID` | only needed if the number ever changes — defaults are baked in. |

## Storage
SQLite (`whatsapp.db` on the Railway disk): `wa_contacts` (one row per number,
with `status` = active / handed_off / opted_out) and `wa_messages` (full in/out
log, deduped on the WhatsApp message id). If the Railway disk resets on redeploy
this resets too — fine for now; move to Postgres if continuity matters later.

## Cutting v2
Copy this `v1/` folder to a `v2/` sibling, make changes there, then update
`agents/whatsapp_agent/__init__.py` to `from .v2 import agent`. Nothing else in
the app needs to change — it only imports `agents.whatsapp_agent.agent`.
