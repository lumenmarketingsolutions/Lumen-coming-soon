"""
MK7 WhatsApp Outreach Agent — Phase 1 (foundation).

Sibling module to outreach.py. Same mental model: audiences → templates →
campaigns → sends, but channel is WhatsApp (Twilio or Meta Cloud API) instead
of Resend / email.

**Phase 1 (this file, today):**
- Sidebar entry + Mission Control + DB schema
- Tables created in setters.db (same DB as the CRM and email outreach) so we
  can join across channels for the cross-channel bridge later
- US-only constraint surfaced in the UI because the sending number is +1
- All routes admin-only — same login as the rest of the CRM

**Phase 2 (next session, blocked on Kendall):**
- Wire the actual send pipeline once we know where the existing WhatsApp portal
  code lives (https://whatsapp.mk7media.com/admin/whatsapp). Two options:
    (a) port the working send code into this repo and use the same Twilio/Meta
        creds, or
    (b) bridge via HTTP to the existing portal's API if it exposes one.
- WhatsApp template management (synced from Meta Business Manager or typed in)
- Inbound webhook → conversation thread in /crm/whatsapp/inbox

**Phase 3:**
- Lead-scraping agents for WhatsApp (phone-focused prompt)
- Cross-channel bridge: derive a WhatsApp audience from an email audience by
  pulling phone numbers out of extras_json, and vice versa
- Wizard channel selection ("email or WhatsApp?")
"""

from flask import Blueprint, render_template, request, jsonify, abort
import os, datetime, json, sqlite3

from crm import db, current_user, admin_required, now_iso

whatsapp_bp = Blueprint("whatsapp", __name__, url_prefix="/crm/whatsapp")


# ── Config ────────────────────────────────────────────────────────────────────
# The +1 sending number is Lumen's Twilio WhatsApp Business number — explicitly
# US-only because cross-border WhatsApp Business requires a number registered
# in the destination region. We surface this in the UI so the user (and the
# agent wizard later) doesn't accidentally target non-US leads.
WHATSAPP_SENDER_NUMBER = os.environ.get("WHATSAPP_SENDER_NUMBER", "+1 (635) 126-5040")
WHATSAPP_COUNTRY_CONSTRAINT = "United States"


# ── DB schema ─────────────────────────────────────────────────────────────────
def init_whatsapp_db():
    """Idempotent. Tables prefixed `wa_*` to match the existing whatsapp.db
    schema. Lives in setters.db (the CRM's DB) so we can join with leads,
    audiences, and the email outreach side later."""
    con = db()
    con.executescript("""
    -- Contacts: deduped by E.164 phone number. We may import these from the
    -- email side later (when a scraped lead also has a phone).
    CREATE TABLE IF NOT EXISTS wa_contacts (
        wa_id TEXT PRIMARY KEY,            -- E.164 phone number (e.g. +14155551234)
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        source TEXT,                       -- 'csv' | 'scraper' | 'bridge' | 'manual'
        opted_out INTEGER NOT NULL DEFAULT 0,
        notes TEXT,
        created_at TEXT NOT NULL,
        last_inbound_at TEXT,
        last_outbound_at TEXT
    );

    -- Audiences for WhatsApp campaigns. Same shape as outreach_audiences.
    CREATE TABLE IF NOT EXISTS wa_audiences (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        note TEXT,
        source TEXT NOT NULL DEFAULT 'csv',   -- csv | derived | bridge_from_email
        parent_audience_id INTEGER,
        member_count INTEGER NOT NULL DEFAULT 0,
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS wa_audience_members (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        audience_id INTEGER NOT NULL REFERENCES wa_audiences(id) ON DELETE CASCADE,
        wa_id TEXT NOT NULL,                 -- references wa_contacts.wa_id (loose FK)
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        extra_json TEXT,
        created_at TEXT NOT NULL,
        UNIQUE(audience_id, wa_id)
    );
    CREATE INDEX IF NOT EXISTS idx_wam_audience ON wa_audience_members(audience_id);
    CREATE INDEX IF NOT EXISTS idx_wam_wa_id    ON wa_audience_members(wa_id);

    -- Templates. WhatsApp Business templates have a `name` and `language`
    -- registered with Meta — for v1 we just store a name; later we'll sync
    -- the catalog from Meta.
    CREATE TABLE IF NOT EXISTS wa_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,                 -- Meta-registered template name
        language TEXT NOT NULL DEFAULT 'en_US',
        body_preview TEXT,                   -- the rendered body for the admin's reference
        variables_json TEXT,                 -- list of variable names the template expects
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS wa_campaigns (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        template_id INTEGER NOT NULL REFERENCES wa_templates(id),
        audience_id INTEGER NOT NULL REFERENCES wa_audiences(id),
        status TEXT NOT NULL DEFAULT 'draft',
            -- draft | sending | paused | completed | canceled
        send_window_hours REAL NOT NULL DEFAULT 4.0,
        started_at TEXT,
        completed_at TEXT,
        total_queued INTEGER NOT NULL DEFAULT 0,
        created_by_user_id INTEGER REFERENCES users(id),
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS wa_sends (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        campaign_id INTEGER NOT NULL REFERENCES wa_campaigns(id) ON DELETE CASCADE,
        member_id INTEGER REFERENCES wa_audience_members(id) ON DELETE SET NULL,
        wa_id TEXT NOT NULL,
        first_name TEXT,
        last_name TEXT,
        company TEXT,
        status TEXT NOT NULL DEFAULT 'queued',
            -- queued | sending | sent | delivered | read | failed | opted_out
        scheduled_at TEXT NOT NULL,
        sent_at TEXT,
        delivered_at TEXT,
        read_at TEXT,
        replied_at TEXT,
        failed_at TEXT,
        wamid TEXT,                          -- WhatsApp message ID returned by sender
        error TEXT,
        retry_count INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_was_campaign ON wa_sends(campaign_id);
    CREATE INDEX IF NOT EXISTS idx_was_queue    ON wa_sends(status, scheduled_at);
    CREATE INDEX IF NOT EXISTS idx_was_wamid    ON wa_sends(wamid);

    -- Inbound messages (replies). Populated by the webhook from Twilio/Meta.
    CREATE TABLE IF NOT EXISTS wa_messages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wa_id TEXT NOT NULL,
        campaign_id INTEGER REFERENCES wa_campaigns(id) ON DELETE SET NULL,
        send_id INTEGER REFERENCES wa_sends(id) ON DELETE SET NULL,
        direction TEXT NOT NULL,             -- 'in' | 'out'
        msg_type TEXT NOT NULL DEFAULT 'text',
        body TEXT,
        wamid TEXT UNIQUE,
        status TEXT,
        received_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_wamsg_wa  ON wa_messages(wa_id, received_at);

    -- Global opt-out list (deduped against on every send).
    CREATE TABLE IF NOT EXISTS wa_suppressions (
        wa_id TEXT PRIMARY KEY,
        reason TEXT NOT NULL,
        source_campaign_id INTEGER,
        created_at TEXT NOT NULL
    );
    """)
    con.commit()
    con.close()


# ── Cross-channel bridge: counts ──────────────────────────────────────────────
def cross_channel_signal(con):
    """Cheap stat: how many email-side audience members ALSO have a phone
    number stashed in extras_json? That's the latent pool of leads we could
    bridge from email scraping into a WhatsApp audience later."""
    try:
        # extras_json on outreach_audience_members stores arbitrary CSV columns
        # from the scrape (phone, instagram, etc.). LIKE match is cheap enough
        # at this scale and avoids parsing JSON for every row.
        row = con.execute("""
            SELECT COUNT(*) FROM outreach_audience_members
            WHERE extra_json LIKE '%"phone":%' AND extra_json NOT LIKE '%"phone": null%'
        """).fetchone()
        return row[0] or 0
    except sqlite3.OperationalError:
        return 0


# ── Mission Control ───────────────────────────────────────────────────────────
@whatsapp_bp.route("/")
@admin_required
def dashboard():
    u = current_user()
    con = db()
    counts = {
        "agents": 0,           # Phase 3 — no agents table yet
        "templates": con.execute("SELECT COUNT(*) FROM wa_templates").fetchone()[0],
        "audiences": con.execute("SELECT COUNT(*) FROM wa_audiences").fetchone()[0],
        "campaigns": con.execute("SELECT COUNT(*) FROM wa_campaigns").fetchone()[0],
        "contacts": con.execute("SELECT COUNT(*) FROM wa_contacts").fetchone()[0],
        "email_leads_with_phone": cross_channel_signal(con),
    }
    con.close()
    return render_template(
        "crm/whatsapp_dashboard.html",
        u=u, counts=counts,
        sender_number=WHATSAPP_SENDER_NUMBER,
        country_constraint=WHATSAPP_COUNTRY_CONSTRAINT,
    )


# Init on import
init_whatsapp_db()
