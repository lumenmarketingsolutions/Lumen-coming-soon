# agents/

Self-contained agents that run inside this app. Each agent gets its own folder,
versioned (`v1/`, `v2/`, …). `agents/<name>/__init__.py` re-exports the live
version so the rest of the app imports a stable path
(`from agents.<name> import agent`) regardless of which version is current.

- **`whatsapp_agent/`** — Claude-powered WhatsApp Cloud API outreach agent for the
  +1 623 512 6504 number. Current: `v1/`. See `whatsapp_agent/v1/README.md`.

The HTTP routes for an agent (webhooks, admin views) stay in `app.py` and the
Jinja templates stay in `templates/` — only the agent's own logic lives here.
