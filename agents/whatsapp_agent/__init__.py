"""MK7 WhatsApp outreach agent — versioned.

`agent` always points at the live version. To cut v2: copy v1/ to v2/,
make your changes there, then change this import to `from .v2 import agent`.
The rest of the app only ever imports `agents.whatsapp_agent.agent`.
"""
from .v1 import agent  # noqa: F401
