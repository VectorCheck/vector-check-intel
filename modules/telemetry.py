"""
VECTOR CHECK AERIAL GROUP INC. — Telemetry & Audit Logging

FIX: Previously created a new Supabase client on every log_action() call.
Now uses a module-level cached client with lazy initialization.
FIX: Previously swallowed all exceptions silently with bare `pass`.
Now logs to stderr for operational visibility without crashing the dashboard.
"""

from supabase import create_client, Client
import streamlit as st
from datetime import datetime, timezone
import logging

logger = logging.getLogger("arms.telemetry")

# Module-level singleton — initialized once, reused for all log calls
_supabase_client: Client | None = None
_client_init_failed: bool = False


def _get_client() -> Client | None:
    """Returns the cached Supabase client, creating it on first call.

    If client creation fails, sets a flag so subsequent calls return
    None immediately without retrying (fail-fast on config errors).
    """
    global _supabase_client, _client_init_failed

    if _supabase_client is not None:
        return _supabase_client

    if _client_init_failed:
        return None

    try:
        url: str = st.secrets["supabase"]["url"]
        key: str = st.secrets["supabase"]["key"]
        _supabase_client = create_client(url, key)
        return _supabase_client
    except Exception as e:
        _client_init_failed = True
        logger.warning("Supabase telemetry client init failed: %s", e)
        return None


def log_action(operator_id: str, lat: float, lon: float, icao: str, action: str) -> None:
    """Logs user actions to the Supabase telemetry_logs table.

    Non-blocking: failures are logged to stderr but never crash the dashboard.
    """
    try:
        client = _get_client()
        if client is None:
            return

        data = {
            "operator_id": operator_id,
            "latitude": lat,
            "longitude": lon,
            "icao": icao,
            "action": action,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        client.table("telemetry_logs").insert(data).execute()
    except Exception as e:
        logger.debug("Telemetry log_action failed: %s", e)
