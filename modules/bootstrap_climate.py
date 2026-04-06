#!/usr/bin/env python3
"""
VECTOR CHECK AERIAL GROUP INC. — Climate Bootstrap Script

Run once to pre-compute 30-year ERA5 climate normals for all VCAG
detachment sites across all 12 months. Stores results in Supabase.

USAGE:
    python bootstrap_climate.py

PREREQUISITES:
    - Open-Meteo Professional plan (for Historical Weather API access)
    - Supabase tables created (run supabase_climate_schema.sql first)
    - Environment variables or .streamlit/secrets.toml configured:
        SUPABASE_URL, SUPABASE_KEY, OPEN_METEO_API_KEY (optional)

ESTIMATED RUNTIME:
    ~10-15 minutes (5 sites × 12 months × 30 years of ERA5 data)

ESTIMATED API COST:
    ~720 fractional API calls (of 5M monthly Professional allowance)
"""

import os
import sys
import time

# Allow running from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client
from modules.climate_ingest import get_climate_context

# =============================================================================
# CONFIGURATION
# =============================================================================

# VCAG Detachment Coordinates (same as USER_DEFAULTS in app.py)
SITES = {
    "VCAG HQ (Belleville, ON)":   {"lat": 44.1628, "lon": -77.3832},
    "Vector1 (Cold Lake, AB)":    {"lat": 54.4642, "lon": -110.1825},
    "Vector2 (Petawawa, ON)":     {"lat": 45.9003, "lon": -77.2818},
    "Vector3 (Bagotville, QC)":   {"lat": 48.3303, "lon": -70.9961},
    "Vector4 (Toronto, ON)":      {"lat": 43.6532, "lon": -79.3832},
}

# Read secrets from environment or .streamlit/secrets.toml
def _get_config():
    """Reads Supabase and Open-Meteo config from environment or secrets file."""
    # Try environment variables first
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_KEY")
    om_key = os.environ.get("OPEN_METEO_API_KEY")

    # Fallback: try parsing .streamlit/secrets.toml
    if not sb_url:
        try:
            import tomllib
            with open(".streamlit/secrets.toml", "rb") as f:
                secrets = tomllib.load(f)
            sb_url = secrets.get("supabase", {}).get("url")
            sb_key = secrets.get("supabase", {}).get("key")
            om_key = secrets.get("open_meteo", {}).get("api_key")
        except Exception:
            pass

    if not sb_url or not sb_key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
        print("  Set as environment variables or in .streamlit/secrets.toml")
        sys.exit(1)

    return sb_url, sb_key, om_key


def main():
    sb_url, sb_key, om_key = _get_config()
    sb = create_client(sb_url, sb_key)

    total_tasks = len(SITES) * 12
    completed = 0
    errors = 0

    print("=" * 60)
    print("VCAG Climate Bootstrap — 30-Year ERA5 Normals")
    print(f"Sites: {len(SITES)} | Months: 12 | Total: {total_tasks} computations")
    print(f"API Key: {'Configured' if om_key else 'None (using free endpoint)'}")
    print("=" * 60)

    for site_name, coords in SITES.items():
        for month in range(1, 13):
            completed += 1
            month_names = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                           "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

            print(f"  [{completed}/{total_tasks}] {site_name} — {month_names[month]}...", end=" ", flush=True)

            try:
                ctx = get_climate_context(
                    lat=coords["lat"],
                    lon=coords["lon"],
                    month=month,
                    sb_client=sb,
                    api_key=om_key,
                )

                if ctx.error:
                    print(f"WARN: {ctx.error}")
                    errors += 1
                elif ctx.cached:
                    print(f"CACHED (already computed)")
                else:
                    print(f"OK — {ctx.wind.sample_count:,} samples, prevailing {ctx.prevailing_dir}")
            except Exception as e:
                print(f"ERROR: {e}")
                errors += 1

            # Respectful rate limiting — 1 second between API calls
            if not (ctx and ctx.cached):
                time.sleep(1.0)

    print("=" * 60)
    print(f"Bootstrap complete. {completed - errors}/{completed} succeeded, {errors} errors.")
    print("Verify with: SELECT * FROM climate_percentiles LIMIT 5;")
    print("=" * 60)


if __name__ == "__main__":
    main()
