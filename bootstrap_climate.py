#!/usr/bin/env python3
"""
VECTOR CHECK AERIAL GROUP INC. — Climate Bootstrap Script (Tiered)

Pre-computes 25-year hourly climate normals for all VCAG detachment sites
across all 12 months. Uses tiered fallback:
    - Tier 1: ECCC station observations (preferred)
    - Tier 2: NASA POWER gridded reanalysis (fallback)

Stores results in Supabase climate_percentiles and climate_wind_rose tables.

USAGE:
    python bootstrap_climate.py

PREREQUISITES:
    - Supabase tables created (run supabase_climate_schema.sql first)
    - .streamlit/secrets.toml present with [supabase] block

ESTIMATED RUNTIME:
    ~2 minutes per site (25 API calls per site at ~0.5s each)
    Total for 5 sites: ~10 minutes

API COST:
    Both ECCC and NASA POWER are FREE with no rate limits or commercial restrictions.
    No API keys required.

RE-RUN SAFETY:
    Safe to re-run. Uses upsert to update existing rows. Idempotent.
"""

import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from supabase import create_client
from modules.climate_ingest import bootstrap_site

# =============================================================================
# CONFIGURATION
# =============================================================================

SITES = {
    "VCAG HQ (Belleville, ON)":   {"lat": 44.1628, "lon": -77.3832},
    "Vector1 (Cold Lake, AB)":    {"lat": 54.4642, "lon": -110.1825},
    "Vector2 (Petawawa, ON)":     {"lat": 45.9003, "lon": -77.2818},
    "Vector3 (Bagotville, QC)":   {"lat": 48.3303, "lon": -70.9961},
    "Vector4 (Toronto, ON)":      {"lat": 43.6532, "lon": -79.3832},
}


def _get_config():
    """Reads Supabase config from environment or .streamlit/secrets.toml."""
    sb_url = os.environ.get("SUPABASE_URL")
    sb_key = os.environ.get("SUPABASE_KEY")

    if not sb_url:
        try:
            import tomllib
        except ImportError:
            try:
                import tomli as tomllib
            except ImportError:
                tomllib = None

        if tomllib is not None:
            for path in (".streamlit/secrets.toml", "/app/.streamlit/secrets.toml"):
                if os.path.exists(path):
                    try:
                        with open(path, "rb") as f:
                            secrets = tomllib.load(f)
                        sb_url = secrets.get("supabase", {}).get("url")
                        sb_key = secrets.get("supabase", {}).get("key")
                        if sb_url:
                            break
                    except Exception:
                        pass

    if not sb_url or not sb_key:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set.")
        print("  Set as environment variables or in .streamlit/secrets.toml")
        sys.exit(1)

    return sb_url, sb_key


def main():
    sb_url, sb_key = _get_config()
    sb = create_client(sb_url, sb_key)

    print("=" * 64)
    print("VCAG Climate Bootstrap (Tiered)")
    print("Tier 1: ECCC (api.weather.gc.ca) — station observations")
    print("Tier 2: NASA POWER (power.larc.nasa.gov) — gridded fallback")
    print(f"Sites: {len(SITES)} | Years: 2001\u20132025")
    print("=" * 64)

    total_succeeded = 0
    total_failed = 0
    start_time = time.time()

    for i, (site_name, coords) in enumerate(SITES.items(), 1):
        print(f"\n[{i}/{len(SITES)}] {site_name}")
        print(f"        Lat: {coords['lat']}, Lon: {coords['lon']}")
        site_start = time.time()

        try:
            result = bootstrap_site(coords["lat"], coords["lon"], sb)

            elapsed = time.time() - site_start

            print(f"        Tier:   {result['tier']}")
            print(f"        Source: {result['source_label']}")
            if result["station"]:
                print(f"        Station: {result['station']['station_name']} (ID: {result['station']['station_id']})")
            print(f"        Years:  {result['years_succeeded']} succeeded, {result['years_failed']} failed")
            print(f"        Months saved: {result['months_saved']}/12")
            print(f"        Elapsed: {elapsed:.1f}s")

            if result["months_saved"] > 0:
                total_succeeded += 1
            else:
                total_failed += 1
                print("        WARNING: No months saved \u2014 check API connectivity")

        except Exception as e:
            print(f"        ERROR: {e}")
            total_failed += 1

    total_elapsed = time.time() - start_time

    print()
    print("=" * 64)
    print(f"Bootstrap complete in {total_elapsed:.0f}s")
    print(f"Sites succeeded: {total_succeeded}/{len(SITES)}")
    print(f"Sites failed:    {total_failed}/{len(SITES)}")
    print("=" * 64)
    print()
    print("Verify in Supabase SQL Editor with:")
    print("  SELECT lat_bin, lon_bin, month, source_label, sample_count")
    print("  FROM climate_percentiles WHERE variable = 'wind' ORDER BY lat_bin, month;")


if __name__ == "__main__":
    main()
