import urllib.request
import json
from datetime import datetime, timezone


def get_model_run_info(model_url: str) -> dict:
    """Fetches the latest model run cycle timestamp from Open-Meteo metadata.

    Each model exposes a meta.json at a predictable path under its static assets.
    The response contains `last_run_initialisation_time` (Unix timestamp) which
    identifies which cycle (00Z, 06Z, 12Z, 18Z, etc.) produced the current data.

    Args:
        model_url: the forecast endpoint (e.g. https://api.open-meteo.com/v1/gem)

    Returns:
        dict with keys: run_cycle_z (e.g. "12Z"), run_date (YYYY-MM-DD),
        run_datetime_utc (datetime), age_hours (int), or empty dict on failure
    """
    # Map forecast endpoints to their metadata paths.
    # These endpoints aggregate multiple underlying models; we probe the
    # primary model for each endpoint to get a representative run cycle.
    meta_map = {
        "v1/gem":      "https://api.open-meteo.com/data/cmc_gem_hrdps_continental/static/meta.json",
        "v1/forecast": "https://api.open-meteo.com/data/ecmwf_ifs025/static/meta.json",
        "v1/gfs":      "https://api.open-meteo.com/data/ncep_gfs025/static/meta.json",
        "v1/ecmwf":    "https://api.open-meteo.com/data/ecmwf_ifs025/static/meta.json",
    }

    meta_url = None
    for key, url in meta_map.items():
        if key in model_url:
            meta_url = url
            break

    if meta_url is None:
        return {}

    try:
        req = urllib.request.Request(meta_url, headers={'User-Agent': 'VectorCheck-App/2.1'})
        with urllib.request.urlopen(req, timeout=5) as response:
            meta = json.loads(response.read().decode('utf-8'))

        ts = meta.get("last_run_initialisation_time")
        if ts is None:
            return {}

        run_dt = datetime.fromtimestamp(int(ts), tz=timezone.utc)
        now = datetime.now(timezone.utc)
        age_hours = int((now - run_dt).total_seconds() / 3600)

        return {
            "run_cycle_z": f"{run_dt.hour:02d}Z",
            "run_date": run_dt.strftime('%Y-%m-%d'),
            "run_datetime_utc": run_dt,
            "age_hours": age_hours,
        }
    except Exception:
        return {}


def get_aviation_weather(icao):
    """Fetches real-time METAR and TAF for the specified ICAO code."""
    try:
        url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=raw"
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            metar = response.read().decode('utf-8').strip()

        url_taf = f"https://aviationweather.gov/api/data/taf?ids={icao}&format=raw"
        req_taf = urllib.request.Request(url_taf, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req_taf, timeout=5) as response_taf:
            taf = response_taf.read().decode('utf-8').strip()

        return metar if metar else "NIL", taf if taf else "NIL"
    except Exception:
        return "NIL", "NIL"


def fetch_mission_data(lat, lon, model_url):
    """
    Fetches tactical surface, absolute AGL boundaries, and 15-layer upper-air NWP data.

    CHANGELOG v2.1:
      - Added snow_depth to hourly_vars. Previously absent, which silently disabled
        the BLSN kinetic gate's snowpack depth trigger (has_snowpack was permanently
        False). The gate now correctly evaluates all three BLSN trigger conditions.
    """

    # Core surface, absolute AGL wind vectors, and thermodynamic variables.
    # snow_depth is in metres; the BLSN gate threshold is 0.05 m (5 cm).
    hourly_vars = (
        "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
        "wind_speed_80m,wind_speed_120m,wind_speed_180m,"
        "wind_direction_80m,wind_direction_120m,wind_direction_180m,"
        "weather_code,visibility,freezing_level_height,"
        "precipitation_probability,precipitation,cape,boundary_layer_height,surface_pressure,"
        "snow_depth"
    )

    # 15-Layer Tactical Column
    p_levels = [1000, 975, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 150]
    for p in p_levels:
        hourly_vars += (
            f",temperature_{p}hPa,relative_humidity_{p}hPa,"
            f"geopotential_height_{p}hPa,wind_speed_{p}hPa,wind_direction_{p}hPa"
        )

    url = (
        f"{model_url}?latitude={lat}&longitude={lon}"
        f"&hourly={hourly_vars}&elevation=nan&timezone=UTC"
    )

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode('utf-8'))
    except Exception as e:
        return {"error": True, "message": str(e)}
