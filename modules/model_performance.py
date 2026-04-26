"""
VECTOR CHECK AERIAL GROUP INC. — Model Performance Scorecard

Computes trailing 24-hour forecast performance per NWP model by comparing
each model's historical predictions (via Open-Meteo Previous Runs API)
against ground-truth observations from the nearest METAR station and any
Kestrel 5500 uploads within the window.

DATA FLOW:
    For each model in the active ensemble:
      1. Fetch past_days=1 from the model's Open-Meteo endpoint
         → yields hourly predictions for the trailing 24h
      2. Fetch METAR history from AviationWeather.gov
         → yields hourly observed conditions for the same window
      3. Optionally fetch Kestrel sessions from Supabase
         → adds operator ground truth at the launch site

    For each paired (forecast hour, observation hour):
      - Compute absolute error per variable (wind, gust, temp, pressure)

    Aggregate across all paired hours:
      - MAE = mean absolute error per model per variable

OUTPUT:
    dict keyed by model name, each containing:
      - wind_mae_kt, gust_mae_kt, temp_mae_c, pressure_mae_hpa
      - sample_count (how many paired hours contributed)
      - best_performer flag (lowest weighted composite error)

COST: $0 — Open-Meteo Previous Runs API uses the same quota as forecast calls.
"""

import urllib.request
import urllib.parse
import json
import logging
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("arms.model_performance")

# Shared constants with ensemble_analysis
from modules.ensemble_analysis import (
    MODEL_ENDPOINTS,
    REGIONAL_MODELS,
    _select_regional_model,
    KMH_TO_KT,
    REQUEST_TIMEOUT_S,
    USER_AGENT,
)

# Variables needed for the scorecard
_PERF_VARS = (
    "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
    "temperature_2m,surface_pressure,relative_humidity_2m"
)

# MAE tolerance thresholds (green / amber / red)
WIND_MAE_GOOD_KT = 2.0     # green if MAE below this
WIND_MAE_WARN_KT = 4.0     # amber up to this, red above
GUST_MAE_GOOD_KT = 3.0
GUST_MAE_WARN_KT = 5.0
TEMP_MAE_GOOD_C = 1.5
TEMP_MAE_WARN_C = 3.0
PRESSURE_MAE_GOOD_HPA = 1.5
PRESSURE_MAE_WARN_HPA = 3.0


# =============================================================================
# HISTORICAL FORECAST FETCH (Open-Meteo Previous Runs)
# =============================================================================

def _fetch_model_history(model_name: str, endpoint_url: str, lat: float, lon: float) -> dict:
    """Fetches 24-hour historical forecast from one model.

    Returns dict with 'times', 'wind_kt', 'gust_kt', 'wind_dir', 'temp_c',
    'pressure_hpa', 'rh' lists, or None on failure.
    """
    url = (
        f"{endpoint_url}?latitude={lat}&longitude={lon}"
        f"&hourly={_PERF_VARS}"
        f"&past_days=1&forecast_days=1"
        f"&timezone=UTC"
    )

    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("Model history fetch failed for %s: %s", model_name, e)
        return None

    h = data.get("hourly")
    if not h or "time" not in h:
        return None

    # Keep only hours that are in the past (already occurred) — these are
    # the only hours we can compare against observations
    now = datetime.now(timezone.utc)
    times_iso = h["time"]

    kept_indices = []
    for i, t_str in enumerate(times_iso):
        try:
            t = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        # Only hours within the last 24 hours, and not in the future
        age_hours = (now - t).total_seconds() / 3600.0
        if 0 <= age_hours <= 24:
            kept_indices.append(i)

    if not kept_indices:
        return None

    def _pick(key, scale=1.0):
        raw = h.get(key, [])
        out = []
        for idx in kept_indices:
            if idx < len(raw) and raw[idx] is not None:
                try:
                    out.append(float(raw[idx]) * scale)
                except (TypeError, ValueError):
                    out.append(None)
            else:
                out.append(None)
        return out

    return {
        "times": [times_iso[i] for i in kept_indices],
        "wind_kt": _pick("wind_speed_10m", KMH_TO_KT),
        "wind_dir": _pick("wind_direction_10m"),
        "gust_kt": _pick("wind_gusts_10m", KMH_TO_KT),
        "temp_c": _pick("temperature_2m"),
        "pressure_hpa": _pick("surface_pressure"),
        "rh": _pick("relative_humidity_2m"),
    }


# =============================================================================
# METAR HISTORY FETCH (AviationWeather.gov)
# =============================================================================

def fetch_metar_history(icao: str, hours: int = 24) -> list:
    """Fetches the last N hours of METAR from AviationWeather.gov.

    Returns list of dicts with 'time' (datetime), 'wind_kt', 'wind_dir',
    'gust_kt', 'temp_c', 'pressure_hpa'. Fields that weren't reported
    are set to None.
    """
    if not icao or icao == "NONE":
        return []

    url = (
        f"https://aviationweather.gov/api/data/metar"
        f"?ids={icao}&format=json&hoursBeforeNow={hours}"
    )

    try:
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_S) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("METAR history fetch failed: %s", e)
        return []

    observations = []
    if not isinstance(data, list):
        return []

    for row in data:
        try:
            # API returns fields like 'obsTime' (unix), 'temp', 'dewp',
            # 'wdir', 'wspd', 'wgst', 'altim' (hPa), 'slp' (hPa)
            obs_time = row.get("obsTime") or row.get("reportTime")
            if obs_time is None:
                continue

            if isinstance(obs_time, (int, float)):
                t = datetime.fromtimestamp(obs_time, tz=timezone.utc)
            elif isinstance(obs_time, str):
                t = datetime.fromisoformat(obs_time.replace("Z", "+00:00"))
            else:
                continue

            def _safe_float(key):
                v = row.get(key)
                if v is None or v == "":
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return None

            wspd = _safe_float("wspd")   # knots
            wgst = _safe_float("wgst")   # knots
            wdir = _safe_float("wdir")   # degrees true
            temp = _safe_float("temp")   # Celsius
            altim = _safe_float("altim") # hPa (altimeter setting)
            slp = _safe_float("slp")     # hPa sea level pressure

            # METAR station pressure isn't always directly available — altim is
            # sea-level-adjusted. For a scorecard use altim as a reasonable
            # approximation at low-elevation airports.
            pressure = altim if altim is not None else slp

            observations.append({
                "time": t,
                "wind_kt": wspd,
                "wind_dir": wdir,
                "gust_kt": wgst,
                "temp_c": temp,
                "pressure_hpa": pressure,
            })
        except Exception:
            continue

    return observations


# =============================================================================
# KESTREL SESSION FETCH (Supabase)
# =============================================================================

def fetch_kestrel_sessions_24h(sb_client, lat: float, lon: float) -> list:
    """Fetches Kestrel sessions near (lat, lon) from the trailing 24 hours.

    Returns list of dicts matching the METAR observation format so they
    can be merged into the truth set.
    """
    if sb_client is None:
        return []

    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        result = (
            sb_client.table("forecast_verifications")
            .select("timestamp,actual_wind_kt,actual_wind_dir,actual_temp_c,actual_pressure_hpa")
            .gte("timestamp", cutoff)
            .gte("lat", lat - 0.2).lte("lat", lat + 0.2)
            .gte("lon", lon - 0.2).lte("lon", lon + 0.2)
            .execute()
        )
    except Exception as e:
        logger.debug("Kestrel session fetch failed: %s", e)
        return []

    observations = []
    for row in result.data or []:
        try:
            t_str = row.get("timestamp")
            if not t_str:
                continue
            t = datetime.fromisoformat(t_str.replace("Z", "+00:00"))

            observations.append({
                "time": t,
                "wind_kt": row.get("actual_wind_kt"),
                "wind_dir": row.get("actual_wind_dir"),
                "gust_kt": None,  # Kestrel session average doesn't capture instantaneous gusts reliably
                "temp_c": row.get("actual_temp_c"),
                "pressure_hpa": row.get("actual_pressure_hpa"),
                "source": "KESTREL",
            })
        except Exception:
            continue

    return observations


# =============================================================================
# PAIRING & MAE COMPUTATION
# =============================================================================

def _match_forecast_to_observation(obs_time: datetime, fcst_times: list) -> int:
    """Returns the index of the forecast hour nearest to obs_time,
    or -1 if no match within 45 minutes.
    """
    if not fcst_times:
        return -1

    obs_ts = obs_time.timestamp()
    best_idx = -1
    best_diff = float("inf")

    for i, t_str in enumerate(fcst_times):
        try:
            t = datetime.fromisoformat(t_str).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        diff = abs(t.timestamp() - obs_ts)
        if diff < best_diff:
            best_diff = diff
            best_idx = i

    # Reject matches more than 45 minutes away — METAR hourly cadence means
    # we expect exact hour matches
    if best_diff > 2700:
        return -1

    return best_idx


def compute_model_mae(model_history: dict, observations: list) -> dict:
    """Computes mean absolute error per variable for one model.

    Args:
        model_history: output of _fetch_model_history
        observations: list of observation dicts (METAR + Kestrel combined)

    Returns:
        dict with wind_mae_kt, gust_mae_kt, temp_mae_c, pressure_mae_hpa,
        sample_count, and per-variable sample counts.
    """
    result = {
        "wind_mae_kt": None,
        "gust_mae_kt": None,
        "temp_mae_c": None,
        "pressure_mae_hpa": None,
        "sample_count": 0,
        "wind_n": 0, "gust_n": 0, "temp_n": 0, "pressure_n": 0,
    }

    if not model_history or not observations:
        return result

    wind_errs, gust_errs, temp_errs, pressure_errs = [], [], [], []

    for obs in observations:
        idx = _match_forecast_to_observation(obs["time"], model_history["times"])
        if idx < 0:
            continue

        # Wind speed
        fw = model_history["wind_kt"][idx] if idx < len(model_history["wind_kt"]) else None
        ow = obs.get("wind_kt")
        if fw is not None and ow is not None:
            wind_errs.append(abs(fw - ow))

        # Gust
        fg = model_history["gust_kt"][idx] if idx < len(model_history["gust_kt"]) else None
        og = obs.get("gust_kt")
        if fg is not None and og is not None:
            gust_errs.append(abs(fg - og))

        # Temp
        ft = model_history["temp_c"][idx] if idx < len(model_history["temp_c"]) else None
        ot = obs.get("temp_c")
        if ft is not None and ot is not None:
            temp_errs.append(abs(ft - ot))

        # Pressure
        fp = model_history["pressure_hpa"][idx] if idx < len(model_history["pressure_hpa"]) else None
        op = obs.get("pressure_hpa")
        if fp is not None and op is not None:
            pressure_errs.append(abs(fp - op))

    def _mae(errs):
        return round(sum(errs) / len(errs), 1) if errs else None

    result["wind_mae_kt"] = _mae(wind_errs)
    result["gust_mae_kt"] = _mae(gust_errs)
    result["temp_mae_c"] = _mae(temp_errs)
    result["pressure_mae_hpa"] = _mae(pressure_errs)
    result["sample_count"] = len(observations)
    result["wind_n"] = len(wind_errs)
    result["gust_n"] = len(gust_errs)
    result["temp_n"] = len(temp_errs)
    result["pressure_n"] = len(pressure_errs)

    return result


def _composite_score(mae_dict: dict) -> float:
    """Computes a weighted composite error score for ranking.

    Lower is better. Wind gets heaviest weight since it's the primary UAS risk.
    Returns infinity if no wind MAE is available (model can't be ranked).
    """
    w = mae_dict.get("wind_mae_kt")
    if w is None:
        return float("inf")

    score = w * 3.0  # wind weighted x3

    g = mae_dict.get("gust_mae_kt")
    if g is not None:
        score += g * 2.0

    t = mae_dict.get("temp_mae_c")
    if t is not None:
        score += t * 1.0

    p = mae_dict.get("pressure_mae_hpa")
    if p is not None:
        score += p * 0.5

    return score


# =============================================================================
# TOP-LEVEL ORCHESTRATION
# =============================================================================

def compute_performance_scorecard(
    lat: float,
    lon: float,
    icao: str,
    sb_client=None,
) -> dict:
    """Produces the complete performance scorecard for all active models.

    Args:
        lat, lon: site coordinates (used to select regional model)
        icao: nearest ICAO for METAR history (can be "NONE")
        sb_client: optional Supabase client for Kestrel data

    Returns:
        dict with:
          - models: list of per-model results
          - best_performer: name of the lowest-error model
          - observation_count: total observations used
          - kestrel_count: how many Kestrel sessions contributed
          - metar_count: how many METAR observations contributed
          - has_data: True if scoring was possible
    """
    # Fetch observations (METAR + Kestrel)
    metar_obs = fetch_metar_history(icao, hours=24) if icao != "NONE" else []
    kestrel_obs = fetch_kestrel_sessions_24h(sb_client, lat, lon) if sb_client else []

    all_observations = metar_obs + kestrel_obs

    if not all_observations:
        return {
            "models": [],
            "best_performer": None,
            "observation_count": 0,
            "metar_count": 0,
            "kestrel_count": 0,
            "has_data": False,
            "message": "No METAR or Kestrel observations available in the last 24 hours.",
        }

    # Determine which models to score — same selection logic as ensemble
    regional_name, regional_url = _select_regional_model(lat, lon)
    active_models = {
        regional_name: regional_url,
        "GFS":   MODEL_ENDPOINTS["GFS"],
        "ECMWF": MODEL_ENDPOINTS["ECMWF"],
        "ICON":  MODEL_ENDPOINTS["ICON"],
    }

    # Fetch each model's history and compute MAE
    model_results = []
    for name, url in active_models.items():
        history = _fetch_model_history(name, url, lat, lon)
        if history is None:
            model_results.append({
                "name": name,
                "status": "UNAVAILABLE",
                "wind_mae_kt": None, "gust_mae_kt": None,
                "temp_mae_c": None, "pressure_mae_hpa": None,
                "sample_count": 0,
                "wind_n": 0, "gust_n": 0, "temp_n": 0, "pressure_n": 0,
                "composite_score": float("inf"),
            })
            continue

        mae = compute_model_mae(history, all_observations)
        mae["name"] = name
        mae["status"] = "OK"
        mae["composite_score"] = _composite_score(mae)
        model_results.append(mae)

    # Identify the best performer (lowest composite score)
    scorable = [m for m in model_results if m.get("composite_score", float("inf")) < float("inf")]
    best = min(scorable, key=lambda m: m["composite_score"])["name"] if scorable else None

    return {
        "models": model_results,
        "best_performer": best,
        "observation_count": len(all_observations),
        "metar_count": len(metar_obs),
        "kestrel_count": len(kestrel_obs),
        "has_data": True,
    }


# =============================================================================
# DISPLAY HELPERS
# =============================================================================

def grade_wind_mae(mae: float) -> str:
    """Returns 'GOOD', 'WARN', or 'POOR' for wind MAE."""
    if mae is None: return "NONE"
    if mae <= WIND_MAE_GOOD_KT: return "GOOD"
    if mae <= WIND_MAE_WARN_KT: return "WARN"
    return "POOR"


def grade_gust_mae(mae: float) -> str:
    if mae is None: return "NONE"
    if mae <= GUST_MAE_GOOD_KT: return "GOOD"
    if mae <= GUST_MAE_WARN_KT: return "WARN"
    return "POOR"


def grade_temp_mae(mae: float) -> str:
    if mae is None: return "NONE"
    if mae <= TEMP_MAE_GOOD_C: return "GOOD"
    if mae <= TEMP_MAE_WARN_C: return "WARN"
    return "POOR"


def grade_pressure_mae(mae: float) -> str:
    if mae is None: return "NONE"
    if mae <= PRESSURE_MAE_GOOD_HPA: return "GOOD"
    if mae <= PRESSURE_MAE_WARN_HPA: return "WARN"
    return "POOR"


GRADE_COLORS = {
    "GOOD": "#4ade80",
    "WARN": "#E58E26",
    "POOR": "#ff6b4a",
    "NONE": "#6B7280",
}
