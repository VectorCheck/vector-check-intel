import urllib.request
import json
from datetime import datetime, timezone


# =============================================================================
# NOAA SWPC ENDPOINT CHAIN
# Tried in order. If one fails or returns no parseable Kp data, the next is
# attempted before falling back to the degradation message.
#
# 1. noaa-planetary-k-index-forecast.json  — 3-day forecast, 3-hr intervals
# 2. noaa-planetary-k-index.json           — recent actuals, 3-hr intervals
# 3. planetary_k_index_1m.json             — 1-minute resolution actuals
# =============================================================================
_ENDPOINTS = [
    "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json",
    "https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json",
    "https://services.swpc.noaa.gov/json/planetary_k_index_1m.json",
]

_REQUEST_TIMEOUT_S = 10   # Raised from 5s — NOAA CDN can be slow under load


def _fetch_json(url: str) -> list | None:
    """
    Fetches a URL and returns parsed JSON, or None on any failure.
    Isolation: exceptions do not propagate to the caller.
    """
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-ARMS/2.1'})
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_S) as resp:
            raw = resp.read().decode('utf-8')
        return json.loads(raw)
    except Exception:
        return None


def _extract_kp_from_row(row) -> tuple[str | None, float | None]:
    """
    Extracts (time_str, kp_float) from a single row regardless of whether
    the row is a list or a dict, and regardless of field name variants.

    Returns (None, None) if the row cannot be parsed.
    Per-row isolation: no exception escapes this function.
    """
    try:
        if isinstance(row, dict):
            time_str = row.get("time_tag") or row.get("time") or row.get("TimeStamp")
            # NOAA has used "kp", "kp_index", and "Kp" at various times
            kp_raw = (
                row.get("kp") if row.get("kp") is not None else
                row.get("kp_index") if row.get("kp_index") is not None else
                row.get("Kp")
            )

        elif isinstance(row, list):
            # Skip header rows — any list whose first element is a string
            # that doesn't parse as a datetime is a header
            if not row or not isinstance(row[0], str):
                return None, None
            # Quick sanity check: if the first element looks like a column name, skip it
            if row[0].lower() in ("time_tag", "time", "timestamp"):
                return None, None
            time_str = row[0]
            kp_raw   = row[1] if len(row) >= 2 else None

        else:
            return None, None

        if not time_str or kp_raw is None:
            return None, None

        # Normalise kp_raw → float
        # NOAA has returned both numeric (JSON number) and string "3.67" values
        kp_float = float(kp_raw)

        # Sanity check: Kp is 0–9
        if not (0.0 <= kp_float <= 9.0):
            return None, None

        return time_str, kp_float

    except Exception:
        return None, None


def _parse_time(time_str: str) -> datetime | None:
    """
    Attempts several known NOAA time formats.
    Returns a UTC-aware datetime, or None on failure.
    """
    # Ordered by frequency of occurrence in NOAA feeds
    formats = [
        "%Y-%m-%d %H:%M:%S",   # standard forecast format: "2026-04-05 12:00:00"
        "%Y-%m-%dT%H:%M:%SZ",  # ISO 8601 with Z
        "%Y-%m-%dT%H:%M:%S",   # ISO 8601 without Z
        "%Y-%m-%d %H:%M",      # truncated
    ]
    for fmt in formats:
        try:
            return datetime.strptime(time_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def _find_best_kp(data: list, target_dt_utc: datetime) -> float | None:
    """
    Iterates over a NOAA JSON payload and returns the Kp value whose
    timestamp is closest to target_dt_utc.

    Each row is processed in complete isolation — a bad row is silently
    skipped and never causes the entire parse to fail.
    """
    best_kp:  float | None = None
    min_diff: float        = float('inf')

    for row in data:
        time_str, kp_float = _extract_kp_from_row(row)

        if time_str is None or kp_float is None:
            continue

        row_dt = _parse_time(time_str)
        if row_dt is None:
            continue

        diff = abs((row_dt - target_dt_utc).total_seconds())
        if diff < min_diff:
            min_diff = diff
            best_kp  = kp_float

    return best_kp


def _kp_to_risk(kp_val: int) -> tuple[str, str]:
    """Translates integer Kp to (risk_label, operational_impact) for UAS ops."""
    if kp_val <= 3:
        return (
            "LOW (G0)",
            "Optimal GNSS lock. Minimal ionospheric scintillation expected. C2 link stable."
        )
    elif kp_val == 4:
        return (
            "MODERATE (G0)",
            "Slight ionospheric degradation possible. Verify minimum satellite count and HDOP before launch."
        )
    elif kp_val == 5:
        return (
            "HIGH (G1)",
            "Minor GNSS positioning errors likely. Potential for intermittent C2 link degradation and compass anomalies."
        )
    else:  # kp_val >= 6
        return (
            "SEVERE (G2+)",
            "CRITICAL: High probability of GNSS loss of lock, flyaways, and C2 link failure. Manual ATTI mode readiness required."
        )


def get_kp_index(target_dt_utc: datetime) -> dict:
    """
    Fetches the Planetary K-index from NOAA SWPC and evaluates GNSS/C2 risk.

    Endpoint chain:
      1. noaa-planetary-k-index-forecast.json  (3-day forecast)
      2. noaa-planetary-k-index.json           (recent actuals)
      3. planetary_k_index_1m.json             (1-min actuals, last resort)

    Each endpoint and each data row is independently isolated — a failure in
    one row never aborts the parse of the remaining rows, and a failure of one
    endpoint automatically triggers the next in the chain.

    Returns a dict with keys: 'kp', 'risk', 'impact'.
    """
    last_error_context = "No endpoints responded."

    for endpoint_url in _ENDPOINTS:
        data = _fetch_json(endpoint_url)

        if data is None:
            last_error_context = f"Network failure on {endpoint_url.split('/')[-1]}."
            continue

        if not isinstance(data, list) or len(data) == 0:
            last_error_context = f"Unexpected payload structure from {endpoint_url.split('/')[-1]}."
            continue

        best_kp = _find_best_kp(data, target_dt_utc)

        if best_kp is not None:
            kp_int        = int(round(best_kp))
            risk, impact  = _kp_to_risk(kp_int)
            return {
                'kp':     str(kp_int),
                'risk':   risk,
                'impact': impact,
            }

        # This endpoint returned parseable JSON but zero valid Kp rows
        last_error_context = f"Valid JSON received from {endpoint_url.split('/')[-1]} but no parseable Kp values found."

    # All endpoints exhausted — return graceful degradation
    return {
        'kp':     "---",
        'risk':   "UNAVAILABLE",
        'impact': (
            f"Space weather data temporarily unavailable ({last_error_context}) "
            "Monitor local GNSS satellite count and HDOP closely prior to launch. "
            "Consider delaying operations if GNSS lock is below minimums."
        ),
    }
