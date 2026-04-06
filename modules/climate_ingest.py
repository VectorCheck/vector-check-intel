"""
VECTOR CHECK AERIAL GROUP INC. — Climate Context Engine

Fetches 30-year ERA5 reanalysis data from the Open-Meteo Historical Weather API,
computes percentile distributions and wind direction frequency tables, caches
the results in Supabase, and provides fast runtime lookups for the dashboard.

ARCHITECTURE:
    1. Dashboard requests climate context for (lat, lon, month)
    2. get_climate_context() checks Supabase cache first
    3. On cache miss: fetches ERA5 in 5-year chunks, computes stats, stores
    4. Returns ClimateContext dataclass with percentiles + wind rose data

API ENDPOINT:
    Standard:     https://archive-api.open-meteo.com/v1/archive
    Customer:     https://customer-archive-api.open-meteo.com/v1/archive?apikey=KEY

API CALL COST (Open-Meteo Professional Plan):
    5 variables × 30 days = ~2 fractional API calls per year-chunk
    6 chunks (5 years each) × 2 calls = ~12 calls per location/month
    12 months × 5 detachments = ~720 calls total bootstrap (of 5M monthly)

SUPABASE TABLES (run once — schema at bottom of this file):
    climate_percentiles  — P10/P25/P50/P75/P90/P99 per variable per site/month
    climate_wind_rose    — 8-direction × 3-speed frequency table per site/month

VARIABLES FETCHED:
    temperature_2m, relative_humidity_2m, wind_speed_10m,
    wind_direction_10m, surface_pressure
"""

import urllib.request
import json
import math
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

logger = logging.getLogger("arms.climate")

# =============================================================================
# CONFIGURATION
# =============================================================================

# Open-Meteo Historical Weather API (ERA5 reanalysis)
# If you have a paid API key, use the customer endpoint for reliability.
# Set OPEN_METEO_API_KEY to None to use the free endpoint.
ARCHIVE_BASE_URL = "https://archive-api.open-meteo.com/v1/archive"
CUSTOMER_ARCHIVE_URL = "https://customer-archive-api.open-meteo.com/v1/archive"

# Variables to fetch — kept minimal to reduce API call cost
_HOURLY_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,wind_direction_10m,surface_pressure"

# ERA5 date range for climate normals
CLIMATE_START_YEAR = 1995
CLIMATE_END_YEAR = 2025
CHUNK_SIZE_YEARS = 5  # Fetch in 5-year blocks to avoid timeouts

# Wind speed bins (knots) for the directional bar gauge
WIND_SPEED_BINS = [(0, 10), (10, 20), (20, 999)]
WIND_SPEED_BIN_LABELS = ["0-10 kt", "10-20 kt", "20+ kt"]

# 8-point compass for wind direction binning
COMPASS_DIRS = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]

# Supabase table names
PERCENTILE_TABLE = "climate_percentiles"
WIND_ROSE_TABLE = "climate_wind_rose"

# Spatial binning — round lat/lon to 0.1° for cache key
# ERA5 is 0.25° resolution, so 0.1° gives adequate granularity
SPATIAL_BIN_RESOLUTION = 0.1

# km/h to knots conversion (Open-Meteo ERA5 returns km/h)
KMH_TO_KT = 0.539957

_REQUEST_TIMEOUT_S = 30  # ERA5 queries can be slow


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class VariablePercentiles:
    """Percentile distribution for a single weather variable."""
    p10: float = 0.0
    p25: float = 0.0
    p50: float = 0.0   # median — the "30-year average" anchor
    p75: float = 0.0
    p90: float = 0.0
    p99: float = 0.0
    mean: float = 0.0
    sample_count: int = 0


@dataclass
class WindRoseBin:
    """Frequency data for one compass direction."""
    direction: str = ""
    total_pct: float = 0.0
    calm_pct: float = 0.0      # 0-10 kt
    moderate_pct: float = 0.0  # 10-20 kt
    strong_pct: float = 0.0    # 20+ kt
    avg_speed_kt: float = 0.0


@dataclass
class ClimateContext:
    """Complete climate context for a location and month."""
    lat_bin: float = 0.0
    lon_bin: float = 0.0
    month: int = 1
    years_range: str = ""

    wind: VariablePercentiles = field(default_factory=VariablePercentiles)
    temp: VariablePercentiles = field(default_factory=VariablePercentiles)
    pressure: VariablePercentiles = field(default_factory=VariablePercentiles)
    rh: VariablePercentiles = field(default_factory=VariablePercentiles)

    wind_rose: list[WindRoseBin] = field(default_factory=list)
    prevailing_dir: str = ""
    prevailing_pct: float = 0.0

    cached: bool = False
    error: str = ""

    def get_percentile_rank(self, variable: str, value: float) -> int:
        """Returns approximate percentile rank (0-100) for a value against the distribution."""
        vp = getattr(self, variable, None)
        if vp is None or vp.sample_count == 0:
            return 50

        if value <= vp.p10:
            return int(10 * (value / max(0.01, vp.p10)))
        elif value <= vp.p25:
            return int(10 + 15 * ((value - vp.p10) / max(0.01, vp.p25 - vp.p10)))
        elif value <= vp.p50:
            return int(25 + 25 * ((value - vp.p25) / max(0.01, vp.p50 - vp.p25)))
        elif value <= vp.p75:
            return int(50 + 25 * ((value - vp.p50) / max(0.01, vp.p75 - vp.p50)))
        elif value <= vp.p90:
            return int(75 + 15 * ((value - vp.p75) / max(0.01, vp.p90 - vp.p75)))
        elif value <= vp.p99:
            return int(90 + 9 * ((value - vp.p90) / max(0.01, vp.p99 - vp.p90)))
        else:
            return 99

    def format_percentile_label(self, percentile: int) -> tuple[str, str]:
        """Returns (label, css_class) for a percentile value."""
        if percentile >= 90:
            return f"P{percentile} — Anomalous", "pH"
        elif percentile >= 75:
            return f"P{percentile} — Elevated", "pM"
        elif percentile <= 10:
            return f"P{percentile} — Unusually low", "pH"
        elif percentile <= 25:
            return f"P{percentile} — Below avg", "pM"
        else:
            return f"P{percentile} — Normal", "pL"


# =============================================================================
# API FETCHING
# =============================================================================

def _build_archive_url(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
) -> str:
    """Constructs the Open-Meteo Historical Weather API URL."""
    base = CUSTOMER_ARCHIVE_URL if api_key else ARCHIVE_BASE_URL
    url = (
        f"{base}?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly={_HOURLY_VARS}&timezone=UTC"
    )
    if api_key:
        url += f"&apikey={api_key}"
    return url


def _fetch_era5_chunk(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    api_key: str | None = None,
) -> dict | None:
    """Fetches one chunk of ERA5 data. Returns parsed JSON or None on failure."""
    url = _build_archive_url(lat, lon, start_date, end_date, api_key)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "VectorCheck-ARMS/2.1"})
        with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT_S) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.warning("ERA5 fetch failed for %s to %s: %s", start_date, end_date, e)
        return None


def _fetch_full_era5(
    lat: float,
    lon: float,
    month: int,
    api_key: str | None = None,
) -> dict:
    """Fetches ERA5 data for a single calendar month across CLIMATE_START_YEAR–CLIMATE_END_YEAR.

    Splits into CHUNK_SIZE_YEARS blocks to avoid request timeouts.
    Returns a merged hourly dict with all variables as flat lists.
    """
    import calendar

    merged: dict[str, list] = {
        "temperature_2m": [],
        "relative_humidity_2m": [],
        "wind_speed_10m": [],
        "wind_direction_10m": [],
        "surface_pressure": [],
    }

    for chunk_start in range(CLIMATE_START_YEAR, CLIMATE_END_YEAR, CHUNK_SIZE_YEARS):
        chunk_end = min(chunk_start + CHUNK_SIZE_YEARS - 1, CLIMATE_END_YEAR - 1)

        for year in range(chunk_start, chunk_end + 1):
            last_day = calendar.monthrange(year, month)[1]
            start_date = f"{year}-{month:02d}-01"
            end_date = f"{year}-{month:02d}-{last_day:02d}"

            data = _fetch_era5_chunk(lat, lon, start_date, end_date, api_key)
            if data is None or "hourly" not in data:
                continue

            hourly = data["hourly"]
            for var in merged:
                values = hourly.get(var, [])
                merged[var].extend(values)

    return merged


# =============================================================================
# STATISTICS COMPUTATION
# =============================================================================

def _compute_percentiles(values: list[float]) -> VariablePercentiles:
    """Computes percentile distribution from a list of float values."""
    clean = sorted(v for v in values if v is not None)
    n = len(clean)
    if n == 0:
        return VariablePercentiles()

    def pct(p: float) -> float:
        idx = (p / 100.0) * (n - 1)
        lo = int(math.floor(idx))
        hi = min(lo + 1, n - 1)
        frac = idx - lo
        return round(clean[lo] + frac * (clean[hi] - clean[lo]), 2)

    return VariablePercentiles(
        p10=pct(10),
        p25=pct(25),
        p50=pct(50),
        p75=pct(75),
        p90=pct(90),
        p99=pct(99),
        mean=round(sum(clean) / n, 2),
        sample_count=n,
    )


def _compute_wind_rose(
    speeds_kmh: list[float | None],
    directions: list[float | None],
) -> list[WindRoseBin]:
    """Computes 8-direction wind frequency table with 3 speed bins.

    Args:
        speeds_kmh: Wind speeds in km/h (Open-Meteo ERA5 native unit)
        directions: Wind directions in degrees (meteorological convention)

    Returns:
        List of 8 WindRoseBin objects, sorted descending by total_pct.
    """
    # Initialize counters: {dir_name: {bin_idx: count, "speeds": [...]}}
    bins: dict[str, dict] = {}
    for d in COMPASS_DIRS:
        bins[d] = {0: 0, 1: 0, 2: 0, "speeds": [], "total": 0}

    total_valid = 0

    for spd_raw, dir_raw in zip(speeds_kmh, directions):
        if spd_raw is None or dir_raw is None:
            continue

        spd_kt = float(spd_raw) * KMH_TO_KT
        dir_deg = float(dir_raw)

        # Map direction to 8-point compass
        dir_idx = int(round(dir_deg / 45.0)) % 8
        dir_name = COMPASS_DIRS[dir_idx]

        # Map speed to bin
        if spd_kt < 10:
            bin_idx = 0
        elif spd_kt < 20:
            bin_idx = 1
        else:
            bin_idx = 2

        bins[dir_name][bin_idx] += 1
        bins[dir_name]["speeds"].append(spd_kt)
        bins[dir_name]["total"] += 1
        total_valid += 1

    if total_valid == 0:
        return [WindRoseBin(direction=d) for d in COMPASS_DIRS]

    result: list[WindRoseBin] = []
    for d in COMPASS_DIRS:
        b = bins[d]
        total = b["total"]
        total_pct = round(100.0 * total / total_valid, 1)
        avg_spd = round(sum(b["speeds"]) / len(b["speeds"]), 1) if b["speeds"] else 0.0

        result.append(WindRoseBin(
            direction=d,
            total_pct=total_pct,
            calm_pct=round(100.0 * b[0] / total_valid, 1),
            moderate_pct=round(100.0 * b[1] / total_valid, 1),
            strong_pct=round(100.0 * b[2] / total_valid, 1),
            avg_speed_kt=avg_spd,
        ))

    # Sort descending by total percentage
    result.sort(key=lambda x: x.total_pct, reverse=True)
    return result


# =============================================================================
# SUPABASE CACHE LAYER
# =============================================================================

def _bin_coord(val: float) -> float:
    """Rounds a coordinate to the spatial bin resolution."""
    return round(round(val / SPATIAL_BIN_RESOLUTION) * SPATIAL_BIN_RESOLUTION, 4)


def _load_from_cache(sb_client, lat_bin: float, lon_bin: float, month: int) -> ClimateContext | None:
    """Attempts to load cached climate context from Supabase.

    Returns a fully populated ClimateContext on hit, or None on miss/error.
    """
    try:
        # Load percentiles
        pct_result = (
            sb_client.table(PERCENTILE_TABLE)
            .select("*")
            .eq("lat_bin", lat_bin)
            .eq("lon_bin", lon_bin)
            .eq("month", month)
            .execute()
        )
        if not pct_result.data:
            return None

        ctx = ClimateContext(
            lat_bin=lat_bin,
            lon_bin=lon_bin,
            month=month,
            years_range=f"{CLIMATE_START_YEAR}–{CLIMATE_END_YEAR}",
            cached=True,
        )

        for row in pct_result.data:
            vp = VariablePercentiles(
                p10=row["p10"], p25=row["p25"], p50=row["p50"],
                p75=row["p75"], p90=row["p90"], p99=row["p99"],
                mean=row["mean_val"],
                sample_count=row["sample_count"],
            )
            var_name = row["variable"]
            if var_name == "wind":
                ctx.wind = vp
            elif var_name == "temp":
                ctx.temp = vp
            elif var_name == "pressure":
                ctx.pressure = vp
            elif var_name == "rh":
                ctx.rh = vp

        # Load wind rose
        wr_result = (
            sb_client.table(WIND_ROSE_TABLE)
            .select("*")
            .eq("lat_bin", lat_bin)
            .eq("lon_bin", lon_bin)
            .eq("month", month)
            .order("total_pct", desc=True)
            .execute()
        )
        if wr_result.data:
            for row in wr_result.data:
                ctx.wind_rose.append(WindRoseBin(
                    direction=row["direction"],
                    total_pct=row["total_pct"],
                    calm_pct=row["calm_pct"],
                    moderate_pct=row["moderate_pct"],
                    strong_pct=row["strong_pct"],
                    avg_speed_kt=row["avg_speed_kt"],
                ))
            if ctx.wind_rose:
                top = ctx.wind_rose[0]
                # Check if top two are from the same quadrant for "W/NW" style labelling
                if len(ctx.wind_rose) >= 2:
                    second = ctx.wind_rose[1]
                    combined = top.total_pct + second.total_pct
                    ctx.prevailing_dir = f"{top.direction} / {second.direction}"
                    ctx.prevailing_pct = round(combined, 1)
                else:
                    ctx.prevailing_dir = top.direction
                    ctx.prevailing_pct = top.total_pct

        return ctx

    except Exception as e:
        logger.debug("Climate cache read failed: %s", e)
        return None


def _save_to_cache(sb_client, ctx: ClimateContext) -> None:
    """Persists computed climate context to Supabase."""
    try:
        now_iso = datetime.now(timezone.utc).isoformat()

        # Upsert percentiles
        for var_name, vp in [("wind", ctx.wind), ("temp", ctx.temp),
                             ("pressure", ctx.pressure), ("rh", ctx.rh)]:
            sb_client.table(PERCENTILE_TABLE).upsert({
                "lat_bin": ctx.lat_bin,
                "lon_bin": ctx.lon_bin,
                "month": ctx.month,
                "variable": var_name,
                "p10": vp.p10, "p25": vp.p25, "p50": vp.p50,
                "p75": vp.p75, "p90": vp.p90, "p99": vp.p99,
                "mean_val": vp.mean,
                "sample_count": vp.sample_count,
                "updated_at": now_iso,
            }).execute()

        # Upsert wind rose
        for wr in ctx.wind_rose:
            sb_client.table(WIND_ROSE_TABLE).upsert({
                "lat_bin": ctx.lat_bin,
                "lon_bin": ctx.lon_bin,
                "month": ctx.month,
                "direction": wr.direction,
                "total_pct": wr.total_pct,
                "calm_pct": wr.calm_pct,
                "moderate_pct": wr.moderate_pct,
                "strong_pct": wr.strong_pct,
                "avg_speed_kt": wr.avg_speed_kt,
                "updated_at": now_iso,
            }).execute()

    except Exception as e:
        logger.warning("Climate cache write failed: %s", e)


# =============================================================================
# PUBLIC API
# =============================================================================

def get_climate_context(
    lat: float,
    lon: float,
    month: int,
    sb_client=None,
    api_key: str | None = None,
) -> ClimateContext:
    """Returns 30-year climate context for a location and calendar month.

    1. Checks Supabase cache (keyed by binned lat/lon + month)
    2. On cache miss: fetches ERA5, computes stats, caches, returns
    3. On API failure: returns ClimateContext with error message

    Args:
        lat:        Latitude (will be binned to 0.1°)
        lon:        Longitude (will be binned to 0.1°)
        month:      Calendar month (1-12)
        sb_client:  Supabase client (optional — cache disabled if None)
        api_key:    Open-Meteo API key (optional — uses free endpoint if None)

    Returns:
        ClimateContext with percentiles, wind rose, and helper methods
    """
    lat_bin = _bin_coord(lat)
    lon_bin = _bin_coord(lon)

    # --- Cache check ---
    if sb_client is not None:
        cached = _load_from_cache(sb_client, lat_bin, lon_bin, month)
        if cached is not None:
            return cached

    # --- Fetch ERA5 ---
    merged = _fetch_full_era5(lat, lon, month, api_key)

    wind_raw = merged.get("wind_speed_10m", [])
    if not wind_raw or len(wind_raw) < 100:
        return ClimateContext(
            lat_bin=lat_bin, lon_bin=lon_bin, month=month,
            error="Insufficient ERA5 data returned. Check API key or endpoint access.",
        )

    # --- Convert wind speeds from km/h to knots ---
    wind_kt = [v * KMH_TO_KT if v is not None else None for v in wind_raw]

    # --- Compute percentiles ---
    ctx = ClimateContext(
        lat_bin=lat_bin,
        lon_bin=lon_bin,
        month=month,
        years_range=f"{CLIMATE_START_YEAR}–{CLIMATE_END_YEAR}",
    )

    ctx.wind = _compute_percentiles(wind_kt)
    ctx.temp = _compute_percentiles(merged.get("temperature_2m", []))
    ctx.pressure = _compute_percentiles(merged.get("surface_pressure", []))
    ctx.rh = _compute_percentiles(merged.get("relative_humidity_2m", []))

    # --- Compute wind rose ---
    ctx.wind_rose = _compute_wind_rose(
        merged.get("wind_speed_10m", []),
        merged.get("wind_direction_10m", []),
    )

    if ctx.wind_rose:
        top = ctx.wind_rose[0]
        if len(ctx.wind_rose) >= 2:
            second = ctx.wind_rose[1]
            ctx.prevailing_dir = f"{top.direction} / {second.direction}"
            ctx.prevailing_pct = round(top.total_pct + second.total_pct, 1)
        else:
            ctx.prevailing_dir = top.direction
            ctx.prevailing_pct = top.total_pct

    # --- Cache result ---
    if sb_client is not None:
        _save_to_cache(sb_client, ctx)

    return ctx


def compute_density_alt_percentile(
    ctx: ClimateContext,
    current_da: int,
    elevation_ft: float,
) -> tuple[int, VariablePercentiles]:
    """Estimates density altitude percentile from temperature and pressure distributions.

    Since ERA5 doesn't directly provide density altitude, we compute it from
    the median temperature and pressure values using the same formula as physics.py,
    then compare against the current DA.

    Returns (percentile_rank, synthetic_da_percentiles).
    """
    from modules.physics import calculate_density_altitude

    # Compute DA at key percentile breakpoints using T and P percentile combos
    # Worst case DA = high temp + low pressure; best case = low temp + high pressure
    da_values = []
    for t_val in [ctx.temp.p10, ctx.temp.p25, ctx.temp.p50, ctx.temp.p75, ctx.temp.p90]:
        for p_val in [ctx.pressure.p90, ctx.pressure.p75, ctx.pressure.p50, ctx.pressure.p25, ctx.pressure.p10]:
            da = calculate_density_altitude(elevation_ft, t_val, p_val)
            da_values.append(da)

    da_values.sort()
    da_pct = _compute_percentiles([float(v) for v in da_values])

    rank = ctx.get_percentile_rank("temp", ctx.temp.p50)  # rough proxy
    # Refine: where does current_da sit in the synthetic distribution
    n = len(da_values)
    if n > 0:
        below = sum(1 for v in da_values if v <= current_da)
        rank = int(100 * below / n)

    return rank, da_pct


# =============================================================================
# SUPABASE SCHEMA — Run once in SQL Editor
# =============================================================================
SCHEMA_SQL = """
-- Climate percentile cache
-- Composite unique key: (lat_bin, lon_bin, month, variable)
CREATE TABLE IF NOT EXISTS climate_percentiles (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lat_bin      FLOAT NOT NULL,
    lon_bin      FLOAT NOT NULL,
    month        INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    variable     TEXT NOT NULL,       -- 'wind', 'temp', 'pressure', 'rh'
    p10          FLOAT NOT NULL,
    p25          FLOAT NOT NULL,
    p50          FLOAT NOT NULL,
    p75          FLOAT NOT NULL,
    p90          FLOAT NOT NULL,
    p99          FLOAT NOT NULL,
    mean_val     FLOAT NOT NULL,
    sample_count INT NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT now(),
    UNIQUE (lat_bin, lon_bin, month, variable)
);

-- Wind direction frequency cache
-- Composite unique key: (lat_bin, lon_bin, month, direction)
CREATE TABLE IF NOT EXISTS climate_wind_rose (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lat_bin       FLOAT NOT NULL,
    lon_bin       FLOAT NOT NULL,
    month         INT NOT NULL CHECK (month BETWEEN 1 AND 12),
    direction     TEXT NOT NULL,      -- 'N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'
    total_pct     FLOAT NOT NULL,
    calm_pct      FLOAT NOT NULL,     -- 0-10 kt
    moderate_pct  FLOAT NOT NULL,     -- 10-20 kt
    strong_pct    FLOAT NOT NULL,     -- 20+ kt
    avg_speed_kt  FLOAT NOT NULL,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    UNIQUE (lat_bin, lon_bin, month, direction)
);

-- Index for fast lookups by location + month
CREATE INDEX IF NOT EXISTS idx_climate_pct_lookup
    ON climate_percentiles (lat_bin, lon_bin, month);
CREATE INDEX IF NOT EXISTS idx_climate_wr_lookup
    ON climate_wind_rose (lat_bin, lon_bin, month);
"""
