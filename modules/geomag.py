"""
VECTOR CHECK AERIAL GROUP INC. — Magnetic Declination Helper

Computes magnetic declination (variation) for any location on Earth.

PRIMARY PATH:
    Uses pyIGRF (https://pypi.org/project/pyIGRF/) which embeds the full
    IGRF-13 spherical harmonic coefficients. Accurate to ~0.5° globally,
    updated every 5 years. Pure Python, no C dependencies.

FALLBACK PATH:
    If pyIGRF is not installed, falls back to a coarse 15°-grid lookup
    table (typical error 2-5°). Still accurate enough for the Kestrel 5500
    vane (±5°) but less precise. A warning is logged on first use.

SIGN CONVENTION:
    Positive = East declination (magnetic north is east of true north)
    Negative = West declination (magnetic north is west of true north)

    To convert a magnetic bearing to true bearing:
        true_bearing = magnetic_bearing + declination
"""

import logging
from datetime import datetime

logger = logging.getLogger("arms.geomag")

# Try to import pyIGRF. If unavailable, _pyigrf_available stays False
# and the fallback lookup table is used.
try:
    import pyIGRF
    _pyigrf_available = True
except ImportError:
    _pyigrf_available = False
    logger.info("pyIGRF not installed; using coarse declination lookup table. "
                "Install with: pip install pyIGRF")


# =============================================================================
# FALLBACK LOOKUP TABLE — coarse 15° grid, WMM 2025 epoch
# Used only when pyIGRF is unavailable. Not as accurate, but works offline.
# =============================================================================

_FALLBACK_GRID = {
    # North America
    (60, -135): 17.0, (60, -120): 14.0, (60, -105): 10.0, (60, -90): -1.0, (60, -75): -18.0, (60, -60): -28.0,
    (45, -120): 14.0, (45, -105): 9.0, (45, -90): -1.0, (45, -75): -14.0, (45, -60): -21.0,
    (30, -105): 7.0, (30, -90): -1.0, (30, -75): -11.0,
    # South America
    (15, -75): -7.0, (0, -75): -8.0, (-15, -75): -6.0, (-30, -60): -11.0, (-45, -75): -5.0,
    # Europe / Africa
    (60, 0): -2.0, (60, 15): 7.0, (60, 30): 13.0, (60, 45): 14.0,
    (45, -15): -6.0, (45, 0): 1.0, (45, 15): 5.0, (45, 30): 8.0, (45, 45): 8.0,
    (30, 0): 1.0, (30, 15): 4.0, (30, 30): 5.0, (30, 45): 5.0,
    (15, 0): -4.0, (15, 15): 0.0, (15, 30): 3.0, (15, 45): 3.0,
    (0, 15): -4.0, (0, 30): 0.0, (0, 45): 0.0,
    (-15, 15): -12.0, (-15, 30): -5.0, (-15, 45): -5.0,
    (-30, 15): -18.0, (-30, 30): -18.0,
    # Asia
    (45, 60): 9.0, (45, 75): 7.0, (45, 90): 8.0, (45, 105): 8.0, (45, 120): 7.0, (45, 135): 6.0, (45, 150): 4.0,
    (30, 60): 4.0, (30, 75): 2.0, (30, 90): 1.0, (30, 105): 0.0, (30, 120): 2.0, (30, 135): 2.0,
    (15, 60): 2.0, (15, 75): 0.0, (15, 90): -1.0, (15, 105): -1.0, (15, 120): -2.0,
    # Australia / NZ / Pacific
    (-15, 120): 2.0, (-15, 135): 5.0, (-15, 150): 8.0, (-15, 165): 12.0,
    (-30, 120): 1.0, (-30, 135): 9.0, (-30, 150): 13.0, (-30, 165): 18.0,
    (-45, 150): 14.0, (-45, 165): 21.0, (-45, 180): 25.0,
    # Pacific / Alaska
    (60, -165): 14.0, (60, -150): 18.0,
    (45, -165): 9.0, (45, -150): 13.0,
    # Polar (rough)
    (75, 0): 12.0, (75, 90): 22.0, (75, -90): -20.0, (75, 180): -5.0,
    (-75, 0): -50.0, (-75, 90): 120.0, (-75, -90): 40.0, (-75, 180): 150.0,
}

_GRID_STEP = 15.0


def _fallback_lookup(lat: float, lon: float) -> float:
    """Inverse-distance-weighted interpolation from the coarse grid."""
    # Normalize longitude to [-180, 180]
    while lon > 180: lon -= 360
    while lon < -180: lon += 360

    total_w = 0.0
    total_wv = 0.0
    for (g_lat, g_lon), val in _FALLBACK_GRID.items():
        # Use great-circle-ish distance in degrees
        dlat = lat - g_lat
        dlon = lon - g_lon
        # Wrap longitude distance
        if dlon > 180: dlon -= 360
        if dlon < -180: dlon += 360
        d = (dlat * dlat + dlon * dlon) ** 0.5
        if d < 0.1:
            return round(val, 1)
        w = 1.0 / (d ** 3)  # cubic inverse-distance emphasizes nearest points
        total_w += w
        total_wv += w * val

    if total_w == 0:
        return 0.0
    return round(total_wv / total_w, 1)


# =============================================================================
# PUBLIC API
# =============================================================================

def get_magnetic_declination(lat: float, lon: float, date: datetime = None) -> float:
    """Returns magnetic declination in degrees for the given location.

    Args:
        lat:  Latitude in decimal degrees (-90 to 90)
        lon:  Longitude in decimal degrees (-180 to 180)
        date: Date for the calculation (defaults to today)

    Returns:
        Declination in degrees. Positive = East, Negative = West.

        With pyIGRF: typical accuracy < 0.5°.
        Without pyIGRF: typical accuracy 2-5°, can be worse in polar regions.
    """
    if date is None:
        date = datetime.utcnow()

    # Convert date to decimal year for IGRF
    year_start = datetime(date.year, 1, 1)
    year_end = datetime(date.year + 1, 1, 1)
    year_frac = date.year + (date - year_start).total_seconds() / (year_end - year_start).total_seconds()

    if _pyigrf_available:
        try:
            # pyIGRF.igrf_value(lat, lon, altitude_km, year) returns:
            #   (D, I, H, X, Y, Z, F) where D is declination in degrees
            result = pyIGRF.igrf_value(lat, lon, 0.0, year_frac)
            declination = result[0]
            return round(declination, 1)
        except Exception as e:
            logger.warning("pyIGRF calculation failed at %f,%f: %s — falling back", lat, lon, e)

    # Fallback path
    return _fallback_lookup(lat, lon)
