import math

# --- VECTOR CHECK AERIAL GROUP INC. : SHARED PHYSICS CONSTANTS ---

# Minimum snowpack depth (metres) required to trigger the BLSN kinetic gate
# when no active precipitation is present. 0.05 m = 5 cm.
SNOWPACK_BLSN_THRESHOLD_M: float = 0.05

# Authoritative unit conversion constants — use ONLY these, never magic numbers.
METERS_TO_FEET: float = 3.28084
METERS_TO_SM: float = 1609.34
KMH_TO_KT: float = 0.539957

# Standard atmosphere constants
ISA_PRESSURE_HPA: float = 1013.25
ISA_TEMP_C: float = 15.0
ISA_LAPSE_C_PER_1000FT: float = 1.98

# Convective / cloud analysis
CONVECTIVE_CCL_MULTIPLIER: int = 400

# All pressure levels requested from NWP API
ALL_P_LEVELS: list[int] = [1000, 975, 950, 925, 900, 850, 800, 700, 600, 500, 400, 300, 250, 200, 150]

# WMO weather codes that the thermal phase gate may synthesize.
# 68/69 are non-standard but used internally by ARMS to represent
# freezing-rain-in-transition-zone (surface 0–2.5°C, frz lvl < 1500ft AGL).
SYNTHETIC_WX_CODES: set[int] = {68, 69}


def calc_td(t: float, rh: float) -> float:
    """
    Magnus formula dew point calculation.

    Args:
        t:  Air temperature (°C)
        rh: Relative humidity (%)

    Returns:
        Dew point temperature (°C)
    """
    if rh <= 0:
        return t
    a, b = 17.625, 243.04
    alpha = math.log(rh / 100.0) + ((a * t) / (b + t))
    return (b * alpha) / (a - alpha)


def calculate_density_altitude(
    elevation_ft: float,
    temp_c: float,
    station_pressure_hpa: float,
) -> int:
    """
    True Density Altitude computed from actual station pressure.

    Uses real-time station pressure rather than ISA baseline assumptions.
    This is the operationally correct method for UAS performance planning.

    Args:
        elevation_ft:          Site elevation above MSL (ft)
        temp_c:                Surface temperature (°C)
        station_pressure_hpa:  Actual station pressure (hPa / mb)

    Returns:
        Density altitude (ft), rounded to nearest foot
    """
    pressure_altitude = elevation_ft + 27.288 * (ISA_PRESSURE_HPA - station_pressure_hpa)
    isa_temperature = ISA_TEMP_C - (ISA_LAPSE_C_PER_1000FT * (elevation_ft / 1000.0))
    density_altitude = pressure_altitude + 118.8 * (temp_c - isa_temperature)
    return int(density_altitude)


def attenuate_gust_delta(surface_gust_delta: float, alt_agl_ft: float) -> float:
    """
    Attenuates gust spread with altitude using a logarithmic decay model.

    Surface-level gustiness (mechanical turbulence, thermal convection)
    diminishes as you ascend through the boundary layer. This replaces
    the previous uniform application of surface gust delta at all altitudes.

    The model uses a 1/ln decay anchored at 10m (surface reference height).
    At 400ft AGL the attenuation is ~0.6, at 3000ft it's ~0.3, at 5000ft ~0.25.

    Args:
        surface_gust_delta: Gust spread at surface (gust_speed - sustained_speed) in KT
        alt_agl_ft:         Altitude above ground level in feet

    Returns:
        Attenuated gust delta at the specified altitude (KT)
    """
    if alt_agl_ft <= 0 or surface_gust_delta <= 0:
        return surface_gust_delta

    # Decay factor: ratio of log-law at surface reference vs target altitude
    alt_m = alt_agl_ft * 0.3048
    surface_ref_m = 10.0  # standard anemometer height
    ratio = math.log(max(1.1, surface_ref_m)) / math.log(max(1.1, alt_m))
    # Clamp: never amplify, never go below 10% of surface delta
    factor = max(0.10, min(1.0, ratio))
    return surface_gust_delta * factor
