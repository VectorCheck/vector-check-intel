import math

# --- VECTOR CHECK AERIAL GROUP INC. : SHARED PHYSICS CONSTANTS ---

# Minimum snowpack depth (metres) required to trigger the BLSN kinetic gate
# when no active precipitation is present. 0.05 m = 5 cm.
SNOWPACK_BLSN_THRESHOLD_M: float = 0.05


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
    station_pressure_hpa: float
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
    pressure_altitude = elevation_ft + 27.288 * (1013.25 - station_pressure_hpa)
    isa_temperature    = 15.0 - (1.98 * (elevation_ft / 1000.0))
    density_altitude   = pressure_altitude + 118.8 * (temp_c - isa_temperature)
    return int(density_altitude)
