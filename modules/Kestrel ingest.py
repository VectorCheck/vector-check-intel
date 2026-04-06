"""
VECTOR CHECK AERIAL GROUP INC. — Kestrel Surface Data Integration

Ingests real-time surface observations from Kestrel weather meters
(5500/5700 Elite series) via Bluetooth LE data export or manual entry,
and cross-references against the active NWP forecast to compute
Model Verification Scores (MVS).

DATA FLOW:
    Kestrel BLE → CSV/JSON export → parse_kestrel_payload()
    Manual entry → build_kestrel_observation()
    Either path → compute_forecast_delta() → UI display

KESTREL OUTPUT FIELDS (5700 Elite):
    Temperature (°C/°F), Relative Humidity (%), Dewpoint (°C/°F),
    Station Pressure (hPa/inHg), Barometric Pressure (hPa/inHg),
    Wind Speed (m/s/kt/mph/km/h), Wind Direction (°mag),
    Crosswind, Headwind, Density Altitude (ft/m),
    Heat Index, Wind Chill, Wet Bulb

INTEGRATION TARGETS:
    - Surface conditions panel: overlay actuals on forecast
    - Impact Matrix: re-evaluate Go/No-Go with ground-truth
    - PDF export: include "Verified by Kestrel" stamp
    - Telemetry log: persist obs for post-mission analysis
"""

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional
import csv
import json
import io
import math

from modules.physics import calc_td, calculate_density_altitude, METERS_TO_FEET


@dataclass
class KestrelObservation:
    """Normalized surface observation from a Kestrel weather meter."""
    timestamp: datetime
    temp_c: float
    rh: int
    dewpoint_c: float
    wind_speed_kt: float
    wind_dir_mag: float          # magnetic bearing
    wind_gust_kt: float
    station_pressure_hpa: float
    density_alt_ft: int
    crosswind_kt: float = 0.0
    headwind_kt: float = 0.0
    wet_bulb_c: float = 0.0
    heat_index_c: float = 0.0
    wind_chill_c: float = 0.0
    source: str = "KESTREL"
    device_model: str = "5700"
    magnetic_declination: float = 0.0  # applied to convert mag→true

    @property
    def wind_dir_true(self) -> float:
        """Wind direction corrected to true north."""
        return (self.wind_dir_mag + self.magnetic_declination) % 360

    def to_dict(self) -> dict:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        d['wind_dir_true'] = self.wind_dir_true
        return d


@dataclass
class ForecastDelta:
    """Quantified difference between Kestrel observation and NWP forecast.

    Positive delta = forecast OVER-predicted the value.
    Negative delta = forecast UNDER-predicted the value.
    """
    temp_delta_c: float = 0.0
    rh_delta_pct: float = 0.0
    dewpoint_delta_c: float = 0.0
    wind_speed_delta_kt: float = 0.0
    wind_dir_delta_deg: float = 0.0
    gust_delta_kt: float = 0.0
    pressure_delta_hpa: float = 0.0
    density_alt_delta_ft: int = 0
    vis_category_match: bool = True

    # Model Verification Score: 0–100 (100 = perfect forecast)
    mvs: int = 100

    # Human-readable assessment
    assessment: str = ""
    details: list[str] = field(default_factory=list)


def build_kestrel_observation(
    temp_c: float,
    rh: int,
    wind_speed_kt: float,
    wind_dir_mag: float,
    wind_gust_kt: float,
    station_pressure_hpa: float,
    elevation_ft: float,
    magnetic_declination: float = 0.0,
    crosswind_kt: float = 0.0,
    headwind_kt: float = 0.0,
    device_model: str = "5700",
) -> KestrelObservation:
    """Constructs a KestrelObservation from manual sidebar entry values.

    Computes dewpoint and density altitude from the raw inputs so the
    operator doesn't need to enter them separately.
    """
    dewpoint = calc_td(temp_c, rh)
    da = calculate_density_altitude(elevation_ft, temp_c, station_pressure_hpa)

    return KestrelObservation(
        timestamp=datetime.now(timezone.utc),
        temp_c=temp_c,
        rh=rh,
        dewpoint_c=dewpoint,
        wind_speed_kt=wind_speed_kt,
        wind_dir_mag=wind_dir_mag,
        wind_gust_kt=wind_gust_kt,
        station_pressure_hpa=station_pressure_hpa,
        density_alt_ft=da,
        crosswind_kt=crosswind_kt,
        headwind_kt=headwind_kt,
        source="KESTREL_MANUAL",
        device_model=device_model,
        magnetic_declination=magnetic_declination,
    )


def parse_kestrel_csv(csv_text: str, magnetic_declination: float = 0.0) -> list[KestrelObservation]:
    """Parses a Kestrel Link CSV export into a list of observations.

    Kestrel Link exports CSV with headers like:
        FORMATTED DATE_TIME, Temperature, Relative Humidity,
        Station Pressure, Wind Speed, Wind Direction, ...

    Unit detection is automatic — the header row contains unit suffixes
    like "(°C)", "(kt)", "(hPa)" which determine conversion factors.
    """
    observations: list[KestrelObservation] = []
    reader = csv.DictReader(io.StringIO(csv_text))

    # Detect units from header
    headers = reader.fieldnames or []
    temp_key = _find_header(headers, "temperature")
    rh_key = _find_header(headers, "relative humidity")
    ws_key = _find_header(headers, "wind speed")
    wd_key = _find_header(headers, "wind direction")
    press_key = _find_header(headers, "station pressure")
    da_key = _find_header(headers, "density altitude")
    xw_key = _find_header(headers, "crosswind")
    hw_key = _find_header(headers, "headwind")
    time_key = _find_header(headers, "date") or _find_header(headers, "time")

    is_fahrenheit = temp_key and "f" in temp_key.lower() and "°f" in temp_key.lower()
    is_mph = ws_key and "mph" in ws_key.lower()
    is_kmh = ws_key and "km" in ws_key.lower()
    is_inhg = press_key and "inhg" in press_key.lower()

    for row in reader:
        try:
            # Temperature
            temp_raw = float(row.get(temp_key, 0))
            temp_c = (temp_raw - 32) * 5 / 9 if is_fahrenheit else temp_raw

            # RH
            rh = int(float(row.get(rh_key, 0)))

            # Wind speed → knots
            ws_raw = float(row.get(ws_key, 0))
            if is_mph:
                ws_kt = ws_raw * 0.868976
            elif is_kmh:
                ws_kt = ws_raw * 0.539957
            else:
                ws_kt = ws_raw  # assume knots

            # Wind direction (magnetic)
            wd_mag = float(row.get(wd_key, 0))

            # Pressure → hPa
            press_raw = float(row.get(press_key, 1013.25))
            press_hpa = press_raw * 33.8639 if is_inhg else press_raw

            # Optional fields
            xw = float(row.get(xw_key, 0)) if xw_key else 0.0
            hw = float(row.get(hw_key, 0)) if hw_key else 0.0

            # Density altitude
            da_raw = float(row.get(da_key, 0)) if da_key else 0
            da_ft = int(da_raw)

            # Timestamp
            ts = _parse_kestrel_time(row.get(time_key, ""))

            obs = KestrelObservation(
                timestamp=ts,
                temp_c=round(temp_c, 1),
                rh=rh,
                dewpoint_c=round(calc_td(temp_c, rh), 1),
                wind_speed_kt=round(ws_kt, 1),
                wind_dir_mag=wd_mag,
                wind_gust_kt=round(ws_kt, 1),  # Kestrel CSV typically has avg, not gust
                station_pressure_hpa=round(press_hpa, 1),
                density_alt_ft=da_ft,
                crosswind_kt=round(xw, 1),
                headwind_kt=round(hw, 1),
                source="KESTREL_CSV",
                magnetic_declination=magnetic_declination,
            )
            observations.append(obs)
        except (ValueError, TypeError, KeyError):
            continue

    return observations


def compute_forecast_delta(
    obs: KestrelObservation,
    fcst_temp_c: float,
    fcst_rh: int,
    fcst_wind_kt: float,
    fcst_wind_dir: float,
    fcst_gust_kt: float,
    fcst_pressure_hpa: float,
    fcst_density_alt: int,
    fcst_vis_sm: float,
) -> ForecastDelta:
    """Computes the quantified difference between a Kestrel observation
    and the corresponding NWP forecast hour.

    Returns a ForecastDelta with per-parameter deltas and an overall
    Model Verification Score (MVS) from 0–100.
    """
    delta = ForecastDelta()

    # --- Per-parameter deltas ---
    delta.temp_delta_c = fcst_temp_c - obs.temp_c
    delta.rh_delta_pct = fcst_rh - obs.rh
    delta.dewpoint_delta_c = calc_td(fcst_temp_c, fcst_rh) - obs.dewpoint_c
    delta.wind_speed_delta_kt = fcst_wind_kt - obs.wind_speed_kt
    delta.gust_delta_kt = fcst_gust_kt - obs.wind_gust_kt
    delta.pressure_delta_hpa = fcst_pressure_hpa - obs.station_pressure_hpa
    delta.density_alt_delta_ft = fcst_density_alt - obs.density_alt_ft

    # Wind direction delta (shortest arc)
    raw_dir_diff = fcst_wind_dir - obs.wind_dir_true
    delta.wind_dir_delta_deg = ((raw_dir_diff + 180) % 360) - 180

    # --- Model Verification Score (MVS) ---
    # Weighted penalty system: each parameter deducts points proportional
    # to its operational significance for UAS flight safety.
    penalties = 0.0

    # Temperature: 2 pts per °C error (max 20)
    penalties += min(20, abs(delta.temp_delta_c) * 2)

    # Wind speed: 3 pts per kt error (max 30) — most operationally critical
    penalties += min(30, abs(delta.wind_speed_delta_kt) * 3)

    # Wind direction: 0.5 pts per degree (max 15)
    penalties += min(15, abs(delta.wind_dir_delta_deg) * 0.5)

    # Gust: 2 pts per kt error (max 20)
    penalties += min(20, abs(delta.gust_delta_kt) * 2)

    # Pressure: 5 pts per hPa error (max 10)
    penalties += min(10, abs(delta.pressure_delta_hpa) * 5)

    # RH: 0.2 pts per % error (max 5)
    penalties += min(5, abs(delta.rh_delta_pct) * 0.2)

    delta.mvs = max(0, int(100 - penalties))

    # --- Human-readable assessment ---
    if delta.mvs >= 85:
        delta.assessment = "EXCELLENT — Model closely tracks observed conditions."
    elif delta.mvs >= 70:
        delta.assessment = "GOOD — Minor model deviations. Forecast remains operationally valid."
    elif delta.mvs >= 50:
        delta.assessment = "FAIR — Significant deviations detected. Increased pilot vigilance required."
    else:
        delta.assessment = "POOR — Model divergence exceeds thresholds. Re-evaluate Go/No-Go with actuals."

    # --- Detail flags ---
    if abs(delta.wind_speed_delta_kt) >= 10:
        direction = "UNDER" if delta.wind_speed_delta_kt < 0 else "OVER"
        delta.details.append(f"Wind speed {direction}-forecast by {abs(delta.wind_speed_delta_kt):.0f} kt")

    if abs(delta.temp_delta_c) >= 3:
        direction = "UNDER" if delta.temp_delta_c < 0 else "OVER"
        delta.details.append(f"Temperature {direction}-forecast by {abs(delta.temp_delta_c):.1f}°C")

    if abs(delta.wind_dir_delta_deg) >= 30:
        delta.details.append(f"Wind direction off by {abs(delta.wind_dir_delta_deg):.0f}°")

    if abs(delta.gust_delta_kt) >= 8:
        direction = "UNDER" if delta.gust_delta_kt < 0 else "OVER"
        delta.details.append(f"Gusts {direction}-forecast by {abs(delta.gust_delta_kt):.0f} kt")

    if abs(delta.pressure_delta_hpa) >= 2:
        delta.details.append(f"Pressure off by {abs(delta.pressure_delta_hpa):.1f} hPa")

    return delta


# --- Internal helpers ---

def _find_header(headers: list[str], keyword: str) -> Optional[str]:
    """Fuzzy-matches a Kestrel CSV header by keyword."""
    keyword_lower = keyword.lower()
    for h in headers:
        if keyword_lower in h.lower():
            return h
    return None


def _parse_kestrel_time(time_str: str) -> datetime:
    """Attempts to parse Kestrel Link timestamp formats."""
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%m/%d/%Y %I:%M:%S %p",
        "%m/%d/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(time_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return datetime.now(timezone.utc)
