import ephem
import math
from datetime import datetime, timezone
from timezonefinder import TimezoneFinder
import pytz

def get_cardinal_direction(azimuth_deg):
    """Converts a 360-degree azimuth into an 8-point cardinal direction."""
    directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
    idx = int(round(azimuth_deg / 45.0)) % 8
    return directions[idx]

def get_astronomical_data(lat, lon, time_utc):
    """Calculates high-precision astronomical data and converts to local target time."""
    # Identify target local timezone based on coordinates
    tf = TimezoneFinder()
    tz_str = tf.timezone_at(lng=lon, lat=lat)
    local_tz = pytz.timezone(tz_str) if tz_str else timezone.utc

    obs = ephem.Observer()
    obs.lat = str(lat)
    obs.lon = str(lon)
    obs.date = time_utc

    sun = ephem.Sun()
    moon = ephem.Moon()
    
    # Calculate exact positions for the specific forecast hour
    sun.compute(obs)
    moon.compute(obs)
    
    # Calculate daily events by setting observer to midnight UTC of the target date
    midnight = datetime(time_utc.year, time_utc.month, time_utc.day, tzinfo=timezone.utc)
    obs_daily = ephem.Observer()
    obs_daily.lat = str(lat)
    obs_daily.lon = str(lon)
    obs_daily.date = midnight

    def get_event(func, body):
        try:
            # Calculate the event in UTC, then immediately convert to the target's local timezone
            dt_utc = func(body).datetime().replace(tzinfo=timezone.utc)
            dt_local = dt_utc.astimezone(local_tz)
            return dt_local.strftime("%H:%M")
        except ephem.AlwaysUpError:
            return "UP 24H"
        except ephem.NeverUpError:
            return "DOWN 24H"
        except Exception:
            return "N/A"

    obs_daily.horizon = '-0:34' # Standard atmospheric refraction for sunrise/sunset
    sunrise = get_event(obs_daily.next_rising, sun)
    sunset = get_event(obs_daily.next_setting, sun)
    
    obs_daily.horizon = '0' # Moonrise/moonset
    moonrise = get_event(obs_daily.next_rising, moon)
    moonset = get_event(obs_daily.next_setting, moon)

    obs_daily.horizon = '-6' # Civil Twilight boundary
    dawn = get_event(obs_daily.next_rising, sun)
    dusk = get_event(obs_daily.next_setting, sun)

    sun_az_deg = math.degrees(sun.az)
    moon_az_deg = math.degrees(moon.az)

    # Grab the human-readable timezone abbreviation (e.g., EST, EDT, PST)
    tz_abbr = datetime.now(local_tz).tzname() if tz_str else "UTC"

    return {
        "sun_dir": get_cardinal_direction(sun_az_deg),
        "sun_alt": int(math.degrees(sun.alt)),
        "moon_dir": get_cardinal_direction(moon_az_deg),
        "moon_alt": int(math.degrees(moon.alt)),
        "moon_ill": int(moon.phase), # Percentage illuminated
        "sunrise": sunrise,
        "sunset": sunset,
        "dawn": dawn,
        "dusk": dusk,
        "moonrise": moonrise,
        "moonset": moonset,
        "tz": tz_abbr
    }
