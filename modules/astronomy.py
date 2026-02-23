import ephem
import math
from datetime import datetime, timezone

def get_astronomical_data(lat, lon, time_utc):
    """Calculates high-precision astronomical data for the given coordinates and time."""
    obs = ephem.Observer()
    obs.lat = str(lat)
    obs.lon = str(lon)
    obs.date = time_utc

    sun = ephem.Sun()
    moon = ephem.Moon()
    
    # Calculate exact positions for the specific forecast hour
    sun.compute(obs)
    moon.compute(obs)
    
    # Calculate daily events by setting observer to midnight of the target date
    midnight = datetime(time_utc.year, time_utc.month, time_utc.day, tzinfo=timezone.utc)
    obs_daily = ephem.Observer()
    obs_daily.lat = str(lat)
    obs_daily.lon = str(lon)
    obs_daily.date = midnight

    def get_event(func, body):
        try:
            dt_utc = func(body).datetime()
            return dt_utc.replace(tzinfo=timezone.utc).strftime("%H:%M Z")
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

    return {
        "sun_az": int(math.degrees(sun.az)),
        "sun_alt": int(math.degrees(sun.alt)),
        "moon_az": int(math.degrees(moon.az)),
        "moon_alt": int(math.degrees(moon.alt)),
        "moon_ill": int(moon.phase), # Percentage illuminated
        "sunrise": sunrise,
        "sunset": sunset,
        "dawn": dawn,
        "dusk": dusk,
        "moonrise": moonrise,
        "moonset": moonset
    }
