import urllib.request
import urllib.error
import json
import ssl

def fetch_mission_data(lat, lon, model_url):
    """
    Fetches raw atmospheric column data.
    Uses the master /v1/forecast endpoint with standardized 100m variables 
    to guarantee seamless blending of HRDPS (2.5km) and RDPS (10km) without API crashes.
    """
    is_gem = "gem" in model_url
    base_url = "https://api.open-meteo.com/v1/forecast"

    hourly_params = [
        "temperature_2m", "relative_humidity_2m", "weather_code", 
        "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", 
        "freezing_level_height", "wind_speed_100m", "wind_direction_100m",
        "temperature_925hPa"
    ]
    
    # WMO Standard Pressure Levels guaranteed to exist in both models
    pressure_levels = [1000, 925, 850, 700]
    for p in pressure_levels:
        hourly_params.extend([
            f"geopotential_height_{p}hPa",
            f"wind_speed_{p}hPa",
            f"wind_direction_{p}hPa"
        ])

    params_str = ",".join(hourly_params)
    
    # The array commands native server blending: try HRDPS first, fallback to RDPS
    model_param = "gem_hrdps_continental,gem_regional" if is_gem else "ecmwf_ifs04"
    
    url = f"{base_url}?latitude={lat}&longitude={lon}&hourly={params_str}&models={model_param}&timezone=UTC&wind_speed_unit=knots"

    try:
        # Ignore SSL certificate verification to prevent firewall/cloud blockages
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-App/8.0'})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            return json.loads(response.read().decode('utf-8'))
            
    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        print(f"API HTTPError {e.code}: {error_msg}") # Prints exact server rejection reason to terminal
        return None
    except Exception as e:
        print(f"API Error: {e}")
        return None

def get_aviation_weather(icao):
    """Fetches raw METAR and TAF directly from the Aviation Weather Center API."""
    metar = "UNAVAILABLE"
    taf = "UNAVAILABLE"
    
    if not icao or icao == "NONE" or icao == "N/A":
        return metar, taf
        
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        metar_url = f"https://aviationweather.gov/api/data/metar?ids={icao}&format=raw&hours=1"
        req_m = urllib.request.Request(metar_url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req_m, context=ctx, timeout=5) as resp:
            m_data = resp.read().decode('utf-8').strip()
            if m_data:
                metar = m_data.split('\n')[0]

        taf_url = f"https://aviationweather.gov/api/data/taf?ids={icao}&format=raw"
        req_t = urllib.request.Request(taf_url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req_t, context=ctx, timeout=5) as resp:
            t_data = resp.read().decode('utf-8').strip()
            if t_data:
                taf = t_data

    except Exception as e:
        pass
        
    return metar, taf
