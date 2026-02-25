import urllib.request
import urllib.error
import json
import ssl

def fetch_mission_data(lat, lon, model_url):
    """
    Fetches raw atmospheric column data.
    Uses a clean Dual-Fetch for GEM models, strictly avoiding unsupported 
    variables (like freezing_level_height) to prevent 400 Bad Request crashes.
    """
    is_gem = "gem" in model_url
    
    try:
        # Ignore SSL certificate verification to prevent firewall/cloud blockages
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        if is_gem:
            # ---------------------------------------------------------
            # FETCH 1: Pure 2.5km HRDPS (Strictly Surface Boundary Layer)
            # ---------------------------------------------------------
            hrdps_params = "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,wind_speed_120m,wind_direction_120m"
            url_sfc = f"https://api.open-meteo.com/v1/gem?latitude={lat}&longitude={lon}&hourly={hrdps_params}&models=gem_hrdps_continental&timezone=UTC&wind_speed_unit=knots"
            
            req_sfc = urllib.request.Request(url_sfc, headers={'User-Agent': 'VectorCheck-App/7.0'})
            with urllib.request.urlopen(req_sfc, context=ctx, timeout=10) as response:
                data_master = json.loads(response.read().decode('utf-8'))

            # ---------------------------------------------------------
            # FETCH 2: Pure 10km RDPS (Upper Trajectory Matrix)
            # ---------------------------------------------------------
            rdps_params = "temperature_925hPa,geopotential_height_1000hPa,wind_speed_1000hPa,wind_direction_1000hPa,geopotential_height_925hPa,wind_speed_925hPa,wind_direction_925hPa,geopotential_height_850hPa,wind_speed_850hPa,wind_direction_850hPa,geopotential_height_700hPa,wind_speed_700hPa,wind_direction_700hPa"
            url_upr = f"https://api.open-meteo.com/v1/gem?latitude={lat}&longitude={lon}&hourly={rdps_params}&models=gem_regional&timezone=UTC&wind_speed_unit=knots"
            
            req_upr = urllib.request.Request(url_upr, headers={'User-Agent': 'VectorCheck-App/7.0'})
            with urllib.request.urlopen(req_upr, context=ctx, timeout=10) as response:
                data_upr = json.loads(response.read().decode('utf-8'))

            # ---------------------------------------------------------
            # MERGE: Stitch the upper air arrays into the master payload
            # ---------------------------------------------------------
            if 'hourly' in data_master and 'hourly' in data_upr:
                for key, val_array in data_upr['hourly'].items():
                    if key != "time":
                        data_master['hourly'][key] = val_array

            return data_master

        else:
            # ---------------------------------------------------------
            # STANDARD FETCH: ECMWF (Global 9km - Natively supports freezing level)
            # ---------------------------------------------------------
            ecmwf_params = "temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m,wind_direction_10m,wind_gusts_10m,freezing_level_height,temperature_925hPa,wind_speed_100m,wind_direction_100m,geopotential_height_1000hPa,wind_speed_1000hPa,wind_direction_1000hPa,geopotential_height_925hPa,wind_speed_925hPa,wind_direction_925hPa,geopotential_height_850hPa,wind_speed_850hPa,wind_direction_850hPa,geopotential_height_700hPa,wind_speed_700hPa,wind_direction_700hPa"
            url_ecmwf = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly={ecmwf_params}&models=ecmwf_ifs04&timezone=UTC&wind_speed_unit=knots"
            
            req_ecmwf = urllib.request.Request(url_ecmwf, headers={'User-Agent': 'VectorCheck-App/7.0'})
            with urllib.request.urlopen(req_ecmwf, context=ctx, timeout=10) as response:
                return json.loads(response.read().decode('utf-8'))

    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        print(f"API HTTPError {e.code}: {error_msg}")
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
