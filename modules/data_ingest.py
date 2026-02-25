import urllib.request
import urllib.error
import json
import ssl

def fetch_mission_data(lat, lon, model_url):
    """
    Fetches raw atmospheric column data.
    Executes a "Base-and-Overlay" strategy: Uses 10km RDPS as the master dataset to 
    guarantee stability, and overlays 2.5km HRDPS surface data if available.
    """
    is_gem = "gem" in model_url
    
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        if is_gem:
            # ---------------------------------------------------------
            # 1. THE BASE FETCH: 10km RDPS (Highly Stable Master Dataset)
            # ---------------------------------------------------------
            rdps_params_list = [
                "temperature_2m", "relative_humidity_2m", "weather_code",
                "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m",
                "temperature_925hPa"
            ]
            for p in [1000, 925, 850, 700]:
                rdps_params_list.extend([
                    f"geopotential_height_{p}hPa", 
                    f"wind_speed_{p}hPa", 
                    f"wind_direction_{p}hPa"
                ])
                
            rdps_params = ",".join(rdps_params_list)
            url_rdps = f"{model_url}?latitude={lat}&longitude={lon}&hourly={rdps_params}&models=gem_regional&timezone=UTC&wind_speed_unit=knots"
            
            req_rdps = urllib.request.Request(url_rdps, headers={'User-Agent': 'VectorCheck-App/10.0'})
            with urllib.request.urlopen(req_rdps, context=ctx, timeout=10) as response:
                data_master = json.loads(response.read().decode('utf-8'))

            # ---------------------------------------------------------
            # 2. THE SUPPLEMENT: 2.5km HRDPS (High-Res Overlay)
            # ---------------------------------------------------------
            hrdps_params = "temperature_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m"
            url_hrdps = f"{model_url}?latitude={lat}&longitude={lon}&hourly={hrdps_params}&models=gem_hrdps_continental&timezone=UTC&wind_speed_unit=knots"
            
            try:
                req_hrdps = urllib.request.Request(url_hrdps, headers={'User-Agent': 'VectorCheck-App/10.0'})
                with urllib.request.urlopen(req_hrdps, context=ctx, timeout=5) as response:
                    data_hrdps = json.loads(response.read().decode('utf-8'))
                    
                    # Safely overwrite the RDPS surface data with the HRDPS high-res data
                    if 'hourly' in data_hrdps:
                        for key in ["temperature_2m", "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"]:
                            if key in data_hrdps['hourly']:
                                data_master['hourly'][key] = data_hrdps['hourly'][key]
            except Exception as e:
                # If HRDPS fails, silently pass. The dashboard will survive using the RDPS base data.
                print(f"HRDPS Overlay failed (continuing with pure RDPS): {e}")

            return data_master

        else:
            # ---------------------------------------------------------
            # STANDARD FETCH: ECMWF (Global 9km)
            # ---------------------------------------------------------
            ecmwf_params_list = [
                "temperature_2m", "relative_humidity_2m", "weather_code", 
                "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", 
                "freezing_level_height", "temperature_925hPa",
                "wind_speed_100m", "wind_direction_100m"
            ]
            for p in [1000, 925, 850, 700]:
                ecmwf_params_list.extend([
                    f"geopotential_height_{p}hPa", 
                    f"wind_speed_{p}hPa", 
                    f"wind_direction_{p}hPa"
                ])
                
            params_str = ",".join(ecmwf_params_list)
            url_ecmwf = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly={params_str}&models=ecmwf_ifs04&timezone=UTC&wind_speed_unit=knots"
            
            req_ecmwf = urllib.request.Request(url_ecmwf, headers={'User-Agent': 'VectorCheck-App/10.0'})
            with urllib.request.urlopen(req_ecmwf, context=ctx, timeout=10) as response:
                return json.loads(response.read().decode('utf-8'))

    except urllib.error.HTTPError as e:
        error_msg = e.read().decode('utf-8')
        print(f"API HTTPError {e.code}: {error_msg}")
        return None
    except Exception as e:
        print(f"API General Error: {e}")
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
