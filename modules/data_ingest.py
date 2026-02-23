import requests
import streamlit as st
import re
from modules.hazard_logic import apply_tactical_highlights

@st.cache_data(ttl=60)
def get_aviation_weather(station):
    headers = {"X-API-Key": "c453505478304bbbae7761f99c8a84ba"}
    try:
        m_res = requests.get(f"https://api.checkwx.com/metar/{station}/decoded?count=3", headers=headers, timeout=10)
        t_res = requests.get(f"https://api.checkwx.com/taf/{station}/decoded", headers=headers, timeout=10)
        m_data = m_res.json()
        metars = [apply_tactical_highlights(r.get('raw_text', '')) for r in m_data.get('data', [])]
        for i in range(len(metars)):
            if "SPECI" in metars[i]: metars[i] = metars[i].replace("SPECI", '<span style="color: #E58E26; font-weight: bold;">SPECI</span>')
        taf_raw = t_res.json().get('data', [{}])[0].get('raw_text', "NO ACTIVE TAF")
        taf_final = re.sub(r'\b(FM\d{6}|TEMPO|PROB\d{2}|BECMG)\b', r'<br><b>\1</b>', apply_tactical_highlights(taf_raw))
        return "<br>".join(metars) if metars else "NO DATA", taf_final
    except Exception: 
        return "LINK FAILURE", "LINK FAILURE"

@st.cache_data(ttl=600)
def fetch_mission_data(lat, lon, model_url):
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    hourly = ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_direction_10m", "weather_code", "freezing_level_height"]
    
    if "gem" in model_url: 
        hourly += ["wind_gusts_10m", "wind_speed_80m", "wind_speed_120m", "wind_direction_80m", "wind_direction_120m"]
    else: 
        hourly += ["wind_speed_100m", "wind_direction_100m"]
        
    hourly += [f"temperature_{p}hPa" for p in p_levels] + [f"dewpoint_{p}hPa" for p in p_levels] + [f"geopotential_height_{p}hPa" for p in p_levels] + [f"wind_speed_{p}hPa" for p in p_levels] + [f"wind_direction_{p}hPa" for p in p_levels]
    
    res = requests.get(model_url, params={"latitude": lat, "longitude": lon, "hourly": hourly, "wind_speed_unit": "kn", "forecast_hours": 48, "timezone": "UTC", "elevation": "nan"})
    return res.json() if res.status_code == 200 else None
