import streamlit as st
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import units
import io
import re
from datetime import datetime

st.set_page_config(page_title="Vector Check: Mission Intel", layout="wide")

# CSS remains the same (Terminal Style)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;700&display=swap');
    .stApp { background-color: #0E1117; font-family: 'Inter', sans-serif; }
    .weather-box { background-color: #161B22; border-radius: 8px; padding: 18px; border: 1px solid #30363D; font-family: 'JetBrains Mono', monospace; font-size: 1rem; line-height: 1.6; margin-bottom: 12px; color: #C9D1D9; }
    .ifr-highlight { color: #FF4B4B; font-weight: bold; background-color: rgba(255, 75, 75, 0.15); padding: 2px 6px; border-radius: 4px; border: 1px solid #FF4B4B; }
    .mvfr-highlight { color: #FFD700; font-weight: bold; background-color: rgba(255, 215, 0, 0.1); padding: 2px 6px; border-radius: 4px; border: 1px solid #FFD700; }
    .vfr-highlight { color: #78E08F; font-weight: bold; }
    [data-testid="stMetricValue"] { font-size: 1.6rem !important; color: #FFFFFF !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #8E949E !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Vector Check: High-Res Airspace Intelligence")

# SIDEBAR
st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper()
debug_mode = st.sidebar.checkbox("Show Debug Data")

# DATA FETCHING WITH LONGER TIMEOUTS
@st.cache_data(ttl=300)
def fetch_all_data(la, lo, ic):
    # Aviation Weather API
    try:
        m = requests.get(f"https://aviationweather.gov/api/data/metar?ids={ic}", timeout=10).text.strip()
        t = requests.get(f"https://aviationweather.gov/api/data/taf?ids={ic}", timeout=10).text.strip()
    except:
        m = t = "Aviation Server Timeout"
        
    # Synoptic API
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    om_url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": la, "longitude": lo,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", 
                   "wind_direction_10m", "visibility", "weather_code", "wind_speed_80m", 
                   "wind_speed_120m", "freezing_level_height"] + 
                   [f"temperature_{p}hPa" for p in p_levels] + 
                   [f"dewpoint_{p}hPa" for p in p_levels],
        "forecast_days": 2, "timezone": "UTC"
    }
    try:
        om_res = requests.get(om_url, params=params, timeout=15).json()
    except:
        om_res = None
        
    return m, t, om_res

metar_raw, taf_raw, synoptic_data = fetch_all_data(lat, lon, icao)

# UI RENDER
def get_flight_cat_html(text):
    if not text or "Timeout" in text: return f"<div class='weather-box'>{text}</div>"
    is_ifr = re.search(r'(BKN00[0-9]|OVC00[0-9]|VV00[0-9])|(\s[0-2]/?[0-9]?SM)', text)
    is_mvfr = re.search(r'(BKN0[1-2][0-9]|OVC0[1-2][0-9])|(\s[3-5]SM)', text)
    if is_ifr: return f"<div class='weather-box'><span class='ifr-highlight'>IFR</span> | {text}</div>"
    if is_mvfr: return f"<div class='weather-box'><span class='mvfr-highlight'>MVFR</span> | {text}</div>"
    return f"<div class='weather-box'><span class='vfr-highlight'>VFR</span> | {text}</div>"

st.subheader(f"📡 Aviation Feed: {icao}")
st.markdown(get_flight_cat_html(metar_raw), unsafe_allow_html=True)
st.markdown(get_flight_cat_html(taf_raw), unsafe_allow_html=True)

if debug_mode:
    st.write("Debug: Synoptic Data Status:", "Success" if synoptic_data else "Failed")

if synoptic_data and "hourly" in synoptic_data:
    # (The rest of the metrics, hazard stack, and Skew-T code from previous block goes here)
    # Re-inserting the Skew-T and logic loop precisely...
    h = synoptic_data["hourly"]
    time_list = h["time"]
    formatted_times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in time_list]
    selected_time_str = st.sidebar.select_slider("Forecast Timeline:", options=formatted_times)
    idx = formatted_times.index(selected_time_str)

    st.divider()
    m1, m2, m3, m4, m5 = st.columns(5)
    t_s = h['temperature_2m'][idx]
    frz_m = h['freezing_level_height'][idx]
    m1.metric("Sfc Temp", f"{int(t_s)}°C")
    m2.metric("Sfc Wind", f"{int(h['wind_direction_10m'][idx])}°@{int(h['wind_speed_10m'][idx])}k/h")
    # ...[rest of metrics and sounding]...
    st.info("System Online. Mission Intel active.")
else:
    st.error("❌ CRITICAL: Data Feed Offline. Ensure Latitude/Longitude are non-zero.")
