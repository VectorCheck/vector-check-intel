import streamlit as st
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import units
import io
import math
from datetime import datetime

# 1. PAGE CONFIG & UI
st.set_page_config(page_title="Vector Check: Mission Intel", layout="wide")

st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.4rem !important; color: #E58E26 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #8E949E !important; }
    table { margin-left: auto; margin-right: auto; text-align: center !important; width: 90%; border-collapse: collapse; background-color: #1B1E23; }
    th { text-align: center !important; color: #8E949E !important; font-weight: bold !important; padding: 10px !important; border-bottom: 2px solid #3E444E !important; text-transform: uppercase; }
    td { text-align: center !important; padding: 8px !important; color: #D1D5DB !important; border-bottom: 1px solid #2D3139 !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Vector Check: High-Res Airspace Intelligence")
st.caption("TACTICAL ADVISORY - HRDPS / HRRR / ECMWF INTEGRATED")

# 2. SIDEBAR
st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper()

model_choice = st.sidebar.selectbox("Select Forecast Model:", 
    options=["HRDPS (Canada 2.5km)", "HRRR (USA 3km)", "ECMWF (Global 9km)"])

model_api_map = {
    "HRDPS (Canada 2.5km)": "https://api.open-meteo.com/v1/gem",
    "HRRR (USA 3km)": "https://api.open-meteo.com/v1/gfs",
    "ECMWF (Global 9km)": "https://api.open-meteo.com/v1/ecmwf"
}

# 3. ROBUST DATA FETCHING
@st.cache_data(ttl=600)
def fetch_mission_data(latitude, longitude, model_url, model_name):
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    params = {
        "latitude": latitude, "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_gusts_10m",
                   "wind_direction_10m", "visibility", "weather_code", "wind_speed_80m", 
                   "wind_speed_120m", "freezing_level_height", "cloud_cover", "pressure_msl"] + 
                   [f"temperature_{p}hPa" for p in p_levels] + [f"dewpoint_{p}hPa" for p in p_levels],
        "wind_speed_unit": "kn", "forecast_days": 2, "timezone": "UTC"
    }
    if "HRRR" in model_name: params["models"] = "hrrr"
    try:
        res = requests.get(model_url, params=params, timeout=15)
        res.raise_for_status()
        return res.json()
    except Exception: return None

@st.cache_data(ttl=300)
def get_aviation_weather(station):
    try:
        m = requests.get(f"https://aviationweather.gov/api/data/metar?ids={station}", timeout=5).text.strip()
        t = requests.get(f"https://aviationweather.gov/api/data/taf?ids={station}", timeout=5).text.strip()
        return m or "No METAR", t or "No TAF"
    except: return "Link Error", "Link Error"

# 4. UTILITIES (Fail-Safe)
def safe_val(val, multiplier=1, default="N/A", precision=0):
    if val is None: return default
    res = val * multiplier
    return f"{res:,.{precision}f}" if precision > 0 else f"{int(round(res)):,}"

def calc_dewpoint(T, RH):
    if T is None or RH is None: return None
    a, b = 17.27, 237.7
    alpha = ((a * T) / (b + T)) + math.log(max(RH, 1)/100.0)
    return (b * alpha) / (a - alpha)

def calc_da(temp, press_mb):
    if temp is None or press_mb is None: return None
    press_alt = (1013.25 - press_mb) * 30
    return press_alt + (118.8 * (temp - 15))

# 5. MAIN RENDER
data = fetch_mission_data(lat, lon, model_api_map[model_choice], model_choice)
metar, taf = get_aviation_weather(icao)

st.subheader(f"📡 {model_choice} Analysis")
c1, c2 = st.columns(2)
c1.code(metar, language="text")
c2.code(taf, language="text")

if data and "hourly" in data:
    h = data["hourly"]
    times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in h["time"]]
    selected_time = st.sidebar.select_slider("Forecast Hour:", options=times)
    idx = times.index(selected_time)
    
    def g(key): return h.get(key)[idx]

    # Derived Calcs
    td = calc_dewpoint(g('temperature_2m'), g('relative_humidity_2m'))
    da = calc_da(g('temperature_2m'), g('pressure_msl'))
    
    # Cloud Base Logic
    cb_val = "SKC"
    if td is not None and g('cloud_cover') > 25:
        base = (g('temperature_2m') - td) * 400
        cb_val = f"{int(max(base, 0)):,}ft"

    # Metrics
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("TEMP/DP", f"{safe_val(g('temperature_2m'))}°/{safe_val(td)}°C")
    m2.metric("DENSITY ALT", f"{safe_val(da)} ft")
    m3.metric("WIND", f"{safe_val(g('wind_direction_10m'))}° @ {safe_val(g('wind_speed_10m'))}kt")
    m4.metric("GUSTS", f"{safe_val(g('wind_gusts_10m'))} kt")
    m5.metric("FREEZING LVL", f"{safe_val(g('freezing_level_height'), 3.28084)} ft")
    m6.metric("CLOUD BASE", cb_val)

    # --- HAZARD STACK ---
    st.subheader("📊 Tactical Hazard Stack")
    w10, w80 = g('wind_speed_10m'), g('wind_speed_80m')
    gst = g('wind_gusts_10m')
    
    stack = []
    if all(v is not None for v in [w10, w80, gst]):
        for alt in [400, 250, 150, 50]:
            # Simple linear interp for the stack (more stable for sparse data)
            spd = w10 + (w80 - w10) * ((alt*0.3048 - 10) / 70)
            cur_gst = spd * (gst / max(w10, 1))
            
            turb = "NIL"
            if cur_gst > 25 or (cur_gst - spd) > 12: turb = "⚠️ SEVERE"
            elif cur_gst > 15: turb = "MOD"
            
            stack.append({"Alt (AGL)": f"{alt}ft", "Wind (kt)": int(spd), "Gust (kt)": int(cur_gst), "Turbulence": turb})
        st.table(pd.DataFrame(stack))
    else:
        st.warning("Insufficient wind data at this level/time for Hazard Stack.")

    # --- SKEW-T ---
    st.divider()
    p_levs = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    t_plot = [g(f'temperature_{p}hPa') for p in p_levs]
    td_plot = [g(f'dewpoint_{p}hPa') for p in p_levs]

    if None not in t_plot:
        fig = plt.figure(figsize=(7, 9))
        fig.patch.set_facecolor('#0E1117')
        skew = SkewT(fig, rotation=45)
        skew.ax.set_facecolor('#1B1E23')
        skew.plot(p_levs, np.array(t_plot) * units.degC, 'r', linewidth=2)
        skew.plot(p_levs, np.array(td_plot) * units.degC, 'g', linewidth=2)
        plt.title(f"Thermodynamic Profile: {icao}", color='white')
        st.pyplot(fig)
