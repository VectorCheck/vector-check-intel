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

# 1. PAGE CONFIG & UI LOCK
st.set_page_config(page_title="Vector Check: Mission Intel", layout="wide")

# CUSTOM CSS: STEALTH THEME (Vector Check Brand Colors)
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.4rem !important; color: #E58E26 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #8E949E !important; }
    .centered-table { display: flex; justify-content: center; margin-bottom: 20px; }
    table { margin-left: auto; margin-right: auto; text-align: center !important; width: 90%; border-collapse: collapse; background-color: #1B1E23; }
    th { text-align: center !important; color: #8E949E !important; font-weight: bold !important; padding: 10px !important; border-bottom: 2px solid #3E444E !important; text-transform: uppercase; }
    td { text-align: center !important; padding: 8px !important; color: #D1D5DB !important; border-bottom: 1px solid #2D3139 !important; }
    .stCode { background-color: #0E1117 !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Vector Check: High-Res Airspace Intelligence")
st.caption("ADVISORY DATA ONLY - FOR TACTICAL PLANNING PURPOSES")

# 2. SIDEBAR PARAMETERS
st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper()

st.sidebar.divider()

# Model Mapping for Vector Check Ops
model_choice = st.sidebar.selectbox("Select Forecast Model:", 
    options=["HRDPS (Canada 2.5km)", "HRRR (USA 3km)", "ECMWF (Global 9km)"],
    index=0)

model_api_map = {
    "HRDPS (Canada 2.5km)": "https://api.open-meteo.com/v1/gem",
    "HRRR (USA 3km)": "https://api.open-meteo.com/v1/gfs",
    "ECMWF (Global 9km)": "https://api.open-meteo.com/v1/ecmwf"
}

# 3. DATA FETCHING & MATH
@st.cache_data(ttl=600)
def fetch_mission_data(latitude, longitude, model_url, model_name):
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    params = {
        "latitude": latitude, "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_gusts_10m",
                   "wind_direction_10m", "visibility", "weather_code", "wind_speed_80m", 
                   "wind_speed_120m", "freezing_level_height", "cloud_cover", "pressure_msl"] + 
                   [f"temperature_{p}hPa" for p in p_levels] + 
                   [f"dewpoint_{p}hPa" for p in p_levels],
        "wind_speed_unit": "kn", "forecast_days": 2, "timezone": "UTC"
    }
    if "HRRR" in model_name:
        params["models"] = "hrrr"
    try:
        res = requests.get(model_url, params=params, timeout=15)
        res.raise_for_status()
        return res.json()
    except Exception as e:
        st.error(f"Data Link Failure: {e}")
        return None

@st.cache_data(ttl=300)
def get_aviation_weather(station):
    metar_url = f"https://aviationweather.gov/api/data/metar?ids={station}"
    taf_url = f"https://aviationweather.gov/api/data/taf?ids={station}"
    try:
        m_res = requests.get(metar_url, timeout=10).text.strip()
        t_res = requests.get(taf_url, timeout=10).text.strip()
        return m_res if m_res else "No METAR available.", t_res if t_res else "No TAF available."
    except:
        return "Sync Error", "Sync Error"

def calc_dewpoint(T, RH):
    # Magnus-Tetens Precision Calculation
    a, b = 17.27, 237.7
    alpha = ((a * T) / (b + T)) + math.log(RH/100.0)
    return (b * alpha) / (a - alpha)

def calc_density_alt(temp, press_mb, elev_ft=0):
    # Standard ISA logic for density altitude
    isa_temp = 15 - (elev_ft / 1000 * 1.98)
    press_alt = elev_ft + (1013.25 - press_mb) * 30
    return press_alt + (118.8 * (temp - isa_temp))

def get_log_wind(z, z1, v1, z2, v2):
    # Logarithmic wind profile for RPA operations
    return v1 + (v2 - v1) * (math.log(z / z1) / math.log(z2 / z1))

# 4. EXECUTION
data = fetch_mission_data(lat, lon, model_api_map[model_choice], model_choice)
metar_raw, taf_raw = get_aviation_weather(icao)

# UI: Text Weather
st.subheader(f"📡 {model_choice} + Live Aviation Text: {icao}")
c1, c2 = st.columns(2)
with c1: st.code(metar_raw, language="text")
with c2: st.code(taf_raw, language="text")

if data and "hourly" in data:
    h = data["hourly"]
    time_list = h["time"]
    formatted_times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in time_list]
    
    st.sidebar.subheader("Mission Timeline")
    selected_time_str = st.sidebar.select_slider("Select Forecast Hour:", options=formatted_times)
    idx = formatted_times.index(selected_time_str)
    
    def safe_get(key): return h.get(key)[idx]

    # Metrics Row
    t_s = safe_get('temperature_2m')
    rh_s = safe_get('relative_humidity_2m')
    p_msl = safe_get('pressure_msl')
    td_s = calc_dewpoint(t_s, rh_s)
    da = calc_density_alt(t_s, p_msl)
    cloud_base_ft = int((t_s - td_s) * 400)

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("TEMP / DP", f"{int(t_s)}° / {int(td_s)}°C")
    m2.metric("DENSITY ALT", f"{int(da):,} ft")
    m3.metric("WIND (10m)", f"{int(safe_get('wind_direction_10m'))}° @ {int(safe_get('wind_speed_10m'))}kt")
    m4.metric("GUSTS", f"{int(safe_get('wind_gusts_10m'))} kt")
    m5.metric("FREEZING LVL", f"{int(safe_get('freezing_level_height') * 3.28084):,}ft")
    m6.metric("CLOUD BASE", f"{max(cloud_base_ft, 0) if safe_get('cloud_cover') > 30 else 'SKC'}ft")

    # --- HAZARD STACK ---
    st.subheader(f"📊 Tactical Hazard Stack (Valid: {selected_time_str})")
    w10, w80, w120 = safe_get("wind_speed_10m"), safe_get("wind_speed_80m"), safe_get("wind_speed_120m")
    gust_ratio = safe_get("wind_gusts_10m") / max(w10, 1)
    
    z_ft = [50, 150, 250, 400]
    stack_data = []
    for alt in z_ft:
        # Interpolate wind at AGL height
        spd = get_log_wind(alt * 0.3048, 10, w10, 80, w80)
        est_gust = spd * gust_ratio
        
        # SPECIAL OPS TURBULENCE LOGIC
        if est_gust > 30 or (est_gust - spd) > 15: turb = "⚠️ SEVERE MECH"
        elif est_gust > 22 or (est_gust - spd) > 10: turb = "MOD MECH"
        elif est_gust > 12: turb = "LGT MECH"
        else: turb = "NIL"
        
        # ICING LOGIC
        ice = "NIL"
        if t_s <= 3 and rh_s > 80:
            ice = "❄️ MOD RIME" if t_s < -8 else "💧 MOD CLEAR"

        stack_data.append({
            "Alt (AGL)": f"{alt} ft", 
            "Wind (kt)": int(round(spd)), 
            "Est Gust (kt)": int(round(est_gust)),
            "Turbulence": turb, 
            "Icing": ice
        })
    
    st.table(pd.DataFrame(stack_data).iloc[::-1])

    # --- SKEW-T SOUNDING ---
    st.divider()
    st.subheader("🌡️ Thermodynamic Profile (Skew-T Sounding)")
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    t_vals = np.array([safe_get(f'temperature_{p}hPa') for p in p_levels])
    td_vals = np.array([safe_get(f'dewpoint_{p}hPa') for p in p_levels])
    
    fig = plt.figure(figsize=(8, 10)) 
    fig.patch.set_facecolor('#0E1117') 
    skew = SkewT(fig, rotation=45)
    skew.ax.set_facecolor('#1B1E23')
    
    # Plot Temp/Dewpoint
    skew.plot(p_levels, t_vals * units.degC, '#EB2F06', linewidth=3, label='Temp')
    skew.plot(p_levels, td_vals * units.degC, '#78E08F', linewidth=3, label='Dewpt')
    
    # Adiabats
    skew.plot_dry_adiabats(color='#E58E26', alpha=0.15)
    skew.plot_moist_adiabats(color='#4A69BD', alpha=0.15)

    plt.title(f"Vector Check: Upper Air Analysis ({model_choice})", color='white', loc='left')
    plt.ylim(1050, 400)
    plt.xlim(-30, 30)
    
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches='tight', facecolor=fig.get_facecolor())
    st.image(buf)
