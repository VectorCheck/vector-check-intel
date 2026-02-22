import streamlit as st
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import units
import io
from datetime import datetime

# 1. PAGE CONFIG
st.set_page_config(page_title="Vector Check: Mission Intel", layout="wide")

# GLOBAL SLEEK DARK THEME
st.markdown("""
    <style>
    /* Force background to stay tactical dark */
    .stApp { background-color: #0E1117; }
    
    /* Make metrics pop */
    [data-testid="stMetricValue"] { font-size: 1.6rem !important; color: #FFFFFF !important; }
    [data-testid="stMetricLabel"] { font-size: 0.9rem !important; color: #8E949E !important; }
    
    /* Sleek divider */
    hr { border-top: 1px solid #3E444E !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Vector Check: High-Res Airspace Intelligence")

# 2. DATA FETCHING (Unchanged logic, optimized for reliability)
st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper()

@st.cache_data(ttl=600)
def fetch_mission_data(latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    params = {
        "latitude": latitude, "longitude": longitude,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", 
                   "wind_direction_10m", "visibility", "weather_code", "wind_speed_80m", 
                   "wind_speed_120m", "freezing_level_height", "cloud_cover", "is_day"] + 
                   [f"temperature_{p}hPa" for p in p_levels] + 
                   [f"dewpoint_{p}hPa" for p in p_levels],
        "forecast_days": 2, "timezone": "UTC"
    }
    try:
        res = requests.get(url, params=params, timeout=15)
        res.raise_for_status()
        return res.json()
    except: return None

data = fetch_mission_data(lat, lon)

if data and "hourly" in data:
    time_list = data["hourly"]["time"]
    formatted_times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in time_list]
    st.sidebar.subheader("Timeline (UTC)")
    selected_time_str = st.sidebar.select_slider("Select Forecast Hour:", options=formatted_times, value=formatted_times[0])
    idx = formatted_times.index(selected_time_str)
else:
    idx = 0

def h_to_p(h_ft): return 1013.25 * (1 - (h_ft / 145366.45))**(1 / 0.190284)

# 3. METAR/TAF DISPLAY
metar_url = f"https://aviationweather.gov/api/data/metar?ids={icao}"
taf_url = f"https://aviationweather.gov/api/data/taf?ids={icao}"
try:
    metar_raw = requests.get(metar_url, timeout=5).text.strip()
    taf_raw = requests.get(taf_url, timeout=5).text.strip()
except:
    metar_raw, taf_raw = "Sync Error", "Sync Error"

st.subheader(f"📡 Official Aviation Text: {icao}")
st.code(metar_raw if metar_raw else "No METAR found.")
st.code(taf_raw if taf_raw else "No TAF found.")

# 4. DASHBOARD LOGIC
if data and "hourly" in data:
    h = data["hourly"]
    def safe_get(key): return h.get(key)[idx]

    # Metrics Row
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    t_s = safe_get('temperature_2m'); rh_s = safe_get('relative_humidity_2m')
    dewpoint_s = t_s - ((100 - rh_s) / 5)
    cloud_base_ft = int((t_s - dewpoint_s) * 400)
    
    m1.metric("Temp", f"{int(t_s)}°C")
    m2.metric("Humidity", f"{int(rh_s)}%")
    m3.metric("Sfc Wind", f"{int(safe_get('wind_direction_10m'))}°@{int(safe_get('wind_speed_10m'))}k/h")
    m4.metric("Visibility", f"{int(safe_get('visibility')/1000)}km")
    m5.metric("Freezing", f"{int(safe_get('freezing_level_height') * 3.28084):,}ft")
    m6.metric("Cld Base", f"{cloud_base_ft if cloud_base_ft > 500 else 'SFC'}ft")

    # HAZARD STACK (Using Native Dataframe for Reliability)
    st.subheader(f"📊 Low-Level Hazard Stack ({selected_time_str})")
    w10, w80, w120 = safe_get("wind_speed_10m"), safe_get("wind_speed_80m"), safe_get("wind_speed_120m")
    z_ft = [400, 300, 200, 100, 50]
    w_interp = np.interp([z * 0.3048 for z in z_ft], [10, 80, 120], [w10, w80, w120])
    
    stack_list = []
    for i, alt in enumerate(z_ft):
        stack_list.append({
            "Altitude": f"{alt} ft AGL",
            "Speed (k/h)": int(round(w_interp[i])),
            "Turbulence": "Light" if w_interp[i] < 15 else "Moderate" if w_interp[i] < 25 else "Severe",
            "Icing Risk": "Nil" if t_s > 2 else "Light" if t_s > -2 else "Moderate"
        })
    
    # Render table with column_config to ensure it stays dark/sleek
    st.dataframe(pd.DataFrame(stack_list), hide_index=True, use_container_width=True)

    # --- THE SOUNDING: VISIBILITY & BACKGROUND FIX ---
    st.divider()
    st.subheader(f"🌡️ Deep Synoptic Profile")
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    t_vals = np.array([safe_get(f'temperature_{p}hPa') for p in p_levels])
    td_vals = np.array([safe_get(f'dewpoint_{p}hPa') for p in p_levels])
    
    fig = plt.figure(figsize=(10, 35)) 
    fig.patch.set_facecolor('#0E1117') # Match app background
    skew = SkewT(fig, rotation=45)
    
    # BRIGHTER INTERNAL PLOT AREA (Force Background)
    skew.ax.set_facecolor('#1C2026') 

    # Force tick labels to light grey
    skew.ax.tick_params(colors='#D1D5DB', labelsize=12)
    skew.ax.xaxis.label.set_color('#D1D5DB')
    skew.ax.yaxis.label.set_color('#D1D5DB')

    # Adiabats (Dashed for sleek look, higher alpha for visibility)
    skew.plot_dry_adiabats(color='#E58E26', alpha=0.3, linewidth=1.1, linestyle='--')
    skew.plot_moist_adiabats(color='#4A69BD', alpha=0.3, linewidth=1.1, linestyle='--')
    
    # Primary Profile (Neon High Contrast)
    skew.plot(p_levels, t_vals * units.degC, '#FF4B4B', linewidth=8, label='Temperature')
    skew.plot(p_levels, td_vals * units.degC, '#00FF41', linewidth=8, label='Dewpoint')
    
    # Fixed Height Labels
    for alt_label in [1000, 3000, 5000, 10000, 15000, 20000]:
        p_val = h_to_p(alt_label)
        skew.ax.text(-39, p_val, f"{alt_label:,} ft", color='#9CA3AF', fontsize=14, ha='right', weight='bold')
        skew.ax.axhline(p_val, color='white', alpha=0.1)
            
    # Freezing Line
    skew.ax.axvline(0, color='#00FFFF', linestyle=':', alpha=0.6, linewidth=2)
    
    plt.ylim(1050, 400); plt.xlim(-40, 40)
    
    # Legend with explicit dark styling
    leg = plt.legend(loc='upper right', prop={'size': 14}, frameon=True)
    leg.get_frame().set_facecolor('#0E1117')
    leg.get_frame().set_edgecolor('#3E444E')
    for text in leg.get_texts():
        text.set_color('#FFFFFF')
    
    # Save with high DPI to prevent "blur" on mobile
    buf = io.BytesIO(); fig.savefig(buf, format="png", bbox_inches='tight', dpi=140, facecolor=fig.get_facecolor())
    st.image(buf, use_container_width=True)
