import streamlit as st
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import units
import io
from datetime import datetime

# 1. PAGE CONFIG & UI LOCK
st.set_page_config(page_title="Vector Check: Mission Intel", layout="wide")

# CUSTOM CSS: STEALTH THEME
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.4rem !important; color: #E58E26 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #8E949E !important; }
    .centered-table { display: flex; justify-content: center; margin-bottom: 20px; }
    table { margin-left: auto; margin-right: auto; text-align: center !important; width: 90%; border-collapse: collapse; background-color: #1B1E23; }
    th { text-align: center !important; color: #8E949E !important; font-weight: bold !important; padding: 10px !important; border-bottom: 2px solid #3E444E !important; text-transform: uppercase; }
    td { text-align: center !important; padding: 8px !important; color: #D1D5DB !important; border-bottom: 1px solid #2D3139 !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("🛡️ Vector Check: High-Res Airspace Intelligence")

# 2. SIDEBAR
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
    except Exception as e:
        st.error(f"Data Link Failure: {e}")
        return None

data = fetch_mission_data(lat, lon)

if data and "hourly" in data:
    time_list = data["hourly"]["time"]
    formatted_times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in time_list]
    st.sidebar.subheader("Timeline (UTC)")
    selected_time_str = st.sidebar.select_slider("Select Forecast Hour:", options=formatted_times, value=formatted_times[0])
    idx = formatted_times.index(selected_time_str)
else:
    idx = 0

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

def get_precip_type(code):
    mapping = {0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast", 45: "Fog", 51: "Drizzle", 56: "Fz Drizzle", 61: "Lgt Rain", 66: "Fz Rain", 71: "Lgt Snow", 95: "TS"}
    return mapping.get(code, "Unknown")

def h_to_p(h_ft):
    return 1013.25 * (1 - (h_ft / 145366.45))**(1 / 0.190284)

# 4. MAIN CONTENT
metar_raw, taf_raw = get_aviation_weather(icao)
st.subheader(f"📡 Aviation Text: {icao}")
st.code(metar_raw, language="text")
st.code(taf_raw, language="text")
st.divider()

if data and "hourly" in data:
    h = data["hourly"]
    def safe_get(key): return h.get(key)[idx]

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    t_s = safe_get('temperature_2m')
    rh_s = safe_get('relative_humidity_2m')
    # Better Dewpoint Calculation
    dewpoint_s = t_s - ((100 - rh_s) / 5) 
    cloud_base_ft = int((t_s - dewpoint_s) * 400)
    
    m1.metric("TEMP", f"{int(t_s)}°C")
    m2.metric("HUMIDITY", f"{int(rh_s)}%")
    m3.metric("WIND (10m)", f"{int(safe_get('wind_direction_10m'))}° @ {int(round(safe_get('wind_speed_10m')))}k/h")
    m4.metric("VISIBILITY", f"{int(safe_get('visibility')/1000)}km")
    m5.metric("FREEZING LVL", f"{int(safe_get('freezing_level_height') * 3.28084):,}ft")
    m6.metric("CLOUD BASE", f"{max(cloud_base_ft, 0) if safe_get('cloud_cover') > 20 else 'SKC'}ft")

    # --- HAZARD STACK ---
    st.subheader(f"📊 Tactical Hazard Stack (Valid: {selected_time_str})")
    w10, w80, w120 = safe_get("wind_speed_10m"), safe_get("wind_speed_80m"), safe_get("wind_speed_120m")
    z_ft = [50, 100, 200, 300, 400]
    w_interp = np.interp([z * 0.3048 for z in z_ft], [10, 80, 120], [w10, w80, w120])
    
    stack_data = []
    for i, alt in enumerate(z_ft):
        spd = int(round(w_interp[i]))
        # Turbulence Logic
        if spd > 35: turb = "⚠️ SEVERE MECH"
        elif spd > 22: turb = "MOD MECH"
        elif spd > 12: turb = "LGT MECH"
        else: turb = "NIL"
        
        # Icing Logic
        ice = "NIL"
        if t_s <= 2 and rh_s > 80:
            if t_s < -10: ice = "❄️ MOD RIME"
            else: ice = "💧 MOD CLEAR"

        stack_data.append({"Alt (AGL)": f"{alt} ft", "Wind (km/h)": spd, "Turbulence": turb, "Icing": ice})
    
    df_stack = pd.DataFrame(stack_data).iloc[::-1]
    st.markdown('<div class="centered-table">', unsafe_allow_html=True)
    st.table(df_stack)
    st.markdown('</div>', unsafe_allow_html=True)

    # --- SKEW-T SOUNDING ---
    st.divider()
    st.subheader(f"🌡️ Thermodynamic Profile (Skew-T)")
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    t_vals = np.array([safe_get(f'temperature_{p}hPa') for p in p_levels])
    td_vals = np.array([safe_get(f'dewpoint_{p}hPa') for p in p_levels])
    
    fig = plt.figure(figsize=(9, 12)) 
    fig.patch.set_facecolor('#0E1117') 
    skew = SkewT(fig, rotation=45)
    skew.ax.set_facecolor('#1B1E23')

    # Lines and Colors
    skew.plot(p_levels, t_vals * units.degC, '#EB2F06', linewidth=4, label='Temp')
    skew.plot(p_levels, td_vals * units.degC, '#78E08F', linewidth=4, label='Dewpt')
    skew.plot_dry_adiabats(color='#E58E26', alpha=0.15)
    skew.plot_moist_adiabats(color='#4A69BD', alpha=0.15)
    
    # Custom Labels
    for alt_label in [2000, 5000, 10000, 15000]:
        p_val = h_to_p(alt_label)
        skew.ax.text(-35, p_val, f"{alt_label}ft", color='#8E949E', fontsize=10, ha='right')

    plt.ylim(1050, 400)
    plt.xlim(-30, 40)
    plt.title("Vector Check: Upper Air Analysis", color='white')
    
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches='tight', facecolor=fig.get_facecolor())
    st.image(buf)
