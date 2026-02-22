import streamlit as st
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from metpy.plots import SkewT
from metpy.units import units
import io
import math
import re
from datetime import datetime

# 1. PAGE CONFIG
st.set_page_config(page_title="Vector Check: Atmospheric Risk Management", layout="wide")

# CUSTOM CSS: STEALTH THEME
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; color: #E58E26 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #8E949E !important; }
    table { margin-left: auto; margin-right: auto; text-align: center !important; width: 90%; border-collapse: collapse; background-color: #1B1E23; }
    th { text-align: center !important; color: #8E949E !important; font-weight: bold !important; padding: 10px !important; border-bottom: 2px solid #3E444E !important; text-transform: uppercase; }
    td { text-align: center !important; padding: 8px !important; color: #D1D5DB !important; border-bottom: 1px solid #2D3139 !important; }
    </style>
    """, unsafe_allow_html=True)

st.title("Atmospheric Risk Management")
st.caption("Vector Check Aerial Group Inc. | Specialized Drone Operations & Weather Consulting")

# 2. SIDEBAR
st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper()

model_choice = st.sidebar.selectbox("Select Forecast Model:", 
    options=["HRDPS (Canada 2.5km)", "ECMWF (Global 9km)"])

model_api_map = {
    "HRDPS (Canada 2.5km)": "https://api.open-meteo.com/v1/gem",
    "ECMWF (Global 9km)": "https://api.open-meteo.com/v1/ecmwf"
}

# 3. ROBUST HELPERS
def safe_val(val, multiplier=1, default="N/A", precision=0):
    if val is None: return default
    res = val * multiplier
    return f"{res:,.{precision}f}" if precision > 0 else f"{int(round(res)):,}"

def get_best_upper_wind(h_data, idx):
    for key, height in [('wind_speed_120m', 120), ('wind_speed_100m', 100), ('wind_speed_80m', 80)]:
        val_list = h_data.get(key)
        if val_list and val_list[idx] is not None:
            return val_list[idx], height
    return None, None

# 4. DATA FETCHING
@st.cache_data(ttl=600)
def fetch_mission_data(latitude, longitude, model_url):
    hourly_params = [
        "temperature_2m", "relative_humidity_2m", "wind_speed_10m", "wind_gusts_10m",
        "wind_direction_10m", "visibility", "weather_code", "pressure_msl",
        "wind_speed_80m", "wind_speed_120m", "wind_speed_100m", "freezing_level_height", "cloud_cover"
    ]
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    hourly_params += [f"temperature_{p}hPa" for p in p_levels] + [f"dewpoint_{p}hPa" for p in p_levels]

    params = {
        "latitude": latitude, "longitude": longitude,
        "hourly": hourly_params,
        "wind_speed_unit": "kn", "forecast_days": 2, "timezone": "UTC"
    }
    try:
        res = requests.get(model_url, params=params, timeout=15)
        res.raise_for_status()
        return res.json()
    except: return None

@st.cache_data(ttl=300)
def get_aviation_weather(station):
    headers = {'User-Agent': 'VectorCheck_Risk_Management_v1.0'}
    try:
        m_url = f"https://aviationweather.gov/api/data/metar?ids={station}"
        t_url = f"https://aviationweather.gov/api/data/taf?ids={station}"
        m_res = requests.get(m_url, headers=headers, timeout=10)
        t_res = requests.get(t_url, headers=headers, timeout=10)
        return m_res.text.strip() or "No METAR", t_res.text.strip() or "No TAF"
    except: return "Link Error", "Link Error"

def highlight_aviation_weather(text):
    if "Link Error" in text: return text
    def vis_replacer(match):
        val_str = match.group(1)
        try:
            val = float(eval(val_str)) if '/' in val_str else float(val_str)
            if val < 3: return f'<span style="color: #ff4b4b; font-weight: bold;">{match.group(0)}</span>'
            if val <= 5: return f'<span style="color: #f6ec15; font-weight: bold;">{match.group(0)}</span>'
        except: pass
        return match.group(0)
    def cloud_replacer(match):
        try:
            height = int(match.group(2)) * 100
            if height < 1000: return f'<span style="color: #ff4b4b; font-weight: bold;">{match.group(0)}</span>'
            if height <= 3000: return f'<span style="color: #f6ec15; font-weight: bold;">{match.group(0)}</span>'
        except: pass
        return match.group(0)
    def wx_replacer(match):
        code = match.group(0)
        if any(x in code for x in ['FZ', 'PL', 'IC', '+']):
            return f'<span style="color: #ff4b4b; font-weight: bold;">{code}</span>'
        if any(x in code for x in ['FG', 'BR']):
            return f'<span style="color: #f6ec15; font-weight: bold;">{code}</span>'
        return code

    text = re.sub(r'(\d+/\d+|\d+)SM', vis_replacer, text)
    text = re.sub(r'(BKN|OVC|VV)(\d{3})', cloud_replacer, text)
    text = re.sub(r'\b(?:\+|-|VC)?(?:FZ|PL|IC|FG|BR|RA|SN|DZ|GR|GS|UP)+\b', wx_replacer, text)
    return text

# 5. MAIN RENDER
data = fetch_mission_data(lat, lon, model_api_map[model_choice])
metar_raw, taf_raw = get_aviation_weather(icao)
metar_h = highlight_aviation_weather(metar_raw)
taf_h = highlight_aviation_weather(taf_raw).replace('TAF ', 'TAF<br>').replace('FM', '<br>FM').replace('TEMPO', '<br>TEMPO').replace('PROB', '<br>PROB')

st.markdown(f"""
    <div style="background-color: #1B1E23; padding: 15px; border: 1px solid #2D3139; border-radius: 5px; font-family: sans-serif; color: #D1D5DB; font-size: 0.9rem; line-height: 1.6;">
        <strong style="color: #8E949E; text-transform: uppercase;">METAR</strong><br>
        {metar_h}<br><br>
        <strong style="color: #8E949E; text-transform: uppercase;">TAF</strong><br>
        {taf_h}
    </div>
    """, unsafe_allow_html=True)

st.divider()

if data and "hourly" in data:
    h = data["hourly"]
    times = [datetime.fromisoformat(t).strftime("%d %b %H:%M Z") for t in h["time"]]
    selected_time = st.sidebar.select_slider("Forecast Hour:", options=times)
    idx = times.index(selected_time)
    
    # --- HARDENED METRIC ROW CALCULATIONS ---
    def get_wx_desc(code):
        if code is None: return "N/A"
        codes = {0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast", 45: "Fog", 48: "Dep Fog", 51: "Drizzle", 56: "FZ Drizzle", 61: "Rain", 66: "FZ Rain", 71: "Snow", 77: "Snow Grains", 80: "Showers", 85: "Snow Showers", 95: "TS"}
        return codes.get(code, "Clear")

    def estimate_cloud_base(t, rh):
        if t is None or rh is None: return "N/A"
        if rh < 55: return "Clear"
        base_ft = (t - (t - ((100 - rh)/5))) * 122 * 3.28084
        return f"{int(round(base_ft, -2))} ft"

    temp = h['temperature_2m'][idx]
    hum  = h['relative_humidity_2m'][idx]
    w_dir = h['wind_direction_10m'][idx]
    w_spd = h['wind_speed_10m'][idx]
    wx_code = h['weather_code'][idx]
    
    # Safety checks for visibility and freezing level
    vis_raw = h.get('visibility', [None]*len(h['time']))[idx]
    vis = (vis_raw * 0.001) if vis_raw is not None else None
    
    frz_raw = h.get('freezing_level_height', [None]*len(h['time']))[idx]
    frz = (frz_raw * 3.28084) if frz_raw is not None else None
    
    c_base = estimate_cloud_base(temp, hum)

    # Render 8-Column Metric Row
    cols = st.columns(8)
    cols[0].metric("TEMP", f"{safe_val(temp, precision=1)}°C")
    cols[1].metric("RH", f"{safe_val(hum)}%")
    cols[2].metric("WIND DIR", f"{safe_val(w_dir)}°")
    cols[3].metric("WIND SPD", f"{safe_val(w_spd)} kt")
    cols[4].metric("PRECIP", get_wx_desc(wx_code))
    cols[5].metric("VIS", f"{safe_val(vis, precision=1)} km")
    cols[6].metric("FRZ LVL", f"{safe_val(frz)} ft")
    cols[7].metric("CLOUD", c_base)

    # --- HAZARD STACK ---
    st.subheader("Tactical Hazard Stack (Estimated AGL Winds)")
    gst = h['wind_gusts_10m'][idx]
    upper_v, upper_h = get_best_upper_wind(h, idx)
    
    if w_spd is not None and upper_v is not None and gst is not None:
        stack = []
        gst_factor = gst / max(w_spd, 1)
        for alt in [400, 300, 200, 100]:
            alt_m = alt * 0.3048
            spd = w_spd + (upper_v - w_spd) * (math.log(alt_m/10) / math.log(upper_h/10))
            cur_gst = spd * gst_factor
            status = "NOMINAL"
            if cur_gst > 25: status = "NO-GO (GUST)"
            elif spd > 20: status = "CAUTION (WIND)"
            stack.append({"Alt (AGL)": f"{alt}ft", "Wind (kt)": int(spd), "Gust (kt)": int(cur_gst), "Status": status})
        st.table(pd.DataFrame(stack))
    else:
        st.warning("Upper-air data or surface wind data unavailable for this hour.")

    # --- SKEW-T ---
    st.divider()
    p_levs = [1000, 950, 925, 900, 850, 800, 700, 600, 500, 400]
    t_plot = [h.get(f'temperature_{p}hPa')[idx] for p in p_levs]
    td_plot = [h.get(f'dewpoint_{p}hPa')[idx] for p in p_levs]

    if None not in t_plot:
        fig = plt.figure(figsize=(6, 8))
        fig.patch.set_facecolor('#0E1117')
        skew = SkewT(fig, rotation=45)
        skew.ax.set_facecolor('#1B1E23')
        skew.plot(p_levs, np.array(t_plot) * units.degC, 'r', linewidth=2, label="Temp")
        skew.plot(p_levs, np.array(td_plot) * units.degC, 'g', linewidth=2, label="Dewpoint")
        plt.title(f"Vertical Profile (UTC: {selected_time})", color='white')
        st.pyplot(fig)
