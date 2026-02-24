import streamlit as st
import pandas as pd
import math
import re
from datetime import datetime, timezone
from timezonefinder import TimezoneFinder
import pytz

# Import Vector Check Modules
from modules.data_ingest import get_aviation_weather, fetch_mission_data
from modules.hazard_logic import get_precip_type, calculate_icing_profile, get_turb_ice
from modules.visualizations import plot_convective_profile
from modules.telemetry import log_action
from modules.astronomy import get_astronomical_data
from modules.space_weather import get_kp_index

# 1. PAGE CONFIG & CSS
st.set_page_config(page_title="Vector Check: Atmospheric Risk Management", layout="wide")
st.markdown("""
    <style>
    [data-testid="stMetricValue"] { font-size: 1.2rem !important; color: #E58E26 !important; }
    [data-testid="stMetricLabel"] { font-size: 0.8rem !important; color: #A0A4AB !important; text-transform: uppercase; }
    .ifr-text { color: #FF4B4B; font-weight: bold; }
    .mvfr-text { color: #FFFF00; font-weight: bold; }
    table { margin-left: auto; margin-right: auto; text-align: center !important; width: 95%; border-collapse: collapse; background-color: #1B1E23; }
    th { text-align: center !important; color: #8E949E !important; font-weight: bold !important; padding: 10px !important; border-bottom: 2px solid #3E444E !important; }
    td { text-align: center !important; padding: 8px !important; color: #D1D5DB !important; border-bottom: 1px solid #2D3139 !important; }
    .obs-text { font-family: "Source Code Pro", monospace; font-size: 0.95rem; line-height: 1.6; color: #D1D5DB; }
    </style>
    """, unsafe_allow_html=True)

# 2. AUTHENTICATION GATEWAY
def check_password():
    def password_entered():
        if st.session_state["username"] in st.secrets.get("passwords", {}) and \
           st.session_state["password"] == st.secrets["passwords"][st.session_state["username"]]:
            st.session_state["password_correct"] = True
            st.session_state["active_operator"] = st.session_state["username"]
        else:
            st.session_state["password_correct"] = False
    if "password_correct" not in st.session_state:
        st.title("Vector Check Aerial Group Inc.")
        st.text_input("Operator ID", key="username")
        st.text_input("Passcode", type="password", key="password")
        st.button("Authenticate", on_click=password_entered)
        return False
    elif not st.session_state["password_correct"]:
        st.error("⚠️ UNAUTHORIZED")
        return False
    return True

if not check_password():
    st.stop()

# 3. SIDEBAR
LOGO_URL = "https://raw.githubusercontent.com/VectorCheck/vector-check-intel/main/VCAG%20Inc%20-%20Logo%20Final.png"
try:
    st.sidebar.image(LOGO_URL, use_container_width=True)
except:
    st.sidebar.title("Vector Check")

st.sidebar.header("Mission Parameters")
lat = st.sidebar.number_input("Latitude", value=44.1628, format="%.4f")
lon = st.sidebar.number_input("Longitude", value=-77.3832, format="%.4f")
icao = st.sidebar.text_input("Nearest ICAO", value="CYTR").upper().strip()
ac_class = st.sidebar.selectbox("Airframe Class:", options=["Small (250g - 25kg)", "Micro (< 250g)", "Heavy (> 25kg)", "Rotary (Helicopter)"])
model_choice = st.sidebar.selectbox("Forecast Model:", options=["HRDPS (Canada 2.5km)", "ECMWF (Global 9km)"])

if st.sidebar.button("Force Refresh"):
    st.cache_data.clear()

# 4. DATA FETCH
model_map = {"HRDPS (Canada 2.5km)": "https://api.open-meteo.com/v1/gem", "ECMWF (Global 9km)": "https://api.open-meteo.com/v1/ecmwf"}
data = fetch_mission_data(lat, lon, model_map[model_choice])
metar_raw, taf_raw = get_aviation_weather(icao)

st.title("Atmospheric Risk Management")
st.caption(f"VCAG System Active | Op: {st.session_state.get('active_operator', 'UNK')}")
st.divider()

# 5. TIME HANDLING
if data and "hourly" in data:
    h = data["hourly"]
    tf = TimezoneFinder()
    tz_name = tf.timezone_at(lng=lon, lat=lat)
    local_tz = pytz.timezone(tz_name) if tz_name else timezone.utc
    
    times = []
    for t in h["time"]:
        dt_u = datetime.fromisoformat(t).replace(tzinfo=timezone.utc)
        dt_l = dt_u.astimezone(local_tz)
        times.append(f"{dt_u.strftime('%d %b %H:%M')} Z | {dt_l.strftime('%H:%M')}")

    sel_time = st.sidebar.select_slider("Forecast Hour:", options=times)
    idx = times.index(sel_time)

    # Variables
    u_wind = data.get("hourly_units", {}).get("wind_speed_10m", "km/h")
    t, rh, ws, wx = h['temperature_2m'][idx], h['relative_humidity_2m'][idx], h['wind_speed_10m'][idx], h['weather_code'][idx]
    wd, gst_raw = int(h['wind_direction_10m'][idx]), h.get('wind_gusts_10m', [ws])[idx]
    gst = max(gst_raw, ws * 1.25)
    td = t - ((100 - rh) / 5)
    
    # UI: Surface
    st.subheader("Forecasted Surface Data")
    c = st.columns(8)
    c[0].metric("Temp", f"{t}°C"); c[1].metric("RH", f"{rh}%"); c[2].metric("Wind", f"{wd:03d}°"); c[3].metric("Spd", f"{int(ws)} {u_wind}")
    c[4].metric("Precip", get_precip_type(wx)); c[5].metric("Vis", f"{int((100-rh)/5)} sm"); c[6].metric("FRZ", "N/A"); c[7].metric("Cloud", f"{int((t-td)*400)}ft")
    st.divider()

    # UI: Hazard Stack
    st.subheader("Tactical Hazard Stack (0-400ft AGL)")
    g_delta = max(0, gst - ws)
    u_v, u_dir, u_h = (h['wind_speed_120m'][idx], h['wind_direction_120m'][idx], 120) if "gem" in model_map[model_choice] else (h['wind_speed_100m'][idx], h['wind_direction_100m'][idx], 100)
    
    t_rows = []
    for alt in [400, 300, 200, 100]:
        s_c = ws + (u_v - ws) * (math.log(alt*0.3048/10) / math.log(u_h/10))
        turb, ice = get_turb_ice(alt, s_c, ws, s_c + g_delta, wx, True, "NIL", ac_class)
        t_rows.append({"Alt": f"{alt}ft", "Spd": int(s_c), "Gust": int(s_c + g_delta), "Turbulence": turb, "Icing": ice})
    st.table(pd.DataFrame(t_rows).set_index("Alt"))

    # Space/Astro
    dt_obj = datetime.fromisoformat(h["time"][idx]).replace(tzinfo=timezone.utc)
    astro = get_astronomical_data(lat, lon, dt_obj, local_tz, "LMT")
    space = get_kp_index(dt_obj)

    st.subheader("Space & Light Profile")
    col_a, col_b = st.columns(2)
    with col_a:
        st.write(f"**Sun:** {astro['sunrise']} - {astro['sunset']} | **Moon:** {astro['moon_ill']}%")
    with col_b:
        st.write(f"**Kp Index:** {space['kp']} | **GNSS Risk:** {space['risk']}")
    st.divider()

    # METAR/TAF (With proper HTML rendering)
    st.subheader(f"Aviation Briefing ({icao})")
    st.markdown(f"""
    <div style="background-color: #1B1E23; padding: 15px; border-radius: 5px; border: 1px solid #3E444E;">
        <div class="obs-text">
            <strong>METAR</strong><br>{metar_raw}<br><br>
            <strong>TAF</strong><br>{taf_raw}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # Export
    st.download_button("📥 Download CSV", data=pd.DataFrame(t_rows).to_csv().encode('utf-8'), file_name=f"VCAG_{icao}.csv")

st.markdown('<div style="text-align: center; color: #8E949E; font-size: 0.8rem; padding: 20px;">FOR SITUATIONAL AWARENESS ONLY</div>', unsafe_allow_html=True)
