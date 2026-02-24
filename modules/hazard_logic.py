# modules/hazard_logic.py

def get_precip_type(wx_code):
    """
    Translates WMO weather codes into standard aviation precip types.
    """
    wx_mapping = {
        0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Fog", 48: "Freezing Fog",
        51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
        56: "Light FZ Drizzle", 57: "Dense FZ Drizzle",
        61: "Light Rain", 63: "Moderate Rain", 65: "Heavy Rain",
        66: "Light FZ Rain", 67: "Heavy FZ Rain",
        71: "Light Snow", 73: "Moderate Snow", 75: "Heavy Snow",
        77: "Snow Grains",
        80: "Light Rain Showers", 81: "Moderate Rain Showers", 82: "Violent Rain Showers",
        85: "Light Snow Showers", 86: "Heavy Snow Showers",
        95: "Thunderstorms", 96: "TSRA w/ Hail", 99: "Heavy TSRA w/ Hail"
    }
    return wx_mapping.get(wx_code, f"Code {wx_code}")

def calculate_icing_profile(h, idx, wx_code):
    """
    Evaluates atmospheric conditions for icing potential based on Temp, RH, and Precip.
    Requires visible moisture (RH > 85% or active precip) and sub-freezing temps.
    """
    temp = h['temperature_2m'][idx]
    rh = h['relative_humidity_2m'][idx]
    
    # Identify visible moisture (High RH or active precip codes)
    visible_moisture = rh >= 85 or (wx_code >= 50 and wx_code <= 99)
    
    # Identify freezing precip codes specifically
    freezing_precip = wx_code in [48, 56, 57, 66, 67]
    
    if freezing_precip:
        return "SEVERE (FZRA/FZDZ)"
    elif visible_moisture and -20 <= temp <= 0:
        if -10 <= temp <= 0:
            return "MODERATE (Clear/Mixed)"
        else:
            return "LIGHT (Rime)"
    elif temp < -20:
        return "TRACE (Ice Crystals)"
    else:
        return "NIL"

def get_turb_ice(alt, s_c, w_spd, g_c, wx, is_stable, icing_cond):
    """
    Efficacy-Audited Turbulence & Icing Engine.
    Calculates mechanical turbulence based on the linear gust spread and base wind speed,
    weighted heavily for low-altitude (0-400ft) uncrewed flight dynamics.
    """
    # 1. CALCULATE GUST SPREAD (The mechanical bump)
    gust_spread = max(0, g_c - s_c)
    
    # 2. EVALUATE TURBULENCE RISK
    turb_risk = "NIL"
    
    # Severe criteria: Massive gust spread or extremely high sustained winds
    if gust_spread >= 15 or s_c >= 30 or g_c >= 35 or wx in [95, 96, 99]:
        turb_risk = "SEVERE"
    # Moderate criteria: Noticeable bumpiness, standard boundary layer mixing
    elif gust_spread >= 10 or s_c >= 20 or (not is_stable and alt <= 400 and s_c >= 15):
        turb_risk = "MODERATE"
    # Light criteria: Mild mechanical friction
    elif gust_spread >= 5 or s_c >= 10 or not is_stable:
        turb_risk = "LIGHT"

    # 3. EVALUATE ICING RISK ALOFT
    # If surface icing is predicted, it generally propagates up through the boundary layer 
    # unless a thermal inversion is actively tracked (which requires higher-level sounding math).
    ice_risk = icing_cond if icing_cond != "NIL" else "NIL"

    return turb_risk, ice_risk
