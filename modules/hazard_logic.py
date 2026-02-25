import re

def get_weather_element(wx_code, wind_spd):
    """Translates WMO weather codes into human-readable text."""
    wx_map = {
        0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Fog", 48: "Freezing Fog",
        51: "Light Drizzle", 53: "Drizzle", 55: "Heavy Drizzle", 
        56: "Freezing Drizzle", 57: "Heavy Freezing Drizzle",
        61: "Light Rain", 63: "Rain", 65: "Heavy Rain", 
        66: "Light Freezing Rain", 67: "Heavy Freezing Rain",
        71: "Light Snow", 73: "Snow", 75: "Heavy Snow", 77: "Snow Grains",
        80: "Light Rain Showers", 81: "Rain Showers", 82: "Heavy Rain Showers",
        85: "Light Snow Showers", 86: "Heavy Snow Showers",
        95: "Thunderstorms", 96: "Thunderstorms with Hail", 99: "Severe Thunderstorms"
    }
    return wx_map.get(wx_code, "NIL")

def calculate_icing_profile(h, idx, wx_code):
    """Evaluates base icing condition from surface parameters with safe NoneType fallbacks."""
    t_raw = h.get('temperature_2m', [0])[idx]
    rh_raw = h.get('relative_humidity_2m', [0])[idx]
    
    # Defensive typing
    t = float(t_raw) if t_raw is not None else 0.0
    rh = int(rh_raw) if rh_raw is not None else 0
    wx = int(wx_code) if wx_code is not None else 0
    
    if wx in [48, 56, 57, 66, 67]:
        return "SEVERE"
    
    if t <= 0:
        if rh >= 90 or wx >= 50:
            return "MODERATE"
        elif rh >= 80:
            return "LIGHT"
            
    return "NIL"

def get_turb_ice(alt, wind_spd, sfc_spd, gust, wx, is_stable, icing_cond, t_temp):
    """
    Evaluates turbulence and icing risk based strictly on baseline WMO criteria,
    utilizing strict internal typing to prevent TypeError crashes from missing API data.
    """
    turb = "NIL"
    
    # Strict typing intercepts 'None' values fed by the API
    w_spd = float(wind_spd) if wind_spd is not None else 0.0
    s_spd = float(sfc_spd) if sfc_spd is not None else 0.0
    g_spd = float(gust) if gust is not None else 0.0
    wx_val = int(wx) if wx is not None else 0
    t_val = float(t_temp) if t_temp is not None else 0.0
    
    gust_delta = max(0, g_spd - s_spd) 
    
    # --- TURBULENCE LOGIC ---
    if w_spd >= 30 or gust_delta >= 15 or wx_val in [95, 96, 99]:
        turb = "SEVERE"
    elif w_spd >= 20 or gust_delta >= 10 or not is_stable:
        turb = "MODERATE"
    elif w_spd >= 15 or gust_delta >= 5:
        turb = "LIGHT"
        
    # --- ICING LOGIC ---
    ice = icing_cond
    
    if alt > 0:
        alt_temp = t_val - ((alt / 1000.0) * 1.98)
        if ice == "NIL" and alt_temp <= 0:
            if wx_val >= 50 or wx_val in [45, 48]:
                ice = "MODERATE"
            
    return turb, ice

def apply_tactical_highlights(text):
    """Applies HTML highlighting to critical METAR/TAF elements."""
    if not text or text == "NIL" or text == "UNAVAILABLE":
        return text
        
    text = re.sub(r'\b(FZ[A-Z]+)\b', r'<span class="fz-warn">\1</span>', text)
    text = re.sub(r'\b(BKN|OVC)(0[0-0][0-9])\b', r'<span class="ifr-text">\1\2</span>', text)
    text = re.sub(r'\b(BKN|OVC)(0[1-2][0-9]|030)\b', r'<span class="mvfr-text">\1\2</span>', text)
    text = re.sub(r'\b([M]?[0-2](?:\s?[1-3]/[2-4])?SM)\b', r'<span class="ifr-text">\1</span>', text)
    
    return text
