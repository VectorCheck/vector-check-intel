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
    """Evaluates base icing condition from surface parameters."""
    t = h.get('temperature_2m', [0])[idx]
    rh = h.get('relative_humidity_2m', [0])[idx]
    
    # Freezing precipitation is immediate severe icing
    if wx_code in [48, 56, 57, 66, 67]:
        return "SEVERE"
    
    # Freezing temps with high moisture
    if t is not None and t <= 0:
        if rh >= 90 or wx_code >= 50:
            return "MODERATE"
        elif rh >= 80:
            return "LIGHT"
            
    return "NIL"

def get_turb_ice(alt, wind_spd, sfc_spd, gust, wx, is_stable, icing_cond, t_temp):
    """
    Evaluates turbulence and icing risk based strictly on meteorological criteria,
    without scaling for specific airframe weights.
    """
    # --- TURBULENCE LOGIC ---
    turb = "NIL"
    gust_delta = max(0, gust - sfc_spd) 
    
    # WMO/Aviation Baseline Constraints
    if wind_spd >= 30 or gust_delta >= 15 or wx in [95, 96, 99]:
        turb = "SEVERE"
    elif wind_spd >= 20 or gust_delta >= 10 or not is_stable:
        turb = "MODERATE"
    elif wind_spd >= 15 or gust_delta >= 5:
        turb = "LIGHT"
        
    # --- ICING LOGIC ---
    ice = icing_cond
    
    # Adjust for altitude lapse rate (~1.98C per 1000 ft)
    if alt > 0 and t_temp is not None:
        alt_temp = t_temp - ((alt / 1000.0) * 1.98)
        if ice == "NIL" and alt_temp <= 0:
            # If we hit freezing aloft and there is moisture/precip in the column
            if wx >= 50 or wx in [45, 48]:
                ice = "MODERATE"
            
    return turb, ice

def apply_tactical_highlights(text):
    """Applies HTML highlighting to critical METAR/TAF elements."""
    if not text or text == "NIL" or text == "UNAVAILABLE":
        return text
        
    # Highlight Freezing conditions (FZRA, FZFG, etc.)
    text = re.sub(r'\b(FZ[A-Z]+)\b', r'<span class="fz-warn">\1</span>', text)
    
    # Highlight IFR Ceilings (OVC/BKN below 1000ft)
    text = re.sub(r'\b(BKN|OVC)(0[0-0][0-9])\b', r'<span class="ifr-text">\1\2</span>', text)
    
    # Highlight MVFR Ceilings (OVC/BKN 1000-3000ft)
    text = re.sub(r'\b(BKN|OVC)(0[1-2][0-9]|030)\b', r'<span class="mvfr-text">\1\2</span>', text)
    
    # Highlight low visibility (< 3SM)
    text = re.sub(r'\b([M]?[0-2](?:\s?[1-3]/[2-4])?SM)\b', r'<span class="ifr-text">\1</span>', text)
    
    return text
