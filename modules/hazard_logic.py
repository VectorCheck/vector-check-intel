import re

# --- VECTOR CHECK AERIAL GROUP INC. : OPERATIONAL CONSTANTS ---
URBAN_VENTURI_MULTIPLIER = 1.25
MECH_TURB_ALTITUDE_CAP_FT = 3000
LAPSE_RATE_STANDARD_C_PER_1000FT = 1.98

def get_weather_element(wx_code, wind_spd):
    """Translates WMO weather codes into human-readable text."""
    wx_map = {
        0: "NIL", 1: "NIL", 2: "NIL", 3: "NIL",
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
    """Evaluates base surface icing condition utilizing official Vector Check matrices."""
    t_raw = h.get('temperature_2m', [0])[idx]
    rh_raw = h.get('relative_humidity_2m', [0])[idx]
    
    t = float(t_raw) if t_raw is not None else 0.0
    rh = int(rh_raw) if rh_raw is not None else 0
    wx = int(wx_code) if wx_code is not None else 0
    
    liquid_wx_codes = [45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 77, 80, 81, 82]
    snow_wx_codes = [71, 73, 75, 85, 86]
    
    if wx in [66, 67, 95, 96, 99]: 
        return "SEV CLR"
    elif wx in [56, 57, 77]: 
        return "MOD MX"
    elif wx == 48: 
        return "MOD RIME"
    
    if t <= 0:
        is_wet_snow = (wx in snow_wx_codes) and (0 >= t >= -3.0)
        
        if (wx in liquid_wx_codes) or is_wet_snow:
            return "MOD MX"
        elif rh >= 90 and (wx not in snow_wx_codes):
            return "MOD RIME"
        elif rh >= 80 and (wx not in snow_wx_codes):
            return "LGT RIME"
            
    return "NIL"

def get_turb_ice(alt, wind_spd, sfc_spd, gust, wx, is_convective, icing_cond, alt_temp, alt_rh, terrain_type="Land", cloud_base_agl=10000):
    """
    Evaluates turbulence and icing risk. 
    Strict Visible Moisture Gate enforced: Separates Dry Snow from Wet Snow/Liquid Precip 
    to prevent false-positive structural icing below cloud decks.
    """
    w_spd = float(wind_spd) if wind_spd is not None else 0.0
    s_spd = float(sfc_spd) if sfc_spd is not None else 0.0
    g_spd = float(gust) if gust is not None else 0.0
    wx_val = int(wx) if wx is not None else 0
    t_val = float(alt_temp) if alt_temp is not None else 0.0
    rh_val = int(alt_rh) if alt_rh is not None else 0
    
    max_wind = max(w_spd, g_spd)
    gust_delta = max(0, g_spd - s_spd) 
    
    turb_type = "MECH" 
    turb_sev = "NIL"
    
    # --- TURBULENCE LOGIC ---
    if alt <= MECH_TURB_ALTITUDE_CAP_FT:
        if terrain_type == "Water":
            if max_wind >= 40: turb_sev = "MOD-SEV"
            elif max_wind >= 35: turb_sev = "MOD"
            elif max_wind >= 15: turb_sev = "LGT"
            
        elif terrain_type == "Mountains":
            if max_wind >= 35: turb_sev = "SEV"
            elif max_wind >= 20: turb_sev = "MOD"
            elif max_wind >= 15: turb_sev = "LGT"

        elif terrain_type == "Urban":
            if max_wind >= 32: turb_sev = "SEV"        
            elif max_wind >= 28: turb_sev = "MOD-SEV"  
            elif max_wind >= 20: turb_sev = "MOD"      
            elif max_wind >= 12: turb_sev = "LGT"      
            
        else: # Land
            if max_wind >= 40: turb_sev = "SEV"
            elif max_wind >= 35: turb_sev = "MOD-SEV"
            elif max_wind >= 25: turb_sev = "MOD"
            elif max_wind >= 15: turb_sev = "LGT"
    else:
        turb_type = "SHEAR"
        if gust_delta >= 15: turb_sev = "SEV"
        elif gust_delta >= 10: turb_sev = "MOD"

    if wx_val in [95, 96, 99]:
        turb_type = "CONV"
        turb_sev = "SEV"

    turb = f"{turb_sev} {turb_type}" if turb_sev != "NIL" else "NIL"
        
    # --- ICING ALOFT LOGIC (VISIBLE MOISTURE GATE) ---
    ice = "NIL"
    
    if alt > 0:
        if t_val > 0 or t_val < -40:
            ice = "NIL"
        else:
            in_cloud = alt >= cloud_base_agl
            
            liquid_wx_codes = [45, 48, 51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 77, 80, 81, 82]
            snow_wx_codes = [71, 73, 75, 85, 86]
            
            has_liquid_precip = wx_val in liquid_wx_codes
            # Wet Snow physically sticks to airframes (Temp between 0 and -3C)
            is_wet_snow = (wx_val in snow_wx_codes) and (0 >= t_val >= -3.0)
            
            # If we are below the cloud deck and encountering dry snow (or nothing), icing is physically impossible.
            if not (in_cloud or has_liquid_precip or is_wet_snow):
                ice = "NIL"
            else:
                if wx_val in [66, 67, 95, 96, 99]: 
                    ice = "SEV CLR"
                elif wx_val in [56, 57, 77]: 
                    ice = "MOD MX"
                elif has_liquid_precip or is_wet_snow:
                    ice = "MOD MX"
                elif wx_val == 48 and alt <= 1000:
                    ice = "MOD RIME" # Freezing fog impact layer
                elif rh_val >= 80: 
                    if 0 >= t_val >= -15:
                        ice = "MOD RIME" if rh_val >= 90 else "LGT RIME"
                    elif -15 > t_val >= -20:
                        ice = "LGT RIME"
                    elif t_val < -20:
                        ice = "LGT RIME" if rh_val >= 95 else "NIL"
            
    return turb, ice

def apply_tactical_highlights(text):
    """
    Applies HTML highlighting to METAR/TAF.
    Strict Audit Rule: Evaluates each line and applies a single color.
    """
    if not text or text == "NIL" or text == "UNAVAILABLE":
        return text
        
    lines = text.split('\n')
    formatted_lines = []
    
    for line in lines:
        if re.search(r'\b(FZ[A-Z]*)\b', line):
            formatted_lines.append(f'<span class="fz-warn">{line}</span>')
            continue
            
        is_ifr = False
        if re.search(r'\b(BKN|OVC|VV)(00[0-9])\b', line): 
            is_ifr = True
        elif re.search(r'\b([M]?[0-2](?:\s?[1-3]/[2-4])?SM)\b', line): 
            is_ifr = True
            
        if is_ifr:
            formatted_lines.append(f'<span class="ifr-text">{line}</span>')
            continue
            
        is_mvfr = False
        if re.search(r'\b(BKN|OVC)(0[1-2][0-9]|030)\b', line): 
            is_mvfr = True
        elif re.search(r'\b([3-5]SM)\b', line): 
            is_mvfr = True
            
        if is_mvfr:
            formatted_lines.append(f'<span class="mvfr-text">{line}</span>')
            continue
            
        formatted_lines.append(line)
        
    return '\n'.join(formatted_lines)
