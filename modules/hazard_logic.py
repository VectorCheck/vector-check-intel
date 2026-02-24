import re

def get_precip_type(wx_code):
    """Translates WMO weather codes into standard aviation weather types."""
    wx_map = {
        0: "Clear", 1: "Mainly Clear", 2: "Partly Cloudy", 3: "Overcast",
        45: "Fog", 48: "Freezing Fog",
        51: "Light Drizzle", 53: "Moderate Drizzle", 55: "Dense Drizzle",
        56: "Light Freezing Drizzle", 57: "Dense Freezing Drizzle",
        61: "Light Rain", 63: "Moderate Rain", 65: "Heavy Rain",
        66: "Light Freezing Rain", 67: "Heavy Freezing Rain",
        71: "Light Snow", 73: "Moderate Snow", 75: "Heavy Snow",
        77: "Snow Grains",
        80: "Light Rain Showers", 81: "Moderate Rain Showers", 82: "Violent Rain Showers",
        85: "Light Snow Showers", 86: "Heavy Snow Showers",
        95: "Thunderstorm", 96: "Thunderstorm w/ Hail", 99: "Heavy Thunderstorm w/ Hail"
    }
    return wx_map.get(wx_code, "Unknown")

def calculate_icing_profile(h, idx, wx):
    """Determines general icing conditions based on RH, Temp, and active precipitation."""
    rh = h['relative_humidity_2m'][idx]
    temp = h['temperature_2m'][idx]
    
    # Visible moisture + freezing temps
    if rh >= 85 and -20 <= temp <= 0:
        return True
    # Specific freezing/winter precipitation WMO codes
    if wx in [48, 56, 57, 66, 67, 71, 73, 75, 77, 85, 86]:
        return True
        
    return False

def get_turb_ice(alt, wind_alt, wind_sfc, gust_alt, wx, is_stable, icing_cond, airframe_class, t_temp):
    """
    Calculates altitude-specific turbulence and icing using manned aviation doctrine
    scaled to Transport Canada RPA airframe classes.
    """
    # 1. AIRFRAME SCALING MULTIPLIER
    # Charts are based on manned/heavy assets. We scale the threshold down for lighter drones.
    if "Micro" in airframe_class:
        scale = 0.4
    elif "Small" in airframe_class:
        scale = 0.6
    else: 
        scale = 1.0 # Heavy / Rotary uses exact chart values

    # 2. EVALUATE MECHANICAL TURBULENCE (Based on Wind/Gust over Land)
    mech_wind = max(wind_alt, gust_alt)
    mech_lvl = 0
    if mech_wind >= (40 * scale):
        mech_lvl = 3 # SEV
    elif mech_wind >= (25 * scale):
        mech_lvl = 2 # MOD
    elif mech_wind >= (15 * scale):
        mech_lvl = 1 # LGT

    # 3. EVALUATE VERTICAL SHEAR (LLWS Proxy per 1000 ft)
    shear_lvl = 0
    if alt > 0:
        # Calculate knots of shear per 1000 feet
        shear_per_1000 = (abs(wind_alt - wind_sfc) / alt) * 1000
        if shear_per_1000 >= (10 * scale):
            shear_lvl = 3 # SEV
        elif shear_per_1000 >= (6 * scale):
            shear_lvl = 2 # MOD
        elif shear_per_1000 >= (3 * scale):
            shear_lvl = 1 # LGT

    # 4. EVALUATE CONVECTIVE TURBULENCE (Based on WMO Precipitation)
    conv_lvl = 0
    if wx in [95, 96, 97, 98, 99]: # Thunderstorms (CB)
        conv_lvl = 3 # SEV
    elif wx in [80, 81, 82, 85, 86]: # Showers (CU / TCU)
        conv_lvl = 2 # MOD

    # 5. DETERMINE DOMINANT THREAT
    max_threat = max(mech_lvl, shear_lvl, conv_lvl)
    
    turb_str = "Nil"
    if max_threat > 0:
        # Determine Severity String
        sev_str = "SEV" if max_threat == 3 else ("MDT" if max_threat == 2 else "LGT")
        
        # Determine Mechanism String (Priority: Convective -> Shear -> Mech)
        if max_threat == conv_lvl:
            type_str = "CVCTV"
        elif max_threat == shear_lvl:
            type_str = "LLWS"
        else:
            type_str = "MECH"
            
        turb_str = f"{sev_str} {type_str}"

    # 6. ICING PROFILES (Standard lapse rate 2°C per 1,000 ft)
    t_alt = t_temp - (alt / 1000.0) * 2.0
    ice_sev = "Nil"
    ice_type = ""

    if icing_cond or wx in [48, 56, 57, 66, 67, 71, 73, 75, 77, 85, 86]:
        # Icing generally occurs between 0C and -20C
        if 0 >= t_alt >= -20:
            
            # Severity Logic
            if wx in [56, 57, 66, 67]: # Freezing Rain/Drizzle is automatically Severe
                ice_sev = "SEV"
            elif wx in [73, 75, 86] or (icing_cond and t_alt >= -10):
                ice_sev = "MDT"
            else:
                ice_sev = "LGT"

            # Type Logic based on droplet freezing speed
            if t_alt >= -5:
                ice_type = "CLR"
            elif t_alt >= -15:
                ice_type = "MXD"
            else:
                ice_type = "RIME"

    ice_str = f"{ice_sev} {ice_type}" if ice_sev != "Nil" else "Nil"

    return turb_str, ice_str

def apply_tactical_highlights(raw_text):
    """Injects warning colors into raw METAR/TAF strings for rapid parsing."""
    if not raw_text or "UNAVAILABLE" in raw_text:
        return raw_text
    
    # Red highlights for tactical hazards
    hazards = ["FZRA", "FZDZ", "TSRA", "TS", "GR", "FC", r"\+FC", "LLWS", "SEV", "ICE"]
    for hazard in hazards:
        raw_text = re.sub(rf"\b({hazard})\b", r'<span class="ifr-text">\1</span>', raw_text)
        
    return raw_text
