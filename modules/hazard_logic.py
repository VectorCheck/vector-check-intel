import math
import re

def apply_tactical_highlights(text):
    """Applies HTML formatting to raw METAR/TAF strings to highlight aviation hazards."""
    if not text: 
        return text
    # Red highlights for severe weather, freezing precip, and hard IFR conditions
    text = re.sub(r'\b(TS\w*|FZ\w*|GR|FC|VA)\b', r'<span class="fz-warn">\1</span>', text)
    text = re.sub(r'\b(BKN|OVC)(00[0-9]|010)\b', r'<span class="ifr-text">\1\2</span>', text)
    text = re.sub(r'\b([0-2]SM|1/4SM|1/2SM|3/4SM)\b', r'<span class="ifr-text">\1</span>', text)
    
    # Yellow highlights for MVFR conditions
    text = re.sub(r'\b([3-5]SM)\b', r'<span class="mvfr-text">\1</span>', text)
    text = re.sub(r'\b(BKN|OVC)(01[1-9]|02[0-9]|030)\b', r'<span class="mvfr-text">\1\2</span>', text)
    
    return text

def get_precip_type(wx_code):
    """Maps WMO weather codes to standard aviation METAR precipitation codes."""
    if wx_code in [0, 1, 2, 3]: return "NONE"
    if wx_code in [45, 48]: return "FOG"
    if wx_code in [51, 53, 55]: return "DZ"
    if wx_code in [56, 57]: return "FZDZ"
    if wx_code in [61, 63, 65]: return "RA"
    if wx_code in [66, 67]: return "FZRA"
    if wx_code in [71, 73, 75]: return "SN"
    if wx_code in [77]: return "SG"
    if wx_code in [80, 81, 82]: return "SHRA"
    if wx_code in [85, 86]: return "SHSN"
    if wx_code in [95, 96, 99]: return "TS"
    return "UNK"

def calculate_icing_profile(hourly_data, idx, wx_code):
    """Calculates tactical icing risk using dynamic T/RH spread."""
    icing_severity = "NONE"
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600]
    
    sfc_t_list = hourly_data.get('temperature_2m')
    sfc_t = sfc_t_list[idx] if sfc_t_list else 0
    inversion_detected = False
    
    for p in p_levels:
        t_list = hourly_data.get(f"temperature_{p}hPa")
        rh_list = hourly_data.get(f"relative_humidity_{p}hPa")
        
        if t_list and rh_list:
            t_v = t_list[idx]
            rh_v = rh_list[idx]
            
            if t_v is not None and rh_v is not None:
                # Calculate dewpoint natively to bypass API limitations
                td_v = t_v - ((100 - rh_v) / 5.0) 
                spread = abs(t_v - td_v)
                
                if sfc_t is not None and sfc_t < 0 and t_v > 0:
                    inversion_detected = True
                    
                if t_v <= 0 and spread <= 3.0:
                    if icing_severity == "NONE":
                        icing_severity = "LGT RIME"
                    if t_v >= -8:
                        icing_severity = "MOD MXD"
                        
    if inversion_detected or wx_code in [56, 57, 66, 67]:
        icing_severity = "SEV FZRA"
        
    return icing_severity

def get_turb_ice(alt, spd, sfc_spd, gust, wx_code, is_stable, icing_cond):
    """Calculates wind shear and applies icing severity."""
    turb = "NONE"
    shear = abs(spd - sfc_spd)
    if gust - spd > 5: turb = "LGT MECH"
    if gust - spd > 10: turb = "MOD MECH"
    if gust - spd > 15: turb = "SEV MECH"
    if is_stable and shear > 15: turb = "LLWS"
    if wx_code in [95, 96, 99]: turb = "SEV CVCTV"
    
    ice = icing_cond
    return turb, ice
