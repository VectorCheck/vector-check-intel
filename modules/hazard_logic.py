import math

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
    """Calculates tactical icing risk and maps the exact altitude base and top."""
    icing_cond = {"severity": "NONE", "base": 99999, "top": -1}
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600]
    
    sfc_t_list = hourly_data.get('temperature_2m')
    sfc_t = sfc_t_list[idx] if sfc_t_list else 0
    inversion_detected = False
    
    for p in p_levels:
        t_list = hourly_data.get(f"temperature_{p}hPa")
        rh_list = hourly_data.get(f"relative_humidity_{p}hPa")
        h_list = hourly_data.get(f"geopotential_height_{p}hPa")
        
        if t_list and rh_list and h_list:
            t_v = t_list[idx]
            rh_v = rh_list[idx]
            h_raw = h_list[idx]
            
            if t_v is not None and rh_v is not None and h_raw is not None:
                h_v = h_raw * 3.28084 # Convert geopotential meters to feet AGL
                td_v = t_v - ((100 - rh_v) / 5.0) # Calculate dewpoint natively
                spread = abs(t_v - td_v)
                
                # Inversion tracking
                if sfc_t is not None and sfc_t < 0 and t_v > 0:
                    inversion_detected = True
                    
                # Micro-layer saturation logic
                if t_v <= 0 and spread <= 3.0:
                    if icing_cond["severity"] == "NONE":
                        icing_cond["severity"] = "LGT RIME"
                    if t_v >= -8:
                        icing_cond["severity"] = "MOD MXD"
                        
                    # Expand the altitude band to cover this layer
                    icing_cond["base"] = min(icing_cond["base"], h_v)
                    icing_cond["top"] = max(icing_cond["top"], h_v)
                    
    # Ultimate escalation triggers
    if inversion_detected or wx_code in [56, 57, 66, 67]:
        icing_cond["severity"] = "SEV FZRA"
        icing_cond["base"] = 0
        icing_cond["top"] = 10000
        
    return icing_cond

def get_turb_ice(alt, spd, sfc_spd, gust, wx_code, is_stable, icing_cond):
    """Calculates wind shear and cross-references drone altitude against icing bands."""
    # Turbulence Logic
    turb = "NONE"
    shear = abs(spd - sfc_spd)
    if gust - spd > 5: turb = "LGT MECH"
    if gust - spd > 10: turb = "MOD MECH"
    if gust - spd > 15: turb = "SEV MECH"
    if is_stable and shear > 15: turb = "LLWS"
    if wx_code in [95, 96, 99]: turb = "SEV CVCTV"
    
    # Icing Logic (Checks if current altitude is within the hazardous layer)
    ice = "NONE"
    if icing_cond["base"] <= alt <= icing_cond["top"] or icing_cond["severity"] == "SEV FZRA":
        ice = icing_cond["severity"]
        
    return turb, ice
