import re
import math

def apply_tactical_highlights(text):
    if not text: return ""
    def precip_match(m): return f'<span class="fz-warn">{m.group(0)}</span>'
    text = re.sub(r'(?<!\S)[-+]?[A-Z]*(?:FZ|PL|TS|GR|SQ)[A-Z]*(?!\S)', precip_match, text)
    
    def vis_match_sm(m):
        raw = m.group(0)
        try:
            clean = raw.upper().replace('SM', '').replace('P', '').replace('M', '').strip()
            val = 0.0
            for p in clean.split():
                if '/' in p:
                    num, den = p.split('/')
                    val += float(num) / float(den)
                else:
                    val += float(p)
            if val < 3: return f'<span class="ifr-text">{raw}</span>'
            if 3 <= val <= 5: return f'<span class="mvfr-text">{raw}</span>'
        except: pass
        return raw
    text = re.sub(r'(?<!\S)[PM]?(?:\d+\s+)?(?:\d+/\d+|\d+)SM(?!\S)', vis_match_sm, text)

    def vis_match_m(m):
        raw = m.group(1)
        try:
            val_m = int(raw)
            if val_m == 9999: return raw 
            val_sm = val_m / 1609.34 
            if val_sm < 3: return f'<span class="ifr-text">{raw}</span>'
            if 3 <= val_sm <= 5: return f'<span class="mvfr-text">{raw}</span>'
        except: pass
        return raw
    text = re.sub(r'(?<!\S)(\d{4})(?![Z/\d])(?!\S)', vis_match_m, text)

    def sky_match(m):
        try:
            h = int(m.group(2)) * 100
            if h < 1000: return f'<span class="ifr-text">{m.group(0)}</span>'
            if 1000 <= h <= 3000: return f'<span class="mvfr-text">{m.group(0)}</span>'
        except: pass
        return m.group(0)
    text = re.sub(r'(?<!\S)(BKN|OVC|VV)(\d{3})(?:CB|TCU)?(?!\S)', sky_match, text)
    return text

def get_precip_type(code):
    if code is None: return "None"
    if code in [0, 1, 2, 3, 45, 48]: return "None"
    if code in [51, 53, 55, 61, 63, 65, 80, 81, 82, 95]: return "Rain"
    if code in [56, 57, 66, 67]: return "Freezing Rain"
    if code in [71, 73, 75, 77, 85, 86]: return "Snow"
    return "Mixed"

def calculate_icing_profile(hourly_data, idx, wx_code):
    """Calculates tactical icing risk using dynamic T/RH spread at pressure altitudes."""
    icing_severity = "NONE"
    p_levels = [1000, 950, 925, 900, 850, 800, 700, 600]
    
    # Grab surface temp to check for inversions
    sfc_t_list = hourly_data.get('temperature_2m')
    sfc_t = sfc_t_list[idx] if sfc_t_list else 0
    inversion_detected = False
    
    for p in p_levels:
        t_list = hourly_data.get(f"temperature_{p}hPa")
        rh_list = hourly_data.get(f"relative_humidity_{p}hPa")
        
        # Ensure the API actually returned these layers before calculating
        if t_list and rh_list:
            t_v = t_list[idx]
            rh_v = rh_list[idx]
            
            if t_v is not None and rh_v is not None:
                # 1. Calculate Dewpoint dynamically: Td = T - ((100 - RH) / 5)
                td_v = t_v - ((100 - rh_v) / 5.0)
                spread = abs(t_v - td_v)
                
                # 2. Inversion Tracking for Freezing Rain (FZRA)
                if sfc_t is not None and sfc_t < 0 and t_v > 0:
                    inversion_detected = True
                
                # 3. Micro-Layer Saturation Sensitivity
                if t_v <= 0 and spread <= 3.0:
                    if icing_severity == "NONE":
                        icing_severity = "LGT RIME"
                    if t_v >= -8: # Warmer sub-zero temps carry mixed/clear ice risk
                        icing_severity = "MOD MXD"
                        
    # Final Escalation Logic
    if inversion_detected:
        icing_severity = "SEV FZRA"
        
    # WMO Weather Code Override
    if wx_code in [56, 57, 66, 67]:
        icing_severity = "SEV FZRA"
        
    return icing_severity

def get_turb_ice(alt, spd, w_spd, cur_gst, wx, is_stable, icing_cond):
    sh_1k = ((spd - w_spd) / alt) * 1000 if alt > 0 else 0
    if wx in [95, 96, 99]: 
        t_type, t_sev = "CVCTV", ("SEV" if cur_gst > 25 else "MDT")
    elif is_stable and sh_1k >= 20: 
        t_type, t_sev = "LLWS", ("SEV" if sh_1k >= 40 else "MDT")
    else:
        t_type = "MECH"
        max_w = max(spd, cur_gst)
        if max_w < 15: t_sev = "NONE"
        elif max_w < 25: t_sev = "LGT"
        elif max_w < 35: t_sev = "MOD"
        else: t_sev = "SEV"
        
    ice = "NONE"
    if icing_cond["base"] <= alt <= icing_cond["top"]: 
        ice = f"{icing_cond['sev']} {icing_cond['type']}"
    elif icing_cond["base"] == 0 and alt < icing_cond["top"]: 
        ice = f"{icing_cond['sev']} {icing_cond['type']}"
        
    return f"{t_sev} {t_type}" if t_sev != "NONE" else "NONE", ice
