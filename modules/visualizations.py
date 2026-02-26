import matplotlib.pyplot as plt
import math

def calc_td(t, rh):
    """Calculates dewpoint from temperature and relative humidity."""
    if rh <= 0: return t
    a = 17.625
    b = 243.04
    alpha = math.log(rh / 100.0) + ((a * t) / (b + t))
    return (b * alpha) / (a - alpha)

def plot_convective_profile(h, idx, t_temp, td, w_spd, sfc_dir, sfc_h):
    """
    Renders a compact, high-resolution digital Tephigram (T/Td profile).
    Safely skips underground pressure levels at high-elevation terrain.
    """
    altitudes = [sfc_h]
    temps = [t_temp]
    dewpoints = [td]
    
    # Safely extract valid layers above ground level
    for p in [1000, 925, 850, 700]:
        gh_list = h.get(f'geopotential_height_{p}hPa')
        t_list = h.get(f'temperature_{p}hPa')
        rh_list = h.get(f'relative_humidity_{p}hPa')
        
        if gh_list and t_list and rh_list and len(gh_list) > idx:
            gh_val = gh_list[idx]
            t_val = t_list[idx]
            rh_val = rh_list[idx]
            
            if gh_val is not None and t_val is not None and rh_val is not None:
                alt_ft = float(gh_val) * 3.28084
                # Only plot the layer if it exists physically above the previous layer
                if alt_ft > altitudes[-1]: 
                    altitudes.append(alt_ft)
                    temps.append(float(t_val))
                    dewpoints.append(calc_td(float(t_val), int(rh_val)))
                    
    # Abort gracefully if the API provided zero valid aloft layers
    if len(altitudes) < 2:
        return None

    # --- STYLE & SCALING ---
    plt.style.use('dark_background')
    # Reduced figsize for compactness, increased DPI for ultra-crisp rendering
    fig, ax = plt.subplots(figsize=(6, 3.5), dpi=200) 
    fig.patch.set_facecolor('#1B1E23')
    ax.set_facecolor('#1B1E23')
    
    # Data Plotting
    ax.plot(temps, altitudes, color='#ff4b4b', label='Temp (°C)', linewidth=1.5, marker='o', markersize=3)
    ax.plot(dewpoints, altitudes, color='#2abf2a', label='Dewpt (°C)', linewidth=1.5, marker='o', markersize=3)
    
    # Cloud / Visible Moisture Indicator (Fills gray where T and Td are within 3C)
    ax.fill_betweenx(altitudes, dewpoints, temps, where=[(t - d) <= 3.0 for t, d in zip(temps, dewpoints)], color='#8E949E', alpha=0.3, label='Moisture / Cloud')

    # Formatting (No Title)
    ax.set_ylabel('Altitude (ft ASL)', color='#A0A4AB', fontsize=9)
    ax.set_xlabel('Temperature (°C)', color='#A0A4AB', fontsize=9)
    ax.tick_params(axis='x', colors='#D1D5DB', labelsize=8)
    ax.tick_params(axis='y', colors='#D1D5DB', labelsize=8)
    
    # Spine Colors
    for spine in ax.spines.values():
        spine.set_color('#3E444E')
        
    ax.grid(color='#2D3139', linestyle='--', linewidth=0.5)
    
    # Legend
    ax.legend(loc='upper right', facecolor='#1B1E23', edgecolor='#3E444E', fontsize=7)
    
    plt.tight_layout()
    return fig
