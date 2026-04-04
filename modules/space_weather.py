import urllib.request
import json
from datetime import datetime, timezone

def get_kp_index(target_dt_utc):
    """
    Fetches Planetary K-index from the NOAA SWPC JSON API.
    Determines GNSS and C2 link risk based on geomagnetic storm scaling.
    """
    url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"
    
    # Graceful degradation fallback if NOAA goes offline
    fallback_data = {
        'kp': "N/A", 
        'risk': "UNAVAILABLE", 
        'impact': "NOAA SWPC connection failed or format changed. Monitor local GNSS satellite counts and HDOP closely prior to launch."
    }

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        # NOAA JSON format: [["time_tag", "kp", "observed", "noaa_scale"], ["2026-04-04 00:00:00", "3.33", ...]]
        if not data or len(data) < 2:
            return fallback_data

        best_kp = None
        min_diff = float('inf')
        
        # Iterate through the forecast array to find the closest time match
        for row in data[1:]:
            time_str, kp_str = row[0], row[1]
            
            # NOAA time_tag format: "YYYY-MM-DD HH:MM:SS"
            row_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            diff = abs((row_dt - target_dt_utc).total_seconds())
            
            if diff < min_diff:
                min_diff = diff
                best_kp = float(kp_str)

        if best_kp is None:
            return fallback_data

        kp_val = int(round(best_kp))
        
        # Tactical Risk Matrix for UAS
        if kp_val <= 3:
            risk = "LOW (G0)"
            impact = "Optimal GNSS lock. Minimal ionospheric scintillation expected. C2 link stable."
        elif kp_val == 4:
            risk = "MODERATE (G0)"
            impact = "Slight ionospheric degradation possible. Verify minimum satellite count and HDOP before launch."
        elif kp_val == 5:
            risk = "HIGH (G1)"
            impact = "Minor GNSS positioning errors likely. Potential for intermittent C2 link degradation and compass anomalies."
        elif kp_val >= 6:
            risk = "SEVERE (G2+)"
            impact = "CRITICAL: High probability of GNSS loss of lock, flyaways, and C2 link failure. Manual ATTI mode readiness required."
            
        return {
            'kp': str(kp_val),
            'risk': risk,
            'impact': impact
        }

    except Exception:
        return fallback_data
