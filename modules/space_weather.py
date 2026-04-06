import urllib.request
import json
from datetime import datetime, timezone

def get_kp_index(target_dt_utc):
    """
    Fetches Planetary K-index from the NOAA SWPC JSON API.
    Determines GNSS and C2 link risk based on geomagnetic storm scaling.
    Dynamically handles both legacy array and new object JSON structures.
    """
    url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"
    
    # Graceful degradation fallback if NOAA connection fails or parsing faults
    fallback_data = {
        'kp': "ERR", 
        'risk': "PARSE_FAIL", 
        'impact': "Connected to NOAA, but no valid Kp numbers found. Monitor local GNSS satellite counts and HDOP closely prior to launch."
    }

    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'VectorCheck-App/2.0'})
        with urllib.request.urlopen(req, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            
        if not data:
            return fallback_data

        best_kp = None
        min_diff = float('inf')
        
        for row in data:
            # Shield against the legacy header row
            if isinstance(row, list) and row[0] == "time_tag":
                continue 
                
            # NOAA POST-MARCH 2026 FORMAT (Dictionaries)
            if isinstance(row, dict):
                time_str = row.get("time_tag")
                kp_val_raw = row.get("kp")
            # NOAA PRE-MARCH 2026 FORMAT (Lists)
            elif isinstance(row, list) and len(row) >= 2:
                time_str = row[0]
                kp_val_raw = row[1]
            else:
                continue

            if not time_str or kp_val_raw is None:
                continue

            # Parse NOAA time_tag format: "YYYY-MM-DD HH:MM:SS"
            try:
                row_dt = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
                
            diff = abs((row_dt - target_dt_utc).total_seconds())
            
            if diff < min_diff:
                min_diff = diff
                best_kp = float(kp_val_raw)

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
