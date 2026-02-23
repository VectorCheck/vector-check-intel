import requests
from datetime import datetime, timezone

def evaluate_gnss_risk(kp):
    """Translates the raw Kp index into tactical drone operational impacts."""
    kp_int = int(round(kp))
    if kp_int <= 3:
        return {"kp": kp, "risk": "LOW", "impact": "Nominal GNSS lock and C2 integrity."}
    elif kp_int == 4:
        return {"kp": kp, "risk": "MODERATE", "impact": "Active state. Minor GNSS jitter. Possible RTK initialization delay."}
    elif kp_int == 5:
        return {"kp": kp, "risk": "HIGH (G1)", "impact": "Minor Geomagnetic Storm. Expect GPS signal degradation and C2 interference."}
    elif kp_int >= 6:
        return {"kp": kp, "risk": "SEVERE (G2+)", "impact": "Major Geomagnetic Storm. High risk of GNSS loss. Manual flight only."}
    
    return {"kp": kp, "risk": "UNKNOWN", "impact": "Data processing error."}

def get_kp_index(target_utc):
    """Fetches the NOAA Planetary K-index forecast and matches it to the target time."""
    url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"
    
    # NOAA blocks default Python requests. We must identify as a legitimate application.
    headers = {
        "User-Agent": "VectorCheckAerialGroup/1.0 (ops.vectorcheck.ca)"
    }
    
    try:
        # Increased timeout to 5 seconds to account for Streamlit Cloud latency
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            # NOAA data format: [["time_tag", "observed", "estimated", "predicted"], ...]
            forecasts = data[1:] # Skip the header row
            
            closest_kp = None
            min_diff = float('inf')
            
            for row in forecasts:
                time_tag = row[0]
                
                # Robust extraction: Safely check array length before pulling data
                predicted_kp = 0.0
                if len(row) > 3 and row[3]:
                    predicted_kp = float(row[3])
                elif len(row) > 2 and row[2]:
                    predicted_kp = float(row[2])
                elif len(row) > 1 and row[1]:
                    predicted_kp = float(row[1])
                
                # NOAA time is "YYYY-MM-DD HH:MM:SS" in UTC
                row_dt = datetime.strptime(time_tag, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
                diff = abs((target_utc - row_dt).total_seconds())
                
                # Find the Kp index closest to the user's selected slider time
                if diff < min_diff:
                    min_diff = diff
                    closest_kp = predicted_kp
                    
            if closest_kp is not None:
                return evaluate_gnss_risk(closest_kp)
                
    except Exception:
        # If the API times out or the JSON changes structure, it falls to the silent return below
        pass
    
    return {"kp": "N/A", "risk": "UNKNOWN", "impact": "NOAA Space Weather API Unreachable or Blocked."}
