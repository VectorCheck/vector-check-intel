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
    """Fetches the NOAA Planetary K-index forecast with impenetrable data scrubbing."""
    url = "https://services.swpc.noaa.gov/products/noaa-planetary-k-index-forecast.json"
    headers = {"User-Agent": "VectorCheckAerialGroup/1.0 (ops.vectorcheck.ca)"}
    
    try:
        response = requests.get(url, headers=headers, timeout=5)
        response.raise_for_status() 
        data = response.json()
        
        closest_kp = None
        min_diff = float('inf')
        
        for row in data:
            if not row or not isinstance(row, list): continue
            
            # Explicitly skip the header row
            if str(row[0]).strip() == "time_tag": continue 
            
            try:
                row_dt = datetime.strptime(str(row[0]), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except Exception:
                continue # Skip rows with broken timestamps
                
            predicted_kp = None
            
            # Brute-force float testing: Scan columns right-to-left
            for i in [3, 2, 1]:
                if len(row) > i and row[i]:
                    try:
                        predicted_kp = float(str(row[i]).strip())
                        break # Successfully found a valid number
                    except ValueError:
                        pass # It was a rogue string like 'observed', keep searching
                        
            if predicted_kp is not None:
                diff = abs((target_utc - row_dt).total_seconds())
                if diff < min_diff:
                    min_diff = diff
                    closest_kp = predicted_kp
                    
        if closest_kp is not None:
            return evaluate_gnss_risk(closest_kp)
        else:
            return {"kp": "ERR", "risk": "PARSE_FAIL", "impact": "Connected to NOAA, but no valid Kp numbers found."}
            
    except requests.exceptions.HTTPError as err:
        return {"kp": "ERR", "risk": "HTTP_ERR", "impact": f"NOAA Firewall Block: {err}"}
    except Exception as e:
        return {"kp": "ERR", "risk": "SYS_ERR", "impact": f"System Exception: {str(e)}"}
