import requests
import json
import os
from datetime import datetime

def fetch_brewery_data(**context):
    """Fetch data from OpenBreweryDB API"""
    url = "https://api.openbrewerydb.org/v1/breweries"
    
    try:
        # Fetch data from API
        response = requests.get(url, params={"per_page": 200})
        response.raise_for_status()
        
        breweries = response.json()
        
        # Save raw JSON (for debugging/audit)
        raw_path = "/spark/data/raw/breweries"
        os.makedirs(raw_path, exist_ok=True)
        
        filename = f"breweries_{context['ts_nodash']}.json"
        filepath = os.path.join(raw_path, filename)
        
        with open(filepath, "w") as f:
            json.dump(breweries, f, indent=2)
        
        print(f"✓ Fetched {len(breweries)} breweries")
        print(f"✓ Saved raw data to: {filepath}")
        
        return filepath
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Error fetching data: {e}")
        raise