from smartapi import SmartConnect
import pandas as pd
import requests
import pyotp
import logging
import json
import time
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize SmartConnect with MPIN Login
def initialize_api():
    # Fetch credentials from .env
    api_key = os.getenv('API_KEY')
    user_name = os.getenv('USER_NAME')
    mpin = os.getenv('MPIN')
    totp_secret = os.getenv('TOTP_SECRET')

    # Validate credentials
    if not all([api_key, user_name, mpin, totp_secret]):
        print("❌ Missing credentials in .env file. Please set API_KEY, USER_NAME, MPIN, and TOTP_SECRET.")
        exit()

    try:
        totp = pyotp.TOTP(totp_secret).now()
    except Exception as e:
        print(f"❌ Error generating TOTP: {str(e)}")
        exit()

    # Initialize SmartConnect (for headers and session management)
    obj = SmartConnect(api_key=api_key)
    
    # Custom loginByMPin request
    url = "https://apiconnect.angelone.in/rest/auth/angelbroking/user/v1/loginByMPin"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": getattr(obj, '_client_local_ip', "127.0.0.1"),
        "X-ClientPublicIP": getattr(obj, '_client_public_ip', "127.0.0.1"),
        "X-MACAddress": getattr(obj, '_mac_address', "00:00:00:00:00:00"),
        "X-PrivateKey": api_key
    }
    payload = {
        "clientcode": user_name,
        "mpin": mpin,
        "totp": totp
    }

    try:
        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get('status') and data.get('data', {}).get('jwtToken'):
            print("✅ API session initialized successfully.")
            print(f"Session response: {json.dumps(data, indent=2)}")
            # Manually set access_token in SmartConnect object
            obj.access_token = data['data']['jwtToken']
            return obj
        else:
            print(f"❌ Login failed: {data.get('message', 'Unknown error')}")
            print(f"Full session response: {json.dumps(data, indent=2)}")
            exit()
    except Exception as e:
        print(f"❌ Login failed: {str(e)}")
        exit()

# Fetch Data for Gainers/Losers and OI BuildUp (POST request)
def fetch_post_data(api_obj, endpoint, datatype, expirytype="NEAR", retries=2, delay=2):
    url = f"https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/{endpoint}"
    headers = {
        "Authorization": f"Bearer {api_obj.access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": getattr(api_obj, '_client_local_ip', "127.0.0.1"),
        "X-ClientPublicIP": getattr(api_obj, '_client_public_ip', "127.0.0.1"),
        "X-MACAddress": getattr(api_obj, '_mac_address', "00:00:00:00:00:00"),
        "X-PrivateKey": os.getenv('API_KEY')
    }
    payload = {
        "datatype": datatype,
        "expirytype": expirytype
    }

    for attempt in range(retries + 1):
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            print(f"Response for {datatype} ({expirytype}, {endpoint}): {json.dumps(data, indent=2)}")
            
            if data.get("status") and "data" in data and data["data"]:
                print(f"✅ Successfully fetched {datatype} ({expirytype}, {endpoint})")
                df = pd.DataFrame(data["data"])
                df['DataType'] = datatype
                df['ExpiryType'] = expirytype
                df['Endpoint'] = endpoint
                return df
            else:
                print(f"⚠️ No data returned for {datatype} ({expirytype}, {endpoint}). Response: {json.dumps(data, indent=2)}")
                return pd.DataFrame()
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error fetching {datatype} ({expirytype}, {endpoint}) (Attempt {attempt + 1}/{retries + 1}): {str(e)}")
            if response.status_code == 403:
                print(f"403 Forbidden details: {json.dumps(response.json(), indent=2)}")
            if attempt < retries:
                print(f"🔄 Retrying after {delay} seconds...")
                time.sleep(delay)
                # Refresh session
                api_obj = initialize_api()
                headers["Authorization"] = f"Bearer {api_obj.access_token}"
        except Exception as e:
            print(f"❌ Error fetching {datatype} ({expirytype}, {endpoint}) (Attempt {attempt + 1}/{retries + 1}): {str(e)}")
            return pd.DataFrame()
    
    print(f"❌ Failed to fetch {datatype} ({expirytype}, {endpoint}) after {retries + 1} attempts")
    return pd.DataFrame()

# Fetch PCR Volume Data (GET request)
def fetch_pcr_volume(api_obj, retries=2, delay=2):
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/marketData/v1/putCallRatio"
    headers = {
        "Authorization": f"Bearer {api_obj.access_token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "X-UserType": "USER",
        "X-SourceID": "WEB",
        "X-ClientLocalIP": getattr(api_obj, '_client_local_ip', "127.0.0.1"),
        "X-ClientPublicIP": getattr(api_obj, '_client_public_ip', "127.0.0.1"),
        "X-MACAddress": getattr(api_obj, '_mac_address', "00:00:00:00:00:00"),
        "X-PrivateKey": os.getenv('API_KEY')
    }

    for attempt in range(retries + 1):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            print(f"Response for PCR Volume (putCallRatio): {json.dumps(data, indent=2)}")
            
            if data.get("status") and "data" in data and data["data"]:
                print(f"✅ Successfully fetched PCR Volume (putCallRatio)")
                df = pd.DataFrame(data["data"])
                df['DataType'] = 'PCR Volume'
                df['ExpiryType'] = 'N/A'
                df['Endpoint'] = 'putCallRatio'
                return df
            else:
                print(f"⚠️ No data returned for PCR Volume (putCallRatio). Response: {json.dumps(data, indent=2)}")
                return pd.DataFrame()
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP error fetching PCR Volume (putCallRatio) (Attempt {attempt + 1}/{retries + 1}): {str(e)}")
            if response.status_code == 403:
                print(f"403 Forbidden details: {json.dumps(response.json(), indent=2)}")
            if attempt < retries:
                print(f"🔄 Retrying after {delay} seconds...")
                time.sleep(delay)
                # Refresh session
                api_obj = initialize_api()
                headers["Authorization"] = f"Bearer {api_obj.access_token}"
        except Exception as e:
            print(f"❌ Error fetching PCR Volume (putCallRatio) (Attempt {attempt + 1}/{retries + 1}): {str(e)}")
            return pd.DataFrame()
    
    print(f"❌ Failed to fetch PCR Volume (putCallRatio) after {retries + 1} attempts")
    return pd.DataFrame()

# Main Execution
if __name__ == '__main__':
    # Initialize API
    api_obj = initialize_api()

    # Define output file
    OUTPUT_FILE = "AngelMarketData.csv"

    # Define endpoints and data types
    endpoints = {
        "gainersLosers": ["PercOIGainers", "PercOILosers", "PercPriceGainers", "PercPriceLosers"],
        "OIBuildup": ["Long Built Up", "Short Built Up", "Short Covering", "Long Unwinding"]
    }
    expiry_types = ["NEAR", "NEXT", "FAR"]
    all_data = []

    # Fetch Gainers/Losers and OI BuildUp data
    for endpoint, data_types in endpoints.items():
        for expiry in expiry_types:
            for datatype in data_types:
                print(f"📥 Fetching {datatype} with expiry {expiry} ({endpoint})...")
                df = fetch_post_data(api_obj, endpoint, datatype, expirytype=expiry)
                if not df.empty:
                    all_data.append(df)
                else:
                    print(f"⚠️ No data fetched for {datatype} ({expiry}, {endpoint})")
                time.sleep(1)  # Delay to avoid rate limiting

    # Fetch PCR Volume data
    print(f"📥 Fetching PCR Volume (putCallRatio)...")
    df = fetch_pcr_volume(api_obj)
    if not df.empty:
        all_data.append(df)
    else:
        print(f"⚠️ No data fetched for PCR Volume (putCallRatio)")
    time.sleep(1)  # Delay to avoid rate limiting

    # Combine and save data
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(OUTPUT_FILE, index=False)
        print(f"✅ Data saved to {OUTPUT_FILE}")
    else:
        print("❌ No data to save.")

    print("✅ Data collection completed.")
