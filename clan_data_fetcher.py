print("DEBUG: Script execution started.") # <--- Added

import pymongo
import requests
import datetime
import time
import os
from dotenv import load_dotenv

print("DEBUG: Imports successful.") # <--- Added

# --- Load Environment Variables ---
print("DEBUG: Attempting to load .env file...") # <--- Added
load_dotenv()
print("DEBUG: load_dotenv() executed.") # <--- Added

# --- MongoDB Atlas Connection ---
print("DEBUG: Attempting to read MONGO_URI env var...") # <--- Added
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = "clan_dashboard_db"

if not MONGO_CONNECTION_STRING:
    print("DEBUG: ERROR - MONGO_URI environment variable not set!") # <--- Added
    # Raise ValueError("MONGO_URI environment variable not set!") # Temporarily comment out raise for debugging
    exit(1) # Exit cleanly if not set
else:
    # Avoid printing the full sensitive string to logs
    print("DEBUG: MONGO_URI environment variable read successfully (value hidden).") # <--- Added

# --- API Fetching --- (Function remains the same)
API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
def fetch_clan_data():
    # ... (keep the existing fetch_clan_data function code here) ...
    print(f"Attempting to fetch data from: {API_URL}")
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        api_response = response.json()
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"]
            print(f"Successfully parsed JSON. Found 'data' key containing {len(clan_list)} clans.")
            return clan_list
        else:
            print(f"API response status not 'ok' or 'data' key missing: {api_response}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred fetching data from the API: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API fetch/parse: {e}")
        return None


# --- MongoDB Insertion Logic --- (Function remains the same)
def insert_clan_data(clan_list, client):
    # ... (keep the existing insert_clan_data function code here) ...
    if not clan_list: print("No clan data provided to insert."); return 0
    print(f"Processing insert/update for {len(clan_list)} clans into MongoDB...");
    db = client[DB_NAME]; clans_collection = db["clans"]; details_collection = db["clan_details"]
    inserted_count = 0; details_updated_count = 0; processed_count = 0
    current_timestamp = datetime.datetime.now(datetime.timezone.utc)
    for clan in clan_list:
        processed_count += 1; clan_name = clan.get("Name"); clan_points = clan.get("Points"); members_count = clan.get("Members"); icon = clan.get("Icon"); country_code = clan.get("CountryCode"); capacity = clan.get("MemberCapacity"); created_api = clan.get("Created")
        if clan_name is None or clan_points is None: print(f"Skipping clan due to missing Name or Points: {clan}"); continue
        try:
            existing_clan_check = clans_collection.find_one({"clan_name": clan_name}, {"_id": 1})
            clan_doc = {"clan_name": clan_name, "current_points": clan_points, "members": members_count, "timestamp": current_timestamp,}
            if not existing_clan_check: clan_doc["first_seen"] = current_timestamp; print(f"Clan '{clan_name}' is new. Setting first_seen.")
            clans_collection.insert_one(clan_doc); inserted_count += 1
            details_doc = {"icon": icon, "country_code": country_code, "member_capacity": capacity, "created_timestamp_api": created_api, "last_checked": current_timestamp}
            details_collection.update_one({"clan_name": clan_name}, {"$set": details_doc}, upsert=True); details_updated_count += 1
        except Exception as e: print(f"Error processing/inserting data for clan '{clan_name}': {e}"); continue
    print(f"Processed {processed_count} clans. Inserted {inserted_count} docs into 'clans'. Upserted {details_updated_count} docs in 'clan_details'.")
    return inserted_count

# --- Main Execution Loop ---
print("DEBUG: Attempting initial connection block...") # <--- Added
mongo_client = None
try:
    print("DEBUG: Before MongoClient()...") # <--- Added
    # Establish connection to MongoDB Atlas
    # Increased timeout slightly
    mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
    print("DEBUG: After MongoClient(), before ping...") # <--- Added
    # The ismaster command is cheap and does not require auth. Retained for connection test.
    mongo_client.admin.command('ismaster') # Use ismaster or ping
    # Or use: mongo_client.admin.command('ping')
    print("DEBUG: MongoDB Atlas connection successful (ping successful)!") # <--- Added
except pymongo.errors.ConnectionFailure as e:
    print(f"DEBUG: MongoDB connection failed: {e}") # <--- Added DEBUG prefix
    if mongo_client: mongo_client.close() # Attempt close
    exit(1) # Exit on connection failure
except Exception as e:
    print(f"DEBUG: An error occurred during initial MongoDB connection: {e}") # <--- Added DEBUG prefix
    import traceback; traceback.print_exc() # Print full traceback for unexpected errors
    if mongo_client: mongo_client.close() # Attempt close
    exit(1) # Exit on other initial errors


print("DEBUG: Starting data fetch loop...") # <--- Added
while True:
    print(f"\n--- Starting new fetch cycle at {datetime.datetime.now()} ---")
    clans = fetch_clan_data()
    if clans:
        # print(f"Successfully retrieved {len(clans)} clan entries from the API.") # Already printed in fetch_clan_data
        insert_clan_data(clans, mongo_client)
    else:
        print("Failed to retrieve clan data from the API this cycle.")

    wait_seconds = 120
    print(f"--- Cycle complete. Waiting for {wait_seconds} seconds... ---")
    time.sleep(wait_seconds)

# Note: Cleanup like client.close() won't be reached in infinite loop without signal handling.