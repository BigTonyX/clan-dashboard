import sys
import pymongo
import requests
import datetime
import time
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
import traceback # Ensure traceback is imported

# Load environment variables from .env file for local execution
load_dotenv()

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
if not MONGO_CONNECTION_STRING:
    print("FATAL: MONGO_URI environment variable not set!", file=sys.stderr); sys.stderr.flush()
    exit(1)
DB_NAME = "clan_dashboard_db"

# --- API URLs ---
CLANS_API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
WAR_END_API_URL = "https://ps99.biggamesapi.io/api/activeClanBattle"

# --- Helper Function to Get War Finish Time ---
def get_war_finish_time():
    """Fetches war finish time and returns as a naive datetime object, or None."""
    try:
        response = requests.get(WAR_END_API_URL, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and
            "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and
            "FinishTime" in raw_data["data"]["configData"]):
            finish_time_unix = raw_data["data"]["configData"]["FinishTime"]
            # Convert Unix timestamp to datetime object (naive, assuming local time zone interpretation is acceptable for comparison)
            return datetime.datetime.fromtimestamp(finish_time_unix)
        else:
            print(f"Warning: Unexpected data structure from war end time API: {raw_data}", file=sys.stderr)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Warning: Could not fetch war end time: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Warning: Error processing war end time: {e}", file=sys.stderr)
        return None

# --- API Fetching ---
def fetch_clan_data():
    """Fetches the top 250 clan data from the Big Games API."""
    print(f"Attempting to fetch data from: {CLANS_API_URL}"); sys.stdout.flush()
    try:
        response = requests.get(CLANS_API_URL, timeout=15)
        response.raise_for_status()
        api_response = response.json()
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"]
            print(f"Successfully parsed JSON. Found {len(clan_list)} clans."); sys.stdout.flush()
            return clan_list
        else:
            print(f"API response status not 'ok' or 'data' key missing: {api_response}", file=sys.stderr); sys.stderr.flush()
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred fetching data from the API: {e}", file=sys.stderr); sys.stderr.flush()
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API fetch/parse: {e}", file=sys.stderr); sys.stderr.flush()
        return None


# --- MongoDB Insertion Logic ---
def insert_clan_data(clan_list, client):
    """Inserts/Updates clan data into MongoDB Atlas."""
    if not clan_list: print("No clan data provided to insert."); sys.stdout.flush(); return 0
    # print(f"Processing insert/update for {len(clan_list)} clans into MongoDB...") # Reduced logging

    db = client[DB_NAME]; clans_collection = db["clans"]; details_collection = db["clan_details"]
    inserted_count = 0; details_processed_count = 0; processed_count = 0
    # Use timezone-aware UTC for internal storage consistency
    current_timestamp_utc = datetime.datetime.now(datetime.timezone.utc)

    for clan in clan_list:
        processed_count += 1; clan_name = clan.get("Name"); clan_points = clan.get("Points"); members_count = clan.get("Members"); icon = clan.get("Icon"); country_code = clan.get("CountryCode"); capacity = clan.get("MemberCapacity"); created_api = clan.get("Created")
        if clan_name is None or clan_points is None: continue

        # --- Insert into 'clans' collection ---
        try:
            existing_clan_check = clans_collection.find_one({"clan_name": clan_name}, {"_id": 1})
            clan_doc = {
                "clan_name": clan_name,
                "current_points": clan_points,
                "members": members_count,
                "timestamp": current_timestamp_utc, # Store UTC timestamp
            }
            if not existing_clan_check:
                 clan_doc["first_seen"] = current_timestamp_utc # Store UTC timestamp
            insert_result = clans_collection.insert_one(clan_doc)
            if insert_result.acknowledged: inserted_count += 1
        except Exception as e_insert:
            print(f"EXCEPTION during insert into clans for {clan_name}: {e_insert}", file=sys.stderr); sys.stderr.flush()
            pass # Continue loop even if one insert fails

        # --- Update 'clan_details' collection ---
        try:
            details_doc = {
                "icon": icon,
                "country_code": country_code,
                "member_capacity": capacity,
                "created_timestamp_api": created_api,
                "last_checked": current_timestamp_utc # Store UTC timestamp
            }
            update_result = details_collection.update_one({"clan_name": clan_name}, {"$set": details_doc}, upsert=True)
            details_processed_count += 1
        except Exception as e_update:
             print(f"Error processing/updating details for clan '{clan_name}': {e_update}", file=sys.stderr); sys.stderr.flush()

    # Keep final summary print & Flush
    print(f"Processed {processed_count} clans. Successful inserts into 'clans': {inserted_count}. Attempted upserts into 'clan_details': {details_processed_count}."); sys.stdout.flush()
    return inserted_count

# --- Main Execution ---
print("Attempting initial MongoDB Atlas connection..."); sys.stdout.flush()
mongo_client = None
try:
    mongo_client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
    mongo_client.admin.command('ping') # Verify connection
    print("MongoDB Atlas connection successful!"); sys.stdout.flush()
except pymongo.errors.ConnectionFailure as e:
    print(f"FATAL: MongoDB connection failed: {e}", file=sys.stderr); sys.stderr.flush()
    if mongo_client: mongo_client.close()
    exit(1)
except Exception as e:
    print(f"FATAL: An error occurred during initial MongoDB connection:", file=sys.stderr); sys.stderr.flush()
    traceback.print_exc(file=sys.stderr); sys.stderr.flush()
    if mongo_client: mongo_client.close()
    exit(1)

print("Starting data fetch loop..."); sys.stdout.flush()
while True:
    current_time_naive = datetime.datetime.now() # Get current time (naive, local assumed)
    print(f"\n--- Starting new fetch cycle at {current_time_naive} ---"); sys.stdout.flush()

    # --- Check if war has ended ---
    finish_time_dt = get_war_finish_time() # Fetch end time (returns naive datetime)
    if finish_time_dt:
        print(f"Fetched War Finish Time: {finish_time_dt}"); sys.stdout.flush()
        if current_time_naive >= finish_time_dt:
            print("War has ended. Stopping data collection."); sys.stdout.flush()
            break # Exit the while True loop
    else:
        # Optionally add a delay or different handling if end time fetch fails repeatedly
        print("Warning: Could not verify war end time. Continuing fetch cycle."); sys.stdout.flush()

    # --- Fetch and Insert Clan Data ---
    clans = fetch_clan_data()
    if clans:
        insert_clan_data(clans, mongo_client)
    else:
        print("Failed to retrieve clan data from the API this cycle."); sys.stdout.flush()

    # --- Wait for next cycle ---
    wait_seconds = 120
    print(f"--- Cycle complete. Waiting for {wait_seconds} seconds... ---"); sys.stdout.flush()
    time.sleep(wait_seconds)

# --- Loop exited (War Ended) ---
print("Data fetcher stopped.")
if mongo_client:
    mongo_client.close()
    print("MongoDB connection closed.")
exit(0) # Clean exit