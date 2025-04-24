import sys
import pymongo
import requests
import datetime
import time
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection

# Load environment variables from .env file for local execution
load_dotenv()

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
if not MONGO_CONNECTION_STRING:
    print("FATAL: MONGO_URI environment variable not set!", file=sys.stderr)
    exit(1)
DB_NAME = "clan_dashboard_db"

# --- API Fetching --- (Function remains the same)
API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
def fetch_clan_data():
    """Fetches the top 250 clan data from the Big Games API."""
    # Keep this print to see fetch attempts
    print(f"Attempting to fetch data from: {API_URL}")
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status()
        api_response = response.json()
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"]
            # Keep this print to see success count
            print(f"Successfully parsed JSON. Found {len(clan_list)} clans.")
            return clan_list
        else:
            print(f"API response status not 'ok' or 'data' key missing: {api_response}", file=sys.stderr)
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred fetching data from the API: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API fetch/parse: {e}", file=sys.stderr)
        return None


# --- MongoDB Insertion Logic ---
def insert_clan_data(clan_list, client):
    """Inserts/Updates clan data into MongoDB Atlas."""
    if not clan_list:
        print("No clan data provided to insert.")
        return 0

    # Keep summary print
    print(f"Processing insert/update for {len(clan_list)} clans into MongoDB...")
    db = client[DB_NAME]
    clans_collection = db["clans"]
    details_collection = db["clan_details"]
    inserted_count = 0
    details_processed_count = 0
    processed_count = 0
    current_timestamp = datetime.datetime.now(datetime.timezone.utc)

    for clan in clan_list:
        processed_count += 1
        clan_name = clan.get("Name")
        clan_points = clan.get("Points")
        members_count = clan.get("Members")
        icon = clan.get("Icon")
        country_code = clan.get("CountryCode")
        capacity = clan.get("MemberCapacity")
        created_api = clan.get("Created")

        if clan_name is None or clan_points is None:
            # Keep skip message for reference if data quality issues arise
            print(f"Skipping clan due to missing Name or Points: {clan}")
            continue

        # --- Insert into 'clans' collection ---
        try:
            existing_clan_check = clans_collection.find_one({"clan_name": clan_name}, {"_id": 1})
            clan_doc = { "clan_name": clan_name, "current_points": clan_points, "members": members_count, "timestamp": current_timestamp,}
            if not existing_clan_check: clan_doc["first_seen"] = current_timestamp
            insert_result = clans_collection.insert_one(clan_doc)
            if insert_result.acknowledged: inserted_count += 1
            # Removed per-clan insert logging
        except Exception as e_insert:
            print(f"EXCEPTION during insert into clans for {clan_name}: {e_insert}", file=sys.stderr)
            pass # Continue processing loop

        # --- Update 'clan_details' collection ---
        try:
            details_doc = {"icon": icon, "country_code": country_code, "member_capacity": capacity, "created_timestamp_api": created_api, "last_checked": current_timestamp}
            update_result = details_collection.update_one({"clan_name": clan_name}, {"$set": details_doc}, upsert=True)
            details_processed_count += 1
        except Exception as e_update:
             print(f"Error processing/updating details for clan '{clan_name}': {e_update}", file=sys.stderr)

    # Keep final summary print
    print(f"Processed {processed_count} clans. Successful inserts into 'clans': {inserted_count}. Attempted upserts into 'clan_details': {details_processed_count}.")
    return inserted_count

# --- Main Execution Loop ---
print("Attempting initial MongoDB Atlas connection...") # Keep initial connection message
mongo_client = None
try:
    mongo_client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=10000)
    mongo_client.admin.command('ping')
    print("MongoDB Atlas connection successful!") # Keep success message
except pymongo.errors.ConnectionFailure as e:
    print(f"FATAL: MongoDB connection failed: {e}", file=sys.stderr)
    if mongo_client: mongo_client.close()
    exit(1)
except Exception as e:
    print(f"FATAL: An error occurred during initial MongoDB connection:", file=sys.stderr)
    import traceback; traceback.print_exc(file=sys.stderr)
    if mongo_client: mongo_client.close()
    exit(1)

print("Starting data fetch loop...") # Keep start message
while True:
    print(f"\n--- Starting new fetch cycle at {datetime.datetime.now()} ---") # Keep cycle start
    clans = fetch_clan_data()
    if clans:
        insert_clan_data(clans, mongo_client)
    else:
        print("Failed to retrieve clan data from the API this cycle.")

    wait_seconds = 120
    # Keep cycle end message
    print(f"--- Cycle complete. Waiting for {wait_seconds} seconds... ---")
    time.sleep(wait_seconds)