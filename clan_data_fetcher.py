import pymongo # Use MongoDB driver
import requests
import datetime
import time
import os

from dotenv import load_dotenv
load_dotenv()

# --- MongoDB Atlas Connection ---
# Load from environment variable for security
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
if not MONGO_CONNECTION_STRING:
     raise ValueError("MONGO_URI environment variable not set!")
DB_NAME = "clan_dashboard_db" # Keep this
DB_NAME = "clan_dashboard_db" # Choose a name for your database

# --- API Fetching --- (Function remains the same as before)
API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
def fetch_clan_data():
    """Fetches the top 250 clan data from the Big Games API."""
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

# --- MongoDB Insertion Logic ---
def insert_clan_data(clan_list, client): # Pass the MongoDB client
    """Inserts/Updates clan data into MongoDB Atlas."""
    if not clan_list:
        print("No clan data provided to insert.")
        return 0

    print(f"Processing insert/update for {len(clan_list)} clans into MongoDB...")
    db = client[DB_NAME] # Select the database
    clans_collection = db["clans"] # Select the time-series collection
    details_collection = db["clan_details"] # Select the details collection

    inserted_count = 0
    details_updated_count = 0
    processed_count = 0
    current_timestamp = datetime.datetime.now(datetime.timezone.utc) # Use timezone-aware UTC

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
            print(f"Skipping clan due to missing Name or Points: {clan}")
            continue

        try:
            # --- Handle 'clans' (time-series) collection ---
            # Check if clan exists to set first_seen (only needed once)
            # Note: This check adds overhead. A more advanced approach might use unique indexes.
            existing_clan_check = clans_collection.find_one({"clan_name": clan_name}, {"_id": 1}) # Check if any record exists

            clan_doc = {
                "clan_name": clan_name,
                "current_points": clan_points,
                "members": members_count,
                "timestamp": current_timestamp,
            }
            # Only add first_seen if this is the very first time we see the clan
            if not existing_clan_check:
                 clan_doc["first_seen"] = current_timestamp
                 print(f"Clan '{clan_name}' is new. Setting first_seen.")

            clans_collection.insert_one(clan_doc)
            inserted_count += 1

            # --- Handle 'clan_details' collection (Insert/Update) ---
            details_doc = {
                "icon": icon,
                "country_code": country_code,
                "member_capacity": capacity,
                "created_timestamp_api": created_api,
                "last_checked": current_timestamp
            }
            # Use update_one with upsert=True: inserts if clan_name doesn't exist, updates if it does.
            # $set operator ensures only the provided fields are updated.
            details_collection.update_one(
                {"clan_name": clan_name}, # Filter: find doc by clan_name
                {"$set": details_doc},     # Update: set these fields
                upsert=True                # Option: insert if not found
            )
            details_updated_count += 1 # Counts both inserts and updates here

        except Exception as e:
             print(f"Error processing/inserting data for clan '{clan_name}': {e}")
             continue # Continue with the next clan

    print(f"Processed {processed_count} clans. Inserted {inserted_count} docs into 'clans'. Upserted {details_updated_count} docs in 'clan_details'.")
    return inserted_count

# --- Main Execution Loop ---
print("Attempting initial connection to MongoDB Atlas...")
mongo_client = None
try:
    # Establish connection to MongoDB Atlas
    mongo_client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
    # The ismaster command is cheap and does not require auth.
    mongo_client.admin.command('ismaster')
    print("MongoDB Atlas connection successful!")
except pymongo.errors.ConnectionFailure as e:
    print(f"MongoDB connection failed: {e}")
    exit() # Exit if we can't connect initially
except Exception as e:
    print(f"An error occurred during initial MongoDB connection: {e}")
    exit()

print("Starting data fetch loop...")
while True:
    print(f"\n--- Starting new fetch cycle at {datetime.datetime.now()} ---")

    clans = fetch_clan_data() # Fetch data from game API

    if clans:
        print(f"Successfully retrieved {len(clans)} clan entries from the API.")
        # Insert data into MongoDB, passing the client object
        insert_clan_data(clans, mongo_client)
    else:
        print("Failed to retrieve clan data from the API this cycle.")

    # Wait for 2 minutes (120 seconds) before the next cycle
    wait_seconds = 120
    print(f"--- Cycle complete. Waiting for {wait_seconds} seconds... ---")
    time.sleep(wait_seconds)

# Close the client when the script theoretically ends (e.g., on Ctrl+C, though loop is infinite)
# This cleanup might not always run in a simple infinite loop script.
# Proper handling would involve try/finally or signal handling.
# if mongo_client:
#    mongo_client.close()
#    print("MongoDB connection closed.")