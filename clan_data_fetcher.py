import sys
import pymongo
import requests
import datetime
import time
import os
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
import traceback # Ensure traceback is imported
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure retry strategy
retry_strategy = Retry(
    total=5,
    backoff_factor=1.0,  # Increased backoff factor
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
    raise_on_status=False,
    respect_retry_after_header=True,
    connect=5,  # Number of retries for connection errors
    read=5,     # Number of retries for read errors
    redirect=5  # Number of retries for redirects
)

# Create session with specific cipher configuration and longer timeouts
session = requests.Session()
session.verify = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
})

# Configure adapter with connection pooling
adapter = HTTPAdapter(
    max_retries=retry_strategy,
    pool_maxsize=10,
    pool_connections=10,
    pool_block=False
)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Increase default timeout
session.timeout = 30  # 30 seconds default timeout

# Configure logging with rotation
log_file = 'clan_fetcher.log'
max_bytes = 10 * 1024 * 1024  # 10MB
backup_count = 5  # Keep 5 backup files

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            log_file,
            maxBytes=max_bytes,
            backupCount=backup_count
        )
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file for local execution
load_dotenv()

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
if not MONGO_CONNECTION_STRING:
    logger.error("FATAL: MONGO_URI environment variable not set!")
    sys.exit(1)
DB_NAME = "clan_dashboard_db"

# --- API URLs ---
CLANS_API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
WAR_END_API_URL = "https://ps99.biggamesapi.io/api/activeClanBattle"

# --- Helper Function to Get War Finish Time ---
def get_war_finish_time():
    """Fetches war finish time and returns as a naive datetime object, or None."""
    try:
        response = session.get(WAR_END_API_URL, timeout=30)
        response.raise_for_status()
        raw_data = response.json()
        if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and
            "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and
            "FinishTime" in raw_data["data"]["configData"]):
            finish_time_unix = raw_data["data"]["configData"]["FinishTime"]
            finish_time = datetime.datetime.fromtimestamp(finish_time_unix)
            # Validate the finish time
            current_time = datetime.datetime.now()
            max_future_days = 30  # Maximum days in the future we consider valid
            max_past_days = 30  # Maximum days in the past we consider valid
            if finish_time > current_time + datetime.timedelta(days=max_future_days):
                logger.warning(f"War finish time {finish_time} is too far in the future")
                return None
            if finish_time < current_time - datetime.timedelta(days=max_past_days):
                logger.warning(f"War finish time {finish_time} is too far in the past")
                return None
            return finish_time
        else:
            logger.warning(f"Unexpected data structure from war end time API: {raw_data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Could not fetch war end time: {e}")
        return None
    except Exception as e:
        logger.warning(f"Error processing war end time: {e}")
        return None

# --- API Fetching ---
def fetch_clan_data():
    """Fetches the top 250 clan data from the Big Games API."""
    logger.info(f"Attempting to fetch data from: {CLANS_API_URL}")
    try:
        response = session.get(CLANS_API_URL, timeout=15)
        response.raise_for_status()
        api_response = response.json()
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"]
            logger.info(f"Successfully parsed JSON. Found {len(clan_list)} clans.")
            return clan_list
        else:
            logger.error(f"API response status not 'ok' or 'data' key missing: {api_response}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"An error occurred fetching data from the API: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred during API fetch/parse: {e}")
        return None

def get_current_battle_info(client):
    """Get the current active battle from battle_id_history."""
    try:
        db = client[DB_NAME]
        current_battle = db["battle_id_history"].find_one({"is_current": True})
        if current_battle:
            print(f"Found current battle: {current_battle['battle_id']}")
        else:
            print("No current battle found")
        return current_battle
    except Exception as e:
        print(f"Error getting current battle: {e}", file=sys.stderr)
        return None

def get_nong_last_points(client, battle_id=None):
    """Get NONG's last recorded points, optionally for a specific battle."""
    try:
        db = client[DB_NAME]
        query = {"clan_name": "NONG"}
        if battle_id:
            query["battle_id"] = battle_id
        
        last_record = db["clans"].find_one(
            query,
            sort=[("timestamp", pymongo.DESCENDING)]
        )
        
        if last_record:
            print(f"Last recorded points for NONG: {last_record['current_points']}")
            return last_record["current_points"]
        return None
    except Exception as e:
        print(f"Error getting NONG's last points: {e}", file=sys.stderr)
        return None

def get_nong_current_points(clan_list):
    """Get NONG's current points from the API data."""
    try:
        nong_data = next(
            (clan for clan in clan_list if clan.get("Name") == "NONG"),
            None
        )
        if nong_data:
            print(f"Current NONG points from API: {nong_data.get('Points')}")
            return nong_data.get("Points")
        print("NONG not found in top 250 clans")
        return None
    except Exception as e:
        print(f"Error getting NONG's current points: {e}", file=sys.stderr)
        return None

def get_war_timing():
    """Get current war timing from war status API."""
    try:
        response = session.get(WAR_END_API_URL, timeout=30)  # Use session with increased timeout
        response.raise_for_status()
        raw_data = response.json()
        
        if ("data" in raw_data and 
            isinstance(raw_data.get("data"), dict) and
            "configData" in raw_data["data"]):
            
            config = raw_data["data"]["configData"]
            start_time = datetime.datetime.fromtimestamp(config["StartTime"])
            finish_time = datetime.datetime.fromtimestamp(config["FinishTime"])
            
            # Validate the times
            current_time = datetime.datetime.now()
            max_future_days = 30  # Maximum days in the future we consider valid
            max_past_days = 30  # Maximum days in the past we consider valid
            
            if finish_time > current_time + datetime.timedelta(days=max_future_days):
                logger.warning(f"War finish time {finish_time} is too far in the future")
                return None
            if start_time < current_time - datetime.timedelta(days=max_past_days):
                logger.warning(f"War start time {start_time} is too far in the past")
                return None
                
            return {
                "start_time": start_time,
                "finish_time": finish_time
            }
        else:
            logger.warning(f"Unexpected data structure from war API: {raw_data}")
            return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Error getting war timing: {e}")
        return None
    except Exception as e:
        logger.warning(f"Error getting war timing: {e}")
        return None

def should_collect_clan_data(client, clan_list):
    """Determine if we should collect clan data based on current state."""
    try:
        # 1. Check war timing boundaries first
        war_timing = get_war_timing()
        if not war_timing:
            print("Could not get war timing")
            return False, None
            
        current_time = datetime.datetime.now()
        if current_time < war_timing["start_time"] or current_time > war_timing["finish_time"]:
            print("Outside war time boundaries")
            return False, None

        # 2. Check if there's an active war in battle_id_history
        current_battle = get_current_battle_info(client)
        if not current_battle:
            print("No active war in battle_id_history")
            return False, None
            
        # 3. Get NONG's current points from API
        nong_current_points = get_nong_current_points(clan_list)
        if not nong_current_points:
            return False, None
            
        # 4. Get NONG's last stored points from the previous war
        last_points = get_nong_last_points(client)
        
        if not last_points:
            # First ever data point
            print("No previous points found - first data collection")
            return True, current_battle["battle_id"]
        
        # 5. Get last battle_id for NONG
        last_battle_points = get_nong_last_points(client, current_battle["battle_id"])
        
        # If we haven't started collecting for this battle yet, check if points have changed significantly
        if not last_battle_points:
            margin = last_points * 0.10  # 10% margin
            points_difference = abs(nong_current_points - last_points)
            
            if points_difference > margin:
                print(f"Points changed significantly (diff: {points_difference}, margin: {margin}) - new war detected")
                return True, current_battle["battle_id"]
            else:
                print(f"Points haven't changed enough (diff: {points_difference}, margin: {margin}) - API still showing previous war data")
                return False, None
        
        # If we're already collecting for this battle, continue collecting
        print("Continuing data collection for current battle")
        return True, current_battle["battle_id"]
        
    except Exception as e:
        print(f"Error in should_collect_clan_data: {e}", file=sys.stderr)
        return False, None

# --- MongoDB Insertion Logic ---
def insert_clan_data(clan_list, client, battle_id):
    """Inserts/Updates clan data into MongoDB Atlas."""
    if not clan_list:
        print("No clan data provided to insert.")
        return 0
        
    db = client[DB_NAME]
    clans_collection = db["clans"]
    details_collection = db["clan_details"]
    inserted_count = 0
    details_processed_count = 0
    processed_count = 0
    current_timestamp_utc = datetime.datetime.now(datetime.timezone.utc)

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
            continue

        # Insert into 'clans' collection with battle_id
        try:
            clan_doc = {
                "clan_name": clan_name,
                "current_points": clan_points,
                "members": members_count,
                "timestamp": current_timestamp_utc,
                "battle_id": battle_id  # Add battle_id
            }
            
            insert_result = clans_collection.insert_one(clan_doc)
            if insert_result.acknowledged:
                inserted_count += 1
                
        except Exception as e_insert:
            print(f"EXCEPTION during insert into clans for {clan_name}: {e_insert}", file=sys.stderr)
            continue

        # Update 'clan_details' collection (unchanged)
        try:
            details_doc = {
                "icon": icon,
                "country_code": country_code,
                "member_capacity": capacity,
                "created_timestamp_api": created_api,
                "last_checked": current_timestamp_utc
            }
            update_result = details_collection.update_one(
                {"clan_name": clan_name},
                {"$set": details_doc},
                upsert=True
            )
            details_processed_count += 1
        except Exception as e_update:
            print(f"Error processing/updating details for clan '{clan_name}': {e_update}", file=sys.stderr)

    print(f"Processed {processed_count} clans. Successful inserts into 'clans': {inserted_count}. Attempted upserts into 'clan_details': {details_processed_count}.")
    return inserted_count

# --- Main Execution ---
def main(mongo_client=None, is_running=None):
    """Main execution function for the clan data fetcher."""
    logger.info("Starting clan data fetcher...")
    # Use provided MongoDB connection or create new one
    if not mongo_client:
        try:
            mongo_client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000)
            mongo_client.admin.command('ping')
            logger.info("MongoDB connection successful!")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return
    try:
        while is_running is None or is_running():
            try:
                current_time_naive = datetime.datetime.now()
                logger.info(f"Starting new fetch cycle at {current_time_naive}")
                # Check if war has ended
                finish_time_dt = get_war_finish_time()
                if finish_time_dt:
                    logger.info(f"Fetched War Finish Time: {finish_time_dt}")
                    if current_time_naive >= finish_time_dt:
                        logger.info("War has ended. Stopping data collection.")
                        return
                else:
                    logger.warning("Could not verify war end time. Continuing fetch cycle.")
                # Fetch and Insert Clan Data
                clans = fetch_clan_data()
                if clans:
                    # Check if we should collect data
                    should_collect, battle_id = should_collect_clan_data(mongo_client, clans)
                    if should_collect and battle_id:
                        insert_clan_data(clans, mongo_client, battle_id)
                    else:
                        logger.info("Skipping data collection this cycle")
                else:
                    logger.warning("Failed to retrieve clan data from the API this cycle.")
                # Wait for next cycle
                wait_seconds = 120
                logger.info(f"Cycle complete. Waiting for {wait_seconds} seconds...")
                time.sleep(wait_seconds)
            except requests.exceptions.RequestException as e:
                logger.error(f"Network error in fetch cycle: {e}")
                time.sleep(60)  # Wait longer on network errors
                continue
            except Exception as e:
                logger.error(f"Error in fetch cycle: {e}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                time.sleep(60)  # Wait longer on other errors
                continue
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt. Shutting down...")
    except Exception as e:
        logger.error(f"Fatal error in main program: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        # Only close the connection if we created it
        if mongo_client and not is_running:
            mongo_client.close()
            logger.info("MongoDB connection closed")
        logger.info("Clan data fetcher stopped")

if __name__ == "__main__":
    main()