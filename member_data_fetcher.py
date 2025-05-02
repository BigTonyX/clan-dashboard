import sys
import pymongo
import requests
import datetime
import time
import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
import traceback
import urllib3
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from logging.handlers import RotatingFileHandler

# Configure logging with rotation
log_file = 'member_fetcher.log'
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

# Disable SSL verification warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Create session with specific cipher configuration and longer timeouts
session = requests.Session()
session.verify = False
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive'
})

# Configure retry strategy
retry_strategy = Retry(
    total=5,
    backoff_factor=0.5,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Load environment variables
load_dotenv()

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
if not MONGO_CONNECTION_STRING:
    print("FATAL: MONGO_URI environment variable not set!", file=sys.stderr)
    sys.stderr.flush()
    exit(1)
DB_NAME = "clan_dashboard_db"

# --- API URLs ---
CLANS_API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"
WAR_END_API_URL = "https://ps99.biggamesapi.io/api/activeClanBattle"
CLAN_DETAILS_URL = "https://ps99.biggamesapi.io/api/clan/{}"

def make_request(url, timeout=30, method='GET', data=None):
    """Make a request using the session with retries and better error handling."""
    try:
        for attempt in range(3):  # Try up to 3 times
            try:
                if method.upper() == 'GET':
                    response = session.get(url, timeout=timeout)
                else:
                    response = session.post(url, json=data, timeout=timeout)
                
                response.raise_for_status()
                
                # Try to parse JSON response
                try:
                    return response.json()
                except json.JSONDecodeError as json_err:
                    print(f"Warning: Invalid JSON response from {url}: {json_err}")
                    print(f"Response content: {response.text[:200]}...")  # Print first 200 chars
                    if attempt < 2:  # If not the last attempt
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    raise
                
            except requests.exceptions.SSLError:
                print(f"SSL Error on attempt {attempt + 1} for {url}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise
            except requests.exceptions.Timeout:
                print(f"Timeout on attempt {attempt + 1} for {url}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
                    continue
                raise
                
    except Exception as e:
        print(f"Error making request to {url}: {e}", file=sys.stderr)
        raise

def get_war_finish_time():
    """Fetches war finish time and returns as a naive datetime object, or None."""
    try:
        raw_data = make_request(WAR_END_API_URL)
        
        if raw_data and isinstance(raw_data, dict):
            data = raw_data.get("data", {})
            if isinstance(data, dict):
                config_data = data.get("configData", {})
                if isinstance(config_data, dict) and "FinishTime" in config_data:
                    finish_time_unix = config_data["FinishTime"]
                    return datetime.datetime.fromtimestamp(finish_time_unix)
        
        print(f"Warning: Unexpected data structure from war end time API: {raw_data}", 
              file=sys.stderr)
        return None
    except Exception as e:
        print(f"Warning: Error fetching war end time: {e}", file=sys.stderr)
        return None

def get_top_clans(limit=2):
    """Fetches the top N clans from the Big Games API."""
    print(f"Fetching top {limit} clans..."); sys.stdout.flush()
    try:
        response = requests.get(CLANS_API_URL, timeout=15)
        response.raise_for_status()
        api_response = response.json()
        
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"][:limit]  # Only get top N clans
            print(f"Successfully found top {len(clan_list)} clans."); sys.stdout.flush()
            return clan_list
        else:
            print(f"API response invalid: {api_response}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"Error fetching top clans: {e}", file=sys.stderr)
        return None

def fetch_member_data(clan_name):
    """Fetches member data for a specific clan."""
    logger.info(f"Fetching member data for clan: {clan_name}")
    try:
        url = CLAN_DETAILS_URL.format(clan_name)
        clan_data = make_request(url)
        
        if clan_data["status"] != "ok" or "data" not in clan_data:
            logger.error(f"Invalid API response for clan {clan_name}: {clan_data.get('status', 'unknown status')}")
            return None
        
        battles = clan_data["data"].get("Battles", {})
        if not battles:
            logger.warning(f"No battles found for clan {clan_name}")
            return None
            
        battle_list = []
        for battle_id, battle_data in battles.items():
            if isinstance(battle_data, dict) and "PointContributions" in battle_data:
                battle_list.append({
                    'battle_id': battle_id,
                    'data': battle_data
                })
        
        if not battle_list:
            logger.warning(f"No valid battles found for clan {clan_name}")
            return None
            
        # Get the last battle (newest) from the list
        latest_battle = battle_list[-1]['data']
        latest_battle_id = battle_list[-1]['battle_id']
        
        logger.info(f"Found latest battle {latest_battle_id} for clan {clan_name}")
        
        return {
            "clan_name": clan_name,
            "battle_id": latest_battle_id,
            "is_active": not latest_battle.get("ProcessedAwards", True),
            "total_points": latest_battle.get("Points", 0),
            "members": latest_battle.get("PointContributions", []),
            "timestamp": datetime.datetime.now()
        }
        
    except Exception as e:
        logger.error(f"Error fetching member data for {clan_name}: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def store_member_data(member_data, mongo_client):
    """Stores member data in MongoDB."""
    if not member_data:
        logger.warning("Attempted to store empty member data")
        return False
        
    try:
        db = mongo_client[DB_NAME]
        members_collection = db["clan_members"]
        
        # Validate member data before storing
        if not all(key in member_data for key in ["clan_name", "battle_id", "members"]):
            logger.error("Invalid member data structure")
            return False
            
        result = members_collection.insert_one(member_data)
        logger.info(f"Successfully stored member data for {member_data['clan_name']} (battle: {member_data['battle_id']})")
        return True
        
    except pymongo.errors.ServerSelectionTimeoutError:
        logger.error("MongoDB server selection timeout - server may be down")
        return False
    except pymongo.errors.ConnectionFailure:
        logger.error("MongoDB connection failure")
        return False
    except Exception as e:
        logger.error(f"Error storing member data: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return False

def get_specific_clans():
    """Fetches data for NONG and NXNG clans."""
    print("Fetching NONG and NXNG clan data..."); sys.stdout.flush()
    try:
        api_response = make_request(CLANS_API_URL)
        
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"]
            target_clans = []
            
            # Find NONG and NXNG in the list
            for clan in clan_list:
                if clan.get("Name") in ["NONG", "NXNG"]:
                    target_clans.append(clan)
                    if len(target_clans) == 2:  # Found both clans
                        break
            
            print(f"Successfully found {len(target_clans)} target clans."); sys.stdout.flush()
            return target_clans
        else:
            print(f"API response invalid: {api_response}", file=sys.stderr)
            return None
    except Exception as e:
        print(f"Error fetching clans: {e}", file=sys.stderr)
        return None

def get_last_battle_id(mongo_client, clan_name):
    """Gets the most recent battle_id for a clan from MongoDB."""
    try:
        db = mongo_client[DB_NAME]
        members_collection = db["clan_members"]
        
        # Get the most recent record for this clan
        last_record = members_collection.find_one(
            {"clan_name": clan_name},
            sort=[("timestamp", -1)]
        )
        
        if last_record and "battle_id" in last_record:
            logger.info(f"Last recorded battle_id for {clan_name}: {last_record['battle_id']}")
            return last_record["battle_id"]
            
        logger.warning(f"No previous battle records found for clan {clan_name}")
        return None
        
    except pymongo.errors.ServerSelectionTimeoutError:
        logger.error("MongoDB server selection timeout while getting last battle ID")
        return None
    except Exception as e:
        logger.error(f"Error getting last battle_id: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None

def get_current_war_info():
    """Fetches current war timing and battle info from activeClanBattle API."""
    try:
        raw_data = make_request(WAR_END_API_URL)
        if raw_data and isinstance(raw_data, dict):
            data = raw_data.get("data", {})
            if isinstance(data, dict):
                config_data = data.get("configData", {})
                if isinstance(config_data, dict) and "FinishTime" in config_data and "StartTime" in config_data:
                    return {
                        "finish_time": datetime.datetime.fromtimestamp(config_data.get("FinishTime")),
                        "start_time": datetime.datetime.fromtimestamp(config_data.get("StartTime")),
                        "config_name": data.get("configName")  # This is the battle_id
                    }
        logger.error(f"Unexpected data structure from war API: {raw_data}")
        return None
    except Exception as e:
        logger.error(f"Error fetching war info: {e}")
        return None

def get_latest_battle_info(mongo_client):
    """Fetches the most recent battle info from battle_id_history."""
    try:
        db = mongo_client[DB_NAME]
        battle_collection = db["battle_id_history"]
        latest_battle = battle_collection.find_one(
            {},
            sort=[("timestamp", pymongo.DESCENDING)]
        )
        return latest_battle
    except Exception as e:
        logger.error(f"Error fetching latest battle info: {e}")
        return None

def is_valid_battle_data(member_data, current_war_info, latest_battle_info):
    """
    Validates if the member data is from the current active war and within war time bounds.
    """
    if not all([member_data, current_war_info]):
        return False

    current_time = datetime.datetime.now()
    
    # Check if we're within war time bounds
    if current_time > current_war_info["finish_time"]:
        logger.info("Current time is past war end time")
        return False
    
    if current_time < current_war_info["start_time"]:
        logger.info("Current time is before war start time")
        return False

    # Check battle_id matches
    member_battle_id = member_data.get("battle_id")
    current_battle_id = current_war_info.get("config_name")

    if member_battle_id != current_battle_id:
        logger.warning(f"Member battle ID {member_battle_id} doesn't match current war ID {current_battle_id}")
        return False

    if latest_battle_info is None:
        # First run, no battles recorded yet
        logger.info("No previous battles recorded, accepting new battle")
        return True

    latest_known_battle_id = latest_battle_info.get("battle_id")
    if member_battle_id != latest_known_battle_id:
        # This might be a new war that needs to be recorded
        logger.info(f"Detected potential new war: {member_battle_id}")
        # We'll handle new war recording separately
        return True

    return True

def store_new_battle(mongo_client, battle_id, start_time):
    """Records a new battle in the battle_id_history collection."""
    try:
        db = mongo_client[DB_NAME]
        battle_collection = db["battle_id_history"]
        
        # First, set all existing battles to is_current: false
        battle_collection.update_many(
            {},  # match all documents
            {"$set": {"is_current": False}}
        )
        
        # Then insert new battle with is_current: true
        battle_collection.insert_one({
            "battle_id": battle_id,
            "timestamp": start_time,
            "is_current": True
        })
        
        logger.info(f"Recorded new battle: {battle_id} as current battle")
        return True
    except Exception as e:
        logger.error(f"Error storing new battle: {e}")
        return False

def main(mongo_client=None, is_running=None):
    """Main execution function for the member data fetcher."""
    logger.info("Starting member data fetcher...")
    
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
                # Get current war information
                current_war_info = get_current_war_info()
                if not current_war_info:
                    logger.warning("No active war information available")
                    time.sleep(120)
                    continue

                # Get latest known battle
                latest_battle_info = get_latest_battle_info(mongo_client)
                
                # Fetch and store member data for NONG and NXNG
                target_clans = get_specific_clans()
                if not target_clans:
                    logger.error("Failed to fetch target clans")
                    time.sleep(120)
                    continue
                    
                for clan in target_clans:
                    clan_name = clan.get("Name")
                    if not clan_name:
                        continue
                        
                    member_data = fetch_member_data(clan_name)
                    if not member_data:
                        continue

                    # Validate the data
                    if is_valid_battle_data(member_data, current_war_info, latest_battle_info):
                        # If this is a new battle, record it
                        if latest_battle_info is None or member_data["battle_id"] != latest_battle_info.get("battle_id"):
                            store_new_battle(mongo_client, 
                                          member_data["battle_id"],
                                          current_war_info["start_time"])
                        
                        # Store the member data
                        if store_member_data(member_data, mongo_client):
                            logger.info(f"Successfully stored member data for {clan_name}")
                        else:
                            logger.error(f"Failed to store member data for {clan_name}")
                    else:
                        logger.info(f"Skipping invalid or expired data for {clan_name}")

                time.sleep(120)  # 2 minute wait between cycles

            except pymongo.errors.ServerSelectionTimeoutError:
                logger.error("MongoDB server selection timeout in main loop")
                time.sleep(120)
            except Exception as e:
                logger.error(f"Error in main loop: {str(e)}")
                logger.error(f"Traceback: {traceback.format_exc()}")
                time.sleep(120)
            
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
        logger.info("Member data fetcher stopped")

if __name__ == "__main__":
    main()