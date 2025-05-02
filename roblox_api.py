import requests
import time
from typing import Dict, Optional, List
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import urllib3
import json
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pymongo.operations import UpdateOne

load_dotenv()

ROBLOX_API_BASE = "https://users.roblox.com/v1/users/"
ROBLOX_BATCH_API = "https://users.roblox.com/v1/users"
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = "clan_dashboard_db"
CACHE_DURATION = 24 * 60 * 60  # 24 hours in seconds

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

# In-memory cache
username_cache: Dict[str, Dict] = {}

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
        print(f"Error making request to {url}: {e}")
        raise

def get_user_data_batch(user_ids: List[str], mongo_client: Optional[MongoClient] = None) -> Dict[str, Dict]:
    """
    Get Roblox user data in batches with caching. Priority:
    1. Check in-memory cache
    2. Check MongoDB cache
    3. Fetch from Roblox API in batches
    """
    result = {}
    uncached_ids = []
    current_time = time.time()
    
    # Create MongoDB client if not provided
    should_close_client = False
    if not mongo_client:
        mongo_client = MongoClient(MONGO_CONNECTION_STRING)
        should_close_client = True

    try:
        db = mongo_client[DB_NAME]
        username_cache_collection = db["username_cache"]

        # 1. Check in-memory cache first
        for user_id in user_ids:
            if user_id in username_cache:
                cached_user = username_cache[user_id]
                if cached_user["name"] != "Unknown":
                    result[user_id] = cached_user
                    continue
            uncached_ids.append(user_id)

        if not uncached_ids:
            return result

        # 2. Check MongoDB cache for remaining IDs
        mongo_cached = username_cache_collection.find({
            "user_id": {"$in": uncached_ids},
            "last_updated": {"$gt": current_time - CACHE_DURATION}
        })

        remaining_ids = set(uncached_ids)
        for cached_data in mongo_cached:
            user_id = cached_data["user_id"]
            if cached_data.get("name") != "Unknown":
                user_info = {
                    "name": cached_data["name"],
                    "display_name": cached_data["display_name"]
                }
                result[user_id] = user_info
                username_cache[user_id] = user_info  # Update in-memory cache
                remaining_ids.remove(user_id)

        if not remaining_ids:
            return result

        # 3. Fetch remaining IDs from Roblox API in batches
        remaining_ids = list(remaining_ids)
        batch_size = 100  # Roblox API limit
        
        for i in range(0, len(remaining_ids), batch_size):
            batch = remaining_ids[i:i + batch_size]
            try:
                response_data = make_request(
                    ROBLOX_BATCH_API,
                    method='POST',
                    data={"userIds": batch},
                    timeout=30
                )
                
                if not response_data or "data" not in response_data:
                    print(f"Warning: Invalid response format for batch {i}")
                    continue

                # Process batch results
                batch_updates = []
                for user_data in response_data.get("data", []):
                    user_id = str(user_data["id"])
                    user_info = {
                        "name": user_data.get("name", "Unknown"),
                        "display_name": user_data.get("displayName", "Unknown")
                    }
                    
                    if user_info["name"] != "Unknown":
                        result[user_id] = user_info
                        username_cache[user_id] = user_info
                        
                        # Prepare MongoDB update
                        batch_updates.append(
                            UpdateOne(
                                {"user_id": user_id},
                                {
                                    "$set": {
                                        "user_id": user_id,
                                        "name": user_info["name"],
                                        "display_name": user_info["display_name"],
                                        "last_updated": current_time
                                    }
                                },
                                upsert=True
                            )
                        )

                # Bulk update MongoDB cache
                if batch_updates:
                    username_cache_collection.bulk_write(batch_updates)

            except Exception as e:
                print(f"Error fetching batch user data: {e}")
                # For failed batch, set all as Unknown
                for user_id in batch:
                    if user_id not in result:
                        result[user_id] = {"name": "Unknown", "display_name": "Unknown"}

        return result

    finally:
        if should_close_client and mongo_client:
            mongo_client.close()

def get_user_data(user_id: str, mongo_client: Optional[MongoClient] = None) -> Dict:
    """
    Get single user data, using the batch function for consistency
    """
    results = get_user_data_batch([user_id], mongo_client)
    return results.get(user_id, {"name": "Unknown", "display_name": "Unknown"})

def get_usernames_batch(user_ids: List[str], mongo_client: Optional[MongoClient] = None) -> Dict[str, Dict]:
    """
    Get usernames for multiple user IDs efficiently using the batch API
    """
    return get_user_data_batch(user_ids, mongo_client) 