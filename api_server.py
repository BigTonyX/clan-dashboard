from fastapi import FastAPI, HTTPException # Make sure HTTPException is added
from typing import List, Optional # Add this
from fastapi import FastAPI, HTTPException, Query # Add Query here
import requests                     # Needed to call the external API
import datetime                     # Needed for time calculations
import time                         # Needed to get the current time easily
import pymongo
import sys
import os
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
from pymongo.collection import Collection

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = "clan_dashboard_db" # Use the same database name as in the fetcher

# Create a global MongoDB client with connection pooling
mongo_client = MongoClient(
    MONGO_CONNECTION_STRING,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=50,  # Maximum number of connections in the pool
    minPoolSize=10,  # Minimum number of connections in the pool
    maxIdleTimeMS=30000,  # Close idle connections after 30 seconds
    connectTimeoutMS=5000,  # Timeout for initial connection
    socketTimeoutMS=5000,  # Timeout for operations
    retryWrites=True,
    retryReads=True
)

# Create the FastAPI app instance
app = FastAPI(
    title="Clan Dashboard API",
    version="0.1.0",
    docs_url=None,
    redoc_url=None
)

# --- Create Indexes ---
try:
    db = mongo_client[DB_NAME]
    clans_collection = db["clans"]
    # Create compound index on battle_id and timestamp
    clans_collection.create_index([("battle_id", pymongo.ASCENDING), ("timestamp", pymongo.DESCENDING)])
    print("Created compound index on battle_id and timestamp")
except Exception as e:
    print(f"Error creating indexes: {e}")

# --- Global icon cache ---
ICON_CACHE = {}

# Define a basic 'root' endpoint
@app.get("/")
async def read_root():
    """Basic endpoint to check if the API is running."""
    return {"message": "Welcome to the Clan Dashboard API!"}

# --- Helper Function ---
def format_timedelta(delta):
    """Formats a timedelta object into 'Xd Yh Zm' string."""
    if delta.total_seconds() < 0:
        return "Ended" # Or handle as needed if time has passed

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    # Always show minutes if >= 0, handle cases near zero
    if minutes >= 0:
        if days > 0 or hours > 0 or minutes > 0: # Add minutes if > 0 OR if days/hours exist
             parts.append(f"{minutes}m")
        elif delta.total_seconds() > 0: # If delta > 0 and days/hours/mins are 0, show "0m"
            parts.append("0m")

    # If parts is still empty after checks (e.g., very small positive duration), return "0m"
    return " ".join(parts) if parts else "0m"

# Countdown endpoint
@app.get("/countdown")
async def get_countdown():
    """Fetches the war end time and returns the formatted countdown."""
    countdown_url = "https://ps99.biggamesapi.io/api/activeClanBattle"
    try:
        response = requests.get(countdown_url, timeout=5, verify=False)
        response.raise_for_status()
        raw_data = response.json()
        
        if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and
            "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and
            "FinishTime" in raw_data["data"]["configData"]):
            
            finish_time_unix = raw_data["data"]["configData"]["FinishTime"]
            finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix)
            now_dt = datetime.datetime.now()
            remaining_delta = finish_time_dt - now_dt
            
            countdown_str = format_timedelta(remaining_delta)
            return {"countdown": countdown_str}
            
    except requests.exceptions.SSLError as e:
        print(f"SSL Error fetching countdown data: {e}")
        return {"countdown": "Unknown"}
    except requests.exceptions.RequestException as e:
        print(f"Error fetching countdown data: {e}")
        return {"countdown": "Unknown"}
    except Exception as e:
        print(f"Unexpected error in countdown: {e}")
        return {"countdown": "Unknown"}

# Dashboard endpoint (OPTIMIZED)
@app.get("/dashboard")
async def get_dashboard_data(battle_id: str):
    """
    Returns the latest leaderboard snapshot for the given battle_id.
    """
    db = mongo_client[DB_NAME]
    snapshots_collection = db["leaderboard_snapshots"]
    snapshot = snapshots_collection.find_one(
        {"battle_id": battle_id},
        sort=[("timestamp", -1)]
    )
    if not snapshot or "top_clans" not in snapshot:
        return []
    
    # Get top 25 clan names
    top_25_clan_names = [clan['clan_name'] for clan in snapshot["top_clans"][:25]]
    
    # --- Lazy-load icon cache for top 25 clans ---
    clan_details_collection = db["clan_details"]
    icons_to_fetch = [name for name in top_25_clan_names if name not in ICON_CACHE]
    if icons_to_fetch:
        for doc in clan_details_collection.find({"clan_name": {"$in": icons_to_fetch}}, {"clan_name": 1, "icon": 1, "_id": 0}):
            ICON_CACHE[doc['clan_name']] = doc.get('icon')
    
    # Add icons to the response
    for clan in snapshot["top_clans"]:
        clan['icon'] = ICON_CACHE.get(clan['clan_name'])
    
    return snapshot["top_clans"]

# Endpoint to calculate needs for a specific clan to reach a target rank (OPTIMIZED & CORRECTED)
@app.get("/clan_reach_target")
async def get_clan_reach_target(clan_name: str, target_rank: int, battle_id: str, forecast_period: int = 360):
    """ Calculates extra points per hour using MongoDB, optimized with bulk queries. """
    print(f"/clan_reach_target called for {clan_name}, target_rank={target_rank}, forecast_period={forecast_period}, battle_id={battle_id}")

    # --- Input Validation ---
    if target_rank <= 0 or target_rank > 250: raise HTTPException(status_code=400, detail="Invalid target_rank.")
    if forecast_period <= 0: raise HTTPException(status_code=400, detail="Invalid forecast_period.")

    client = None; minutes_remaining = 0; war_finish_time_dt = None; extra_points_per_hour = None

    try:
        # --- Fetch War End Time ---
        try:
            countdown_url = "https://ps99.biggamesapi.io/api/activeClanBattle"
            response = requests.get(countdown_url, timeout=5, verify=False)
            response.raise_for_status()
            raw_data = response.json()
            # Determine live battle ID from returned configName
            live_battle_id = raw_data.get("data", {}).get("configName")
            if live_battle_id != battle_id:
                # Not the requested battle, treat as ended
                print(f"Active battle ({live_battle_id}) != requested ({battle_id}); treating as over")
                minutes_remaining = 0
            else:
                # Existing logic: use FinishTime for current war
                finish_time_unix = raw_data["data"]["configData"]["FinishTime"]
                war_finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix)
                remaining_delta = war_finish_time_dt - datetime.datetime.now()
                if remaining_delta.total_seconds() > 0:
                    minutes_remaining = remaining_delta.total_seconds() / 60
                else:
                    minutes_remaining = 0  # War ended between fetch and now
                print(f"War ends at: {war_finish_time_dt}, Minutes remaining: {minutes_remaining:.2f}")
        except requests.exceptions.SSLError as e:
            print(f"SSL Error fetching war end time: {e}")
            minutes_remaining = 0
        except Exception as cd_err:
            print(f"Error fetching war end time: {cd_err}")
            minutes_remaining = 0

        # --- Connect to MongoDB ---
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000); db = client[DB_NAME]; clans_collection = db["clans"]; client.admin.command('ping'); print("MongoDB connection successful.")

        # === Query 1: Get Latest Data ===
        latest_doc_fetch = clans_collection.find_one(
            {"battle_id": battle_id},
            sort=[("timestamp", pymongo.DESCENDING)]
        );
        if not latest_doc_fetch or not latest_doc_fetch.get('timestamp'): 
            raise HTTPException(status_code=503, detail="No current data.")
        latest_ts_dt = latest_doc_fetch['timestamp']
        query_latest = {
            "timestamp": latest_ts_dt,
            "battle_id": battle_id
        }
        latest_docs_cursor = clans_collection.find(
            query_latest, 
            {"_id":0, "clan_name":1, "current_points":1, "timestamp":1}
        ).sort("current_points", pymongo.DESCENDING).limit(250);
        ranked_latest_map = {doc['clan_name']: doc for doc in latest_docs_cursor} # Store as map keyed by name
        if not ranked_latest_map: raise HTTPException(status_code=503, detail="No current clan data available.")
        top_clan_names = list(ranked_latest_map.keys()) # Get names

        # --- War over check moved here ---
        if minutes_remaining <= 0:
            # War is over: determine final rank of the clan
            sorted_clans = sorted(ranked_latest_map.values(), key=lambda x: x['current_points'], reverse=True)
            final_rank = None
            for idx, clan in enumerate(sorted_clans, 1):
                if clan['clan_name'] == clan_name:
                    final_rank = idx
                    break
            return {"extra_points_per_hour": None, "final_rank": final_rank}

        hours_remaining = minutes_remaining / 60.0

        user_clan_current_info = ranked_latest_map.get(clan_name)
        if not user_clan_current_info: raise HTTPException(status_code=404, detail=f"Clan '{clan_name}' not found.")

        # === OPTIMIZATION: Bulk Queries for History ===
        # --- Bulk Query for First Seen Timestamps ---
        print("Executing bulk query for first_seen (using timestamp) data...")
        pipeline_first_seen = [
            {'$match': {'clan_name': {'$in': top_clan_names}, 'battle_id': battle_id}},
            {'$sort': {'timestamp': pymongo.ASCENDING}},
            {'$group': {'_id': '$clan_name', 'first_seen_ts': {'$first': '$timestamp'}}}
        ]
        print(f"First seen pipeline (by timestamp): {pipeline_first_seen}")
        first_seen_map = {row['_id']: row['first_seen_ts'] for row in clans_collection.aggregate(pipeline_first_seen)}
        print(f"Found first_seen data for {len(first_seen_map)} clans.")

        # --- Bulk Query for Past Forecast Data ---
        print(f"Executing bulk query for past forecast data (period={forecast_period} min)...")
        target_forecast_past_dt = latest_ts_dt - datetime.timedelta(minutes=forecast_period)
        pipeline_past_forecast = [ {'$match': { 'clan_name': {'$in': top_clan_names}, 'timestamp': {'$lte': target_forecast_past_dt}, 'battle_id': battle_id }}, {'$sort': {'timestamp': pymongo.DESCENDING}}, {'$group': { '_id': '$clan_name', 'past_points_forecast': {'$first': '$current_points'} }} ]
        past_data_map_forecast = {row['_id']: row['past_points_forecast'] for row in clans_collection.aggregate(pipeline_past_forecast)}
        print(f"Found past forecast data for {len(past_data_map_forecast)} clans.")

        # === Calculate Projections In Python ===
        print("Calculating projections...")
        projections = {} # clan_name -> projected_score
        projection_eligibility = {} # clan_name -> bool (has_6h_data)
        six_hours_ago = latest_ts_dt - datetime.timedelta(hours=6)

        for c_name in top_clan_names:
            current_info = ranked_latest_map[c_name]
            current_points = current_info['current_points']
            projected_points = None # Default
            has_6h_data = False

            # Check 6h rule using pre-fetched first_seen data
            first_seen_dt = first_seen_map.get(c_name)
            if first_seen_dt and isinstance(first_seen_dt, datetime.datetime):
                # Compare naively, assuming latest_ts_dt is also naive or compatible
                if first_seen_dt.replace(tzinfo=None) <= six_hours_ago.replace(tzinfo=None):
                    has_6h_data = True
            projection_eligibility[c_name] = has_6h_data

            # Calculate projection if eligible and past data exists
            if has_6h_data:
                past_points = past_data_map_forecast.get(c_name)
                if past_points is not None:
                     forecast_gain = current_points - past_points
                     if forecast_period > 0:
                         gain_rate_per_minute = forecast_gain / forecast_period
                         projected_points = current_points + (gain_rate_per_minute * minutes_remaining)

            # Store projection score (or current points if ineligible/no projection)
            projections[c_name] = projected_points if projected_points is not None else current_points

        # === Determine Target Score ===
        # Sort by projection score to find the clan at the target rank
        # Need info including name and projection score for sorting
        projection_list_for_sort = [{'clan_name': cn, 'score': projections[cn]} for cn in top_clan_names]
        projected_ranked_list = sorted(projection_list_for_sort, key=lambda x: x['score'], reverse=True)

        if target_rank > len(projected_ranked_list):
            raise HTTPException(status_code=400, detail=f"Target rank {target_rank} out of range.")

        target_rank_clan_name = projected_ranked_list[target_rank - 1]['clan_name']
        target_rank_projected_score = projections[target_rank_clan_name]

        # Check eligibility of user clan AND target rank clan
        user_clan_eligible = projection_eligibility.get(clan_name, False)
        target_clan_eligible = projection_eligibility.get(target_rank_clan_name, False)
        all_projections_valid = user_clan_eligible and target_clan_eligible

        # === Calculate Extra Points ===
        user_clan_projected_score = projections[clan_name]
        score_difference = target_rank_projected_score - user_clan_projected_score

        if not all_projections_valid:
            print("Calculation not possible due to projection ineligibility.")
            extra_points_per_hour = None # Remains None
        elif score_difference <= 0:
            extra_points_per_hour = 0
        else:
            if hours_remaining <= 0: extra_points_per_hour = float('inf')
            else: extra_points_per_hour = score_difference / hours_remaining

    # --- Error Handling & Connection Closing ---
    except pymongo.errors.ConnectionFailure as e: print(f"MongoDB connection error: {e}"); raise HTTPException(status_code=503, detail="DB connection error.")
    except HTTPException: raise
    except Exception as e: print(f"Unexpected error: {e}"); import traceback; traceback.print_exc(); raise HTTPException(status_code=500, detail=f"Internal error: {e}")
    finally:
        if client: client.close(); print("MongoDB connection closed for /clan_reach_target.")

    # Format final result
    if extra_points_per_hour is None: result_value = None
    elif extra_points_per_hour == float('inf'): result_value = "Infinity"
    else: result_value = round(extra_points_per_hour)
    return {"extra_points_per_hour": result_value }

# Endpoint to get historical data for comparing clans
@app.get("/clan_comparison")
async def get_clan_comparison(
    battle_id: str,
    clan_names: List[str] = Query(..., min_length=1, max_length=3, title="Clan Names", description="List of 1 to 3 clan names to compare."),
    time_period: int = Query(60, gt=0, title="Time Period (minutes)", description="Lookback period in minutes.")
):
    """ Fetches historical point data from MongoDB for clan comparison. """
    print(f"/clan_comparison called for clans: {clan_names}, time_period: {time_period}, battle_id: {battle_id}")

    client = None # Initialize client variable
    comparison_data = []

    try:
        # Connect to MongoDB Atlas
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        clans_collection = db["clans"] # Use the time-series collection

        # Calculate start timestamp (use timezone-aware UTC)
        now_dt_utc = datetime.datetime.now(datetime.timezone.utc)
        start_dt_utc = now_dt_utc - datetime.timedelta(minutes=time_period)
        print(f"Fetching comparison data from: {start_dt_utc}")

        # Construct MongoDB query
        query_filter = {
            "clan_name": {"$in": clan_names}, # Match clans in the list
            "timestamp": {"$gte": start_dt_utc}, # Match timestamps within the period
            "battle_id": battle_id # Match the specific battle
        }
        # Define projection to return only necessary fields + exclude MongoDB's _id
        projection = {
            "_id": 0, # Exclude the default MongoDB ID
            "clan_name": 1,
            "timestamp": 1,
            "current_points": 1
        }
        # Define sort order
        sort_order = [("clan_name", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING)]

        print(f"Executing comparison query for {len(clan_names)} clans...")
        cursor = clans_collection.find(query_filter, projection).sort(sort_order)

        # Convert results to list
        # Need to convert datetime objects back to ISO strings for JSON response
        raw_results = list(cursor)
        for doc in raw_results:
             # Ensure timestamp is JSON serializable (ISO format string)
             if isinstance(doc.get('timestamp'), datetime.datetime):
                  doc['timestamp'] = doc['timestamp'].isoformat()
             comparison_data.append(doc)

        print(f"Query returned {len(comparison_data)} comparison data points.")

    except pymongo.errors.ConnectionFailure as e:
         print(f"MongoDB connection error in /clan_comparison: {e}")
         raise HTTPException(status_code=503, detail="Database connection error.")
    except Exception as e:
        print(f"Unexpected error in /clan_comparison: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error in comparison endpoint: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed for /clan_comparison.")

    return comparison_data

# New endpoint to fetch battle IDs
@app.get("/battle_ids")
async def get_battle_ids():
    """Fetches all battle IDs from the battle_id_history collection, sorted by timestamp."""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        battle_id_collection = db["battle_id_history"]

        # Fetch all battle IDs, sorted by timestamp in descending order
        battle_ids = list(battle_id_collection.find(
            {},
            {"_id": 0, "battle_id": 1, "timestamp": 1}
        ).sort("timestamp", pymongo.DESCENDING))

        # Convert timestamps to ISO format strings
        for record in battle_ids:
            if "timestamp" in record and isinstance(record["timestamp"], datetime.datetime):
                record["timestamp"] = record["timestamp"].isoformat()

        return battle_ids

    except Exception as e:
        print(f"Error fetching battle IDs: {e}")
        raise HTTPException(status_code=500, detail=f"Error fetching battle IDs: {str(e)}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed for /battle_ids")