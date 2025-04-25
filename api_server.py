from fastapi import FastAPI, HTTPException # Make sure HTTPException is added
from typing import List # Add this
from fastapi import FastAPI, HTTPException, Query # Add Query here
import uvicorn
import requests                     # Needed to call the external API
import datetime                     # Needed for time calculations
import time                         # Needed to get the current time easily
import pymongo
import sys
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
from pymongo.collection import Collection
from fastapi.middleware.cors import CORSMiddleware

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = "clan_dashboard_db" # Use the same database name as in the fetcher

# Create the FastAPI app instance
app = FastAPI(title="Clan Dashboard API", version="0.1.0")
# Create the FastAPI app instance (This line should already exist)
app = FastAPI(title="Clan Dashboard API", version="0.1.0")

# --- CORS Middleware ---
# Define allowed origins (domains) that can access your API
# Using ["*"] allows all origins, which is simple for development
# and okay for a public API like this.
# For more security later, you might restrict this to the specific domain
# where your frontend will eventually be hosted.
# Adding "null" specifically allows requests from local file:/// pages.
origins = [
    "*", # Allows all origins
    # Or be more specific:
    # "null", # Allow local file:/// origin
    # "http://localhost", # Allow local development server
    # "http://127.0.0.1",
    # Add your future frontend deployment URL here e.g. "https://your-frontend.onrender.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,       # List of origins allowed
    allow_credentials=True,    # Allow cookies (not needed now, but common)
    allow_methods=["*"],       # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],       # Allow all headers
)
# --- End CORS Middleware ---


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
@app.get("/api/countdown")
async def get_countdown():
    """Fetches the war end time and returns the formatted countdown."""
    countdown_url = "https://ps99.biggamesapi.io/api/activeClanBattle"
    # Removed the print here as fetching is confirmed working
    # print(f"Attempting to fetch countdown data from: {countdown_url}")

    try:
        response = requests.get(countdown_url, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        # Removed print here as extraction is confirmed working
        # print(f"Successfully fetched raw data: {raw_data}")

        if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and
            "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and
            "FinishTime" in raw_data["data"]["configData"]):

            finish_time_unix = raw_data["data"]["configData"]["FinishTime"]
            # Removed print here as extraction is confirmed working
            # print(f"Successfully extracted FinishTime (Unix): {finish_time_unix}")

            try:
                finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix)
                now_dt = datetime.datetime.now()
                remaining_delta = finish_time_dt - now_dt
                # Removed print here as calculation is confirmed working
                # print(f"Calculated remaining timedelta: {remaining_delta}")

                # --- Format the delta using the helper function ---
                countdown_str = format_timedelta(remaining_delta)
                print(f"Formatted countdown string: {countdown_str}") # Keep a log

                # --- Return the final JSON structure ---
                return {"countdown": countdown_str}

            except ValueError as ve:
                print(f"Error converting timestamp or calculating delta: {ve}")
                raise HTTPException(status_code=500, detail="Error processing finish time.")

        else:
            print(f"Error: Unexpected data structure in response: {raw_data}")
            raise HTTPException(status_code=500, detail="Unexpected data structure from countdown API.")

    except requests.exceptions.RequestException as e:
        print(f"Error fetching countdown data: {e}")
        raise HTTPException(status_code=503, detail=f"Could not fetch countdown data: {e}")
    except Exception as e:
         print(f"An unexpected error occurred processing countdown data: {e}")
         import traceback
         traceback.print_exc()
         raise HTTPException(status_code=500, detail=f"Internal server error processing countdown data: {e}")




# Dashboard endpoint (OPTIMIZED)
@app.get("/api/dashboard")
async def get_dashboard_data(time_period: int = 60, forecast_period: int = 360):
    """ Fetches and calculates clan data from MongoDB using bulk queries. """
    print(f"/api/dashboard called with time_period={time_period}, forecast_period={forecast_period}")

    client = None; dashboard_results = []; minutes_remaining = 0; war_finish_time_dt = None

    try:
        # --- Fetch War End Time ---
        try: # Fetch countdown data
            countdown_url="https://ps99.biggamesapi.io/api/activeClanBattle"; response=requests.get(countdown_url, timeout=5); response.raise_for_status(); raw_data=response.json()
            if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and "FinishTime" in raw_data["data"]["configData"]):
                finish_time_unix = raw_data["data"]["configData"]["FinishTime"]; war_finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix); remaining_delta = war_finish_time_dt - datetime.datetime.now()
                if remaining_delta.total_seconds() > 0: minutes_remaining = remaining_delta.total_seconds() / 60
                print(f"War ends at: {war_finish_time_dt}, Minutes remaining: {minutes_remaining:.2f}")
            else: print("Could not get valid war end time.")
        except Exception as cd_err: print(f"Error fetching war end time: {cd_err}") # Continue even if countdown fails

        # --- Connect to MongoDB ---
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000); db = client[DB_NAME]; clans_collection = db["clans"]; client.admin.command('ping'); print("MongoDB connection successful for dashboard.")

        # === Get Latest Timestamp ===
        latest_doc = clans_collection.find_one(sort=[("timestamp", pymongo.DESCENDING)]);
        if not latest_doc or not latest_doc.get('timestamp'): print("No data found."); return []
        latest_ts_dt = latest_doc['timestamp']; latest_timestamp_str = latest_ts_dt.isoformat(); print(f"Latest timestamp: {latest_timestamp_str}")

        # === Query 1: Get Latest Data for Top 25 ===
        print("Executing query 1: Get latest top 25 data...")
        query_latest = {"timestamp": latest_ts_dt}
        projection_latest = {"_id":0, "clan_name":1, "current_points":1, "members":1, "timestamp":1} # Only fetch needed fields initially
        latest_docs_cursor = clans_collection.find(query_latest, projection=projection_latest).sort("current_points", pymongo.DESCENDING).limit(25);
        ranked_latest_list = []
        rank = 1
        for doc in latest_docs_cursor:
             doc['latest_timestamp'] = doc['timestamp'].isoformat() # Add string version
             doc['current_rank'] = rank
             ranked_latest_list.append(dict(doc)) # Use dict() to ensure mutability if needed
             rank += 1
        print(f"Query 1 processed {len(ranked_latest_list)} rows.")
        if not ranked_latest_list: return []
        top_25_clan_names = [row['clan_name'] for row in ranked_latest_list]

        # === Bulk Queries for Historical Data ===
        first_seen_map = {}; past_data_map_gain = {}; past_data_map_forecast = {}
        six_hours_ago = latest_ts_dt - datetime.timedelta(hours=6)

        # --- Bulk Query for First Seen ---
        try:
            print("Executing bulk query for first_seen...")
            pipeline_first_seen = [ {'$match': {'clan_name': {'$in': top_25_clan_names}, 'first_seen': {'$ne': None}}}, {'$sort': {'timestamp': pymongo.ASCENDING}}, {'$group': {'_id': '$clan_name', 'first_seen_ts': {'$first': '$first_seen'}}} ]
            first_seen_map = {row['_id']: row['first_seen_ts'] for row in clans_collection.aggregate(pipeline_first_seen)}
            print(f"Found first_seen data for {len(first_seen_map)} clans.")
        except Exception as fs_err: print(f"Error querying first_seen: {fs_err}")

        # --- Bulk Query for Gain Data ---
        try:
            target_past_dt_gain = latest_ts_dt - datetime.timedelta(minutes=time_period)
            print(f"Target past timestamp for gain: {target_past_dt_gain.isoformat()}")
            pipeline_past_gain = [ {'$match': { 'clan_name': {'$in': top_25_clan_names}, 'timestamp': {'$lte': target_past_dt_gain} }}, {'$sort': {'timestamp': pymongo.DESCENDING}}, {'$group': { '_id': '$clan_name', 'past_points': {'$first': '$current_points'} }} ]
            past_data_map_gain = {row['_id']: row['past_points'] for row in clans_collection.aggregate(pipeline_past_gain)}
            print(f"Gain query returned {len(past_data_map_gain)} results.")
        except Exception as q2_err: print(f"Error in gain aggregation: {q2_err}")

        # --- Bulk Query for Forecast Data ---
        try:
            target_past_dt_forecast = latest_ts_dt - datetime.timedelta(minutes=forecast_period)
            print(f"Target past timestamp for forecast: {target_past_dt_forecast.isoformat()}")
            pipeline_past_forecast = [ {'$match': { 'clan_name': {'$in': top_25_clan_names}, 'timestamp': {'$lte': target_past_dt_forecast} }}, {'$sort': {'timestamp': pymongo.DESCENDING}}, {'$group': { '_id': '$clan_name', 'past_points_forecast': {'$first': '$current_points'} }} ]
            past_data_map_forecast = {row['_id']: row['past_points_forecast'] for row in clans_collection.aggregate(pipeline_past_forecast)}
            print(f"Forecast query returned {len(past_data_map_forecast)} results.")
        except Exception as q3_err: print(f"Error in forecast aggregation: {q3_err}")

        # === Calculate All Fields In Python ===
        print("Calculating all fields...")
        projections = {} # Store projections for forecast ranking

        # --- First pass: Calculate Gain, Check 6h, Calc Projection ---
        for clan_result in ranked_latest_list: # Modify the list in place
            clan_name = clan_result['clan_name']
            current_points = clan_result['current_points']

            # Calculate X-Minute Gain
            past_points_gain = past_data_map_gain.get(clan_name)
            clan_result['x_minute_gain'] = (current_points - past_points_gain) if past_points_gain is not None else None

            # Check 6-hour rule
            has_6h_data = False
            first_seen_dt = first_seen_map.get(clan_name)
            if first_seen_dt and isinstance(first_seen_dt, datetime.datetime):
                if first_seen_dt.replace(tzinfo=None) <= six_hours_ago.replace(tzinfo=None): has_6h_data = True
            clan_result['has_6h_data'] = has_6h_data # Keep temporarily

            # Calculate Projection Score
            projected_points = None
            if has_6h_data and minutes_remaining > 0 and forecast_period > 0:
                past_points_forecast = past_data_map_forecast.get(clan_name)
                if past_points_forecast is not None:
                    forecast_gain = current_points - past_points_forecast
                    if forecast_period > 0:
                        gain_rate_per_minute = forecast_gain / forecast_period
                        projected_points = current_points + (gain_rate_per_minute * minutes_remaining)
            # Store projection score or fallback to current points for ranking
            projections[clan_name] = projected_points if projected_points is not None else current_points

        # --- Rank Projections ---
        # Create list including projection score for sorting
        list_for_proj_rank = [{'clan_name': cn, 'score': projections[cn]} for cn in top_25_clan_names]
        projected_ranked_list = sorted(list_for_proj_rank, key=lambda x: x['score'], reverse=True)
        forecast_ranks = {clan['clan_name']: rank + 1 for rank, clan in enumerate(projected_ranked_list)}

        # --- Second pass: Calculate Gap, TimeToCatch, Assign Forecast Rank ---
        final_dashboard_results = []
        for i, clan_result in enumerate(ranked_latest_list):
            clan_name = clan_result['clan_name']

            # Calculate Gap
            clan_result['gap'] = 0 if clan_result['current_rank'] == 1 else ranked_latest_list[i-1]['current_points'] - clan_result['current_points']

            # Calculate Time to Catch
            time_to_catch_str = "N/A"; gain_difference = 0
            if clan_result['current_rank'] > 1:
                clan_above_name = ranked_latest_list[i-1]['clan_name']
                # Need the gain for the clan above, look it up in our processed list
                clan_above_result = next((c for c in ranked_latest_list if c['clan_name'] == clan_above_name), None)
                current_gain = clan_result['x_minute_gain']
                above_gain = clan_above_result.get('x_minute_gain') if clan_above_result else None
                if (current_gain is not None and above_gain is not None and isinstance(current_gain,(int,float)) and isinstance(above_gain,(int,float)) and current_gain > above_gain):
                    gain_difference = current_gain - above_gain
                    if gain_difference > 0 and time_period > 0:
                        try: minutes_to_catch=(clan_result['gap']*time_period)/gain_difference; time_to_catch_str=format_timedelta(datetime.timedelta(minutes=minutes_to_catch));
                        except Exception as calc_err: print(f"Error calculating T2C for {clan_name}: {calc_err}"); time_to_catch_str = "Error"
            clan_result['time_to_catch'] = time_to_catch_str

            # Assign Forecast Rank
            clan_result['forecast'] = forecast_ranks.get(clan_name) if clan_result['has_6h_data'] else None

            # Remove helper field
            del clan_result['has_6h_data']
            # Clean up timestamps for JSON
            if isinstance(clan_result.get('first_seen'), datetime.datetime): clan_result['first_seen'] = clan_result['first_seen'].isoformat()
            if 'timestamp' in clan_result: del clan_result['timestamp']

            final_dashboard_results.append(clan_result)

    # --- Error Handling & Connection Closing ---
    except pymongo.errors.ConnectionFailure as e: print(f"MongoDB connection error: {e}"); raise HTTPException(status_code=503, detail="DB connection error.")
    except HTTPException: raise
    except Exception as e: print(f"Unexpected error: {e}"); import traceback; traceback.print_exc(); raise HTTPException(status_code=500, detail=f"Internal error: {e}")
    finally:
        if client: client.close(); print("MongoDB connection closed for /api/dashboard.")

    return final_dashboard_results

# Endpoint to calculate needs for a specific clan to reach a target rank (OPTIMIZED & CORRECTED)
@app.get("/api/clan_reach_target")
async def get_clan_reach_target(clan_name: str, target_rank: int, forecast_period: int = 360):
    """ Calculates extra points per hour using MongoDB, optimized with bulk queries. """
    print(f"/api/clan_reach_target called for {clan_name}, target_rank={target_rank}, forecast_period={forecast_period}")

    # --- Input Validation ---
    if target_rank <= 0 or target_rank > 250: raise HTTPException(status_code=400, detail="Invalid target_rank.")
    if forecast_period <= 0: raise HTTPException(status_code=400, detail="Invalid forecast_period.")

    client = None; minutes_remaining = 0; war_finish_time_dt = None; extra_points_per_hour = None

    try:
        # --- Fetch War End Time ---
        try:
            countdown_url="https://ps99.biggamesapi.io/api/activeClanBattle"; response=requests.get(countdown_url, timeout=5); response.raise_for_status(); raw_data=response.json()
            if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and "FinishTime" in raw_data["data"]["configData"]):
                finish_time_unix = raw_data["data"]["configData"]["FinishTime"]; war_finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix); remaining_delta = war_finish_time_dt - datetime.datetime.now()
                if remaining_delta.total_seconds() > 0: minutes_remaining = remaining_delta.total_seconds() / 60
                else: minutes_remaining = 0 # Handle case where war ended between fetch and now
                print(f"War ends at: {war_finish_time_dt}, Minutes remaining: {minutes_remaining:.2f}")
            else: raise HTTPException(status_code=503, detail="Could not get valid war end time")
        except Exception as cd_err: raise HTTPException(status_code=503, detail=f"Could not get war end time: {cd_err}")

        if minutes_remaining <= 0: return {"extra_points_per_hour": 0} # Return 0 if war ended
        hours_remaining = minutes_remaining / 60.0

        # --- Connect to MongoDB ---
        client = MongoClient(MONGO_CONNECTION_STRING, serverSelectionTimeoutMS=5000); db = client[DB_NAME]; clans_collection = db["clans"]; client.admin.command('ping'); print("MongoDB connection successful.")

        # === Query 1: Get Latest Data ===
        latest_doc_fetch = clans_collection.find_one(sort=[("timestamp", pymongo.DESCENDING)]);
        if not latest_doc_fetch or not latest_doc_fetch.get('timestamp'): raise HTTPException(status_code=503, detail="No current data.")
        latest_ts_dt = latest_doc_fetch['timestamp']
        query_latest = {"timestamp": latest_ts_dt}
        latest_docs_cursor = clans_collection.find(query_latest, {"_id":0, "clan_name":1, "current_points":1, "timestamp":1}).sort("current_points", pymongo.DESCENDING).limit(250);
        ranked_latest_map = {doc['clan_name']: doc for doc in latest_docs_cursor} # Store as map keyed by name
        if not ranked_latest_map: raise HTTPException(status_code=503, detail="No current clan data available.")
        top_clan_names = list(ranked_latest_map.keys()) # Get names

        user_clan_current_info = ranked_latest_map.get(clan_name)
        if not user_clan_current_info: raise HTTPException(status_code=404, detail=f"Clan '{clan_name}' not found.")

        # === OPTIMIZATION: Bulk Queries for History ===
        # --- Bulk Query for First Seen Timestamps ---
        print("Executing bulk query for first_seen timestamps...")
        pipeline_first_seen = [ {'$match': {'clan_name': {'$in': top_clan_names}, 'first_seen': {'$ne': None}}}, {'$sort': {'timestamp': pymongo.ASCENDING}}, {'$group': {'_id': '$clan_name', 'first_seen_ts': {'$first': '$first_seen'}}} ]
        first_seen_map = {row['_id']: row['first_seen_ts'] for row in clans_collection.aggregate(pipeline_first_seen)}
        print(f"Found first_seen data for {len(first_seen_map)} clans.")

        # --- Bulk Query for Past Forecast Data ---
        print(f"Executing bulk query for past forecast data (period={forecast_period} min)...")
        target_forecast_past_dt = latest_ts_dt - datetime.timedelta(minutes=forecast_period)
        pipeline_past_forecast = [ {'$match': { 'clan_name': {'$in': top_clan_names}, 'timestamp': {'$lte': target_forecast_past_dt} }}, {'$sort': {'timestamp': pymongo.DESCENDING}}, {'$group': { '_id': '$clan_name', 'past_points_forecast': {'$first': '$current_points'} }} ]
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
        if client: client.close(); print("MongoDB connection closed for /api/clan_reach_target.")

    # Format final result
    if extra_points_per_hour is None: result_value = None
    elif extra_points_per_hour == float('inf'): result_value = "Infinity"
    else: result_value = round(extra_points_per_hour)
    return {"extra_points_per_hour": result_value }

# Endpoint to get historical data for comparing clans
@app.get("/api/clan_comparison")
async def get_clan_comparison(
    clan_names: List[str] = Query(..., min_length=1, max_length=3, title="Clan Names", description="List of 1 to 3 clan names to compare."),
    time_period: int = Query(60, gt=0, title="Time Period (minutes)", description="Lookback period in minutes.")
):
    """ Fetches historical point data from MongoDB for clan comparison. """
    print(f"/api/clan_comparison called for clans: {clan_names}, time_period: {time_period}")

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
            "timestamp": {"$gte": start_dt_utc} # Match timestamps within the period
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
         print(f"MongoDB connection error in /api/clan_comparison: {e}")
         raise HTTPException(status_code=503, detail="Database connection error.")
    except Exception as e:
        print(f"Unexpected error in /api/clan_comparison: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error in comparison endpoint: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed for /api/clan_comparison.")

    return comparison_data

# --- This part allows running directly with 'python api_server.py' (optional but convenient) ---
# Note: For development, running with 'uvicorn api_server:app --reload' is usually preferred.
if __name__ == "__main__":
    print("Starting API server using uvicorn...")
    uvicorn.run(app, host="127.0.0.1", port=8000)