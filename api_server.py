from fastapi import FastAPI, HTTPException # Make sure HTTPException is added
from typing import List # Add this
from fastapi import FastAPI, HTTPException, Query # Add Query here
import uvicorn
import requests                     # Needed to call the external API
import datetime                     # Needed for time calculations
import time                         # Needed to get the current time easily
import sqlite3                      # Needed to interact with the SQLite database


# Create the FastAPI app instance
app = FastAPI(title="Clan Dashboard API", version="0.1.0")

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

# --- Helper Function for Score Projection ---
def calculate_projected_score(
    clan_name: str,
    current_info: dict,
    forecast_period_minutes: int,
    minutes_remaining_war: float,
    cursor: sqlite3.Cursor # Pass the database cursor
):
    """Calculates projected score for a single clan."""

    projected_points = None # Default
    forecast_gain = None

    # 1. Check 6-hour rule
    has_6h_data = False
    first_seen_str = current_info.get('first_seen')
    latest_ts_dt = datetime.datetime.fromisoformat(current_info['latest_timestamp'])
    six_hours_ago = latest_ts_dt - datetime.timedelta(hours=6)
    if first_seen_str:
        try:
            first_seen_dt = datetime.datetime.fromisoformat(first_seen_str)
            if first_seen_dt <= six_hours_ago:
                has_6h_data = True
        except ValueError: pass # Ignore invalid date format

    # 2. Fetch past data for forecast (if rule passed)
    if has_6h_data and minutes_remaining_war > 0 and forecast_period_minutes > 0:
        past_points_forecast = None
        try:
            target_forecast_past_dt = latest_ts_dt - datetime.timedelta(minutes=forecast_period_minutes)
            target_forecast_past_ts_str = target_forecast_past_dt.isoformat()

            query_past_forecast = """
            SELECT current_points as past_points_forecast
            FROM clans
            WHERE clan_name = ? AND timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT 1;
            """
            cursor.execute(query_past_forecast, (clan_name, target_forecast_past_ts_str))
            result = cursor.fetchone()
            if result:
                past_points_forecast = result['past_points_forecast']

        except Exception as q_err:
            print(f"Error querying past forecast data for {clan_name}: {q_err}")

        # 3. Calculate Projection (if rule passed and past data found)
        if past_points_forecast is not None:
            forecast_gain = current_info['current_points'] - past_points_forecast
            if forecast_period_minutes > 0: # Avoid division by zero
                gain_rate_per_minute = forecast_gain / forecast_period_minutes
                projected_points = current_info['current_points'] + (gain_rate_per_minute * minutes_remaining_war)

    return projected_points, has_6h_data, forecast_gain # Return projection, rule status, and gain


# Dashboard endpoint
@app.get("/api/dashboard")
async def get_dashboard_data(time_period: int = 60, forecast_period: int = 360):
    """ Fetches and calculates clan data using helper for projection. """
    print(f"/api/dashboard called with time_period={time_period}, forecast_period={forecast_period}")
    db_path = "clan_data.db"; conn = None; dashboard_results = []
    minutes_remaining = 0; war_finish_time_dt = None

    try:
        # --- Fetch War End Time --- (Same as before)
        try:
            countdown_url="https://ps99.biggamesapi.io/api/activeClanBattle"; response=requests.get(countdown_url, timeout=5); response.raise_for_status(); raw_data=response.json()
            if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and "FinishTime" in raw_data["data"]["configData"]):
                finish_time_unix = raw_data["data"]["configData"]["FinishTime"]; war_finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix); remaining_delta = war_finish_time_dt - datetime.datetime.now()
                if remaining_delta.total_seconds() > 0: minutes_remaining = remaining_delta.total_seconds() / 60
                print(f"War ends at: {war_finish_time_dt}, Minutes remaining: {minutes_remaining:.2f}")
            else: print("Could not get valid war end time from countdown API.")
        except Exception as cd_err: print(f"Error fetching or processing war end time: {cd_err}")

        # --- Database Connection --- (Same as before)
        conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row; cursor = conn.cursor()

        # === Query 1: Get Latest Data === (Same as before)
        query_latest = """
        WITH LatestTimestamp AS ( SELECT MAX(timestamp) as max_ts FROM clans ),
        RankedClans AS ( SELECT clan_name, current_points, members, timestamp as latest_timestamp, first_seen, RANK() OVER (ORDER BY current_points DESC) as current_rank FROM clans WHERE timestamp = (SELECT max_ts FROM LatestTimestamp) )
        SELECT * FROM RankedClans WHERE current_rank <= 25 ORDER BY current_rank;
        """
        cursor.execute(query_latest); latest_rows = cursor.fetchall(); latest_timestamp_str = latest_rows[0]['latest_timestamp'] if latest_rows else None
        if not latest_rows or not latest_timestamp_str: return []
        ranked_latest_list = [dict(row) for row in latest_rows]; top_clan_names = [row['clan_name'] for row in ranked_latest_list]

        # === Query 2: Get Past Data for X-Minute Gain === (Same as before)
        past_data_map_gain = {}
        try:
            latest_ts_dt = datetime.datetime.fromisoformat(latest_timestamp_str); target_past_dt = latest_ts_dt - datetime.timedelta(minutes=time_period); target_past_ts_str = target_past_dt.isoformat(); placeholders = ','.join('?' * len(top_clan_names))
            query_past_gain = f"""WITH RP AS (SELECT c.clan_name, c.current_points as past_points, ROW_NUMBER() OVER (PARTITION BY c.clan_name ORDER BY c.timestamp DESC) as rn FROM clans c WHERE c.clan_name IN ({placeholders}) AND c.timestamp <= ?) SELECT clan_name, past_points FROM RP WHERE rn = 1;"""; cursor.execute(query_past_gain, top_clan_names + [target_past_ts_str]); past_data_map_gain = {row['clan_name']: dict(row) for row in cursor.fetchall()}; print(f"Gain query returned {len(past_data_map_gain)} rows.")
        except Exception as q2_err: print(f"Error in gain query: {q2_err}")

        # === Combine Data and Calculate All Fields ===
        print("Calculating all fields using helper...")
        processed_results = {}

        # --- First pass: Use helper to get projection, calculate gain --- (Same as before)
        for i, current_clan_info in enumerate(ranked_latest_list):
            clan_name = current_clan_info['clan_name']; clan_result = current_clan_info.copy()
            past_info_gain = past_data_map_gain.get(clan_name); clan_result['x_minute_gain'] = (current_clan_info['current_points'] - past_info_gain['past_points']) if past_info_gain else None
            projected_points, has_6h_data, _ = calculate_projected_score(clan_name=clan_name, current_info=clan_result, forecast_period_minutes=forecast_period, minutes_remaining_war=minutes_remaining, cursor=cursor)
            clan_result['projected_points'] = projected_points; clan_result['has_6h_data'] = has_6h_data
            processed_results[clan_name] = clan_result

        # --- Second pass: Calculate Gap, TimeToCatch, and Forecast Rank ---
        projected_ranked_list = sorted(processed_results.values(), key=lambda x: x['projected_points'] if x['projected_points'] is not None else x['current_points'], reverse=True)
        forecast_ranks = {clan['clan_name']: rank + 1 for rank, clan in enumerate(projected_ranked_list)}

        final_dashboard_results = []
        for i, current_clan_info in enumerate(ranked_latest_list):
            clan_name = current_clan_info['clan_name']
            clan_result = processed_results[clan_name] # Get result with projection info

            # Calculate Gap (Same as before)
            clan_result['gap'] = 0 if clan_result['current_rank'] == 1 else ranked_latest_list[i-1]['current_points'] - current_clan_info['current_points']

            # --- Calculate Time to Catch ---
            time_to_catch_str = "N/A" # Default value
            if clan_result['current_rank'] > 1:
                clan_above_name = ranked_latest_list[i-1]['clan_name']
                clan_above_result = processed_results.get(clan_above_name)
                current_gain = clan_result['x_minute_gain']
                above_gain = clan_above_result.get('x_minute_gain') if clan_above_result else None

                # Check condition: Both gains must be valid numbers, and current > above
                if (current_gain is not None and above_gain is not None and
                    isinstance(current_gain, (int, float)) and isinstance(above_gain, (int, float)) and
                    current_gain > above_gain):

                    gain_difference = current_gain - above_gain
                    # --- Correction: Nest the next check inside the block where gain_difference is valid ---
                    if gain_difference > 0 and time_period > 0: # Avoid division by zero or invalid period
                        try: # Add try-except for potential math errors
                            minutes_to_catch = (clan_result['gap'] * time_period) / gain_difference
                            catch_timedelta = datetime.timedelta(minutes=minutes_to_catch)
                            # Use the same helper function as the countdown endpoint
                            time_to_catch_str = format_timedelta(catch_timedelta)
                        except Exception as calc_err:
                            print(f"Error calculating minutes_to_catch for {clan_name}: {calc_err}")
                            time_to_catch_str = "Error" # Indicate calculation error

            clan_result['time_to_catch'] = time_to_catch_str
            # --- End Time to Catch Correction ---

            # Assign Forecast Rank using helper result
            clan_result['forecast'] = forecast_ranks.get(clan_name) if clan_result['has_6h_data'] else None

            # Remove helper fields before returning
            del clan_result['projected_points']
            del clan_result['has_6h_data']

            final_dashboard_results.append(clan_result)

    # --- Error Handling & Connection Closing --- (Same as before)
    except sqlite3.Error as e: print(f"Database error in /api/dashboard: {e}"); raise HTTPException(status_code=500, detail=f"DB error: {e}")
    except Exception as e: print(f"Unexpected error in /api/dashboard: {e}"); import traceback; traceback.print_exc(); raise HTTPException(status_code=500, detail=f"Internal error: {e}")
    finally:
        if conn: conn.close(); print("Database connection closed for /api/dashboard.")

    return final_dashboard_results

# Endpoint to calculate needs for a specific clan to reach a target rank
@app.get("/api/clan_reach_target")
async def get_clan_reach_target(clan_name: str, target_rank: int, forecast_period: int = 360):
    """
    Calculates the extra points per hour a specific clan needs to gain
    to reach the target rank by the end of the war.
    """
    print(f"/api/clan_reach_target called for {clan_name}, target_rank={target_rank}, forecast_period={forecast_period}")

    # --- Input Validation ---
    if target_rank <= 0 or target_rank > 250: # Basic rank validation
        raise HTTPException(status_code=400, detail="Invalid target_rank. Must be between 1 and 250.")
    if forecast_period <= 0:
         raise HTTPException(status_code=400, detail="Invalid forecast_period. Must be positive.")


    db_path = "clan_data.db"; conn = None
    minutes_remaining = 0; war_finish_time_dt = None

    try:
        # --- Fetch War End Time --- (Same as in dashboard)
        try:
            countdown_url="https://ps99.biggamesapi.io/api/activeClanBattle"; response=requests.get(countdown_url, timeout=5); response.raise_for_status(); raw_data=response.json()
            if ("data" in raw_data and isinstance(raw_data.get("data"), dict) and "configData" in raw_data["data"] and isinstance(raw_data["data"].get("configData"), dict) and "FinishTime" in raw_data["data"]["configData"]):
                finish_time_unix = raw_data["data"]["configData"]["FinishTime"]; war_finish_time_dt = datetime.datetime.fromtimestamp(finish_time_unix); remaining_delta = war_finish_time_dt - datetime.datetime.now()
                if remaining_delta.total_seconds() > 0: minutes_remaining = remaining_delta.total_seconds() / 60
                print(f"War ends at: {war_finish_time_dt}, Minutes remaining: {minutes_remaining:.2f}")
            else: print("Could not get valid war end time."); raise HTTPException(status_code=503, detail="Could not get war end time")
        except Exception as cd_err: print(f"Error fetching war end time: {cd_err}"); raise HTTPException(status_code=503, detail=f"Could not get war end time: {cd_err}")

        if minutes_remaining <= 0:
             print("War has ended or ending immediately, cannot calculate needs.")
             return {"extra_points_per_hour": 0} # Or maybe indicate differently?

        hours_remaining = minutes_remaining / 60.0

        # --- Database Connection ---
        conn = sqlite3.connect(db_path); conn.row_factory = sqlite3.Row; cursor = conn.cursor()

        # === Query Latest Data (Might need more than 25 to find target rank) ===
        # Let's fetch Top 250 to be safer for finding target rank's projected score
        query_latest = """
        WITH LatestTimestamp AS ( SELECT MAX(timestamp) as max_ts FROM clans ),
        RankedClans AS ( SELECT clan_name, current_points, members, timestamp as latest_timestamp, first_seen, RANK() OVER (ORDER BY current_points DESC) as current_rank FROM clans WHERE timestamp = (SELECT max_ts FROM LatestTimestamp) )
        SELECT * FROM RankedClans WHERE current_rank <= 250 ORDER BY current_rank;
        """
        cursor.execute(query_latest); latest_rows = cursor.fetchall()
        if not latest_rows: raise HTTPException(status_code=503, detail="No current clan data available.")
        ranked_latest_list = [dict(row) for row in latest_rows]

        # Find the specific clan's current data
        user_clan_current_info = next((clan for clan in ranked_latest_list if clan['clan_name'] == clan_name), None)
        if not user_clan_current_info:
             raise HTTPException(status_code=404, detail=f"Clan '{clan_name}' not found in latest Top 250 data.")

        # === Calculate Projections for ALL relevant clans ===
        projections = {} # Store clan_name -> projected_score
        all_clan_projections_valid = True
        for clan_info in ranked_latest_list:
            c_name = clan_info['clan_name']
            projected_score, has_6h, _ = calculate_projected_score(
                clan_name=c_name, current_info=clan_info, forecast_period_minutes=forecast_period,
                minutes_remaining_war=minutes_remaining, cursor=cursor )

            # Store projection; use current points if projection failed or ineligible
            projections[c_name] = projected_score if (has_6h and projected_score is not None) else clan_info['current_points']
            # Track if the specific clans we need had valid projections
            if c_name == clan_name and (not has_6h or projected_score is None):
                 print(f"Warning: User clan {clan_name} ineligible for projection.")
                 all_clan_projections_valid = False # Cannot calculate if user clan invalid


        # === Determine Target Score ===
        # Rank based on projections to find the clan at target_rank
        projected_ranked_list = sorted(ranked_latest_list, key=lambda x: projections[x['clan_name']], reverse=True)

        if target_rank > len(projected_ranked_list):
            raise HTTPException(status_code=400, detail=f"Target rank {target_rank} is out of range for available data.")

        target_rank_clan_name = projected_ranked_list[target_rank - 1]['clan_name']
        target_rank_projected_score = projections[target_rank_clan_name]

        # Check if the target rank clan had a valid projection (needed for fair comparison)
        target_rank_clan_current_info = next(clan for clan in ranked_latest_list if clan['clan_name'] == target_rank_clan_name)
        _, target_rank_has_6h_data, _ = calculate_projected_score(target_rank_clan_name, target_rank_clan_current_info, forecast_period, minutes_remaining, cursor)
        if not target_rank_has_6h_data or projections[target_rank_clan_name] == target_rank_clan_current_info['current_points']:
             print(f"Warning: Target rank {target_rank} clan {target_rank_clan_name} ineligible for projection.")
             all_clan_projections_valid = False # Cannot calculate if target clan invalid


        # === Calculate Extra Points ===
        user_clan_projected_score = projections[clan_name]
        score_difference = target_rank_projected_score - user_clan_projected_score

        if not all_clan_projections_valid:
             extra_points_per_hour = None # Indicate calculation not possible
             print("Calculation not possible due to projection ineligibility.")
        elif score_difference <= 0:
            extra_points_per_hour = 0 # Already projected to meet or exceed
        else:
            if hours_remaining <= 0: # Avoid division by zero if war ended
                 extra_points_per_hour = float('inf') # Needs infinite points if no time left
            else:
                 extra_points_per_hour = score_difference / hours_remaining

    # --- Error Handling & Connection Closing ---
    except sqlite3.Error as e: print(f"Database error in /api/clan_reach_target: {e}"); raise HTTPException(status_code=500, detail=f"DB error: {e}")
    except HTTPException: raise # Re-raise HTTP exceptions directly
    except Exception as e: print(f"Unexpected error in /api/clan_reach_target: {e}"); import traceback; traceback.print_exc(); raise HTTPException(status_code=500, detail=f"Internal error: {e}")
    finally:
        if conn: conn.close(); print("Database connection closed for /api/clan_reach_target.")

    # Format final result
    if extra_points_per_hour is None:
        result_value = None # Or maybe a specific string like "Ineligible"
    elif extra_points_per_hour == float('inf'):
        result_value = "Infinity"
    else:
        result_value = round(extra_points_per_hour) # Return as integer

    return {"extra_points_per_hour": result_value }

# Endpoint to get historical data for comparing clans
@app.get("/api/clan_comparison")
async def get_clan_comparison(
    # Use Query(...) to handle list parameters from URL query string
    clan_names: List[str] = Query(..., min_length=1, max_length=3, title="Clan Names", description="List of 1 to 3 clan names to compare."),
    time_period: int = Query(60, gt=0, title="Time Period (minutes)", description="Lookback period in minutes.") # Default 60 mins
):
    """
    Fetches historical point data for up to 3 specified clans over a given time period.
    """
    print(f"/api/clan_comparison called for clans: {clan_names}, time_period: {time_period}")

    # Basic validation done by FastAPI (min/max_length, gt=0), could add more if needed

    db_path = "clan_data.db"; conn = None
    comparison_data = []

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Calculate start timestamp
        now_dt = datetime.datetime.now()
        start_dt = now_dt - datetime.timedelta(minutes=time_period)
        start_ts_str = start_dt.isoformat()
        print(f"Fetching comparison data from: {start_ts_str}")

        # Construct query with placeholders for clan names list
        placeholders = ','.join('?' * len(clan_names))
        query = f"""
        SELECT
            clan_name,
            timestamp,
            current_points
        FROM clans
        WHERE clan_name IN ({placeholders})
          AND timestamp >= ?
        ORDER BY clan_name, timestamp;
        """

        query_params = clan_names + [start_ts_str]
        print(f"Executing comparison query for {len(clan_names)} clans...")
        cursor.execute(query, query_params)
        rows = cursor.fetchall()
        print(f"Query returned {len(rows)} comparison data points.")

        # Convert rows to list of dictionaries
        comparison_data = [dict(row) for row in rows]


    except sqlite3.Error as e:
        print(f"Database error in /api/clan_comparison: {e}")
        raise HTTPException(status_code=500, detail=f"Database error accessing clan data: {e}")
    except Exception as e:
        print(f"Unexpected error in /api/clan_comparison: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Internal server error in comparison endpoint: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed for /api/clan_comparison.")

    return comparison_data # Return the list of historical data points

# --- This part allows running directly with 'python api_server.py' (optional but convenient) ---
# Note: For development, running with 'uvicorn api_server:app --reload' is usually preferred.
if __name__ == "__main__":
    print("Starting API server using uvicorn...")
    uvicorn.run(app, host="127.0.0.1", port=8000)