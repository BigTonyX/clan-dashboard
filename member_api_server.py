from fastapi import FastAPI, HTTPException
from typing import List, Optional
from fastapi import FastAPI, HTTPException, Query
import uvicorn
import requests
import datetime
import time
import pymongo
import sys
import os
import traceback
from dotenv import load_dotenv
load_dotenv()
from pymongo import MongoClient
from pymongo.collection import Collection
from fastapi.middleware.cors import CORSMiddleware
from roblox_api import get_usernames_batch
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# --- MongoDB Atlas Connection ---
MONGO_CONNECTION_STRING = os.environ.get("MONGO_URI")
DB_NAME = "clan_dashboard_db"

# Create the FastAPI app instance
app = FastAPI(title="Clan Member Tracking API", version="0.1.0")

# --- CORS Middleware ---
origins = ["*"]  # Allow all origins for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Root endpoint ---
@app.get("/")
async def read_root():
    """Basic endpoint to check if the API is running."""
    logger.info("Root endpoint accessed")
    return {"message": "Welcome to the Clan Member Tracking API!"}

# --- Member tracking endpoint ---
@app.get("/api/member-tracking/{clan_name}")
async def get_member_tracking(clan_name: str):
    """Get the latest member data for a specific clan."""
    logger.info("Received request for clan: {clan_name}")
    client = None
    try:
        logger.info(f"Connecting to MongoDB for clan: {clan_name}")
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        members_collection = db["clan_members"]

        # Get the latest record for the clan
        latest_data = members_collection.find_one(
            {"clan_name": clan_name},
            sort=[("timestamp", pymongo.DESCENDING)]
        )

        if not latest_data:
            logger.warning(f"No data found for clan: {clan_name}")
            raise HTTPException(status_code=404, detail=f"No data found for clan {clan_name}")

        # Extract all member IDs
        logger.info(f"Processing members for clan: {clan_name}")
        member_ids = []
        invalid_members = 0
        try:
            members = latest_data.get("members", [])
            logger.info(f"Found {len(members)} members for {clan_name}")
            for member in members:
                if member.get("UserID"):
                    member_ids.append(str(member["UserID"]))
                else:
                    invalid_members += 1
            
            if invalid_members > 0:
                logger.warning(f"Found {invalid_members} members with missing UserID in {clan_name}")
                
        except Exception as e:
            logger.error(f"Error processing members: {e}")
            logger.error(f"Latest data structure: {latest_data}")
            raise HTTPException(status_code=500, detail=f"Error processing member data: {str(e)}")
        
        if not member_ids:
            logger.warning(f"No valid member IDs found for clan: {clan_name}")
            return {
                "status": "warning",
                "clan_name": clan_name,
                "total_points": latest_data.get("total_points", 0),
                "is_active": latest_data.get("is_active", False),
                "battle_id": latest_data.get("battle_id"),
                "timestamp": latest_data.get("timestamp", datetime.datetime.now()),
                "members": []
            }

        # Fetch all usernames in a single batch
        logger.info(f"Fetching usernames for {len(member_ids)} members of {clan_name}")
        try:
            usernames = get_usernames_batch(member_ids, client)
        except Exception as e:
            logger.error(f"Error fetching usernames: {e}")
            logger.error(traceback.format_exc())
            raise HTTPException(status_code=500, detail=f"Error fetching usernames: {str(e)}")

        # Add username information to member data
        members_with_names = []
        for member in latest_data.get("members", []):
            try:
                if not member.get("UserID"):
                    continue
                    
                user_id = str(member["UserID"])
                user_info = usernames.get(user_id, {"name": "Unknown", "display_name": "Unknown"})
                
                members_with_names.append({
                    "UserID": user_id,
                    "username": user_info["name"],
                    "display_name": user_info["display_name"],
                    "points": member["Points"],
                    "battle_id": latest_data.get("battle_id")  # Include battle_id for each member
                })
            except Exception as e:
                logger.error(f"Error processing member {member}: {e}")
                continue

        response_data = {
            "status": "ok",
            "clan_name": clan_name,
            "total_points": latest_data.get("total_points", 0),
            "is_active": latest_data.get("is_active", False),
            "battle_id": latest_data.get("battle_id"),  # Include battle_id in response
            "timestamp": latest_data.get("timestamp", datetime.datetime.now()),
            "members": members_with_names
        }
        logger.info(f"Successfully processed data for {clan_name} with {len(members_with_names)} members")
        return response_data

    except Exception as e:
        logger.error(f"Unexpected error in get_member_tracking: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if client:
            client.close()

# --- Member history endpoint ---
@app.get("/api/member-history/{clan_name}")
async def get_member_history(clan_name: str, battle_id: Optional[str] = None, userId: Optional[str] = None):
    """Get historical member data for a specific clan, optionally filtered by battle_id and userId."""
    logger.info(f"Received history request - clan: {clan_name}, userId: {userId}, battle_id: {battle_id}")
    client = None
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        members_collection = db["clan_members"]

        # Build the base query
        query = {"clan_name": clan_name}
        if battle_id:
            query["battle_id"] = battle_id

        # If userId is provided, use aggregation pipeline to filter at database level
        if userId:
            # First try to match records that contain this user
            query["members"] = {
                "$elemMatch": {
                    "UserID": {
                        "$in": [userId, int(userId)]  # Match both string and int versions
                    }
                }
            }
            logger.info(f"Using query with userId filter: {userId}")

        # Get historical data with the query
        historical_data = list(members_collection.find(
            query,
            sort=[("timestamp", pymongo.DESCENDING)]
        ))
        logger.info(f"Found {len(historical_data)} historical records for {clan_name}")

        if not historical_data:
            logger.warning(f"No historical data found for clan: {clan_name}" + 
                          (f" and battle: {battle_id}" if battle_id else "") + 
                          (f" and userId: {userId}" if userId else ""))
            raise HTTPException(status_code=404, detail=f"No historical data found for clan {clan_name}")

        # Get unique member IDs across all historical data
        all_member_ids = set()
        for data in historical_data:
            member_ids = [str(member["UserID"]) for member in data.get("members", [])]
            all_member_ids.update(member_ids)

        # Remove None or empty values
        all_member_ids = {id for id in all_member_ids if id}
        
        # Fetch all usernames in a single batch
        logger.info(f"Fetching usernames for {len(all_member_ids)} unique members")
        usernames = get_usernames_batch(list(all_member_ids), client)

        # Process historical data
        processed_history = []
        for data in historical_data:
            members_with_names = []
            for member in data.get("members", []):
                if not member.get("UserID"):
                    logger.warning(f"Skipping member with missing UserID: {member}")
                    continue
                    
                user_id = str(member["UserID"])
                user_info = usernames.get(user_id, {"name": "Unknown", "display_name": "Unknown"})
                
                members_with_names.append({
                    "UserID": user_id,
                    "username": user_info["name"],
                    "display_name": user_info["display_name"],
                    "points": member.get("Points", member.get("points", 0))  # Try both cases
                })

            processed_history.append({
                "timestamp": data.get("timestamp", datetime.datetime.now()),
                "total_points": data.get("total_points", 0),
                "is_active": data.get("is_active", False),
                "battle_id": data.get("battle_id"),  # Include battle_id in response
                "members": members_with_names
            })

        logger.info(f"Successfully processed historical data for {clan_name}")
        return {
            "status": "ok",
            "clan_name": clan_name,
            "battle_id": battle_id,  # Include battle_id in response
            "history": processed_history
        }

    except Exception as e:
        logger.error(f"Error in get_member_history: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if client:
            client.close()

@app.get("/api/member-history/{clan_name}/recent")
async def get_recent_member_history(clan_name: str, hours: int = 24):
    """Get recent historical data for a clan's members."""
    client = None
    try:
        start_time = time.time()
        logger.info(f"Starting recent history fetch for clan: {clan_name}")
        
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[DB_NAME]
        
        # Calculate the cutoff time
        cutoff_time = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
        logger.info(f"Fetching records since: {cutoff_time}")
        
        # Query MongoDB for recent records
        collection = db["clan_members"]
        
        # Add timing for MongoDB query
        query_start = time.time()
        
        # First check if we have any records at all
        total_records = collection.count_documents({"clan_name": clan_name})
        logger.info(f"Total records for clan {clan_name}: {total_records}")
        
        # Get recent records
        records = list(collection.find(
            {
                "clan_name": clan_name,
                "timestamp": {"$gte": cutoff_time}
            },
            sort=[("timestamp", pymongo.DESCENDING)]  # Sort by newest first
        ))
        query_time = time.time() - query_start
        logger.info(f"MongoDB query took {query_time:.2f} seconds")
        
        if not records:
            logger.warning(f"No recent records found. Fetching last 100 records instead.")
            # If no recent records, get the last 100 records
            records = list(collection.find(
                {"clan_name": clan_name},
                sort=[("timestamp", pymongo.DESCENDING)],
                limit=100
            ))
            
        logger.info(f"Found {len(records)} records for {clan_name}")
        if records:
            latest_time = records[0].get('timestamp')
            oldest_time = records[-1].get('timestamp')
            logger.info(f"Time range: {oldest_time} to {latest_time}")
            
        # Process usernames for members
        username_start = time.time()
        all_member_ids = set()
        for record in records:
            member_ids = [str(member["UserID"]) for member in record.get("members", [])]
            all_member_ids.update(member_ids)
        
        # Fetch all usernames in a single batch
        logger.info(f"Fetching usernames for {len(all_member_ids)} unique members")
        usernames = get_usernames_batch(list(all_member_ids), client)
        username_time = time.time() - username_start
        logger.info(f"Username processing took {username_time:.2f} seconds")

        # Process historical data
        process_start = time.time()
        processed_history = []
        for data in records:
            members_with_names = []
            for member in data.get("members", []):
                if not member.get("UserID"):
                    continue
                    
                user_id = str(member["UserID"])
                user_info = usernames.get(user_id, {"name": "Unknown", "display_name": "Unknown"})
                
                members_with_names.append({
                    "UserID": user_id,
                    "username": user_info["name"],
                    "display_name": user_info["display_name"],
                    "points": member["Points"]
                })

            processed_history.append({
                "timestamp": data.get("timestamp", datetime.datetime.now()),
                "total_points": data.get("total_points", 0),
                "is_active": data.get("is_active", False),
                "members": members_with_names
            })
        
        process_time = time.time() - process_start
        logger.info(f"Data processing took {process_time:.2f} seconds")
        
        total_time = time.time() - start_time
        logger.info(f"Total recent history operation took {total_time:.2f} seconds")
        
        return {
            "clan_name": clan_name,
            "history": processed_history
        }
        
    except Exception as e:
        logger.error("Error in get_recent_member_history: %s", str(e))
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if client:
            client.close()

# Run the server if executed directly
if __name__ == "__main__":
    logger.info("="*50)
    logger.info("Starting Member Tracking API server")
    logger.info(f"Server will be available at: http://127.0.0.1:8001")
    logger.info("="*50)
    uvicorn.run(app, host="127.0.0.1", port=8001) 