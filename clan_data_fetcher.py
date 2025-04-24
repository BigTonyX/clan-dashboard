import sqlite3
import requests
import datetime
import time # We'll need this later for the loop

print("Libraries imported successfully.")

# --- Constants ---
DATABASE_NAME = "clan_data.db" # Name for our SQLite database file
# API URL for top 250 clans, sorted by points descending [cite: 3]
API_URL = "https://biggamesapi.io/api/clans?page=1&pageSize=250&sort=Points&sortOrder=desc"

# --- Database Setup ---
def setup_database():
    """Creates/Updates the SQLite database tables."""
    print(f"Setting up database: {DATABASE_NAME}")
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()

        # --- Update clans table (Add members column) ---
        # We'll rely on "IF NOT EXISTS" for initial creation.
        # For existing databases, adding columns typically needs ALTER TABLE,
        # but for simplicity now, ensure the CREATE includes all columns.
        # If the table exists *without* the members column, this won't add it.
        # A more robust solution would use ALTER TABLE, but let's try this first.
        # Best approach if starting fresh: delete the old clan_data.db file.
        create_clans_table_sql = """
        CREATE TABLE IF NOT EXISTS clans (
            clan_name TEXT NOT NULL,
            current_points INTEGER NOT NULL,
            members INTEGER,          -- Added members column
            timestamp TEXT NOT NULL,
            first_seen TEXT
        );
        """
        cursor.execute(create_clans_table_sql)
        print("Table 'clans' checked/created.")
        # --- Optional: Add 'members' column if it doesn't exist (more robust) ---
        try:
            cursor.execute("ALTER TABLE clans ADD COLUMN members INTEGER;")
            print("Added 'members' column to existing 'clans' table.")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                print("'members' column already exists in 'clans' table.")
            else:
                print(f"Note: Could not add 'members' column, may already exist or other issue: {e}")


        # --- Create clan_details table ---
        create_details_table_sql = """
        CREATE TABLE IF NOT EXISTS clan_details (
            clan_name TEXT PRIMARY KEY,  -- Clan name is the unique key
            icon TEXT,
            country_code TEXT,
            member_capacity INTEGER,
            created_timestamp_api INTEGER, -- Storing the 'Created' field from API
            last_checked TEXT          -- When we last updated this row
        );
        """
        cursor.execute(create_details_table_sql)
        print("Table 'clan_details' checked/created.")

        conn.commit()
        print("Database setup complete.")

    except sqlite3.Error as e:
        print(f"An error occurred during database setup: {e}")
    finally:
        if conn:
            conn.close()
            print("Database connection closed after setup.")

# --- API Fetching ---
def fetch_clan_data():
    """Fetches the top 250 clan data from the Big Games API."""
    print(f"Attempting to fetch data from: {API_URL}")
    try:
        response = requests.get(API_URL, timeout=15)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

        # --- Debugging: Print raw response text ---
        print("--- Raw API Response Text ---")
        # Limit printing raw text if it's very long (optional)
        raw_text = response.text
        print(raw_text[:1000] + "..." if len(raw_text) > 1000 else raw_text)
        print("--- End Raw API Response Text ---")

        # Now try to parse the JSON
        api_response = response.json()

        # --- ** Correction: Access the list inside the 'data' key ** ---
        if isinstance(api_response, dict) and api_response.get("status") == "ok" and "data" in api_response:
            clan_list = api_response["data"] # Get the actual list of clans
            print(f"Successfully parsed JSON. Found 'data' key containing {len(clan_list)} clans.")

            # Let's print the first clan's data structure from the list (if available)
            if isinstance(clan_list, list) and len(clan_list) > 0:
                 # Use the correct field names "Name" and "Points" based on your example
                 first_clan = clan_list[0]
                 print(f"Data structure for first clan (Name, Points): {first_clan.get('Name')}, {first_clan.get('Points')}")
            else:
                 print(f"The 'data' key does not contain a list or is empty.")

            return clan_list # Return the list of clans

        else:
            print("API response status is not 'ok' or 'data' key is missing.")
            print(f"Full API Response Structure: {api_response}")
            return None

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.content}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
        return None
    except Exception as e:
        import traceback
        print(f"An unexpected error occurred during API fetch/parse:")
        print(f"Error Type: {type(e)}")
        print(f"Error Details: {e}")
        traceback.print_exc()
        return None

# --- Database Insertion ---
def insert_clan_data(clan_list):
    """Inserts time-series data into 'clans' and updates 'clan_details'."""
    if not clan_list:
        print("No clan data provided to insert.")
        return 0

    print(f"Processing insert/update for {len(clan_list)} clans...")
    conn = None
    clans_inserted_count = 0
    details_updated_count = 0
    processed_count = 0
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        cursor = conn.cursor()
        current_timestamp = datetime.datetime.now().isoformat() # Timestamp for this batch

        # --- SQL Statements ---
        check_sql = "SELECT 1 FROM clans WHERE clan_name = ? LIMIT 1"
        # Updated clans insert
        insert_clans_sql = """
        INSERT INTO clans (clan_name, current_points, members, timestamp, first_seen)
        VALUES (?, ?, ?, ?, ?);
        """
        # Details insert/update (using REPLACE)
        insert_details_sql = """
        INSERT OR REPLACE INTO clan_details
            (clan_name, icon, country_code, member_capacity, created_timestamp_api, last_checked)
        VALUES (?, ?, ?, ?, ?, ?);
        """

        # --- Process each clan ---
        for clan in clan_list:
            processed_count += 1
            clan_name = clan.get("Name")
            clan_points = clan.get("Points")
            members_count = clan.get("Members") # Get members count

            if clan_name is None or clan_points is None: # Members can be optional? Add if needed.
                print(f"Skipping clan due to missing Name or Points: {clan}")
                continue

            try:
                # --- Handle 'clans' table insertion ---
                cursor.execute(check_sql, (clan_name,))
                exists = cursor.fetchone()
                first_seen_timestamp = None
                if not exists:
                    first_seen_timestamp = current_timestamp

                clans_data_tuple = (clan_name, clan_points, members_count, current_timestamp, first_seen_timestamp)
                cursor.execute(insert_clans_sql, clans_data_tuple)
                clans_inserted_count += 1

                # --- Handle 'clan_details' table insertion/update ---
                icon = clan.get("Icon")
                country_code = clan.get("CountryCode")
                capacity = clan.get("MemberCapacity")
                created_api = clan.get("Created") # API 'Created' timestamp

                details_data_tuple = (clan_name, icon, country_code, capacity, created_api, current_timestamp)
                cursor.execute(insert_details_sql, details_data_tuple)
                # We don't easily know if it was INSERT or REPLACE, just count executions
                details_updated_count += 1

            except sqlite3.Error as e:
                 print(f"Error processing clan '{clan_name}': {e}")
                 continue # Continue with the next clan

        # Commit all changes after processing all clans
        if clans_inserted_count > 0 or details_updated_count > 0:
             conn.commit()
             print(f"Commit successful: {clans_inserted_count} rows added to 'clans', {details_updated_count} rows inserted/replaced in 'clan_details'.")
        else:
             print("No records were modified in this cycle.")


    except sqlite3.Error as e:
        print(f"An overall database error occurred during insertion phase: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            print(f"Database connection closed. Processed {processed_count} clans from API.")

    return clans_inserted_count # Return count for the main time-series table


# --- Main Execution ---

# Set up the database once at the beginning
print("Performing initial database setup...")
setup_database()
print("Initial setup complete. Starting data fetch loop...")

# Main loop to fetch and insert data periodically
while True:
    print(f"\n--- Starting new fetch cycle at {datetime.datetime.now()} ---")

    # Fetch the data
    clans = fetch_clan_data() # This is your list of clan dictionaries

    # Check if data was fetched successfully
    if clans:
        print(f"Successfully retrieved {len(clans)} clan entries from the API.")
        # Insert the data
        insert_clan_data(clans)
    else:
        print("Failed to retrieve clan data from the API this cycle.")

    # Wait for 2 minutes (120 seconds) before the next cycle
    wait_seconds = 120
    print(f"--- Cycle complete. Waiting for {wait_seconds} seconds... ---")
    time.sleep(wait_seconds)