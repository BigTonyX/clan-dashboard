import threading
import time
import logging
from logging.handlers import RotatingFileHandler
import sys
import traceback
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import clan_data_fetcher
import member_data_fetcher

# Configure logging with rotation
log_file = 'combined_fetcher.log'
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

class MongoManager:
    _instance = None
    
    def __init__(self):
        if not MongoManager._instance:
            self.client = None
            self.init_connection()
    
    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = MongoManager()
        return cls._instance
    
    def init_connection(self):
        try:
            if not self.client:
                self.client = MongoClient(
                    os.environ.get("MONGO_URI"),
                    maxPoolSize=50,  # Adjust based on needs
                    serverSelectionTimeoutMS=5000
                )
                # Test connection
                self.client.admin.command('ping')
                logger.info("MongoDB connection pool initialized")
        except ConnectionFailure as e:
            logger.error(f"Could not connect to MongoDB: {e}")
            raise
    
    def get_client(self):
        if not self.client:
            self.init_connection()
        return self.client
    
    def cleanup(self):
        if self.client:
            self.client.close()
            self.client = None
            logger.info("MongoDB connections closed")

class FetcherThread:
    def __init__(self, name, fetcher_main, mongo_client):
        self.name = name
        self.fetcher_main = fetcher_main
        self.mongo_client = mongo_client
        self.running = True
        self.thread = None

    def start(self):
        self.running = True
        self.thread = threading.Thread(
            target=self._run_fetcher,
            name=self.name,
            daemon=False
        )
        self.thread.start()

    def _run_fetcher(self):
        try:
            # Pass both the MongoDB client and running flag
            self.fetcher_main(self.mongo_client, lambda: self.running)
        except Exception as e:
            logger.error(f"{self.name} crashed: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    def stop(self):
        self.running = False

    def is_alive(self):
        return self.thread and self.thread.is_alive()

def main():
    """Main function to run both fetchers in separate threads."""
    logger.info("Starting combined fetcher")
    
    # Initialize MongoDB connection pool
    mongo_manager = MongoManager.get_instance()
    mongo_client = mongo_manager.get_client()
    
    # Create fetcher threads
    clan_fetcher = FetcherThread("ClanFetcher", clan_data_fetcher.main, mongo_client)
    member_fetcher = FetcherThread("MemberFetcher", member_data_fetcher.main, mongo_client)
    
    # Start both threads
    clan_fetcher.start()
    member_fetcher.start()
    
    try:
        while True:
            if not clan_fetcher.is_alive():
                logger.error("Clan fetcher thread died, restarting...")
                clan_fetcher.start()
            
            if not member_fetcher.is_alive():
                logger.error("Member fetcher thread died, restarting...")
                member_fetcher.start()
            
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal, initiating graceful shutdown...")
        
        # Stop fetchers
        clan_fetcher.stop()
        member_fetcher.stop()
        
        # Give threads time to complete current operations
        logger.info("Waiting up to 30 seconds for threads to complete...")
        clan_fetcher.thread.join(timeout=30)
        member_fetcher.thread.join(timeout=30)
        
        if clan_fetcher.is_alive() or member_fetcher.is_alive():
            logger.warning("Some threads did not shut down gracefully")
        
    except Exception as e:
        logger.error(f"Error in main thread: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        # Cleanup MongoDB connections
        mongo_manager.cleanup()
        logger.info("Combined fetcher stopped")

if __name__ == "__main__":
    # Load environment variables
    load_dotenv()
    main() 