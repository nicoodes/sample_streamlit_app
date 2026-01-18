import sys
import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

# Add src to path so we can import modules
sys.path.append(os.path.join(os.path.dirname(__file__), "src"))

from tenis_api import get_fixtures
from db_utils import insert_dataframe_to_db

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Debug: Print what we got (remove password from logs!)
print(f"DB_USER: {DB_USER is not None and len(DB_USER) > 0}")
print(f"DB_PASSWORD: {DB_PASSWORD is not None and len(DB_PASSWORD) > 0}")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_PORT: {DB_PORT}")
print(f"DB_NAME: {DB_NAME}")

def fetch_and_insert_today_fixtures():
    """
    Fetch today's fixtures and insert them into the database.
    
    Returns:
        dict with 'success' (bool) and 'message' (str)
    """
    # Get today's date in YYYY-MM-DD format
    today = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Fetching fixtures for {today}...")
    
    # Fetch fixtures for today
    df = get_fixtures(date_start=today, date_stop=today)
    
    if df is None or df.empty:
        return {
            "success": False,
            "message": f"No fixtures found for {today}"
        }
    
    print(f"Found {len(df)} fixtures. Inserting into database...")
    
    # Insert into database
    result = insert_dataframe_to_db(
        df=df,
        schema="sample",
        table_name="fixtures"
    )
    
    return result


if __name__ == "__main__":
    # Run the main function
    result = fetch_and_insert_today_fixtures()
    print(result["message"])
