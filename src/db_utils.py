import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


def get_db_connection():
    """Create and return a database connection using credentials from .env file."""
    USER = os.getenv("DB_USER")
    PASSWORD = os.getenv("DB_PASSWORD")
    HOST = os.getenv("DB_HOST")
    PORT = os.getenv("DB_PORT")
    DBNAME = os.getenv("DB_NAME")
    
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    return connection


def insert_dataframe_to_db(df: pd.DataFrame, schema: str, table_name: str) -> dict:
    """
    Insert a pandas DataFrame into a PostgreSQL table.
    
    Args:
        df: pandas DataFrame to insert
        schema: Database schema name (e.g., 'public')
        table_name: Name of the target table
        
    Returns:
        dict with 'success' (bool), 'message' (str), and 'rows_inserted' (int)
    """
    connection = None
    cursor = None
    
    try:
        # Establish connection
        connection = get_db_connection()
        cursor = connection.cursor()
        
        # Get column names from DataFrame
        columns = df.columns.tolist()
        columns_str = ", ".join(columns)
        
        # Create placeholders for VALUES clause
        placeholders = ", ".join(["%s"] * len(columns))
        
        # Build the INSERT query
        query = f"""
            INSERT INTO {schema}.{table_name} ({columns_str})
            VALUES ({placeholders})
        """
        
        # Convert DataFrame to list of tuples (each row)
        # Replace NaN with None for proper NULL handling in PostgreSQL
        data = [tuple(None if pd.isna(x) else x for x in row) for row in df.itertuples(index=False, name=None)]
        
        # Execute batch insert for better performance
        execute_batch(cursor, query, data, page_size=1000)
        
        # Commit the transaction
        connection.commit()
        
        rows_inserted = len(data)
        
        return {
            "success": True,
            "message": f"Successfully inserted {rows_inserted} rows into {schema}.{table_name}",
            "rows_inserted": rows_inserted
        }
        
    except Exception as e:
        if connection:
            connection.rollback()
        return {
            "success": False,
            "message": f"Failed to insert data: {str(e)}",
            "rows_inserted": 0
        }
        
    finally:
        # Clean up
        if cursor:
            cursor.close()
        if connection:
            connection.close()
