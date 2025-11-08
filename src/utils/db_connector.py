import psycopg2
import psycopg2.extras
import configparser
import sys
import json

class DatabaseConnector:
    """A dedicated class to handle all PostgreSQL database interactions."""
    
    def __init__(self, config_path='config.ini'):
        """Initializes the connector and establishes a database connection."""
        self.conn = None
        try:
            config = configparser.ConfigParser()
            config.read(config_path)
            
            self.conn = psycopg2.connect(
                host=config['POSTGRES']['HOST'],
                port=config['POSTGRES']['PORT'],
                dbname=config['POSTGRES']['DATABASE'],
                user=config['POSTGRES']['USER'],
                password=config['POSTGRES']['PASSWORD']
            )
            print("✅ Database connection established successfully.")
        except psycopg2.OperationalError as e:
            print(f"❌ Critical Error: Could not connect to the database.")
            print(f"   Please ensure the PostgreSQL service is running and accessible.")
            print(f"   Error details: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"❌ An unexpected error occurred during DB connection: {e}")
            sys.exit(1)

    def fetch_one_as_dict(self, query, params=None):
        """Executes a query and returns a single result as a dictionary."""
        if not self.conn:
            print("⚠️ Cannot fetch data, no database connection.")
            return None
            
        try:
            # Use a RealDictCursor to get results as dictionaries
            with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                return cur.fetchone()
        except (psycopg2.DatabaseError) as e:
            print(f"❌ Database query failed: {e}")
            # In a real app, you might want to try reconnecting here
            self.conn.rollback() 
            return None

    def store_event(self, event_data):
        """Stores an enriched event in the event_logs table."""
        if not self.conn:
            print("⚠️ Cannot store event, no database connection.")
            return False
            
        try:
            # Extract data for indexed columns
            event_type = event_data.get('eventType')
            timestamp = event_data.get('timestamp')
            
            # Convert event_data dict to a JSON string for insertion
            event_json = json.dumps(event_data)
            
            query = """
                INSERT INTO event_logs (event_type, timestamp, event_data)
                VALUES (%s, %s, %s)
            """
            with self.conn.cursor() as cur:
                cur.execute(query, (event_type, timestamp, event_json))
            self.conn.commit()
            return True
            
        except (psycopg2.DatabaseError) as e:
            print(f"❌ Database insert failed for event: {e}")
            self.conn.rollback() 
            return False
        except Exception as e:
            print(f"❌ An error occurred during event storage: {e}")
            return False

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()
            print("🔌 Database connection closed.")


# A small, focused class DatabaseConnector that centralizes PostgreSQL connection management and basic querying.

# Reads DB connection settings from a config.ini file using configparser.

# Establishes a psycopg2 connection in __init__, printing success or exiting on failure.

# Provides fetch_one_as_dict(query, params) to run a query and return a single row as a Python dict (via RealDictCursor).

# Handles DB errors by printing an error, rolling back the transaction, and returning None.

# Exposes close() to cleanly close the DB connection.