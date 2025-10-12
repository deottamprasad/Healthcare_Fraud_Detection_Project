import psycopg2
import psycopg2.extras
import configparser
import sys

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