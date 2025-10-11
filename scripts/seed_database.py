import psycopg2
import pandas as pd
import configparser
import os
import sys

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    config = configparser.ConfigParser()
    # Assuming the script is run from the root directory
    config.read('config.ini')
    
    try:
        conn = psycopg2.connect(
            host=config['POSTGRES']['HOST'],
            port=config['POSTGRES']['PORT'],
            dbname=config['POSTGRES']['DATABASE'],
            user=config['POSTGRES']['USER'],
            password=config['POSTGRES']['PASSWORD']
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"❌ Critical Error: Could not connect to the database.")
        print(f"   Please ensure PostgreSQL is running and accessible at the configured address.")
        print(f"   Error details: {e}")
        sys.exit(1) # Exit the script if DB connection fails

def create_tables(conn):
    """Creates the staff, patients, and devices tables in the database."""
    commands = (
        """
        DROP TABLE IF EXISTS staff, patients, devices CASCADE;
        """,
        """
        CREATE TABLE staff (
            user_id VARCHAR(50) PRIMARY KEY,
            full_name VARCHAR(100),
            role VARCHAR(50),
            department VARCHAR(50),
            access_level INTEGER,
            is_on_shift BOOLEAN
        );
        """,
        """
        CREATE TABLE patients (
            patient_id VARCHAR(50) PRIMARY KEY,
            full_name VARCHAR(100),
            current_room VARCHAR(20),
            assigned_doctor_id VARCHAR(50) REFERENCES staff(user_id)
        );
        """,
        """
        CREATE TABLE devices (
            device_id VARCHAR(50) PRIMARY KEY,
            device_type VARCHAR(50),
            location VARCHAR(50),
            department VARCHAR(50),
            ip_address VARCHAR(15)
        );
        """
    )
    
    try:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
        conn.commit()
        print("✅ Tables created successfully (staff, patients, devices).")
    except (psycopg2.DatabaseError) as e:
        print(f"❌ Error creating tables: {e}")
        conn.rollback()
        raise e

def populate_table(conn, table_name, file_path):
    """Populates a table from a given CSV file."""
    try:
        df = pd.read_csv(file_path)
        # Prepare the data for fast insertion
        buffer = ','.join(df.columns)
        # Create an in-memory string buffer for the CSV data
        from io import StringIO
        sio = StringIO()
        df.to_csv(sio, index=False, header=False, sep='\t')
        sio.seek(0)

        with conn.cursor() as cur:
            # Use COPY FROM for highly efficient bulk insertion
            cur.copy_expert(f"COPY {table_name} ({buffer}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t')", sio)
        conn.commit()
        print(f"   -> Successfully populated '{table_name}' with {len(df)} records.")
    except (Exception) as e:
        print(f"❌ Error populating table {table_name}: {e}")
        conn.rollback()
        raise e

def main():
    """Main function to orchestrate the database seeding process."""
    print("\n--- Starting Database Seeding Process ---")
    conn = None
    try:
        conn = get_db_connection()
        create_tables(conn)

        print("\nPopulating tables from enrichment CSVs...")
        base_path = 'data/enrichment/'
        populate_table(conn, 'staff', os.path.join(base_path, 'staff.csv'))
        populate_table(conn, 'patients', os.path.join(base_path, 'patients.csv'))
        populate_table(conn, 'devices', os.path.join(base_path, 'devices.csv'))

        print("\n✅ Database seeding completed successfully!")
    except Exception as e:
        print(f"\n❌ A critical error occurred during seeding: {e}")
    finally:
        if conn:
            conn.close()
            print("\n--- Database connection closed. ---")

if __name__ == '__main__':
    main()

    

# This script is designed to initialize and populate a PostgreSQL database for a hospital system.

# 1. Connects to PostgreSQL using credentials from a config.ini file.

# 2. Creates three tables: staff, patients, and devices, dropping them first if they exist.

# 3. Populates these tables from CSV files located in data/enrichment/ using efficient bulk insertion.

# 4. Handles errors gracefully, prints clear messages, and ensures the DB connection is closed at the end.

# Essentially, it automates the database setup and seeding process for development or testing.