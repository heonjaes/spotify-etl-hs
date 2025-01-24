import os
import psycopg2
from psycopg2 import sql
from psycopg2.errors import OperationalError, DuplicateDatabase
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path="../../env/.env")

def get_db_connection(dbname, user, password, host, port=5432):
    """
    Establish a connection to the PostgreSQL database.
    """
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except OperationalError as e:
        print(f"Error connecting to database '{dbname}': {e}")
        raise

def create_db():
    """
    Create the 'spotify_data' database if it doesn't already exist.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    if not all([db_user, db_password, db_host]):
        raise ValueError("Environment variables DB_USER, DB_PASSWORD, and DB_HOST must be set.")

    conn = get_db_connection('postgres', db_user, db_password, db_host)
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE DATABASE spotify_data;")
        print("Database created successfully.")
    except DuplicateDatabase:
        print("Database 'spotify_data' already exists.")
    finally:
        cursor.close()
        conn.close()

def create_tables():
    """
    Create the 'artists' and 'tracks' tables in the 'spotify_data' database.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    if not all([db_user, db_password, db_host]):
        raise ValueError("Environment variables DB_USER, DB_PASSWORD, and DB_HOST must be set.")

    conn = get_db_connection('spotify_data', db_user, db_password, db_host)
    cursor = conn.cursor()

    try:
        # Create artists table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY,
            artist_name TEXT NOT NULL,
            genres TEXT,
            followers INT,
            popularity INT
        );
        """)

        # Create tracks table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS tracks (
            track_id VARCHAR PRIMARY KEY,
            track_name TEXT NOT NULL,
            artist_id VARCHAR NOT NULL,
            played_at TIMESTAMPTZ NOT NULL,
            FOREIGN KEY (artist_id) REFERENCES artists(artist_id),
            UNIQUE (track_id, played_at)
        );
        """)

        conn.commit()
        print("Tables created successfully.")
    except Exception as e:
        print(f"Error creating tables: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    create_db()
    create_tables()
