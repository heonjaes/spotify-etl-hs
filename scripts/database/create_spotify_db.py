import os
import psycopg2
from psycopg2.errors import OperationalError, DuplicateDatabase
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path="../.env")


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
    Create the 'spotify_db' database if it doesn't already exist.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    if not all([db_user, db_password, db_host]):
        raise ValueError("Environment variables DB_USER, DB_PASSWORD, and DB_HOST must be set.")

    try:
        # Connect to the default 'postgres' database first
        conn = psycopg2.connect(
            dbname="postgres", user=db_user, password=db_password, host=db_host
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # Create 'spotify_db' if it does not exist
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = 'spotify_db';")
        if cursor.fetchone() is None:
            cursor.execute("CREATE DATABASE spotify_db;")
            print("Database 'spotify_db' created successfully.")
        else:
            print("Database 'spotify_db' already exists.")

    except Exception as e:
        print(f"Error creating database: {e}")
    finally:
        cursor.close()
        conn.close()


def create_tables():
    """
    Create tables in the 'spotify_db' database.
    """
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    if not all([db_user, db_password, db_host]):
        raise ValueError("Environment variables DB_USER, DB_PASSWORD, and DB_HOST must be set.")

    try:
        # Now connect to the newly created 'spotify_db'
        conn = get_db_connection('spotify_db', db_user, db_password, db_host)
        cursor = conn.cursor()

        # Create Artists Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR PRIMARY KEY,
                artist_name TEXT NOT NULL,
                genres JSONB,
                followers INT,
                popularity INT
            );
        """)

        # Create Tracks Table (Fixing the incorrect foreign key reference)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id VARCHAR PRIMARY KEY,
                track_name TEXT NOT NULL,
                album_name TEXT,
                popularity INT,
                duration_ms INT,
                album_release_date DATE,
                explicit BOOLEAN
            );
        """)

        # Create Listening History Table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS listening_history (
                played_at TIMESTAMPTZ NOT NULL,
                track_id VARCHAR NOT NULL,
                artist_ids TEXT NOT NULL,
                FOREIGN KEY (track_id) REFERENCES tracks(track_id),
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
    create_db()  # Ensure the database exists before connecting
    create_tables()  # Create tables after the database is available
