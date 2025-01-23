import os
import psycopg2
from psycopg2 import sql
from psycopg2.errors import OperationalError

def get_db_connection(dbname, user, password, host, port=5432):
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except OperationalError as e:
        print(f"Error connecting to database: {e}")
        raise

def create_db():
    conn = get_db_connection('postgres', os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'))
    conn.autocommit = True
    cursor = conn.cursor()

    try:
        cursor.execute("CREATE DATABASE spotify_data;")
        print("Database created successfully.")
    except psycopg2.errors.DuplicateDatabase:
        print("Database already exists.")
    finally:
        cursor.close()
        conn.close()

def create_tables():
    conn = get_db_connection('spotify_data', os.getenv('DB_USER'), os.getenv('DB_PASSWORD'), os.getenv('DB_HOST'))
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
