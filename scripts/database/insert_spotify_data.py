import os
import json
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(dotenv_path="../../.env")

# Set up processed data directory
PROCESSED_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed"))

def insert_spotify_data():
    """Inserts Spotify data from Parquet files into a PostgreSQL database."""
    
    # Retrieve database credentials
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_name = os.getenv('DB_NAME')

    # Ensure all credentials are available
    if not all([db_user, db_password, db_host, db_name]):
        raise SystemExit("Error: Missing database environment variables.")

    try:
        # Establish database connection
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host
        )
        cursor = conn.cursor()

        # ✅ Insert data from artist_dimension.parquet
        artist_df = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, "artist_dimension.parquet"))
        artist_df = artist_df.where(pd.notna(artist_df), None)  # Replace NaN with None

        for _, row in artist_df.iterrows():
            genres_str = row["genres"] if row["genres"] else None  # Use the comma-separated string directly
            cursor.execute("""
                INSERT INTO artists (artist_id, artist_name, genres, followers, popularity)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id) DO NOTHING;
            """, (row["artist_id"], row["artist_name"], genres_str, row["followers"], row["popularity"]))

        # ✅ Insert data from track_dimension.parquet
        track_df = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, "track_dimension.parquet"))
        track_df = track_df.where(pd.notna(track_df), None)

        for _, row in track_df.iterrows():
            cursor.execute("""
                INSERT INTO tracks (track_id, track_name, album_name, popularity, duration_ms, album_release_date, explicit)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id) DO NOTHING;
            """, (
                row["track_id"], row["track_name"], row["album_name"], row["popularity"],
                row["duration_ms"], row["album_release_date"], row["explicit"]
            ))

        # ✅ Insert data from listening_history_fact.parquet
        listening_history_df = pd.read_parquet(os.path.join(PROCESSED_DATA_DIR, "listening_history_fact.parquet"))
        listening_history_df = listening_history_df.where(pd.notna(listening_history_df), None)

        for _, row in listening_history_df.iterrows():
            artist_ids_str = row["artist_ids"] if row["artist_ids"] else None  # Use the comma-separated string directly
            cursor.execute("""
                INSERT INTO listening_history (played_at, track_id, artist_ids)
                VALUES (%s, %s, %s)
                ON CONFLICT (played_at, track_id) DO NOTHING;
            """, (row["played_at"], row["track_id"], artist_ids_str))

        # Commit changes
        conn.commit()
        print("✅ Data inserted successfully.")

    except psycopg2.Error as db_err:
        print(f"❌ Database Error: {db_err}, {row}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        if conn:
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    insert_spotify_data()
