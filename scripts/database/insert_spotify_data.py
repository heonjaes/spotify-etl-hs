import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv(dotenv_path="../../env/.env")

def get_db_connection(dbname, user, password, host, port=5432):
    """Establish a connection to the PostgreSQL database."""
    try:
        return psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
    except psycopg2.OperationalError as e:
        print(f"Error connecting to database '{dbname}': {e}")
        raise

def insert_artist_data(artist_df):
    """Inserts artist data into the 'artists' table."""
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    conn = get_db_connection('spotify_data', db_user, db_password, db_host)
    cursor = conn.cursor()

    try:
        for index, row in artist_df.iterrows():
            cursor.execute("""
                INSERT INTO artists (artist_id, artist_name, genres, followers, popularity, image_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (artist_id) DO NOTHING;
            """, (row['Artist ID'], row['Artist Name'], row['Genres'], row['Followers'], row['Popularity'], row['Image']))
        conn.commit()
        print("Artist data inserted successfully.")
    except Exception as e:
        print(f"Error inserting artist data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def insert_track_data(track_df):
    """Inserts track data into the 'tracks' table."""
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')

    conn = get_db_connection('spotify_data', db_user, db_password, db_host)
    cursor = conn.cursor()

    try:
        for index, row in track_df.iterrows():
            cursor.execute("""
                INSERT INTO tracks (
                    track_id, track_name, artist_id, played_at, track_uri, track_popularity, track_duration_ms, 
                    track_preview_url, track_external_url, track_album_name, track_album_id, 
                    track_album_uri, track_album_release_date, track_album_type, track_album_total_tracks, 
                    track_album_image_url
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id, played_at) DO NOTHING;
            """, (
                row['Track ID'], row['Track Name'], row['Artist ID'], row['Played At'], row['Track URI'],
                row['Track Popularity'], row['Track Duration'], row['Track Preview URL'],
                row['Track External URL'], row['Track Album Name'], row['Track Album ID'],
                row['Track Album URI'], row['Track Album Release Date'], row['Track Album Type'],
                row['Track Album Total Tracks'], row['Track Album Images']
            ))
        conn.commit()
        print("Track data inserted successfully.")
    except Exception as e:
        print(f"Error inserting track data: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Load data from CSV
artist_df = pd.read_csv('../../data/artists.csv')
track_df = pd.read_csv('../../data/listening_history.csv')

# Insert data into the database
insert_artist_data(artist_df)
insert_track_data(track_df)
