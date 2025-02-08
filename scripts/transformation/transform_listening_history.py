import os
import pandas as pd
import json
from scripts.extraction.extract_track_features import get_latest_listening_history_file

# Define directories (aligned with Docker-mounted volumes)
RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/spotify_api"))
PROCESSED_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/processed"))


def read_json_file(file_path):
    """Reads a JSON file and returns it as a Pandas DataFrame."""
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return pd.json_normalize(data)
    return pd.DataFrame()


def transform_listening_history():
    """
    Reads raw listening history JSON files, extracts key features, and saves as Parquet.
    """
    # Fetch the latest history files
    latest_history = get_latest_listening_history_file()
    track_features = latest_history.replace("listening_history", "track_features")
    artist_features = latest_history.replace("listening_history", "artist_features")

    # Read the JSON files into Pandas DataFrames
    df_history = read_json_file(os.path.join(RAW_DATA_DIR, latest_history))
    df_track = read_json_file(os.path.join(RAW_DATA_DIR, track_features))
    df_artist = read_json_file(os.path.join(RAW_DATA_DIR, artist_features))

    if df_history.empty or df_track.empty or df_artist.empty:
        print("One or more required files are missing or empty. Exiting.")
        return

    # Extract key features for Listening History Fact Table
    listening_history_df = df_history[["played_at", "track.id", "track.artists"]].copy()
    listening_history_df.rename(columns={"track.id": "track_id", "track.artists": "artist_ids"}, inplace=True)
    listening_history_df["artist_ids"] = listening_history_df["artist_ids"].apply(
        lambda x: ",".join([artist["id"] for artist in x]) if isinstance(x, list) else "")

    # Save as Parquet
    listening_history_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, "listening_history_fact.parquet"), index=False)

    # Extract dimensional features for Track Dimension Table
    track_dimension_df = df_track[
        ["id", "name", "popularity", "duration_ms", "album.release_date", "explicit", "album.name"]]
    track_dimension_df.rename(columns={"id": "track_id", "name": "track_name", "album.name": "album_name", 'album.release_date': 'album_release_date'},
                              inplace=True)
    track_dimension_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, "track_dimension.parquet"), index=False)

    # Extract dimensional features for Artist Dimension Table
    artist_dimension_df = df_artist[["id", "name", "popularity", "followers.total", "genres"]]
    artist_dimension_df.rename(columns={"id": "artist_id", "name": "artist_name", "popularity": "popularity",
                                        "followers.total": "followers"}, inplace=True)
    
    # Convert the genres list to a JSON string
    artist_dimension_df["genres"] = artist_dimension_df["genres"].apply(lambda x: json.dumps(x) if isinstance(x, list) else None)
    
    artist_dimension_df.to_parquet(os.path.join(PROCESSED_DATA_DIR, "artist_dimension.parquet"), index=False)

    print(f"Data transformed and saved to {PROCESSED_DATA_DIR}/listening_history_fact.parquet")
    print(f"Track dimension data saved to {PROCESSED_DATA_DIR}/track_dimension.parquet")
    print(f"Artist dimension data saved to {PROCESSED_DATA_DIR}/artist_dimension.parquet")


# Only run when script is executed directly
if __name__ == "__main__":
    transform_listening_history()
