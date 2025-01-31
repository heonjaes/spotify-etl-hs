import os
import pandas as pd
import time
from datetime import datetime
from scripts.auth.connect_spotify_api import connect_to_spotify_api

RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw"))


def get_latest_listening_history_file():
    """Finds the most recent listening history file."""
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.startswith("listening_history_") and f.endswith(".csv")]
    if not files:
        raise FileNotFoundError("No listening history file found.")
    latest_file = max(files, key=lambda f: datetime.strptime(f.split("_")[-1].split(".")[0], "%Y-%m-%d"))
    return os.path.join(RAW_DATA_DIR, latest_file)


def extract_unique_artist_ids(file_path):
    """Extracts unique Artist IDs from the listening history file."""
    df = pd.read_csv(file_path)
    return df["Artist ID"].drop_duplicates().tolist()


def fetch_artist_details(artist_ids):
    """Fetches artist details using the Spotify API in batches of 50."""
    scope = "user-read-recently-played"
    sp = connect_to_spotify_api(scope=scope)

    artist_data = []
    batch_size = 50  # Spotify API allows up to 50 artists per request

    for i in range(0, len(artist_ids), batch_size):
        batch = artist_ids[i:i + batch_size]
        try:
            response = sp.artists(batch)
            for artist in response["artists"]:
                artist_data.append({
                    "Artist ID": artist["id"],
                    "Artist Name": artist["name"],
                    "Genres": ", ".join(artist["genres"]),
                    "Followers": artist["followers"]["total"],
                    "Popularity": artist["popularity"],
                    "Image": artist["images"][0]["url"] if artist["images"] else None
                })
        except Exception as e:
            print(f"Error fetching batch starting at index {i}: {e}")

        time.sleep(1)  # Avoid rate-limiting

    return artist_data


def save_artist_details(artist_data, history_file):
    """Saves artist details to a processed CSV file."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    file_name = os.path.basename(history_file).replace("listening_history", "artist_details")
    output_file = os.path.join(RAW_DATA_DIR, file_name)

    pd.DataFrame(artist_data).to_csv(output_file, index=False)
    print(f"Artist details saved to {output_file}")


def extract_artist_features():
    """Main function to extract and save artist details."""
    history_file = get_latest_listening_history_file()
    artist_ids = extract_unique_artist_ids(history_file)
    artist_data = fetch_artist_details(artist_ids)
    save_artist_details(artist_data, history_file)


if __name__ == "__main__":
    extract_artist_features()
