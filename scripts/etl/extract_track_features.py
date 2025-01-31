import os
import pandas as pd
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


def extract_unique_track_ids(file_path):
    """Extracts unique Track IDs from the listening history file."""
    df = pd.read_csv(file_path)
    return df["Track ID"].drop_duplicates().tolist()


def fetch_track_features(track_ids):
    """Fetch track features using the Spotify API."""
    scope = "user-library-read"
    sp = connect_to_spotify_api(scope=scope)

    track_features = []
    batch_size = 50  # Spotify API allows up to 50 tracks per request
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i + batch_size]
        response = sp.tracks(batch)
        for track in response["tracks"]:
            track_features.append({
                "Track ID": track["id"],
                "Track Name": track["name"],
                "Artist Name": track["artists"][0]["name"],
                "Album Name": track["album"]["name"],
                "Album Image": track["album"]['images'][0]['url'] if track["album"]['images'] else None,
                "Release Date": track["album"]["release_date"],
                "Popularity": track["popularity"]
            })
    return track_features


def save_track_features(track_features, history_file):
    """Saves track features to a processed CSV file."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    file_name = os.path.basename(history_file).replace("listening_history", "track_features")
    output_file = os.path.join(RAW_DATA_DIR, file_name)

    pd.DataFrame(track_features).to_csv(output_file, index=False)
    print(f"Track features saved to {output_file}")


def extract_track_features():
    """Main function to extract and save track features."""
    history_file = get_latest_listening_history_file()
    track_ids = extract_unique_track_ids(history_file)
    track_features = fetch_track_features(track_ids)
    save_track_features(track_features, history_file)


if __name__ == "__main__":
    extract_track_features()
