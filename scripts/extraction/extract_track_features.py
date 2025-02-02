import os
import json
from datetime import datetime
from scripts.auth.connect_spotify_api import connect_to_spotify_api

RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/spotify_api"))


def get_latest_listening_history_file():
    """Finds the most recent listening history file."""
    files = [f for f in os.listdir(RAW_DATA_DIR) if f.startswith("listening_history_") and f.endswith(".json")]
    if not files:
        raise FileNotFoundError("No listening history file found.")
    latest_file = max(files, key=lambda f: datetime.strptime(f.split("_")[-1].split(".")[0], "%Y-%m-%dT%H-%M-%S"))
    return os.path.join(RAW_DATA_DIR, latest_file)


def extract_raw_listening_history(file_path):
    """Extracts raw listening history data from the JSON file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def fetch_track_features(track_ids):
    """Fetch track features using the Spotify API."""
    scope = "user-library-read"
    sp = connect_to_spotify_api(scope=scope)

    track_features = []
    batch_size = 50
    for i in range(0, len(track_ids), batch_size):
        batch = track_ids[i:i + batch_size]
        response = sp.tracks(batch)
        for track in response["tracks"]:
            track_features.append(track)
    return track_features


def save_track_features_as_json(track_features, history_file):
    """Saves track features to a JSON file."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    file_name = os.path.basename(history_file).replace("listening_history", "track_features")
    output_file = os.path.join(RAW_DATA_DIR, file_name)

    with open(output_file, 'w') as f:
        json.dump(track_features, f, indent=4)
    print(f"Track features saved to {output_file}")


def extract_track_features():
    """Main function to extract and save track features."""
    history_file = get_latest_listening_history_file()
    listening_history = extract_raw_listening_history(history_file)

    # Extract unique Track IDs from the raw listening history
    track_ids = list({track["track"]["id"] for track in listening_history if "track" in track})

    # Fetch track features using the extracted Track IDs
    track_features = fetch_track_features(track_ids)

    # Save the track features to a JSON file
    save_track_features_as_json(track_features, history_file)


if __name__ == "__main__":
    extract_track_features()
