import os
import json
import time
from scripts.auth.connect_spotify_api import connect_to_spotify_api
from scripts.extraction.extract_track_features import get_latest_listening_history_file

RAW_DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw/spotify_api"))


def extract_raw_listening_history(file_path):
    """Extracts raw listening history data from the JSON file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


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
                artist_data.append(artist)
        except Exception as e:
            print(f"Error fetching batch starting at index {i}: {e}")

        time.sleep(1)  # Avoid rate-limiting

    return artist_data


def save_artist_details_as_json(artist_data, history_file):
    """Saves artist details to a JSON file."""
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    file_name = os.path.basename(history_file).replace("listening_history", "artist_details")
    output_file = os.path.join(RAW_DATA_DIR, file_name)

    with open(output_file, 'w') as f:
        json.dump(artist_data, f, indent=4)
    print(f"Artist details saved to {output_file}")


def extract_artist_features():
    """Main function to extract and save artist details."""
    history_file = get_latest_listening_history_file()
    listening_history = extract_raw_listening_history(history_file)

    # Extract unique Artist IDs from the raw listening history
    artist_ids = list({track["track"]["artists"][0]["id"] for track in listening_history if "track" in track})

    # Fetch artist details using the extracted Artist IDs
    artist_data = fetch_artist_details(artist_ids)

    # Save the artist details to a JSON file
    save_artist_details_as_json(artist_data, history_file)


if __name__ == "__main__":
    extract_artist_features()
