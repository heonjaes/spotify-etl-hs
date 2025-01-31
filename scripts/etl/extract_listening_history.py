import os
import pandas as pd
from datetime import datetime, timezone
from scripts.auth.connect_spotify_api import connect_to_spotify_api

LAST_EXTRACTION_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/metadata/last_extraction.txt"))
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../data/raw"))

def get_last_extraction_timestamp():
    """
    Reads the last extraction timestamp from a file.
    If the file doesn't exist, return the start of 2025.
    """
    if os.path.exists(LAST_EXTRACTION_FILE):
        with open(LAST_EXTRACTION_FILE, "r") as f:
            return int(f.read().strip())  # Convert string timestamp to integer (milliseconds)
    return int(datetime(2025, 1, 1).timestamp() * 1000)  # Default: Start of 2025

def save_last_extraction_timestamp(timestamp):
    """Saves the last extraction timestamp to a file."""
    os.makedirs(os.path.dirname(LAST_EXTRACTION_FILE), exist_ok=True)
    with open(LAST_EXTRACTION_FILE, "w") as f:
        f.write(str(timestamp))


def extract_listening_history():
    """
    Extract all tracks played since the last extraction timestamp and update the record.
    """
    last_extraction_timestamp = get_last_extraction_timestamp()
    print(f"Last extraction timestamp: {last_extraction_timestamp}")

    scope = "user-read-recently-played"
    sp = connect_to_spotify_api(scope=scope)

    track_data = []
    limit = 50
    latest_timestamp = last_extraction_timestamp  # Set it to last_extraction_timestamp initially

    while True:
        # Convert milliseconds to datetime
        timestamp_dt = datetime.fromtimestamp(latest_timestamp / 1000, tz=timezone.utc)
        print(f"Fetching data after timestamp: {timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        # Fetch tracks from Spotify API
        response = sp.current_user_recently_played(limit=limit, after=latest_timestamp)

        if not response["items"]:
            print(f"No new tracks since {timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')}")
            break  # Stop if no new tracks are returned

        # print(response["items"][0]['track'])
        for item in response["items"]:
            track = item["track"]
            artist = track["artists"][0]

            # Add track data to list
            track_data.append({
                "Track ID": track["id"],
                "Artist ID": artist["id"],
                "Track Name": track["name"],
                "Artist Name": artist["name"],
                "Played At": item["played_at"],
            })

            # Update latest timestamp (most recent song played)
            played_at_timestamp = int(
                datetime.strptime(item["played_at"], "%Y-%m-%dT%H:%M:%S.%fZ")
                .replace(tzinfo=timezone.utc)
                .timestamp() * 1000  # Convert to milliseconds
            )
            latest_timestamp = max(latest_timestamp, played_at_timestamp)

        # No pagination condition: Keep going even if fewer than 50 items are returned
        print(f"Fetched {len(response['items'])} tracks")

    if track_data:
        # Save track data to CSV
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        df = pd.DataFrame(track_data)

        # Name to and from date of extraction
        from_date = datetime.fromtimestamp(last_extraction_timestamp / 1000, tz=timezone.utc).strftime('%Y-%m-%d')
        to_date = datetime.now().strftime('%Y-%m-%d')
        output_file = os.path.join(OUTPUT_DIR, f"listening_history_{from_date}_to_{to_date}.csv")
        df.to_csv(output_file, index=False)
        print(f"Data saved to {output_file}")

        # Save the latest timestamp to track progress
        save_last_extraction_timestamp(latest_timestamp)
    else:
        print("No tracks were fetched.")


