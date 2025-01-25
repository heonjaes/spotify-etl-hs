import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from dotenv import load_dotenv
import time
import pandas as pd
from datetime import datetime, timedelta

# Load environment variables
load_dotenv(dotenv_path="../../env/.env")

# Set the appropriate scope to read user's listening history
scope = "user-read-recently-played"

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = os.getenv('REDIRECT_URI')

# Initialize Spotipy
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=scope
    )
)

def fetch_tracks_since(after_timestamp):
    """Fetch up to 50 tracks played since the given timestamp."""
    results = sp.current_user_recently_played(limit=50, after=after_timestamp)
    return results['items']

# Ensure the 'data' directory exists
output_dir = "../../data/raw"
os.makedirs(output_dir, exist_ok=True)

# Initialize empty lists to hold data
track_data = []

# Calculate 'after' timestamps for the past 30 days
now = datetime.now()
after_timestamps = [
    int((now - timedelta(days=i)).timestamp() * 1000)  # Milliseconds since epoch
    for i in range(5, 0, -1)  # From 30 days ago to 1 day ago
]

# Fetch tracks for each day and collect data
for after in after_timestamps:
    day_tracks = fetch_tracks_since(after)
    print(f"Fetched {len(day_tracks)} tracks for after {datetime.fromtimestamp(after // 1000)}.")

    for item in day_tracks:
        track = item['track']
        artist = track['artists'][0]

        # Collect all possible track features
        track_data.append({
            "Track ID": track['id'],
            "Artist ID": artist['id'],
            "Track Name": track['name'],
            "Track URI": track['uri'],
            "Track Popularity": track['popularity'],
            "Track Duration": track['duration_ms'],
            "Track Preview URL": track['preview_url'],
            "Track External URL": track['external_urls']['spotify'],
            "Track Album Name": track['album']['name'],
            "Track Album ID": track['album']['id'],
            "Track Album URI": track['album']['uri'],
            "Track Album Release Date": track['album']['release_date'],
            "Track Album Type": track['album']['album_type'],
            "Track Album Total Tracks": track['album']['total_tracks'],
            "Track Album Images": track['album']['images'][0]['url'],
            "Played At": item['played_at']
        })

    time.sleep(1)  # Optional: Pause to avoid rate-limiting

# Convert collected data into a Pandas DataFrame
track_df = pd.DataFrame(track_data)

# Save DataFrame to CSV file
output_file = os.path.join(output_dir, "listening_history.csv")
track_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"Data saved to {output_file}")
