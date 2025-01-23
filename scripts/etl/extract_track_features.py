import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import dotenv
import time
import pandas as pd

# Load environment variables
dotenv.load_dotenv(dotenv_path="../../env/.env")

# Set the appropriate scope to read track audio features
scope = "user-library-read user-read-private playlist-read-private"  # Update the scope

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REDIRECT_URI = os.environ.get('REDIRECT_URI')

# Initialize Spotipy
sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=scope
    )
)

def fetch_audio_features(track_id):
    """Fetch audio features for a track."""
    try:
        audio_features = sp.audio_features(track_id)
        return audio_features[0] if audio_features else {}
    except spotipy.exceptions.SpotifyException as e:
        print(f"Error fetching audio features for track {track_id}: {e}")
        return {}

# Load listening history CSV (from the extract_listening_history.py script)
input_file = "../../data/listening_history.csv"
df = pd.read_csv(input_file)

# Initialize empty list for track features
track_features_data = []

# Fetch audio features for each track in the listening history
for _, row in df.iterrows():
    track_id = row['Track ID']
    
    # Fetch the audio features for this track
    audio_features = fetch_audio_features(track_id)
    
    # Add the track's audio features to the list
    track_features_data.append({
        "Track ID": track_id,
        "Danceability": audio_features.get('danceability', None),
        "Energy": audio_features.get('energy', None),
        "Speechiness": audio_features.get('speechiness', None),
        "Acousticness": audio_features.get('acousticness', None),
        "Instrumentalness": audio_features.get('instrumentalness', None),
        "Liveness": audio_features.get('liveness', None),
        "Valence": audio_features.get('valence', None),
        "Tempo": audio_features.get('tempo', None),
        "Key": audio_features.get('key', None),
        "Mode": audio_features.get('mode', None),
        "Time Signature": audio_features.get('time_signature', None),
        "Track Name": row['Track Name'],
        "Artist ID": row['Artist ID'],
        "Played At": row['Played At']
    })
    
    time.sleep(1)  # Optional: Pause to avoid rate-limiting

# Convert the track features data into a Pandas DataFrame
track_features_df = pd.DataFrame(track_features_data)

# Save DataFrame to CSV file
output_dir = "../../data"
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "track_features.csv")
track_features_df.to_csv(output_file, index=False, encoding='utf-8')

print(f"Track features saved to {output_file}")
