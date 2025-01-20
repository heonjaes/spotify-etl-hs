import pandas as pd
import os
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import dotenv
import time

# Load environment variables
dotenv.load_dotenv(dotenv_path="../../env/.env")

# Set the appropriate scope to read user's listening history
scope = "user-read-recently-played"

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

def fetch_artist_details(artist_id):
    """Fetches details about an artist using their Spotify ID."""
    try:
        artist = sp.artist(artist_id)
        return {
            "Artist ID": artist_id,
            "Artist Name": artist['name'],
            "Genres": ", ".join(artist['genres']),
            "Followers": artist['followers']['total'],
            "Popularity": artist['popularity']
        }
    except Exception as e:
        print(f"Error fetching details for artist {artist_id}: {e}")
        return None

# Step 1: Load the listening history data
input_file = '../../data/listening_history.csv'

# Check if file exists
if not os.path.exists(input_file):
    raise FileNotFoundError(f"The file {input_file} was not found. Make sure you fetch the listening history first.")

# Load the listening history data into a DataFrame
track_df = pd.read_csv(input_file)

# Step 2: Extract artist details
artist_data = []
unique_artist_ids = track_df['Artist ID'].unique()  # Avoid duplicates

for artist_id in unique_artist_ids:
    artist_details = fetch_artist_details(artist_id)
    if artist_details:
        artist_data.append(artist_details)

    print(artist_details['name'])
    time.sleep(1)  # Optional: Pause to avoid rate-limiting

# Step 3: Create a DataFrame for artist data
artist_df = pd.DataFrame(artist_data)

# Step 4: Save the artist data to a CSV file
os.makedirs("../../data", exist_ok=True)  # Ensure the directory exists
artist_df.to_csv('../../data/artists.csv', index=False, encoding='utf-8')

print(f"Artist details saved to ../../data/artists.csv")
