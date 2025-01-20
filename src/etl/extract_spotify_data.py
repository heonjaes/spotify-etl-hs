import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import dotenv

dotenv.load_dotenv(dotenv_path="../../env/.env")

# Set the appropriate scope
scope = "user-read-recently-played user-top-read"

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
REDIRECT_URI = os.environ.get('REDIRECT_URI')

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        redirect_uri=REDIRECT_URI,
        scope=scope
    )
)

# Fetch recently played tracks
recent_tracks = sp.current_user_recently_played(limit=50)
for idx, item in enumerate(recent_tracks['items']):
    track = item['track']
    print(idx, item)
    # print(f"{idx + 1}. {track['artists'][0]['name']} - {track['name']}")

