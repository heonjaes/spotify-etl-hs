import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
import dotenv 

dotenv.load_dotenv(dotenv_path = "../../env/.env")

scope = "user-library-read"

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET =  os.environ.get('CLIENT_SECRET')
REDIRECT_URI = os.environ.get('REDIRECT_URI')

sp = spotipy.Spotify(
    auth_manager=SpotifyOAuth(
        client_id = CLIENT_ID, 
        client_secret = CLIENT_SECRET, 
        redirect_uri = REDIRECT_URI,
        cache_path = None,
        scope=scope))

results = sp.current_user_saved_tracks()
for idx, item in enumerate(results['items']):
    track = item['track']
    print(idx, track['artists'][0]['name'], "-", track['name'])