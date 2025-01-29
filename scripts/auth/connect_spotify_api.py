import spotipy
from spotipy.oauth2 import SpotifyOAuth
import os
from dotenv import load_dotenv

def connect_to_spotify_api(scope="user-read-recently-played"):
    """
    Connect to Spotify API using OAuth.
    
    Args:
        scope (str): The scope of the access. Defaults to 'user-read-recently-played'.
        
    Returns:
        spotipy.Spotify: The authenticated Spotify API client.
    """
    # Load environment variables from .env file
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
    load_dotenv(env_path)

    CLIENT_ID = os.getenv('CLIENT_ID')
    CLIENT_SECRET = os.getenv('CLIENT_SECRET')
    REDIRECT_URI = os.getenv('REDIRECT_URI')
    
    # Initialize Spotipy with OAuth and provided scope
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            redirect_uri=REDIRECT_URI,
            scope=scope
        )
    )
    
    print(f"Spotify API connected successfully with scope: {scope}.")
    return sp

def main():
    sp = connect_to_spotify_api()

if __name__ == "__main__":
    main()