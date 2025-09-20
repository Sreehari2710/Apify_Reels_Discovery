import os
from dotenv import load_dotenv

load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HASHTAG_ACTOR_ID = os.getenv("HASHTAG_ACTOR_ID")
BRANDPAGE_ACTOR_ID = os.getenv("BRANDPAGE_ACTOR_ID")
TAGGED_ACTOR_ID = os.getenv("TAGGED_ACTOR_ID")
PROFILE_ACTOR_ID = os.getenv("PROFILE_ACTOR_ID")
YOUTUBE_ACTOR_ID = os.getenv("YOUTUBE_ACTOR_ID")  
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_FILE = "service_account.json"
