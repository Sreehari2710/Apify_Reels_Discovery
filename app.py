import os
import time
from flask import Flask, render_template, Response
from dotenv import load_dotenv
import logging

from config import APIFY_TOKEN

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

if not APIFY_TOKEN:
    raise RuntimeError("Set your APIFY_TOKEN in environment or .env file")
app = Flask(__name__)

# ----------------------------
# Import scraper Blueprints
# ----------------------------
from scrapers.hashtag_scraper import bp_hashtag
from scrapers.brandpage_reels_scraper import bp_brandpage_reels
from scrapers.brandpage_tagged_scraper import bp_brandpage_tagged
from scrapers.profile_scraper import bp_profile
from scrapers.youtube_scraper import bp_youtube

# ----------------------------
# Register Blueprints
# ----------------------------
app.register_blueprint(bp_hashtag)
app.register_blueprint(bp_brandpage_reels)
app.register_blueprint(bp_brandpage_tagged)
app.register_blueprint(bp_profile)
app.register_blueprint(bp_youtube)

# ----------------------------
# Health & Page Routes
# ----------------------------
@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "healthy", "timestamp": time.time()}

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/filter", methods=["GET"])
def filter_page():
    return render_template("filter.html")

# ----------------------------
# Run Server
# ----------------------------
if __name__ == "__main__":
    app.run(debug=True)
