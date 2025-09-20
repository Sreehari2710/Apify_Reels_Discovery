import os
import time
from flask import Flask, render_template, request, jsonify, Response, send_file
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials # Changed to service_account instead of generic Credentials
from googleapiclient.discovery import build
import io
import csv

from config import APIFY_TOKEN, HASHTAG_ACTOR_ID, BRANDPAGE_ACTOR_ID, TAGGED_ACTOR_ID, PROFILE_ACTOR_ID, GOOGLE_SHEET_ID, SERVICE_ACCOUNT_FILE, REDIS_URL
from utils import normalize_hashtags, parse_csv_column, make_apify_request, extract_contact_info_from_bio

# Load environment variables
load_dotenv()

if not APIFY_TOKEN:
    raise RuntimeError("Set your APIFY_TOKEN in environment or .env file")

app = Flask(__name__)

from celery import Celery

def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=REDIS_URL,
        backend=REDIS_URL
    )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)


# ----------------------------
# HELPER FUNCTIONS FOR GOOGLE SHEETS
# ----------------------------
def get_gsheet_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"Service account file not found at {SERVICE_ACCOUNT_FILE}")
        return None
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def append_to_gsheet(data):
    try:
        service = get_gsheet_service()
        if service and GOOGLE_SHEET_ID:
            body = {"values": data}
            service.values().append(
                spreadsheetId=GOOGLE_SHEET_ID,
                range="Sheet1!A1",
                valueInputOption="RAW",
                body=body
            ).execute()
    except Exception as e:
        print(f"Failed to append to Google Sheet: {e}")


# ----------------------------
# TASK STATUS CHECK ENDPOINT
# ----------------------------
@app.route("/status/<task_id>")
def task_status(task_id):
    task = celery.AsyncResult(task_id)
    if task.state == 'PENDING':
        response = {
            'state': task.state,
            'status': 'Pending...'}
    elif task.state != 'FAILURE':
        response = {
            'state': task.state,
            'status': task.info.get('status', 'Processing...'),
            'result': task.info.get('result', None)
        }
        if 'result' in task.info:
            response['result'] = task.info['result']
    else:
        # Something went wrong in the background job
        response = {
            'state': task.state,
            'status': str(task.info),  # this is the exception raised
        }
    return jsonify(response)


# ----------------------------
# DOWNLOAD TASK RESULT ENDPOINT
# ----------------------------
@app.route("/download/<task_id>/<filename>")
def download_task_result(task_id, filename):
    task = celery.AsyncResult(task_id)
    if task.state == 'SUCCESS':
        csv_content = task.result # This assumes the task returns the CSV content as a string
        csv_bytes = io.BytesIO(csv_content.encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename)
    elif task.state == 'PENDING' or task.state == 'STARTED':
        return Response("Task is still processing.", status=202)
    else:
        return Response(f"Task {task_id} failed or not found.", status=404)

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
