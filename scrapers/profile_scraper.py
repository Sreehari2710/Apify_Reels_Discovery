import io
import csv
import os
import pandas as pd
from flask import Blueprint, request, send_file, Response
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from utils import make_apify_request, extract_contact_info_from_bio
from config import APIFY_TOKEN, PROFILE_ACTOR_ID, GOOGLE_SHEET_ID, SERVICE_ACCOUNT_FILE

bp_profile = Blueprint("profile_scraper", __name__)

# ----------------- Google Sheet Helpers -----------------
def get_gsheet_service():
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
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

# ----------------- Categorize followers -----------------
def get_category(followers: int) -> str:
    if followers < 10_000:
        return "nano"
    elif 10_000 <= followers < 150_000:
        return "micro"
    elif 150_000 <= followers < 500_000:
        return "mid-tier"
    elif 500_000 <= followers < 1_000_000:
        return "macro"
    else:
        return "mega"

# ----------------- Fetch All Profiles -----------------
def fetch_profiles(usernames):
    url = f"https://api.apify.com/v2/acts/{PROFILE_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 600}
    try:
        results = make_apify_request(url, params, {"usernames": usernames})
        unique_results = {p.get("username"): p for p in results}.values()
        return list(unique_results)
    except Exception as e:
        print(f"Error fetching profiles: {e}")
        return []

# ----------------- Main Filter Route -----------------
@bp_profile.route("/filter-csv", methods=["POST"])
def filter_csv():
    try:
        if "csv_file" not in request.files:
            return Response("Upload a CSV", status=400)

        csv_file = request.files["csv_file"]
        df = pd.read_csv(csv_file)
        headers = set(df.columns.str.strip().str.lower())
        rows = df.to_dict("records")

        csv_type = None
        usernames = []
        query_map = {}  # username -> query

        # ----------------- Detect CSV type -----------------
        if {"hashtag", "username", "user_link", "caption_text"}.issubset(headers):
            csv_type = "hashtag"
            for row in rows:
                u = row.get("username", "").strip()
                if u:
                    usernames.append(u)
                    query_map[u] = row.get("hashtag", "")
        elif {"brandpage", "insta profile url", "collaborated account url", "reel url", "likes", "comments"}.issubset(headers):
            csv_type = "brandpage_reels"
            for row in rows:
                u = row.get("collaborated account url", "").strip("/").split("/")[-1]
                if u:
                    usernames.append(u)
                    query_map[u] = row.get("brandpage", "")
        elif {"brandpage", "owner_username", "reel_url", "likes", "comments", "shares", "views"}.issubset(headers):
            csv_type = "brandpage_tagged"
            for row in rows:
                u = row.get("owner_username", "").strip()
                if u:
                    usernames.append(u)
                    query_map[u] = row.get("brandpage", "")
        else:
            return Response("Unrecognized CSV format", status=400)

        usernames = list(set(usernames))
        if not usernames:
            return Response("No valid usernames found in CSV", status=400)

        # ----------------- Fetch profiles -----------------
        profiles = fetch_profiles(usernames)

        # ----------------- Prepare CSV -----------------
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
        writer.writerow([
            "query_type",
            "query",
            "username",
            "url",
            "followers",
            "categories",   # ✅ new column
            "postcount",
            "bio",
            "email",
            "phone"
        ])

        rows_to_append = []
        for p in profiles:
            username = p.get("username", "")
            bio = p.get("biography", "") or ""
            email, phone = extract_contact_info_from_bio(bio)
            query_value = query_map.get(username, request.form.get("query", ""))

            followers = int(p.get("followersCount") or 0)
            category = get_category(followers)

            row_data = [
                csv_type,
                query_value,
                username,
                f"https://www.instagram.com/{username}/",
                followers,
                category,
                p.get("postsCount", ""),
                bio.replace("\n", " "),
                email,
                phone
            ]
            writer.writerow(row_data)
            rows_to_append.append(row_data)

        append_to_gsheet(rows_to_append)

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        filename = (request.form.get("filename") or "filtered_profiles") + ".csv"

        return send_file(
            csv_bytes,
            mimetype="text/csv",
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)
