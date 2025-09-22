import io
import csv
import os
import pandas as pd
from flask import Blueprint, request, Response, send_file
from utils import make_apify_request, extract_contact_info_from_bio
from config import APIFY_TOKEN, PROFILE_ACTOR_ID
from g_sheets import append_to_gsheet

bp_profile = Blueprint("profile_scraper", __name__)

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

# ----------------- Scraper Function -----------------
def filter_and_scrape_profiles(csv_file_content: str, form_data: dict):
    df = pd.read_csv(io.StringIO(csv_file_content))
    headers = set(df.columns.str.strip().str.lower())
    rows = df.to_dict("records")

    csv_type = None
    usernames = []
    query_map = {}  # username -> query

    # Detect CSV type
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
        raise ValueError("Unrecognized CSV format. Could not find required columns.")

    usernames = list(set(usernames))
    if not usernames:
        raise ValueError("No valid usernames found in CSV file.")

    # Fetch profiles
    profiles = fetch_profiles_sync(usernames)

    # Prepare CSV
    output = io.StringIO()
    writer = csv.writer(output, quoting=csv.QUOTE_ALL)
    writer.writerow([
        "query_type", "query", "username", "url", "followers",
        "categories", "postcount", "bio", "email", "phone"
    ])

    rows_to_append = []
    for p in profiles:
        username = p.get("username", "")
        bio = p.get("biography", "") or ""
        email, phone = extract_contact_info_from_bio(bio)
        query_value = query_map.get(username, form_data.get("query", ""))

        followers = int(p.get("followersCount") or 0)
        category = get_category(followers)

        row_data = [
            csv_type, query_value, username, f"https://www.instagram.com/{username}/",
            followers, category, p.get("postsCount", ""),
            bio.replace("\n", " "), email, phone
        ]
        writer.writerow(row_data)
        rows_to_append.append(row_data)

    append_to_gsheet(rows_to_append)

    return output.getvalue()

def fetch_profiles_sync(usernames):
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
        csv_content = csv_file.stream.read().decode("utf-8")
        form_data = request.form.to_dict()

        # Run the filtering task synchronously
        csv_output_content = filter_and_scrape_profiles(csv_content, form_data)

        filename = (request.form.get("filename") or "filtered_profiles") + ".csv"
        csv_bytes = io.BytesIO(csv_output_content.encode("utf-8"))
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename)

    except Exception as e:
        return Response(f"Error processing request: {str(e)}", status=500)
