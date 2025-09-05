import csv
import io
import os
import typing as t
from flask import Flask, request, send_file, render_template, Response
import requests
from dotenv import load_dotenv
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# Load environment variables
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HASHTAG_ACTOR_ID = "apify~instagram-hashtag-scraper"
BRANDPAGE_ACTOR_ID = "apify~instagram-reel-scraper"
TAGGED_ACTOR_ID = "apify~instagram-tagged-scraper"
PROFILE_ACTOR_ID = "logical_scrapers~instagram-profile-scraper"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_FILE = "service_account.json"

if not APIFY_TOKEN:
    raise RuntimeError("Set your APIFY_TOKEN in environment or .env file")

app = Flask(__name__)

# ----------------------------
# Utilities
# ----------------------------

def normalize_hashtags(value: str) -> t.List[str]:
    if not value:
        return []
    return [h.strip().lstrip("#") for h in value.replace(",", "\n").splitlines() if h.strip()]

def parse_csv_column(file_storage, column: str) -> t.List[str]:
    """Parse CSV and extract given column values."""
    if not file_storage:
        return []
    file_storage.stream.seek(0)
    reader = csv.DictReader(io.StringIO(file_storage.read().decode("utf-8", errors="ignore")))
    if column not in reader.fieldnames:
        raise ValueError(f"CSV must contain '{column}' column")
    values = []
    for row in reader:
        val = row[column].strip()
        if val:
            values.append(val)
    return values

# ----------------------------
# HASHTAG SCRAPER
# ----------------------------

def fetch_apify_hashtag_data(hashtags: t.List[str], per_hashtag: int) -> t.List[dict]:
    url = f"https://api.apify.com/v2/acts/{HASHTAG_ACTOR_ID}/run-sync-get-dataset-items?token={APIFY_TOKEN}&waitForFinish=1200"
    payload = {
        "hashtags": hashtags,
        "resultsLimit": per_hashtag,
        "resultsType": "stories",  # Only reels
        "proxy": {"useApifyProxy": True}
    }
    r = requests.post(url, json=payload, timeout=600)
    r.raise_for_status()
    return r.json()

def extract_row(item: dict, hashtag: str):
    fullname = item.get("user", {}).get("fullName") or item.get("ownerFullName") or ""
    username = item.get("user", {}).get("username") or item.get("ownerUsername") or ""
    shortcode = item.get("shortcode") or item.get("shortCode") or ""
    url = f"https://www.instagram.com/reel/{shortcode}/" if shortcode else item.get("url", "")
    likes = item.get("likeCount") or item.get("likesCount") or ""
    comments = item.get("commentCount") or item.get("commentsCount") or ""
    return {
        "hashtag": hashtag,
        "owner fullname": fullname,
        "owner username": username,
        "url": url,
        "likecount": likes,
        "comments": comments
    }

@app.route("/fetch", methods=["POST"])
def fetch_hashtag():
    try:
        tags = []
        single = request.form.get("hashtag", "").strip()
        tags.extend(normalize_hashtags(single))
        if "csv_file" in request.files and request.files["csv_file"].filename:
            tags.extend(parse_csv_column(request.files["csv_file"], "hashtag"))
        tags = list(dict.fromkeys(tags))
        if not tags:
            return Response("Provide at least one hashtag", status=400)

        per_hashtag = max(1, min(int(request.form.get("limit", 20)), 1000))
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "reels_export"))
        items = fetch_apify_hashtag_data(tags, per_hashtag)

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=["hashtag", "owner fullname", "owner username", "url", "likecount", "comments"])
        writer.writeheader()
        for i in items:
            tag = (i.get("hashtags")[0] if i.get("hashtags") else "")
            writer.writerow(extract_row(i, tag))

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

# ----------------------------
# BRANDPAGE REELS SCRAPER
# ----------------------------

def fetch_brandpage_reels(brand_page: str, per_page: int) -> t.List[dict]:
    url = f"https://api.apify.com/v2/acts/{BRANDPAGE_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 1200}
    payload = {
        "username": [brand_page],
        "resultsLimit": per_page,
        "includeSharesCount": False,
        "proxy": {"useApifyProxy": True},
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=600)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching reels for {brand_page}: {e}")
        return []

@app.route("/brandpage-reels", methods=["POST"])
def brandpage_reels():
    try:
        brandpages = []
        single = request.form.get("brandpage", "").strip()
        if single:
            brandpages.append(single)
        if "csv_file" in request.files and request.files["csv_file"].filename:
            brandpages.extend(parse_csv_column(request.files["csv_file"], "brandpage"))
        brandpages = list(dict.fromkeys(brandpages))

        if not brandpages:
            return Response("Provide at least one brandpage", status=400)

        per_page = max(1, min(int(request.form.get("limit", 20)), 100))
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "brandpage_reels"))

        output = io.StringIO()
        fieldnames = [
            "brandpage",
            "insta profile url",
            "collaborated account url",
            "reel url",
            "likes",
            "comments",
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for bp in brandpages:
            reels = fetch_brandpage_reels(bp, per_page)
            for item in reels:
                reel_url = item.get("url", "")
                comments = item.get("commentsCount", "")
                likes = item.get("likesCount", "")
                profile_url = f"https://www.instagram.com/{bp}/"
                collabs = item.get("coauthorProducers", [])

                for collab in collabs:
                    collab_url = f"https://www.instagram.com/{collab.get('username','')}/"
                    if collab_url != profile_url:
                        writer.writerow({
                            "brandpage": bp,
                            "insta profile url": profile_url,
                            "collaborated account url": collab_url,
                            "reel url": reel_url,
                            "likes": likes,
                            "comments": comments
                        })

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(
            csv_bytes,
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{filename_base}.csv"
        )

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

# ----------------------------
# BRANDPAGE TAGGED SCRAPER
# ----------------------------

def fetch_brandpage_tagged(brand_page: str, limit: int) -> t.List[dict]:
    url = f"https://api.apify.com/v2/acts/{TAGGED_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 1200}
    payload = {
        "username": [brand_page],
        "resultsLimit": limit,
        "proxy": {"useApifyProxy": True},
    }
    try:
        r = requests.post(url, params=params, json=payload, timeout=600)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []
    except Exception as e:
        print(f"Error fetching tagged reels for {brand_page}: {e}")
        return []

@app.route("/brandpage-tagged", methods=["POST"])
def brandpage_tagged():
    try:
        brandpages = []
        single = request.form.get("brandpage", "").strip()
        if single:
            brandpages.append(single)
        if "csv_file" in request.files and request.files["csv_file"].filename:
            brandpages.extend(parse_csv_column(request.files["csv_file"], "brandpage"))
        brandpages = list(dict.fromkeys(brandpages))

        if not brandpages:
            return Response("Provide at least one brandpage", status=400)

        limit = max(1, min(int(request.form.get("limit", 20)), 100))
        filename_base = "".join(
            c if c.isalnum() else "_" for c in (request.form.get("filename") or "brandpage_tagged")
        )

        output = io.StringIO()
        fieldnames = [
            "brandpage",
            "owner_username",
            "reel_url",
            "likes",
            "comments",
            "shares",
            "views"
        ]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for bp in brandpages:
            tagged_posts = fetch_brandpage_tagged(bp, limit)
            for post in tagged_posts:
                reel_url = post.get("url", "")
                likes = post.get("likesCount", "")
                comments = post.get("commentsCount", "")
                shares = post.get("reshareCount", "")
                views = post.get("videoPlayCount") or post.get("igPlayCount", "")
                owner_username = post.get("ownerUsername", "")

                writer.writerow({
                    "brandpage": bp,
                    "owner_username": owner_username,
                    "reel_url": reel_url,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "views": views
                })

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(
            csv_bytes,
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"{filename_base}.csv"
        )

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

# ----------------------------
# PROFILE SCRAPER + FILTERING
# ----------------------------

def get_gsheet_service():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def append_to_gsheet(data):
    service = get_gsheet_service()
    body = {"values": data}
    service.values().append(
        spreadsheetId=GOOGLE_SHEET_ID,
        range="Sheet1!A1",
        valueInputOption="RAW",
        body=body
    ).execute()

def fetch_profiles(usernames):
    url = f"https://api.apify.com/v2/acts/{PROFILE_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 1200}
    payload = {"username": usernames}
    r = requests.post(url, params=params, json=payload, timeout=600)
    r.raise_for_status()
    return r.json()

@app.route("/filter-csv", methods=["POST"])
def filter_csv():
    try:
        if "csv_file" not in request.files:
            return Response("Upload a CSV", status=400)
        csv_file = request.files["csv_file"]
        df = pd.read_csv(csv_file)

        # Identify CSV type
        if "owner username" in df.columns:  # hashtag CSV
            csv_type = "hashtag"
            usernames = df["owner username"].dropna().unique().tolist()
            query_map = dict(zip(df["owner username"], df.get("hashtag", "")))

        elif "collaborated account url" in df.columns:  # reels CSV
            csv_type = "reels"
            df["username"] = df["collaborated account url"].dropna().apply(
                lambda x: x.strip("/").split("/")[-1]
            )
            usernames = df["username"].unique().tolist()
            query_map = dict(zip(df["username"], df.get("brandpage", "")))

        elif "owner_username" in df.columns:  # tagged CSV
            csv_type = "tagged"
            usernames = df["owner_username"].dropna().unique().tolist()
            query_map = dict(zip(df["owner_username"], df.get("brandpage", "")))

        else:
            return Response("CSV columns not recognized", status=400)

        profiles = fetch_profiles(usernames)
        results = []

        for p in profiles:
            if p.get("followers", 0) < 1000:
                continue

            username = p.get("username", "")
            profile_url = f"https://www.instagram.com/{username}/" if username else ""
            query = query_map.get(username, "")

            post_count = (p.get("video_count", 0) or 0) + (p.get("image_count", 0) or 0)

            # Related accounts
            related_accounts = p.get("social_links", []) + p.get("website_links", [])
            related_accounts_str = ", ".join(related_accounts)

            # Emails
            email = None
            if p.get("emails"):
                email = p["emails"][0]
            elif p.get("all_emails"):
                email = p["all_emails"][0]

            # Phones
            phone = None
            if p.get("phones"):
                phone = p["phones"][0]
            elif p.get("all_phone_numbers"):
                phone = p["all_phone_numbers"][0]

            results.append([
                csv_type,
                query,
                username,
                profile_url,
                p.get("followers", ""),
                post_count,
                p.get("bio", ""),
                related_accounts_str,
                email or "",
                phone or ""
            ])

        append_to_gsheet(results)

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([
            "query type", "query", "username", "url", "followers",
            "postcount", "Bio", "related accounts", "email", "phone number"
        ])
        writer.writerows(results)

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        filename = (request.form.get("filename") or "filtered") + ".csv"
        return send_file(
            csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename
        )

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

# ----------------------------
# ROUTES
# ----------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")

@app.route("/filter", methods=["GET"])
def filter_page():
    return render_template("filter.html")

if __name__ == "__main__":
    app.run(debug=True)
