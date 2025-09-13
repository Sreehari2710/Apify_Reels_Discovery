import csv
import io
import os
import typing as t
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, request, send_file, render_template, Response, jsonify
import requests
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
HASHTAG_ACTOR_ID = "coderx~instagram-hashtag-username-scraper"  # Fixed: use ~ instead of /
BRANDPAGE_ACTOR_ID = "apify~instagram-reel-scraper"
TAGGED_ACTOR_ID = "apify~instagram-tagged-scraper"
PROFILE_ACTOR_ID = "logical_scrapers~instagram-profile-scraper"
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
SERVICE_ACCOUNT_FILE = "service_account.json"

# Load IG cookies JSON
IG_COOKIES = os.getenv("IG_COOKIES")
if not IG_COOKIES:
    raise RuntimeError("Set IG_COOKIES in .env file")
try:
    IG_COOKIES_JSON = json.loads(IG_COOKIES)
except Exception:
    raise RuntimeError("IG_COOKIES must be valid JSON")

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
    return [row[column].strip() for row in reader if row[column].strip()]

def make_apify_request(url: str, params: dict, payload: dict, max_retries: int = 3) -> t.List[dict]:
    """Make Apify API request with retries and better error handling."""
    for attempt in range(max_retries):
        try:
            # Reduced timeout from 600 to 120 seconds
            r = requests.post(url, params=params, json=payload, timeout=120)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.Timeout:
            print(f"Timeout on attempt {attempt + 1}")
            if attempt == max_retries - 1:
                return []
            time.sleep(2 ** attempt)  # Exponential backoff
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            if attempt == max_retries - 1:
                return []
            time.sleep(1)
    return []

# ----------------------------
# HASHTAG SCRAPER - OPTIMIZED
# ----------------------------
def fetch_single_hashtag(keyword: str, max_items: int) -> t.List[dict]:
    """Fetch data for a single hashtag."""
    url = f"https://api.apify.com/v2/acts/{HASHTAG_ACTOR_ID}/run-sync-get-dataset-items"
    
    payload = {
        "Max_items": min(max_items, 200),  # Limit per request to avoid timeouts
        "cookies": IG_COOKIES_JSON,
        "keyword": keyword
    }
    params = {
        "token": APIFY_TOKEN,
        "waitForFinish": 300  # Reduced from 1200 to 300 seconds
    }
    
    return make_apify_request(url, params, payload)

def fetch_apify_hashtag_data(keywords: t.List[str], max_items: int) -> t.List[dict]:
    """
    Fetch Instagram usernames by hashtags using parallel processing.
    """
    results = []
    
    # Process in smaller batches to avoid timeouts
    batch_size = min(5, len(keywords))  # Process max 5 hashtags in parallel
    
    for i in range(0, len(keywords), batch_size):
        batch = keywords[i:i + batch_size]
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_keyword = {
                executor.submit(fetch_single_hashtag, keyword, max_items): keyword 
                for keyword in batch
            }
            
            for future in as_completed(future_to_keyword):
                keyword = future_to_keyword[future]
                try:
                    data = future.result(timeout=150)  # 150 second timeout per thread
                    results.extend(data)
                    print(f"Successfully fetched {len(data)} items for keyword: {keyword}")
                except Exception as e:
                    print(f"Error fetching data for '{keyword}': {e}")
    
    return results

def extract_row(item: dict):
    """
    Extract row data based on the actual output format from the actor.
    According to the documentation, the output fields are:
    - search_keyword (the hashtag)
    - caption (extracted hashtags from captions)
    - username (Instagram handle)
    - user_pk (unique user ID)
    - id (post ID)
    - code (unique post code)
    """
    return {
        "hashtag": item.get("search_keyword", ""),
        "caption": item.get("caption", ""),
        "username": item.get("username", "")
    }

@app.route("/fetch", methods=["POST"])
def fetch_hashtag():
    try:
        tags = normalize_hashtags(request.form.get("hashtag", "").strip())
        if "csv_file" in request.files and request.files["csv_file"].filename:
            tags.extend(parse_csv_column(request.files["csv_file"], "hashtag"))
        tags = list(dict.fromkeys(tags))
        
        if not tags:
            return Response("Provide at least one hashtag", status=400)

        # Allow up to 1000 items but warn about potential timeouts
        max_items = max(1, min(int(request.form.get("limit", 20)), 1000))
        if max_items > 500:
            print(f"Warning: Requesting {max_items} items may cause timeouts")
        
        # Limit hashtags based on item count to manage total load
        max_hashtags = 10 if max_items <= 100 else 5 if max_items <= 500 else 2
        if len(tags) > max_hashtags:
            tags = tags[:max_hashtags]
            print(f"Limited to first {max_hashtags} hashtags due to high item count ({max_items})")
        
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "hashtag_export"))

        items = fetch_apify_hashtag_data(tags, max_items)

        output = io.StringIO()
        fieldnames = ["hashtag", "caption", "username"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            row = extract_row(item)
            row = {k: v for k, v in row.items() if k in fieldnames}
            writer.writerow(row)

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)

# ----------------------------
# BRANDPAGE REELS SCRAPER - OPTIMIZED
# ----------------------------
def fetch_single_brandpage_reels(brand_page: str, per_page: int) -> t.List[dict]:
    """Fetch reels for a single brand page."""
    url = f"https://api.apify.com/v2/acts/{BRANDPAGE_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 300}  # Reduced timeout
    payload = {
        "username": [brand_page],
        "resultsLimit": min(per_page, 100),  # Limit per request
        "includeSharesCount": False,
        "proxy": {"useApifyProxy": True},
    }
    return make_apify_request(url, params, payload)

def fetch_brandpage_reels_parallel(brand_pages: t.List[str], per_page: int) -> t.List[dict]:
    """Fetch reels for multiple brand pages in parallel."""
    results = []
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_page = {
            executor.submit(fetch_single_brandpage_reels, page, per_page): page 
            for page in brand_pages
        }
        
        for future in as_completed(future_to_page):
            page = future_to_page[future]
            try:
                data = future.result(timeout=150)
                results.extend([(page, item) for item in data])
                print(f"Successfully fetched {len(data)} reels for: {page}")
            except Exception as e:
                print(f"Error fetching reels for {page}: {e}")
    
    return results

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

        # Limit brandpages to prevent timeouts
        if len(brandpages) > 10:
            brandpages = brandpages[:10]

        per_page = max(1, min(int(request.form.get("limit", 20)), 100))  # Reduced max
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

        page_reel_data = fetch_brandpage_reels_parallel(brandpages, per_page)
        
        for bp, item in page_reel_data:
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
# BRANDPAGE TAGGED SCRAPER - OPTIMIZED
# ----------------------------
def fetch_single_brandpage_tagged(brand_page: str, limit: int) -> t.List[dict]:
    """Fetch tagged posts for a single brand page."""
    url = f"https://api.apify.com/v2/acts/{TAGGED_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 300}
    payload = {
        "username": [brand_page],
        "resultsLimit": min(limit, 100),
        "proxy": {"useApifyProxy": True},
    }
    return make_apify_request(url, params, payload)

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

        # Limit brandpages
        if len(brandpages) > 10:
            brandpages = brandpages[:10]

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

        # Use parallel processing
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_page = {
                executor.submit(fetch_single_brandpage_tagged, bp, limit): bp 
                for bp in brandpages
            }
            
            for future in as_completed(future_to_page):
                bp = future_to_page[future]
                try:
                    tagged_posts = future.result(timeout=150)
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
                except Exception as e:
                    print(f"Error processing {bp}: {e}")

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
# PROFILE SCRAPER + FILTERING - OPTIMIZED
# ----------------------------
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

def fetch_profiles_batch(usernames: t.List[str]) -> t.List[dict]:
    """Fetch profiles in batches to avoid timeouts."""
    url = f"https://api.apify.com/v2/acts/{PROFILE_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 300}
    
    # Process in smaller batches
    batch_size = 20
    all_results = []
    
    for i in range(0, len(usernames), batch_size):
        batch = usernames[i:i + batch_size]
        payload = {"username": batch}
        
        try:
            results = make_apify_request(url, params, payload)
            all_results.extend(results)
            print(f"Processed batch {i//batch_size + 1}: {len(results)} profiles")
        except Exception as e:
            print(f"Error processing batch {i//batch_size + 1}: {e}")
    
    return all_results

@app.route("/filter-csv", methods=["POST"])
def filter_csv():
    try:
        if "csv_file" not in request.files:
            return Response("Upload a CSV", status=400)
        csv_file = request.files["csv_file"]
        df = pd.read_csv(csv_file)

        # Identify CSV type
        if "username" in df.columns and "hashtag" in df.columns:  # hashtag CSV
            csv_type = "hashtag"
            usernames = df["username"].dropna().unique().tolist()
            query_map = dict(zip(df["username"], df.get("hashtag", "")))

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
            return Response(f"CSV columns not recognized. Found: {list(df.columns)}", status=400)

        # Limit usernames to prevent timeouts
        if len(usernames) > 100:
            usernames = usernames[:100]
            print(f"Limited to first 100 usernames to prevent timeout")

        profiles = fetch_profiles_batch(usernames)
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
            related_accounts_str = ", ".join([str(acc).replace("\n", " ").replace(",", ";") for acc in related_accounts])

            # Emails
            email_list = p.get("emails") or p.get("all_emails") or []
            email_str = ", ".join([str(e).replace("\n", " ").replace(",", ";") for e in email_list])

            # Phones
            phone_list = p.get("phones") or p.get("all_phone_numbers") or []
            phone_str = ", ".join([str(ph).replace("\n", " ").replace(",", ";") for ph in phone_list])

            # Bio
            bio = str(p.get("bio", "")).replace("\n", " ").replace(",", ";")

            results.append([
                csv_type,
                query,
                username,
                profile_url,
                p.get("followers", ""),
                post_count,
                bio,
                related_accounts_str,
                email_str,
                phone_str
            ])

        # Try to append to Google Sheet (optional)
        append_to_gsheet(results)

        # Write CSV with quoting
        output = io.StringIO()
        writer = csv.writer(output, quoting=csv.QUOTE_ALL)
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
# STATUS ENDPOINT FOR MONITORING
# ----------------------------
@app.route("/health", methods=["GET"])
def health_check():
    return jsonify({"status": "healthy", "timestamp": time.time()})

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