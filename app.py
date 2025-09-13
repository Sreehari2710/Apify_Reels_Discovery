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
    print(f"Making Apify request to: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")
    print(f"Params: {params}")
    
    for attempt in range(max_retries):
        try:
            # Progressive timeout: start with 180s, increase for retries
            timeout = 180 + (attempt * 60)
            print(f"Attempt {attempt + 1}/{max_retries} with timeout: {timeout}s")
            
            r = requests.post(url, params=params, json=payload, timeout=timeout)
            
            print(f"Response status: {r.status_code}")
            print(f"Response headers: {dict(r.headers)}")
            
            r.raise_for_status()
            data = r.json()
            
            print(f"Response data type: {type(data)}")
            print(f"Response data length: {len(data) if isinstance(data, (list, dict)) else 'N/A'}")
            
            if isinstance(data, list):
                if data:
                    print(f"First item sample: {data[0]}")
                    print(f"All available fields in first item: {list(data[0].keys()) if data[0] else 'No keys'}")
                else:
                    print("Empty list returned")
                return data
            else:
                print(f"Unexpected response format: {data}")
                # Sometimes Apify returns errors as objects, let's check
                if isinstance(data, dict) and "error" in data:
                    print(f"API Error: {data['error']}")
                return []
                
        except requests.exceptions.Timeout:
            print(f"Timeout ({timeout}s) on attempt {attempt + 1}")
            if attempt == max_retries - 1:
                print("All attempts failed due to timeout")
                return []
            time.sleep(2 ** attempt)  # Exponential backoff
            
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            try:
                if hasattr(e, 'response') and e.response is not None:
                    print(f"Response text: {e.response.text[:500]}")
            except:
                pass
            if attempt == max_retries - 1:
                print("All attempts failed due to request errors")
                return []
            time.sleep(1)
            
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print(f"Response text: {r.text[:500]}")
            if attempt == max_retries - 1:
                return []
            time.sleep(1)
            
    return []

# ----------------------------
# HASHTAG SCRAPER - OPTIMIZED
# ----------------------------
def fetch_single_hashtag(keyword: str, max_items: int) -> t.List[dict]:
    """Fetch data for a single hashtag with correct parameter names."""
    url = f"https://api.apify.com/v2/acts/{HASHTAG_ACTOR_ID}/run-sync-get-dataset-items"
    
    # Based on the actor documentation, the correct parameters are:
    payload = {
        "hashtag": keyword,  # Changed from "keyword" to "hashtag"
        "max_items": max_items,  # Changed from "Max_items" to "max_items"
        "cookies": IG_COOKIES_JSON
    }
    params = {
        "token": APIFY_TOKEN,
        "waitForFinish": 900  # Increased timeout for larger requests
    }
    
    print(f"Fetching hashtag '{keyword}' with {max_items} items")
    return make_apify_request(url, params, payload, max_retries=2)

def fetch_apify_hashtag_data(keywords: t.List[str], max_items: int) -> t.List[dict]:
    """
    Fetch Instagram usernames by hashtags using sequential processing for large requests.
    """
    results = []
    
    # For 1000 items, process sequentially to avoid overwhelming the server
    if max_items >= 500:
        print(f"Processing {len(keywords)} hashtags sequentially for {max_items} items each...")
        for i, keyword in enumerate(keywords):
            print(f"Processing hashtag {i+1}/{len(keywords)}: {keyword}")
            try:
                data = fetch_single_hashtag(keyword, max_items)
                results.extend(data)
                print(f"Successfully fetched {len(data)} items for: {keyword}")
                # Small delay between requests to be respectful
                if i < len(keywords) - 1:
                    time.sleep(2)
            except Exception as e:
                print(f"Error fetching data for '{keyword}': {e}")
    else:
        # For smaller requests, use parallel processing
        batch_size = min(3, len(keywords))
        
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_keyword = {
                executor.submit(fetch_single_hashtag, keyword, max_items): keyword 
                for keyword in keywords
            }
            
            for future in as_completed(future_to_keyword):
                keyword = future_to_keyword[future]
                try:
                    data = future.result(timeout=300)
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
        print("=== FETCH HASHTAG REQUEST STARTED ===")
        
        tags = normalize_hashtags(request.form.get("hashtag", "").strip())
        print(f"Raw hashtag input: '{request.form.get('hashtag', '')}'")
        print(f"Normalized tags: {tags}")
        
        if "csv_file" in request.files and request.files["csv_file"].filename:
            csv_tags = parse_csv_column(request.files["csv_file"], "hashtag")
            tags.extend(csv_tags)
            print(f"CSV tags added: {csv_tags}")
            
        tags = list(dict.fromkeys(tags))
        print(f"Final unique tags: {tags}")
        
        if not tags:
            print("ERROR: No hashtags provided")
            return Response("Provide at least one hashtag", status=400)

        # Allow up to 1000 items but warn about potential timeouts
        max_items = max(1, min(int(request.form.get("limit", 20)), 1000))
        print(f"Max items requested: {max_items}")
        
        if max_items > 500:
            print(f"Warning: Requesting {max_items} items may cause timeouts")
        
        # Limit hashtags based on item count to manage total load
        max_hashtags = 10 if max_items <= 100 else 5 if max_items <= 500 else 2
        if len(tags) > max_hashtags:
            print(f"Limiting from {len(tags)} to {max_hashtags} hashtags due to high item count")
            tags = tags[:max_hashtags]
        
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "hashtag_export"))
        print(f"Output filename base: {filename_base}")

        print("=== STARTING APIFY DATA FETCH ===")
        items = fetch_apify_hashtag_data(tags, max_items)
        print(f"Total items fetched: {len(items)}")
        
        if items:
            print(f"Sample item: {items[0]}")
        else:
            print("WARNING: No items returned from Apify")

        output = io.StringIO()
        fieldnames = ["hashtag", "caption", "username"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        rows_written = 0
        for item in items:
            row = extract_row(item)
            row = {k: v for k, v in row.items() if k in fieldnames}
            writer.writerow(row)
            rows_written += 1
        
        print(f"CSV rows written: {rows_written}")
        
        csv_content = output.getvalue()
        print(f"CSV content length: {len(csv_content)} characters")
        print(f"CSV preview: {csv_content[:200]}...")

        csv_bytes = io.BytesIO(csv_content.encode("utf-8"))
        csv_bytes.seek(0)
        
        print("=== FETCH HASHTAG REQUEST COMPLETED ===")
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        print(f"ERROR in fetch_hashtag: {str(e)}")
        import traceback
        traceback.print_exc()
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
# QUICK TEST ENDPOINT - IMPROVED
# ----------------------------
@app.route("/test-apify", methods=["GET", "POST"])
def test_apify():
    """Test endpoint to verify Apify connection and token."""
    try:
        if not APIFY_TOKEN:
            return jsonify({"error": "APIFY_TOKEN not set"})
            
        # Test with a simple actor run - try different parameters
        url = f"https://api.apify.com/v2/acts/{HASHTAG_ACTOR_ID}/run-sync-get-dataset-items"
        
        # Start with very minimal request
        params = {
            "token": APIFY_TOKEN,
            "waitForFinish": 120  # Shorter wait for test
        }
        
        # Try minimal payload first
        test_payloads = [
            {
                "hashtag": "fitness",
                "max_items": 5,
                "cookies": IG_COOKIES_JSON
            },
            {
                "hashtag": "travel", 
                "max_items": 3,
                "cookies": IG_COOKIES_JSON
            },
            # Test without cookies to see if that's the issue
            {
                "hashtag": "food",
                "max_items": 2
            }
        ]
        
        results = []
        
        for i, payload in enumerate(test_payloads):
            try:
                print(f"Testing payload {i+1}: {payload}")
                r = requests.post(url, params=params, json=payload, timeout=150)
                
                result = {
                    "test": i+1,
                    "payload": payload,
                    "status_code": r.status_code,
                    "response_length": len(r.text),
                    "success": r.status_code == 200
                }
                
                if r.status_code == 200:
                    try:
                        data = r.json()
                        result["data_type"] = type(data).__name__
                        result["data_length"] = len(data) if isinstance(data, list) else "N/A"
                        result["first_item"] = data[0] if isinstance(data, list) and len(data) > 0 else None
                    except:
                        result["json_parse_error"] = True
                        result["response_preview"] = r.text[:200]
                else:
                    result["error_response"] = r.text[:200]
                    
                results.append(result)
                
                # Stop if we found a working configuration
                if r.status_code == 200 and isinstance(r.json(), list) and len(r.json()) > 0:
                    break
                    
            except Exception as e:
                results.append({
                    "test": i+1,
                    "payload": payload,
                    "error": str(e)
                })
        
        return jsonify({
            "hashtag_actor_id": HASHTAG_ACTOR_ID,
            "apify_token_set": bool(APIFY_TOKEN),
            "cookies_set": bool(IG_COOKIES_JSON),
            "test_results": results,
            "recommendation": "Check which test worked best and use those parameters"
        })
        
    except Exception as e:
        import traceback
        return jsonify({
            "error": str(e),
            "traceback": traceback.format_exc()
        })

@app.route("/validate-cookies", methods=["GET"])
def validate_cookies():
    """Validate Instagram cookies format and content."""
    try:
        if not IG_COOKIES_JSON:
            return jsonify({"error": "IG_COOKIES not set"})
        
        validation = {
            "cookies_set": True,
            "cookies_type": type(IG_COOKIES_JSON).__name__,
            "cookies_length": len(IG_COOKIES_JSON) if isinstance(IG_COOKIES_JSON, (list, dict)) else "N/A"
        }
        
        if isinstance(IG_COOKIES_JSON, list):
            validation["is_list"] = True
            validation["first_cookie_keys"] = list(IG_COOKIES_JSON[0].keys()) if IG_COOKIES_JSON else []
            
            # Check for required cookie fields
            required_fields = ["name", "value", "domain"]
            if IG_COOKIES_JSON:
                first_cookie = IG_COOKIES_JSON[0]
                validation["has_required_fields"] = all(field in first_cookie for field in required_fields)
                validation["cookie_domains"] = list(set([cookie.get("domain", "") for cookie in IG_COOKIES_JSON[:5]]))
        else:
            validation["is_list"] = False
            validation["actual_type"] = str(type(IG_COOKIES_JSON))
        
        return jsonify(validation)
        
    except Exception as e:
        return jsonify({"error": str(e), "cookies_raw": str(IG_COOKIES_JSON)[:200]})

@app.route("/debug", methods=["GET"])
def debug_info():
    """Debug endpoint to check environment variables and configuration."""
    return jsonify({
        "apify_token_set": bool(APIFY_TOKEN),
        "apify_token_preview": APIFY_TOKEN[:10] + "..." if APIFY_TOKEN else None,
        "ig_cookies_set": bool(IG_COOKIES_JSON),
        "ig_cookies_count": len(IG_COOKIES_JSON) if IG_COOKIES_JSON else 0,
        "hashtag_actor_id": HASHTAG_ACTOR_ID,
        "google_sheet_id_set": bool(GOOGLE_SHEET_ID),
        "service_account_file_exists": os.path.exists(SERVICE_ACCOUNT_FILE),
        "environment_variables": {
            "APIFY_TOKEN": "SET" if os.getenv("APIFY_TOKEN") else "NOT SET",
            "IG_COOKIES": "SET" if os.getenv("IG_COOKIES") else "NOT SET",
            "GOOGLE_SHEET_ID": "SET" if os.getenv("GOOGLE_SHEET_ID") else "NOT SET"
        }
    })

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