import io
import csv
import typing as t
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Blueprint, request, send_file, Response
from utils import normalize_hashtags, parse_csv_column, make_apify_request
from config import APIFY_TOKEN, HASHTAG_ACTOR_ID

bp_hashtag = Blueprint("hashtag_scraper", __name__)

# ----------------------------
# Fetch Single Hashtag
# ----------------------------
def fetch_single_hashtag(keyword: str, max_items: int) -> t.List[dict]:
    """Fetch data for a single hashtag using Apify actor."""
    url = f"https://api.apify.com/v2/acts/{HASHTAG_ACTOR_ID}/run-sync-get-dataset-items"
    payload = {"hashtags": [keyword], "resultsLimit": min(max_items, 1000)}
    params = {"token": APIFY_TOKEN, "waitForFinish": 300}

    data = make_apify_request(url, params, payload)
    print(f"[INFO] Fetched {len(data)} items for hashtag: {keyword}")
    return data

# ----------------------------
# Parallel Hashtag Fetch
# ----------------------------
def fetch_apify_hashtag_data(keywords: t.List[str], max_items: int) -> t.List[dict]:
    results = []
    batch_size = min(5, len(keywords))

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
                    data = future.result(timeout=150)
                    results.extend(data)
                    print(f"[SUCCESS] {len(data)} items added for '{keyword}'")
                except Exception as e:
                    print(f"[ERROR] Failed to fetch data for '{keyword}': {e}")

    return results

# ----------------------------
# Extract Row for CSV
# ----------------------------
def extract_row(item: dict):
    """Safely extract nested data from Apify output."""
    username = item.get("user", {}).get("username", "") if isinstance(item.get("user"), dict) else item.get("user.username", "")
    caption_text = item.get("caption", {}).get("text", "") if isinstance(item.get("caption"), dict) else item.get("caption.text", "")
    return {
        "hashtag": item.get("hashtag", ""),
        "username": username,
        "user_link": item.get("link_user", ""),
        "caption_text": caption_text
    }

# ----------------------------
# Flask Route: /fetch
# ----------------------------
@bp_hashtag.route("/fetch", methods=["POST"])
def fetch_hashtag():
    try:
        # Collect hashtags from form or CSV
        tags = normalize_hashtags(request.form.get("hashtag", "").strip())
        if "csv_file" in request.files and request.files["csv_file"].filename:
            tags.extend(parse_csv_column(request.files["csv_file"], "hashtag"))

        tags = list(dict.fromkeys([t for t in tags if t]))
        if not tags:
            return Response("Provide at least one hashtag", status=400)

        # Max items per hashtag
        max_items = max(1, min(int(request.form.get("limit", 20)), 1000))
        if max_items > 500:
            print(f"[WARNING] Requesting {max_items} items may cause timeouts.")

        # Limit number of hashtags to avoid large requests
        max_hashtags = 10 if max_items <= 100 else 5 if max_items <= 500 else 2
        if len(tags) > max_hashtags:
            tags = tags[:max_hashtags]
            print(f"[INFO] Limited to first {max_hashtags} hashtags due to high item count ({max_items}).")

        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "hashtag_export"))

        # Fetch data
        items = fetch_apify_hashtag_data(tags, max_items)

        # Write CSV
        output = io.StringIO()
        fieldnames = ["hashtag", "username", "user_link", "caption_text"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for item in items:
            writer.writerow(extract_row(item))

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        print(f"[ERROR] {e}")
        return Response(f"Error: {str(e)}", status=500)
