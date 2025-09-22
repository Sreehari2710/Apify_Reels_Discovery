import io
import csv
from flask import Blueprint, request, Response, send_file
from utils import make_apify_request, parse_csv_column
from config import APIFY_TOKEN, YOUTUBE_ACTOR_ID

bp_youtube = Blueprint("youtube_scraper", __name__)

# ----------------------------
# Scraper Function
# ----------------------------
def scrape_youtube_keywords(keywords: list, results_count: int):
    """Fetch YouTube data for all keywords in a single API call."""
    if not keywords:
        return ""

    url = f"https://api.apify.com/v2/acts/{YOUTUBE_ACTOR_ID}/run-sync-get-dataset-items"
    payload = {"query": keywords, "resultsCount": min(results_count, 1000)}
    params = {"token": APIFY_TOKEN, "waitForFinish": 600}

    items = make_apify_request(url, params, payload)

    for item in items:
        item["_query_keyword"] = item.get("query") or item.get("keyword") or ""

    # CSV output
    output = io.StringIO()
    fieldnames = ["keyword", "url", "channelName", "viewCount"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for it in items:
        writer.writerow({
            "keyword": it.get("_query_keyword", ""),
            "url": it.get("url") or (f"https://www.youtube.com/watch?v={it.get('id')}" if it.get("id") else ""),
            "channelName": it.get("channelName") or it.get("channel_title") or "",
            "viewCount": it.get("viewCount") or it.get("views") or ""
        })
    
    # Optionally append to Google Sheet if needed for this scraper
    # gsheet_data = [[item[key] for key in fieldnames] for item in items]
    # append_to_gsheet(gsheet_data)

    return output.getvalue()

@bp_youtube.route("/youtube-keyword", methods=["POST"])
def youtube_keyword():
    try:
        keywords = []
        single = (request.form.get("keyword") or "").strip()
        if single:
            keywords.append(single)

        if "csv_file" in request.files and request.files["csv_file"].filename:
            keywords.extend(parse_csv_column(request.files["csv_file"], "keyword"))

        keywords = list(dict.fromkeys([k for k in keywords if k]))
        if not keywords:
            return Response("Provide at least one keyword", status=400)

        results_count = max(1, min(int(request.form.get("limit", 1000)), 1000))

        # Run the scraping task synchronously
        csv_content = scrape_youtube_keywords(keywords, results_count)

        # Return the CSV file as a download
        filename = (request.form.get("filename") or "youtube_keyword_export") + ".csv"
        csv_bytes = io.BytesIO(csv_content.encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename)

    except Exception as e:
        return Response(f"Error processing request: {str(e)}", status=500)
