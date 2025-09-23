import io
import csv
from flask import Blueprint, request, Response, send_file
import logging
from utils import make_apify_request, parse_csv_column
from config import APIFY_TOKEN, BRANDPAGE_ACTOR_ID

bp_brandpage_reels = Blueprint("brandpage_reels", __name__)

# ----------------------------
# Scraper Function
# ----------------------------
def scrape_brandpage_reels(brand_pages: list, results_limit: int):
    logging.info(f"Starting scrape for brandpages: {brand_pages} with limit: {results_limit}")
    url = f"https://api.apify.com/v2/acts/{BRANDPAGE_ACTOR_ID}/run-sync-get-dataset-items"
    payload = {
        "username": brand_pages,
        "resultsLimit": min(results_limit, 1000),
        "includeSharesCount": False,
        "proxy": {"useApifyProxy": True},
    }
    params = {"token": APIFY_TOKEN, "waitForFinish": 600}
    all_reels = make_apify_request(url, params, payload)

    logging.info(f"Fetched {len(all_reels)} reels from Apify")

    # Deduplicate reels using a dictionary to preserve order
    unique_reels = {}
    for item in all_reels:
        key = (item.get("shortCode"), item.get("ownerUsername"))
        unique_reels[key] = item

    logging.info(f"Deduplicated to {len(unique_reels)} unique reels")

    results = []
    for item in unique_reels.values():  # Iterate through values of the dictionary
        owner = item.get("ownerUsername", "")
        if owner in brand_pages:
             results.append((owner, item))

    # Process data and prepare for CSV
    processed_data = []
    for bp, item in results:
        reel_url = item.get("url", "")
        comments = item.get("commentsCount", "")
        likes = item.get("likesCount", "")

        profile_url = f"https://www.instagram.com/{bp}/"
        collabs = item.get("coauthorProducers", []) or []
        main_user = item.get("ownerUsername", "")

        # Case 1: brandpage posted, collaborators exist
        if bp == main_user and collabs:
            for collab in collabs:
                collab_username = collab.get("username", "")
                collab_url = f"https://www.instagram.com/{collab_username}/"
                if collab_url != profile_url and collab_url:
                    processed_data.append({"brandpage": bp, "insta profile url": profile_url, "collaborated account url": collab_url, "reel url": reel_url, "likes": likes, "comments": comments})

        # Case 2: brandpage is collaborator on someone else's reel
        elif any(c.get("username", "") == bp for c in collabs):
            main_url = f"https://www.instagram.com/{main_user}/"
            if main_url:
                processed_data.append({"brandpage": bp, "insta profile url": profile_url, "collaborated account url": main_url, "reel url": reel_url, "likes": likes, "comments": comments})

    logging.info(f"Identified {len(processed_data)} collaborated reels")

    # Generate CSV content
    output = io.StringIO()
    fieldnames = ["brandpage", "insta profile url", "collaborated account url", "reel url", "likes", "comments"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(processed_data)

    logging.info(f"Generated CSV content with {len(processed_data)} rows")


    # Optionally append to Google Sheet if needed for this scraper
    # Example: if you want to save processed_data to Google Sheets
    # gsheet_data = [[item[key] for key in fieldnames] for item in processed_data]
    # append_to_gsheet(gsheet_data)

    return output.getvalue()

# ----------------------------
# Flask Route
# ----------------------------
@bp_brandpage_reels.route("/brandpage-reels", methods=["POST"])
def brandpage_reels():
    try:
        brandpages = []
        single = (request.form.get("brandpage") or "").strip()
        if single:
            brandpages.append(single)
        if "csv_file" in request.files and request.files["csv_file"].filename:
            brandpages.extend(parse_csv_column(request.files["csv_file"], "brandpage"))
        brandpages = list(dict.fromkeys(brandpages))  # Remove duplicates

        if not brandpages:
            return Response("Provide at least one brandpage", status=400)
        if len(brandpages) > 10:
            brandpages = brandpages[:10]

        results_limit = max(1, min(int(request.form.get("limit", 1000)), 1000))

        # Run the scraping task synchronously
        csv_content = scrape_brandpage_reels(brandpages, results_limit)

        # Return the CSV file as a download
        filename = (request.form.get("filename") or "brandpage_reels_export") + ".csv"
        csv_bytes = io.BytesIO(csv_content.encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=filename)

    except Exception as e:
        return Response(f"Error processing request: {str(e)}: {e}", status=500)
