import io
import csv
from flask import Blueprint, request, send_file, Response
from utils import make_apify_request, parse_csv_column
from config import APIFY_TOKEN, BRANDPAGE_ACTOR_ID

bp_brandpage_reels = Blueprint("brandpage_reels", __name__)

# ----------------------------
# Fetch all brandpage reels in a single call
# ----------------------------
def fetch_brandpage_reels_all(brand_pages: list, results_limit: int = 1000):
    if not brand_pages:
        return []

    url = f"https://api.apify.com/v2/acts/{BRANDPAGE_ACTOR_ID}/run-sync-get-dataset-items"
    payload = {
        "username": brand_pages,
        "resultsLimit": min(results_limit, 1000),
        "includeSharesCount": False,
        "proxy": {"useApifyProxy": True},
    }
    params = {"token": APIFY_TOKEN, "waitForFinish": 600}

    all_reels = make_apify_request(url, params, payload)

    # Deduplicate using shortCode + ownerUsername
    unique_reels = {(item.get("shortCode"), item.get("ownerUsername")): item for item in all_reels}

    results = []
    for item in unique_reels.values():
        owner = item.get("ownerUsername", "")
        if owner in brand_pages:
            results.append((owner, item))
    return results

# ----------------------------
# Flask Route
# ----------------------------
@bp_brandpage_reels.route("/brandpage-reels", methods=["POST"])
def brandpage_reels():
    try:
        # Collect brand pages from form and CSV
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
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "brandpage_reels"))

        # Prepare CSV output
        output = io.StringIO()
        fieldnames = ["brandpage", "insta profile url", "collaborated account url", "reel url", "likes", "comments"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        # Fetch all reels
        page_reel_data = fetch_brandpage_reels_all(brandpages, results_limit)

        for bp, item in page_reel_data:
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
                    if collab_url != profile_url:
                        writer.writerow({
                            "brandpage": bp,
                            "insta profile url": profile_url,
                            "collaborated account url": collab_url,
                            "reel url": reel_url,
                            "likes": likes,
                            "comments": comments
                        })

            # Case 2: brandpage is collaborator on someone else's reel
            elif any(c.get("username", "") == bp for c in collabs):
                main_url = f"https://www.instagram.com/{main_user}/"
                writer.writerow({
                    "brandpage": bp,
                    "insta profile url": profile_url,
                    "collaborated account url": main_url,
                    "reel url": reel_url,
                    "likes": likes,
                    "comments": comments
                })

        # Send CSV
        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)
