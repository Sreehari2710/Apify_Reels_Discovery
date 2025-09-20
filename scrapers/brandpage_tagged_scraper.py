import io
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Blueprint, request, send_file, Response
from utils import parse_csv_column, make_apify_request
from config import APIFY_TOKEN, TAGGED_ACTOR_ID

bp_brandpage_tagged = Blueprint("brandpage_tagged", __name__)

def fetch_single_brandpage_tagged(brand_page: str, limit: int):
    url = f"https://api.apify.com/v2/acts/{TAGGED_ACTOR_ID}/run-sync-get-dataset-items"
    params = {"token": APIFY_TOKEN, "waitForFinish": 600}  # increased wait time
    payload = {
        "username": [brand_page],
        "resultsLimit": min(limit, 1000),
        "proxy": {"useApifyProxy": True},
    }
    return make_apify_request(url, params, payload)

@bp_brandpage_tagged.route("/brandpage-tagged", methods=["POST"])
def brandpage_tagged():
    try:
        brandpages = []
        single = (request.form.get("brandpage") or "").strip()
        if single:
            brandpages.append(single)
        if "csv_file" in request.files and request.files["csv_file"].filename:
            brandpages.extend(parse_csv_column(request.files["csv_file"], "brandpage"))
        brandpages = list(dict.fromkeys(brandpages))
        if not brandpages:
            return Response("Provide at least one brandpage", status=400)
        if len(brandpages) > 10:
            brandpages = brandpages[:10]

        limit = 1000
        filename_base = "".join(c if c.isalnum() else "_" for c in (request.form.get("filename") or "brandpage_tagged"))

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

        # Parallel fetch
        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_page = {executor.submit(fetch_single_brandpage_tagged, bp, limit): bp for bp in brandpages}
            for future in as_completed(future_to_page):
                bp = future_to_page[future]
                try:
                    tagged_posts = future.result(timeout=150)
                    for post in tagged_posts:
                        writer.writerow({
                            "brandpage": bp,
                            "owner_username": post.get("ownerUsername", ""),
                            "reel_url": post.get("url", ""),
                            "likes": post.get("likesCount", ""),
                            "comments": post.get("commentsCount", ""),
                            "shares": post.get("reshareCount", ""),
                            "views": post.get("videoPlayCount") or post.get("igPlayCount", "")
                        })
                except Exception as e:
                    print(f"Error processing {bp}: {e}")
                    continue

        csv_bytes = io.BytesIO(output.getvalue().encode("utf-8"))
        csv_bytes.seek(0)
        return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=f"{filename_base}.csv")

    except Exception as e:
        return Response(f"Error: {str(e)}", status=500)
