import csv
import io
import re
import requests
import typing as t

# ----------------------------
# Hashtag and CSV Utilities
# ----------------------------
def normalize_hashtags(value: str) -> t.List[str]:
    """Split input string into clean hashtags."""
    if not value:
        return []
    return [h.strip().lstrip("#") for h in value.replace(",", "\n").splitlines() if h.strip()]

def parse_csv_column(file_storage, column: str) -> t.List[str]:
    """Extract values from a specific column in an uploaded CSV file."""
    if not file_storage:
        return []
    file_storage.stream.seek(0)
    reader = csv.DictReader(io.StringIO(file_storage.read().decode("utf-8", errors="ignore")))
    if column not in reader.fieldnames:
        raise ValueError(f"CSV must contain '{column}' column")
    return [row[column].strip() for row in reader if row[column].strip()]

# ----------------------------
# Apify Request Utility
# ----------------------------
def make_apify_request(url: str, params: dict, payload: dict, max_retries: int = 5) -> t.List[dict]:
    """Send POST request to Apify with retries."""
    for attempt in range(max_retries):
        try:
            r = requests.post(url, params=params, json=payload, timeout=600)
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                return []
        except requests.exceptions.RequestException:
            if attempt == max_retries - 1:
                return []
    return []

# ----------------------------
# Contact Info Extraction
# ----------------------------
EMAIL_REGEX = r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}"
PHONE_REGEX = r"\+?\d[\d\-\(\) ]{7,}\d"

def extract_contact_info_from_bio(bio: str):
    """
    Extract emails and phone numbers from bio text.
    Returns: (emails_str, phones_str) with multiple items joined by comma.
    """
    if not bio:
        return "", ""
    emails = re.findall(EMAIL_REGEX, bio)
    phones = re.findall(PHONE_REGEX, bio)
    emails_str = ", ".join(set(emails))
    phones_str = ", ".join(set(phones))
    return emails_str, phones_str
