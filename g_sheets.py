import os
from google.oauth2.service_account import Credentials
import logging
from googleapiclient.discovery import build
from config import GOOGLE_SHEET_ID, SERVICE_ACCOUNT_FILE

def get_gsheet_service():
    """Initializes and returns the Google Sheets service client."""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        logging.error(f"Service account file not found at {SERVICE_ACCOUNT_FILE}")
        return None
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def append_to_gsheet(data):
    """Appends data to the configured Google Sheet."""
    try:
        service = get_gsheet_service()
        if service and GOOGLE_SHEET_ID:
            body = {"values": data}
            service.values().append(
                spreadsheetId=GOOGLE_SHEET_ID, range="Sheet1!A1", valueInputOption="RAW", body=body
            ).execute()
    except Exception as e:
        logging.error(f"Failed to append to Google Sheet: {e}")