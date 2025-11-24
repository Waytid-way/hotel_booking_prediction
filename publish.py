import os
import gspread
from gspread.exceptions import SpreadsheetNotFound
from gspread import service_account
from typing import Optional
import logging

def authorize_gsheet(json_key_path: str, sheet_name: str) -> Optional[gspread.Worksheet]:
    try:
        if not os.path.exists(json_key_path):
            raise FileNotFoundError(f"Service account JSON key not found at: {json_key_path}")

        gc = service_account(filename=json_key_path)

        spreadsheet = gc.open(sheet_name)
        worksheet = spreadsheet.sheet1

        return worksheet

    except FileNotFoundError as e:
        logging.error(str(e))
        raise
    except IOError as e:
        logging.error(f"I/O error while reading the service account key file: {e}")
        raise
    except SpreadsheetNotFound:
        logging.error(f"Spreadsheet '{sheet_name}' not found or not accessible.")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during Google Sheets authorization: {e}")
        raise