import os
import logging
from dotenv import load_dotenv
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

from core.transform import get_engine
from core.transform import get_production_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")

load_dotenv(ENV_PATH)

#AUTH Google Sheets

def authorize_gsheet():
    json_path = os.getenv("GSHEET_KEY_PATH", "google_service_account.json")
    sheet_name = os.getenv("GSHEET_NAME")

    if not sheet_name:
        raise ValueError("ต้องตั้ง GSHEET_NAME ใน .env")

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"ไม่พบ credentials JSON: {json_path}")

    gc = gspread.service_account(filename=json_path)
    sh = gc.open(sheet_name)
    worksheet = sh.sheet1

    log.info(f"เชื่อม Google Sheets สำเร็จ → {sh.title} / Sheet: {worksheet.title}")
    return worksheet

#MAIN PUBLISH FUNCTION

def publish():
    log.info("กำลัง Publish production.bi_data → Google Sheets...")

    try:
        engine = get_engine()

        #ดึงข้อมูลจาก production.bi_data
        df = get_production_data(engine)

        if df.empty:
            log.warning("production.bi_data ว่าง → ข้ามการส่งออก")
            return

        log.info(f"เตรียมส่งออกจำนวน {len(df):,} แถว")

        worksheet = authorize_gsheet()

        worksheet.clear()
        log.info("เคลียร์ข้อมูลเก่าใน Sheet เรียบร้อย")

        set_with_dataframe(
            worksheet,
            df,
            include_index=False,
            include_column_header=True,
            resize=True
        )

        log.info("ส่งข้อมูลขึ้น Google Sheets สำเร็จ!")

    except APIError as e:
        log.error(f"Google Sheets API error: {e}")
        raise

    except Exception as e:
        log.error(f"Publish ล้มเหลว: {e}")
        raise


if __name__ == "__main__":
    publish()