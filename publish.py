
import os
import logging
from dotenv import load_dotenv
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound


from transform import get_engine, get_production_data  
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

load_dotenv()

def authorize_gsheet():
    json_path = os.getenv("GSHEET_JSON_KEY_PATH", "credentials.json")
    sheet_name = os.getenv("GSHEET_NAME")
    
    if not sheet_name:
        raise ValueError("ต้องตั้ง GSHEET_NAME ใน .env (ชื่อ Spreadsheet จริง ๆ)")
    
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"ไม่เจอไฟล์ credentials: {json_path} — เช็ค path ใน .env ด้วยนะ")
    
    gc = gspread.service_account(filename=json_path)
    sh = gc.open(sheet_name)
    worksheet = sh.sheet1  
    log.info(f"เชื่อม Google Sheets สำเร็จ → {sh.title} > {worksheet.title}")
    return worksheet

def main() -> None: 
    log.info("เริ่ม Publish production_data → Google Sheets")

    try:
        
        engine = get_engine()
        df = get_production_data(engine)

        if df.empty:
            log.warning("production_data ว่างเปล่า → ข้ามการอัพเดต")
            return

        log.info(f"เตรียมอัพโหลด {len(df):,} แถว...")

        
        worksheet = authorize_gsheet()

        
        worksheet.clear()
        log.info("เคลียร์ Sheet เก่าเรียบร้อย")

        
        set_with_dataframe(
            worksheet,
            df,
            include_index=False,
            include_column_header=True,
            resize=True,  
            allow_formulas=False 
        )
        log.info(f"อัพโหลดสำเร็จ → {len(df):,} แถว ไปยัง Google Sheets")

    except APIError as e:
        if "Quota exceeded" in str(e):
            log.error("Google Sheets API เกิน quota! รอ 1-2 ชม. หรือเช็ค plan")
        else:
            log.error(f"Google API error: {e}")
        raise
    except Exception as e:
        log.error(f"Publish ล้มเหลว: {e}")
        raise

    log.info("Publish เสร็จเรียบร้อย! ไปเช็ค Google Sheets ดูข้อมูลใหม่ได้เลย")

if __name__ == "__main__":
    main()