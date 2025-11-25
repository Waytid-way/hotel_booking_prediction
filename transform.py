import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
 
# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)
 
load_dotenv()
 
def get_engine():
    conn_str = (
        f"postgresql+psycopg2://"
        f"{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@"
        f"{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5432')}/"
        f"{os.getenv('PG_DB')}"
    )
    return create_engine(conn_str, pool_pre_ping=True, future=True)
 
def categorize_lead_time(days):

    if days <= 30:
        return "0-30 Days (Last Minute)"
    elif days <= 90:
        return "31-90 Days (Mid-term)"
    else:
        return "> 90 Days (Advance)"
 
def clean_and_engineer_bi(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
 
    # 1. Duplicates
    n_before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log.info(f"Drop duplicates → {n_before - len(df):,} rows")
 
    # 2. Missing values & Data Type Fixes
    df["children"] = df["children"].fillna(0).astype("int32")
    df["country"] = df["country"].fillna("Unknown") # เก็บไว้ทั้งหมด เพื่อทำ Map Chart
    df["agent"] = df["agent"].fillna(0).astype("int32")
    df["company"] = df["company"].fillna(0).astype("int32")
 
    # 3. Arrival Date Construction (สร้างวันที่จริง)
    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
    df["month_num"] = df["arrival_date_month"].map(month_map)
   
    date_df = df[["arrival_date_year", "month_num", "arrival_date_day_of_month"]].rename(
        columns={
            "arrival_date_year": "year",
            "month_num": "month",
            "arrival_date_day_of_month": "day",
        }
    )
    df["arrival_date"] = pd.to_datetime(date_df, errors="coerce")
   
    # Drop rows with invalid dates
    df = df.dropna(subset=["arrival_date"]).reset_index(drop=True)
 
    # 4. Feature Engineering for BI (Human Readable)
   
    # 4.1 Revenue Calculation
    df["total_nights"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    df["total_revenue"] = df["adr"] * df["total_nights"]
 
    # 4.2 Lead Time Grouping (ทำใน Python เลย เร็วกว่าไปทำใน Looker)
    df["lead_time_group"] = df["lead_time"].apply(categorize_lead_time)
 
    # 4.3 Cancellation Label (ทำให้อ่านง่ายขึ้นในกราฟ)
    df["status_label"] = df["is_canceled"].map({0: "Check-Out", 1: "Canceled"})
 
    # 5. กรองเฉพาะข้อมูลที่เป็นไปไม่ได้ทางฟิสิกส์ (Negative)
    # แต่เก็บ High Value (Outliers) ไว้สำหรับ BI
    valid = (df["adr"] >= 0) & (df["total_nights"] > 0)
    n_invalid = len(df) - valid.sum()
    df = df[valid].reset_index(drop=True)
    if n_invalid > 0:
        log.info(f"Drop {n_invalid:,} rows → Impossible values (ADR < 0 or Nights = 0)")
 
    # --- REMOVED: ADR Outlier Removal (IQR) ---
    # BI ต้องโชว์รายได้จริง แม้จะเป็นลูกค้า VIP ที่จ่ายแพงผิดปกติ
 
    # --- REMOVED: One-Hot Encoding ---
    # BI ต้องการคอลัมน์ 'hotel' และ 'country' แบบเดิมเพื่อทำ Dropdown Filter/Map
 
    # 6. เลือก Column ที่จำเป็น
    # เก็บ year, month ไว้เผื่อทำ Filter ง่ายๆ ไม่ต้องพึ่ง Date Function
    keep_cols = [
        "hotel", "is_canceled", "status_label", "lead_time", "lead_time_group",
        "arrival_date", "arrival_date_year", "arrival_date_month",
        "total_nights", "adults", "children", "country", "market_segment",
        "distribution_channel", "is_repeated_guest", "previous_cancellations",
        "reserved_room_type", "assigned_room_type", "deposit_type", "agent",
        "company", "customer_type", "adr", "total_revenue",
        "required_car_parking_spaces", "total_of_special_requests"
    ]
   
    # กรองเอาเฉพาะ column ที่มีจริง (เผื่อบางอัน drop ไปแล้ว)
    final_cols = [c for c in keep_cols if c in df.columns]
    df = df[final_cols]
 
    log.info(f"Final BI shape → {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df
 
def main():
    engine = get_engine()
 
    log.info("Start BI Data Transformation Pipeline")
   
    # ใช้ Chunksize เพื่อประหยัด Memory
    chunk_iter = pd.read_sql("SELECT * FROM raw_data", con=engine, chunksize=50_000)
 
    cleaned_chunks = []
    for i, chunk in enumerate(chunk_iter, start=1):
        log.info(f"Processing chunk {i}...")
        cleaned = clean_and_engineer_bi(chunk)
        if not cleaned.empty:
            cleaned_chunks.append(cleaned)
 
    if not cleaned_chunks:
        log.error("No data processed!")
        return
 
    final_df = pd.concat(cleaned_chunks, ignore_index=True)
    log.info(f"Writing {len(final_df):,} rows to 'production_data'...")
 
    # Write to DB (Replace mode เพื่อล้างข้อมูลเก่าที่เป็น ML format ออก)
    final_df.to_sql(
        "production_data",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10_000,
    )
 
    log.info("Pipeline Complete: 'production_data' is ready for Looker Studio.")
 
if __name__ == "__main__":
    main()