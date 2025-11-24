import os
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

load_dotenv()


def get_engine():
    """เชื่อม DB แบบไม่ hard-code อะไรเลย"""
    conn_str = (
        f"postgresql+psycopg2://"
        f"{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@"
        f"{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5432')}/"
        f"{os.getenv('PG_DB')}"
    )
    return create_engine(conn_str, pool_pre_ping=True, future=True)


def clean_and_engineer(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # 1. Duplicates – มีจริงใน dataset นี้ ~2-3k
    n_before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log.info(f"Drop duplicates → {n_before - len(df):,} rows")

    # 2. Missing values (ตามที่คนส่วนใหญ่ทำใน Kaggle)
    df["children"] = df["children"].fillna(0).astype("int32")
    df["country"] = df["country"].fillna("Unknown")
    df["agent"] = df["agent"].fillna(0).astype("int32")
    df["company"] = df["company"].fillna(0).astype("int32")

    # 3. Arrival date – แก้ bug เดิมด้วยการ rename ชัด ๆ
    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }

    df["month_num"] = df["arrival_date_month"].map(month_map)

    # ถ้า month ไม่ match (ซึ่งไม่น่าเกิด) ให้กลายเป็น NaT แล้วกรองออก
    date_df = df[["arrival_date_year", "month_num", "arrival_date_day_of_month"]].rename(
        columns={
            "arrival_date_year": "year",
            "month_num": "month",
            "arrival_date_day_of_month": "day",
        }
    )

    df["arrival_date"] = pd.to_datetime(date_df, errors="coerce")
    df["day_of_week"] = df["arrival_date"].dt.day_name()

    n_bad_date = df["arrival_date"].isna().sum()
    df = df.dropna(subset=["arrival_date"]).reset_index(drop=True)
    if n_bad_date:
        log.info(f"Drop {n_bad_date:,} rows with invalid arrival_date")

    # 4. Nights & Revenue
    df["total_nights"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    df["total_revenue"] = df["adr"] * df["total_nights"]

    # 5. กรองของเสีย
    valid = (df["adr"] > 0) & (df["total_nights"] > 0)
    n_invalid = len(df) - valid.sum()
    df = df[valid].reset_index(drop=True)
    log.info(f"Drop {n_invalid:,} rows → ADR <= 0 หรือ total_nights <= 0")

    # 6. ADR outliers – IQR ตามที่คนทำ Kaggle ชอบใช้
    q1, q3 = df["adr"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    n_outlier = len(df) - df["adr"].between(lower, upper).sum()
    df = df[df["adr"].between(lower, upper)].reset_index(drop=True)
    log.info(f"Drop {n_outlier:,} ADR outliers (IQR)")

    # 7. Encoding – ง่าย ๆ แต่พอใช้ ML ได้เลย
    df = pd.get_dummies(df, columns=["hotel"], dtype=int)

    # Country → top 5 + Other (ป้องกัน column explosion)
    top5 = df["country"].value_counts().head(5).index
    df["country"] = df["country"].where(df["country"].isin(top5), "Other")
    df = pd.get_dummies(df, columns=["country"], dtype=int)

    # 8. ลบของที่ไม่ใช้แล้ว
    drop_cols = [
        "arrival_date_year", "arrival_date_month", "arrival_date_day_of_month",
        "stays_in_weekend_nights", "stays_in_week_nights"
    ]
    df = df.drop(columns=[c for c in drop_cols if c in df.columns])

    log.info(f"Final shape → {df.shape[0]:,} rows × {df.shape[1]} columns")
    return df

def get_production_data(engine) -> pd.DataFrame:
    """
    ดึงข้อมูล production_data ล่าสุดสำหรับ publish
    """
    query = "SELECT * FROM production_data ORDER BY arrival_date DESC"  # เรียงตามวันที่ใหม่สุด
    try:
        df = pd.read_sql(query, con=engine)
        log.info(f"ดึง production_data สำเร็จ → {len(df):,} แถว")
        return df
    except Exception as e:
        log.error(f"อ่าน production_data ล้มเหลว: {e}")
        raise

def main():
    engine = get_engine()

    log.info("Start loading raw_data (chunked)")
    chunk_iter = pd.read_sql("SELECT * FROM raw_data", con=engine, chunksize=20_000)

    cleaned_chunks = []
    for i, chunk in enumerate(chunk_iter, start=1):
        log.info(f"Processing chunk {i} – {len(chunk):,} rows")
        cleaned = clean_and_engineer(chunk)
        if not cleaned.empty:
            cleaned_chunks.append(cleaned)

    if not cleaned_chunks:
        log.error("No data after cleaning – something went wrong")
        return

    final_df = pd.concat(cleaned_chunks, ignore_index=True)
    log.info(f"Total cleaned rows: {len(final_df):,}")

    log.info("Writing to production_data (replace)")
    final_df.to_sql(
        "production_data",
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10_000,
    )

    log.info("Done – production_data พร้อมใช้งาน")


if __name__ == "__main__":
    main()

    