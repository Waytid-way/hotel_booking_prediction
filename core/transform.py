import os
import logging
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")
load_dotenv(ENV_PATH)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def get_engine():
    pg_user = os.getenv("PG_USER", "pipeline_user")
    pg_pass = os.getenv("PG_PASS", "supersecretpassword")
    pg_host = os.getenv("PG_HOST", "postgres")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db   = os.getenv("PG_DB", "hotel_db")
    conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
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
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    log.info(f"Removed duplicates: {before - len(df):,}")
    df["children"] = df["children"].fillna(0).astype("int32")
    df["country"] = df["country"].fillna("Unknown")
    df["agent"] = df["agent"].fillna(0).astype("int32")
    df["company"] = df["company"].fillna(0).astype("int32")

    month_map = {
        "January": 1, "February": 2, "March": 3, "April": 4,
        "May": 5, "June": 6, "July": 7, "August": 8,
        "September": 9, "October": 10, "November": 11, "December": 12
    }
    df["month_num"] = df["arrival_date_month"].map(month_map)

    #Required columns must exist
    required = ["arrival_date_year", "month_num", "arrival_date_day_of_month"]
    missing_required = [c for c in required if c not in df.columns]

    if missing_required:
        log.error(f"Missing required date columns: {missing_required}")
        return pd.DataFrame()

    #STEP 0: Force convert all date components to numeric
    for col in ["arrival_date_year", "month_num", "arrival_date_day_of_month"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    #STEP 1: Drop invalid components BEFORE assembling
    invalid_years = df["arrival_date_year"].isna().sum() + (df["arrival_date_year"] <= 0).sum()
    df = df[df["arrival_date_year"] > 0].reset_index(drop=True)
    if invalid_years > 0:
        log.warning(f"Dropped {invalid_years} rows due to invalid year")

    invalid_days = df["arrival_date_day_of_month"].isna().sum() + (df["arrival_date_day_of_month"] <= 0).sum()
    df = df[df["arrival_date_day_of_month"] > 0].reset_index(drop=True)
    if invalid_days > 0:
        log.warning(f"Dropped {invalid_days} rows due to invalid day of month")

    invalid_months = df["month_num"].isna().sum() + (df["month_num"] <= 0).sum()
    df = df[df["month_num"] > 0].reset_index(drop=True)
    if invalid_months > 0:
        log.warning(f"Dropped {invalid_months} rows due to invalid month")

    #STEP 2: Assemble datetime safely
    import datetime as dt

    def safe_date(row):
        try:
            return dt.datetime(
                int(row["arrival_date_year"]),
                int(row["month_num"]),
                int(row["arrival_date_day_of_month"])
            )
        except:
            return pd.NaT

    df["arrival_date"] = df.apply(safe_date, axis=1)

    invalid_dates = df["arrival_date"].isna().sum()
    df = df.dropna(subset=["arrival_date"]).reset_index(drop=True)

    if invalid_dates > 0:
        log.warning(f"Dropped {invalid_dates} rows due to invalid assembled dates")


    df["total_nights"] = df["stays_in_weekend_nights"] + df["stays_in_week_nights"]
    df["total_revenue"] = df["adr"] * df["total_nights"]
    df["lead_time_group"] = df["lead_time"].apply(categorize_lead_time)
    df["status_label"] = df["is_canceled"].map({0: "Check-Out", 1: "Canceled"})

    valid = (df["adr"] >= 0) & (df["total_nights"] > 0)
    dropped = len(df) - valid.sum()
    df = df[valid].reset_index(drop=True)
    if dropped > 0:
        log.info(f"Dropped invalid rows: {dropped:,}")

    keep_cols = [
        "hotel", "is_canceled", "status_label", "lead_time", "lead_time_group",
        "arrival_date", "arrival_date_year", "arrival_date_month",
        "total_nights", "adults", "children", "country", "market_segment",
        "distribution_channel", "is_repeated_guest", "previous_cancellations",
        "reserved_room_type", "assigned_room_type", "deposit_type", "agent",
        "company", "customer_type", "adr", "total_revenue",
        "required_car_parking_spaces", "total_of_special_requests"
    ]
    final_cols = [c for c in keep_cols if c in df.columns]
    df = df[final_cols]

    log.info(f"Final shape → {df.shape[0]:,} rows × {df.shape[1]} cols")
    return df

def transform():
    log.info("เริ่มทำ BI Data Transformation")
    engine = get_engine()
    try:
        chunk_iter = pd.read_sql(
            "SELECT * FROM raw_data.hotel_bookings_raw",
            con=engine,
            chunksize=50_000
        )
    except SQLAlchemyError as e:
        log.error(f"Cannot read raw_data: {e}")
        raise

    cleaned_chunks = []
    for i, chunk in enumerate(chunk_iter, start=1):
        log.info(f"Processing chunk {i}...")
        cleaned_chunks.append(clean_and_engineer_bi(chunk))

    final_df = pd.concat(cleaned_chunks, ignore_index=True)
    log.info(f"Writing cleaned result → production.bi_data")

    final_df.to_sql(
        "bi_data",
        con=engine,
        schema="production",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=10_000
    )

    log.info("Transform Completed!")

def get_production_data(engine):
    try:
        df = pd.read_sql("SELECT * FROM production.bi_data", con=engine)
        log.info(f"โหลด production.bi_data → {len(df):,} rows")
        return df
    except Exception as e:
        log.error(f"โหลด production.bi_data ไม่ได้: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    transform()
