import os
import sys
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

#Load .env from config/.env

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(BASE_DIR, "config", ".env")
load_dotenv(ENV_PATH)

def get_db_engine() -> Engine:
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")

    if not all([pg_db, pg_user, pg_pass]):
        raise ValueError("Missing required database credentials: PG_DB, PG_USER, PG_PASS")

    connection_url = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"

    engine = create_engine(
        connection_url,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        echo=False
    )
    return engine

def test_connection(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection successful.")
        return True

    except OperationalError as e:
        print(f"Database connection failed: {e}")
        return False

    except Exception as e:
        print(f"Unexpected error: {e}")
        return False

def ingest(chunk_size: int = 10000) -> None:

    CSV_PATH = os.path.join(BASE_DIR, "data", "hotel_bookings.csv")

    if not os.path.exists(CSV_PATH):
        print(f"File not found: {CSV_PATH}")
        sys.exit(1)

    print(f"Reading CSV: {CSV_PATH}")

    chunk_iter = pd.read_csv(CSV_PATH, chunksize=chunk_size, low_memory=False)

    engine = get_db_engine()

    if not test_connection(engine):
        print("Connection failed. Exiting.")
        sys.exit(1)

    first_chunk = True
    total_rows = 0

    for chunk in chunk_iter:
        rows = len(chunk)
        total_rows += rows
        if_exists = "replace" if first_chunk else "append"

        chunk.to_sql(
            name="hotel_bookings_raw",
            schema="raw_data",
            con=engine,
            if_exists=if_exists,
            index=False,
            method="multi",
            chunksize=chunk_size
        )

        print(f"Loaded {rows:,} rows (total: {total_rows:,})")
        first_chunk = False

    print(f"Ingestion complete. Total rows: {total_rows:,}")


if __name__ == "__main__":
    ingest()