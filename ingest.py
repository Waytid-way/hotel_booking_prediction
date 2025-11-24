#!/usr/bin/env python3

import os
import sys
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv

load_dotenv()

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

def main_ingest(csv_path: str = "hotel_bookings.csv", chunk_size: int = 10_000) -> None:
    if not os.path.exists(csv_path):
        print(f"File not found: {csv_path}")
        sys.exit(1)

    print(f"Reading local CSV: {csv_path}")

    chunk_iter = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)

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
            name="raw_data",
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=str, default="hotel_bookings.csv", help="Path to CSV file")
    parser.add_argument("--chunksize", type=int, default=10_000, help="Chunk size")
    args = parser.parse_args()
    main_ingest(args.csv, args.chunksize)