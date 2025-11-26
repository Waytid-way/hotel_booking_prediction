import os
<<<<<<< HEAD
=======
import sys
>>>>>>> 21ff55e155f3eb9ee2ad79805daf749db3d71516
import time
import logging
from typing import Callable

import sqlalchemy.exc
import requests.exceptions
from dotenv import load_dotenv

<<<<<<< HEAD
=======
from sqlalchemy import create_engine, text

def setup_db():
    """
    Create required schemas and grant permissions before ingestion.
    Safe to run repeatedly.
    """
    log.info("Checking database setup...")

    PG_HOST = os.getenv("PG_HOST", "postgres")
    PG_PORT = os.getenv("PG_PORT", "5432")
    PG_DB   = os.getenv("PG_DB", "hotel_db")
    PG_USER = os.getenv("PG_USER", "pipeline_user")
    PG_PASS = os.getenv("PG_PASS", "supersecretpassword")

    conn_url = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

    try:
        engine = create_engine(conn_url)

        with engine.connect() as conn:
            #Create schemas
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_data"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS production"))

            #Grant permissions
            grant_sql = f"""
            GRANT ALL PRIVILEGES ON SCHEMA raw_data TO {PG_USER};
            GRANT ALL PRIVILEGES ON SCHEMA production TO {PG_USER};
            ALTER DEFAULT PRIVILEGES IN SCHEMA raw_data GRANT ALL ON TABLES TO {PG_USER};
            ALTER DEFAULT PRIVILEGES IN SCHEMA production GRANT ALL ON TABLES TO {PG_USER};
            """
            conn.execute(text(grant_sql))
            conn.commit()

        log.info("Database setup complete (schemas ready).")

    except Exception as e:
        log.error(f"setup_db() failed: {e}")
        raise

>>>>>>> 21ff55e155f3eb9ee2ad79805daf749db3d71516
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

<<<<<<< HEAD
#โหลด .env ก่อน
load_dotenv()

def run_with_retry(func: Callable[[], None], max_retries: int = 3) -> None:

=======
#Load .env

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "config", ".env"))

CORE_DIR = os.path.join(BASE_DIR, "core")
sys.path.append(CORE_DIR)

#Import core

try:
    from core import ingest
except Exception as e:
    log.error(f"โหลด ingest.py ไม่สำเร็จ → {e}")
    raise

try:
    from core import transform
except Exception as e:
    log.error(f"โหลด transform.py ไม่สำเร็จ → {e}")
    raise

try:
    from core import publish
except Exception as e:
    log.warning(f"โหลด publish.py ไม่สำเร็จ (skip ขั้นตอน publish) → {e}")
    def publish():
        log.info("skip publish stage")

#Retry executor
def run_with_retry(func: Callable[[], None], max_retries: int = 3) -> None:
>>>>>>> 21ff55e155f3eb9ee2ad79805daf749db3d71516
    delay = 2

    for attempt in range(1, max_retries + 1):
        try:
<<<<<<< HEAD
            log.info(f"เริ่ม {func.__module__}.{func.__name__} (ครั้งที่ {attempt})")
            func()
            log.info(f"เสร็จ {func.__name__}")
=======
            log.info(f"เริ่ม: {func.__name__} (ครั้งที่ {attempt})")
            func()
            log.info(f"เสร็จ: {func.__name__}")
>>>>>>> 21ff55e155f3eb9ee2ad79805daf749db3d71516
            return

        except (sqlalchemy.exc.OperationalError, requests.exceptions.ConnectionError) as e:
            if attempt == max_retries:
                log.error(f"ล้มเหลวถาวร → {func.__name__} หลัง {max_retries} ครั้ง")
                raise

            log.warning(f"Connection/DB error: {e} → รอ {delay}s แล้วลองใหม่...")
            time.sleep(delay)
<<<<<<< HEAD
            delay *= 2  # backoff

        except Exception as e:
            # ผิด logic หรือ bug จริง → ไม่ retry ให้เจอเร็ว ๆ
            log.error(f"Error ไม่สามารถ retry ได้ใน {func.__name__}: {e}")
            raise

try:
    from ingest import main as ingest_main
except ImportError:
    from ingest import main_ingest as ingest_main

try:
    from transform import main as transform_main
except ImportError:
    from transform import main_transform as transform_main

try:
    from publish import main as publish_main
except ImportError:
    log.error("ไม่พบ publish.py → ข้ามขั้นตอน publish")
    def publish_main(): log.info("publish.py ยังไม่มี → skip")
    publish_main.__name__ = "publish_main"


def run_pipeline() -> None:
    log.info("เริ่ม Hotel Booking Pipeline ทั้งชุด")

    run_with_retry(ingest_main, max_retries=3)
    run_with_retry(transform_main, max_retries=3)
    run_with_retry(publish_main, max_retries=3)

    log.info("Pipeline ครบ 3 ขั้นตอนเรียบร้อย – production_data พร้อมใช้งาน!")


if __name__ == "__main__":
    run_pipeline()
=======
            delay *= 2

        except Exception as e:
            log.error(f"Error ใน {func.__name__}: {e}")
            raise

#Run Pipeline
def run_pipeline() -> None:
    log.info("เริ่ม Hotel Booking Data Pipeline")
    setup_db()

    run_with_retry(ingest.ingest)
    run_with_retry(transform.transform)
    run_with_retry(publish.publish)

    log.info("Pipeline เสร็จสมบูรณ์ — Production data พร้อมใช้งาน")

if __name__ == "__main__":
    run_pipeline()
>>>>>>> 21ff55e155f3eb9ee2ad79805daf749db3d71516
