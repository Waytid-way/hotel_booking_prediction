import os
import time
import logging
from typing import Callable

import sqlalchemy.exc
import requests.exceptions
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

#โหลด .env ก่อน
load_dotenv()

def run_with_retry(func: Callable[[], None], max_retries: int = 3) -> None:

    delay = 2

    for attempt in range(1, max_retries + 1):
        try:
            log.info(f"เริ่ม {func.__module__}.{func.__name__} (ครั้งที่ {attempt})")
            func()
            log.info(f"เสร็จ {func.__name__}")
            return

        except (sqlalchemy.exc.OperationalError, requests.exceptions.ConnectionError) as e:
            if attempt == max_retries:
                log.error(f"ล้มเหลวถาวร → {func.__name__} หลัง {max_retries} ครั้ง")
                raise

            log.warning(f"Connection/DB error: {e} → รอ {delay}s แล้วลองใหม่...")
            time.sleep(delay)
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