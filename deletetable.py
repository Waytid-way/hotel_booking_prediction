from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()

engine = create_engine(
    f"postgresql+psycopg2://"
    f"{os.getenv('PG_USER')}:{os.getenv('PG_PASS')}@"
    f"{os.getenv('PG_HOST', 'localhost')}:{os.getenv('PG_PORT', '5432')}/"
    f"{os.getenv('PG_DB')}"
)

with engine.connect() as conn:
    conn.execute(text('DROP TABLE IF EXISTS "transform_data"'))

print("ลบตารางขยะทั้งหมดเรียบร้อยแล้ว")