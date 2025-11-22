import pandas as pd
from sqlalchemy import create_engine

# โหลด CSV
df = pd.read_csv("hotel_bookings.csv")

# สร้าง connection
engine = create_engine("postgresql://admin:admin@localhost:5432/hotel_dw")

# Load เข้า table fact_bookings
df.to_sql("fact_bookings", engine, index=False, if_exists="replace")

print("Initial Load Completed")