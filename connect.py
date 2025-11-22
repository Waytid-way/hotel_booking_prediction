from sqlalchemy import create_engine

engine = create_engine("postgresql://admin:admin@localhost:5432/hotel_dw")

try:
    conn = engine.connect()
    print("Connected OK")
except Exception as e:
    print("Error:", e)