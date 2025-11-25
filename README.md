
---

# **Hotel Booking Data Pipeline – README**

## **Overview**

โปรเจกต์นี้เป็นระบบ ETL Pipeline เต็มรูปแบบที่ออกแบบเพื่อประมวลผลชุดข้อมูล Hotel Booking จาก Kaggle โดยประกอบด้วยสามขั้นตอนหลัก ได้แก่ Ingest (โหลดข้อมูล CSV เข้าฐานข้อมูล), Transform (ทำความสะอาดข้อมูลและ Feature Engineering) และ Publish (ส่งออกข้อมูลไปยัง Google Sheets สำหรับงาน BI/Visualization)

โครงสร้างทั้งหมดทำงานผ่าน Docker Compose เพื่อให้การรันมีความเสถียรและสามารถทำซ้ำได้ในทุกสภาพแวดล้อม

---

## **Features**

* รองรับการทำงานแบบ ETL ครบทั้ง Ingest → Transform → Publish
* แยก Schema ในฐานข้อมูลเป็น `raw_data` และ `production`
* แยกโมดูลชัดเจน (ingest.py, transform.py, publish.py)
* รองรับ Chunk Processing สำหรับข้อมูลใหญ่
* Pipeline มีระบบ Retry อัตโนมัติสำหรับ Connection/DB Error
* ปรับใช้กับ Google Sheets API ผ่าน Service Account
* รันบน Docker Compose ได้ทันทีโดยไม่ต้องติดตั้งเครื่องมือเพิ่มเติมในเครื่อง

---

## **Project Structure**

```
hotel_booking/
├── config/
│   ├── .env
│   └── __init__.py
│
├── core/
│   ├── ingest.py
│   ├── transform.py
│   ├── publish.py
│   └── __init__.py
│
├── data/
│   └── hotel_bookings.csv
│
├── google_service_account.json
│
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── run_pipeline.py
└── README.md
```

---

## **Prerequisites**

ก่อนเริ่มใช้งาน จำเป็นต้องมี:

* Docker และ Docker Compose
* Google Cloud Service Account พร้อมเปิดใช้งาน:

  * Google Sheets API
  * Google Drive API
* JSON Credentials (`google_service_account.json`)

---

## **Environment Setup**

สร้างไฟล์ `.env` ภายใต้ `config/`

```
PG_HOST=postgres
PG_PORT=5432
PG_DB=hotel_db
PG_USER=pipeline_user
PG_PASS=supersecretpassword

GSHEET_KEY_PATH=google_service_account.json
GSHEET_NAME=Hotel Booking Data Staging
```

---

## **Google Sheets Setup**

### 1. สร้าง Google Sheet ใหม่

* เปิด Google Sheets
* ตั้งชื่อให้ตรงกับ `.env` เช่น `Hotel Booking Data Staging`

### 2. ตั้งค่า Google Cloud

* เปิด Google Cloud Console
* เปิดใช้งาน:

  * Google Sheets API
  * Google Drive API

### 3. สร้าง Service Account

* ไปที่ IAM & Admin → Service Accounts
* สร้าง Service Account ใหม่
* Assign Role: Editor

### 4. สร้าง JSON Key

* Service Account → แท็บ Keys
* Add Key → JSON
* ดาวน์โหลดและวางไฟล์ไว้ที่ root ของโปรเจกต์
  (ชื่อไฟล์ต้องตรงกับ `GSHEET_KEY_PATH` ใน `.env`)

### 5. แชร์ Google Sheet

เปิด Google Sheet → Share
ใส่ `client_email` จาก JSON เช่น:

```
hotel-pipeline@myproject.iam.gserviceaccount.com
```

ให้สิทธิ์ Editor

---

## **Running the System**

### 1. สั่ง Reset + Build ใหม่ทั้งหมด

```
docker compose down --volumes --remove-orphans
docker compose up -d --build
```

ตรวจสอบสถานะ:

```
docker ps
```

ต้องเห็น:

* `hotel_pg`   — Healthy
* `hotel_pipeline` — Up

---

### 2. เข้าสู่ Container และรัน Pipeline

```
docker exec -it hotel_pipeline bash
python run_pipeline.py
```

Pipeline จะทำงานครบ 3 ขั้นตอน:

1. `setup_db()` – สร้าง schema และตั้งสิทธิ์
2. `ingest.ingest()` – โหลดข้อมูล CSV → PostgreSQL/raw_data
3. `transform.transform()` – Clean + Feature Engineering → PostgreSQL/production
4. `publish.publish()` – ส่งข้อมูลขึ้น Google Sheets

---

## **Pipeline Workflow**

### **1. Ingest**

* อ่านข้อมูลจาก CSV (`data/hotel_bookings.csv`)
* โหลดเข้า PostgreSQL table:
  `raw_data.hotel_bookings_raw`
* รองรับ Chunk Size เพื่อป้องกัน Memory Overflow

### **2. Transform**

* ลบค่าซ้ำ
* จัดการ Missing Value
* สร้างคอลัมน์ใหม่ เช่น:

  * arrival_date (validate)
  * total_nights
  * total_revenue
  * lead_time_group
  * status_label
* ตัดข้อมูลที่ไม่สมเหตุผล เช่น nights ≤ 0
* บันทึกลง PostgreSQL table:
  `production.bi_data`

### **3. Publish**

* ดึงข้อมูลจาก `production.bi_data`
* ล้างข้อมูลใน Google Sheet
* อัปโหลดข้อมูลทั้งหมดขึ้น Sheet ใหม่

---

## **Technologies Used**

* Python 3.11
* Pandas
* SQLAlchemy
* gspread
* PostgreSQL 16
* Docker Compose
* dotenv

---

## **Future Improvements**

* เพิ่ม Incremental Load
* รองรับ Data Lake (S3 / MinIO)
* เพิ่ม Scheduled Jobs ผ่าน Airflow / Prefect
* ทำ Data Quality Report (DQ Metrics)
* รองรับหลาย Data Sources เช่น API, Parquet

---

## **Conclusion**

ระบบนี้เป็นตัวอย่างของ Data Engineering Pipeline ที่มีคุณภาพ production-ready โครงสร้างชัดเจน รองรับการขยายในอนาคต และเข้าใจง่ายสำหรับผู้เริ่มต้น รวมถึงสามารถนำไปต่อยอดเป็นระบบจริงในบริษัทได้ทันที

---
