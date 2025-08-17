import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv(".env")

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "kafka://kafka:9092")

# OLTP Postgres (reuse from Lesson 1 .env)
OLTP_USER = os.getenv("OLTP_USER", "app")
OLTP_PASSWORD = os.getenv("OLTP_PASSWORD", "app_password")
OLTP_DB = os.getenv("OLTP_DB", "ledgercraft_oltp")
OLTP_PORT = int(os.getenv("OLTP_PORT", "5432"))
OLTP_HOST = os.getenv("OLTP_HOST", "oltp-postgres")

DATABASE_URL = f"postgresql://{OLTP_USER}:{OLTP_PASSWORD}@{OLTP_HOST}:{OLTP_PORT}/{OLTP_DB}"

# Velocity settings (tune via env if desired)
WINDOW_SECONDS = int(os.getenv("VELOCITY_WINDOW_SECONDS", "300"))              # 5 minutes
COUNT_THRESHOLD = int(os.getenv("VELOCITY_COUNT_THRESHOLD", "5"))
SUM_THRESHOLD_VND = Decimal(os.getenv("VELOCITY_SUM_THRESHOLD_VND", "2000000"))
SUM_THRESHOLD_USD = Decimal(os.getenv("VELOCITY_SUM_THRESHOLD_USD", "100"))
