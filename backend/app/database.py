import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/gridguard"
)


def get_connection():
    return psycopg.connect(DATABASE_URL)


def get_db():
    conn = get_connection()

    try:
        yield conn
    finally:
        conn.close()