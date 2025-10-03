from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

SQL_PATH = Path(__file__).parent / "sql/build_analytics_mart.sql"

try:
    with open(SQL_PATH) as file:
        data = file.read()
except FileNotFoundError as err:
    print(f"ERROR: File not found. Reason: {err}")

user = os.getenv("POSTGRES_USER")
password = os.getenv("POSTGRES_PASSWORD")
host = os.getenv("POSTGRES_HOST")
db = os.getenv("POSTGRES_DB")

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")

if data:
    try:
        with engine.begin() as conn:
            for statement in data.split(";"):
                stmt = statement.strip()
                if stmt:
                    conn.execute(text(stmt))
    except SQLAlchemyError as err:
        print(f"ERROR:Failed to connect to database. Reason: {err}")
else:
    print("--- SKIPPING SQL EXECUTION: SQL data is empty. ---")
