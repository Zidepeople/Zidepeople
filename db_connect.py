import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine


def build_database_url() -> str:
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "3306")
    name = os.getenv("DB_NAME")

    if not all([user, password, host, name]):
        raise ValueError(
            "Missing DB environment variables. Set DB_USER, DB_PASSWORD, DB_HOST, and DB_NAME."
        )

    return (
        f"mysql+pymysql://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{name}"
    )

try:
    DATABASE_URL = build_database_url()
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()

    print("✅ Database connected successfully")

    connection.close()

except Exception as e:
    print("❌ Connection failed:", e)