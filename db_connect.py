from sqlalchemy import create_engine

DATABASE_URL = "mysql+pymysql://zide_admin@zidepeople-dev:Suggestpassword56@zidepeople-dev.mysql.database.azure.com:3306/zidepeople_db"

try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()

    print("✅ Database connected successfully")

    connection.close()

except Exception as e:
    print("❌ Connection failed:", e)