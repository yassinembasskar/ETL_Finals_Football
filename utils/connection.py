# utils/connection.py
import os
import psycopg2
from dotenv import load_dotenv
from utils.logging_setup import setup_logger

load_dotenv()

logger = setup_logger("connection", "logs/connection.log")

def get_connection():
    """
    Create and return a new PostgreSQL database connection.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD")
        )
        logger.info("✅ Connected to PostgreSQL Database")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"❌ Database connection error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise
