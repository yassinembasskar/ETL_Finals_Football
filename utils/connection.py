# utils/connection.py
import os
import cx_Oracle
from dotenv import load_dotenv
from utils.logging_setup import setup_logger

# Load environment variables
load_dotenv()

# Logger for DB connection
logger = setup_logger("connection", "logs/connection.log")

def get_connection():
    """
    Create and return a new Oracle database connection.
    """
    try:
        dsn = cx_Oracle.makedsn(
            os.getenv("ORACLE_HOST"),
            os.getenv("ORACLE_PORT"),
            service_name=os.getenv("ORACLE_SERVICE")
        )
        conn = cx_Oracle.connect(
            user=os.getenv("ORACLE_USER"),
            password=os.getenv("ORACLE_PASSWORD"),
            dsn=dsn
        )
        logger.info("✅ Connected to Oracle Database")
        return conn
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        logger.error(f"❌ Database connection error: {error.message}")
        raise  # propagate exception so caller knows
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        raise