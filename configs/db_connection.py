import psycopg2
from dotenv import load_dotenv
import os
import logging

from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()


class DatabaseConnection:
    def __init__(self):
        self.connection_params = {
            "host": os.getenv("POSTGRES_HOST"),
            "database": os.getenv("POSTGRES_DB"),
            "user": os.getenv("POSTGRES_USER"),
            "password": os.getenv("POSTGRES_PASSWORD"),
            "port": os.getenv("POSTGRES_PORT"),
        }

    def get_connection(self):
        """
        Get a connection to the database.
        """
        return psycopg2.connect(**self.connection_params)

    def test_connection(self):
        """Test database connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logger.info(f"Database connected: {version[0]}")
            cursor.close()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {str(e)}")
            return False


db = DatabaseConnection()

if __name__ == "__main__":
    # Test the connection
    if db.test_connection():
        print("Database connection successful")
    else:
        print("Database connection failed")