import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import Dict, List, Any
from datetime import datetime

from configs.db_connection import db
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class DatabaseOperations:
    def __init__(self):
        self.db = db

    def insert_reddit_data(self, post_data: Dict[str, Any]) -> int:
        """
        Insert Reddit data into the database and return the ID of the inserted row.
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            query = """
                INSERT INTO reddit_posts (title, body, subreddit, post_score, comment_count, created_utc)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
            """

            cursor.execute(query, (
                post_data['title'],
                post_data['body'],
                post_data['subreddit'],
                post_data['post_score'],
                post_data['comment_count'],
                post_data['created_utc']
            ))

            post_id = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()

            logger.info(f"Inserted Reddit data with ID: {post_id}")
            return post_id

        except Exception as e:
            logger.error(f"Error inserting Reddit data: {str(e)}")
            return None

    def insert_ticker_mentions(self, post_id: int, ticker_sentiments: Dict[str, Any]) -> int:
        """
        Insert ticker mentions with sentiment data into the database and return the ID of the inserted row.
        """

        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()

            for ticker, sentiment_data in ticker_sentiments.items():
                query = """
                    INSERT INTO ticker_mentions (post_id, ticker, sentiment_label, sentiment_score, context)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (post_id, ticker) DO UPDATE SET
                        sentiment_label = EXCLUDED.sentiment_label,
                        sentiment_score = EXCLUDED.sentiment_score,
                        context = EXCLUDED.context
                """

                cursor.execute(query, (
                    post_id,
                    ticker,
                    sentiment_data['label'],
                    sentiment_data['score'],
                    sentiment_data.get('context', '')
                ))

            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Inserted {len(ticker_sentiments)} ticker mentions for post {post_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting ticker mentions: {str(e)}")
            return False

    def refresh_materialized_view(self):
        """
        Refresh the materialized view
        """
        
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            cursor.execute("REFRESH MATERIALIZED VIEW mv_ticker_mentions;")
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info("Refreshed materialized view")
            return True
            
        except Exception as e:
            logger.error(f"Error refreshing view: {str(e)}")
            return False

    def get_dashboard_data(self) -> List[Dict[str, Any]]:
        """
        Get data for dashboard:
        - Top tickers by mention count
        """

        try:
            conn = self.db.get_connection()
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            cursor.execute("SELECT * FROM mv_ticker_mentions ORDER BY mention_count DESC;")
            results = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return [dict(row) for row in results]
            
        except Exception as e:
            logger.error(f"Error getting dashboard data: {str(e)}")
            return []

    def _test_operations(self) -> int:
        """
        Test database operations with sample data
        """

        try:
            # Test inserting a sample post
            sample_post = {
                'title': 'Test post about $AAPL',
                'body': 'I love Apple stock!',
                'subreddit': 'investing',
                'post_score': 100,
                'comment_count': 25,
                'created_utc': datetime.now()
            }
            
            post_id = self.insert_reddit_data(sample_post)
            
            if post_id:
                # Test inserting ticker mentions
                sample_sentiments = {
                    'AAPL': {
                        'label': 'positive',
                        'score': 0.85,
                        'context': 'I love Apple stock!'
                    }
                }
                
                success = self.insert_ticker_mentions(post_id, sample_sentiments)
                
                if success:
                    logger.info("Database operations test successful!")
                    return post_id
            
            return None
            
        except Exception as e:
            logger.error(f"Database operations test failed: {str(e)}")
            return None

    def _cleanup_test_data(self, post_id: int):
        """
        Clean up test data by deleting the post and its ticker mentions
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Delete ticker mentions first
            cursor.execute("DELETE FROM ticker_mentions WHERE post_id = %s", (post_id,))
            
            # Delete the post
            cursor.execute("DELETE FROM reddit_posts WHERE id = %s", (post_id,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned up test data for post ID: {post_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up test data: {str(e)}")
            return False


db_ops = DatabaseOperations()


if __name__ == "__main__":
    post_id = db_ops._test_operations()
    
    if post_id:
        print("Database operations working!")
        
        # Clean up the test data
        if db_ops._cleanup_test_data(post_id):
            print("Test data cleaned up successfully!")
        else:
            print("Failed to clean up test data!")
    else:
        print("Database operations failed!")