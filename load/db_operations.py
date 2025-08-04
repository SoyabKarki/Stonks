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

    def insert_news_articles(self, ticker: str, articles: List[Dict[str, Any]]) -> bool:
        """Insert news articles for a ticker"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for article in articles:
                query = """
                    INSERT INTO news_articles (ticker, title, description, url, source, published_at, content)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """
                
                cursor.execute(query, (
                    ticker,
                    article['title'],
                    article['description'],
                    article['url'],
                    article['source'],
                    article['published_at'],
                    article['content']
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Inserted {len(articles)} news articles for {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting news articles: {str(e)}")
            return False

    def insert_stock_data(self, ticker: str, stock_data: Dict[str, Any]) -> bool:
        """Insert stock data for a ticker"""
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            for date, data in stock_data['daily_data'].items():
                query = """
                    INSERT INTO stock_data (ticker, date, open_price, high_price, low_price, close_price, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (ticker, date) DO UPDATE SET
                        open_price = EXCLUDED.open_price,
                        high_price = EXCLUDED.high_price,
                        low_price = EXCLUDED.low_price,
                        close_price = EXCLUDED.close_price,
                        volume = EXCLUDED.volume
                """
                
                cursor.execute(query, (
                    ticker,
                    date,
                    data['open'],
                    data['high'],
                    data['low'],
                    data['close'],
                    data['volume']
                ))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Inserted stock data for {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"Error inserting stock data: {str(e)}")
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

    def _test_reddit_and_ticker_operations(self) -> int:
        """
        Test database operations for reddit data and ticker mentions with sample data
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

    def _test_stock_data_operations(self) -> bool:
        """
        Test stock data operations with sample data
        """
        try:
            # Test inserting sample stock data
            sample_stock_data = {
                'daily_data': {
                    '2024-01-15': {
                        'open': 150.00,
                        'high': 155.50,
                        'low': 149.75,
                        'close': 152.25,
                        'volume': 1000000
                    },
                    '2024-01-16': {
                        'open': 152.25,
                        'high': 158.00,
                        'low': 151.50,
                        'close': 156.75,
                        'volume': 1200000
                    }
                }
            }
            
            success = self.insert_stock_data('AAPL', sample_stock_data)
            
            if success:
                logger.info("Stock data operations test successful!")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Stock data operations test failed: {str(e)}")
            return False

    def _test_news_articles_operations(self) -> bool:
        """
        Test news articles operations with sample data
        """
        try:
            # Test inserting sample news articles
            sample_articles = [
                {
                    'title': 'Apple Reports Strong Q4 Earnings',
                    'description': 'Apple Inc. reported better-than-expected quarterly earnings...',
                    'url': 'https://example.com/apple-earnings',
                    'source': 'Financial Times',
                    'published_at': datetime.now(),
                    'content': 'Apple Inc. reported better-than-expected quarterly earnings...'
                },
                {
                    'title': 'Apple Stock Hits New High',
                    'description': 'Shares of Apple reached a new all-time high...',
                    'url': 'https://example.com/apple-stock-high',
                    'source': 'Reuters',
                    'published_at': datetime.now(),
                    'content': 'Shares of Apple reached a new all-time high...'
                }
            ]
            
            success = self.insert_news_articles('AAPL', sample_articles)
            
            if success:
                logger.info("News articles operations test successful!")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"News articles operations test failed: {str(e)}")
            return False

    def _test_all_operations(self):
        """
        Test all database operations with sample data
        """
        logger.info("Starting comprehensive database operations test...")
        
        # Test Reddit and ticker mentions operations
        post_id = self._test_reddit_and_ticker_operations()
        
        # Test stock data operations
        stock_success = self._test_stock_data_operations()
        
        # Test news articles operations
        news_success = self._test_news_articles_operations()
        
        # Clean up all test data
        cleanup_results = []
        
        if post_id:
            cleanup_results.append(self._cleanup_test_data(post_id))
        
        if stock_success:
            cleanup_results.append(self._cleanup_stock_test_data('AAPL'))
        
        if news_success:
            cleanup_results.append(self._cleanup_news_test_data('AAPL'))
        
        # Report results
        if post_id and stock_success and news_success:
            logger.info("All database operations tests passed!")
            print("All database operations working!")
            
            if all(cleanup_results):
                print("All test data cleaned up successfully!")
            else:
                print("Some test data cleanup failed!")
        else:
            logger.error("Some database operations tests failed!")
            print("Some database operations failed!")
            
            if post_id:
                print("Reddit data operations: PASSED")
            else:
                print("Reddit data operations: FAILED")
                
            if stock_success:
                print("Stock data operations: PASSED")
            else:
                print("Stock data operations: FAILED")
                
            if news_success:
                print("News articles operations: PASSED")
            else:
                print("News articles operations: FAILED")



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

    def _cleanup_stock_test_data(self, ticker: str):
        """
        Clean up test stock data by deleting the stock data entries
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Delete stock data for the ticker
            cursor.execute("DELETE FROM stock_data WHERE ticker = %s", (ticker,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned up test stock data for ticker: {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up test stock data: {str(e)}")
            return False

    def _cleanup_news_test_data(self, ticker: str):
        """
        Clean up test news data by deleting the news articles entries
        """
        try:
            conn = self.db.get_connection()
            cursor = conn.cursor()
            
            # Delete news articles for the ticker
            cursor.execute("DELETE FROM news_articles WHERE ticker = %s", (ticker,))
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Cleaned up test news data for ticker: {ticker}")
            return True
            
        except Exception as e:
            logger.error(f"Error cleaning up test news data: {str(e)}")
            return False


db_ops = DatabaseOperations()


if __name__ == "__main__":
    # Test all operations
    db_ops._test_all_operations()