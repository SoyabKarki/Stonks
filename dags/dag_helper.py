import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
import time
import sys

sys.path.insert(0, '/opt/airflow')

from extract.reddit_data import get_subreddit_data
from extract.daily_stock_data import get_daily_stock_data
from extract.news_data import get_news_for_ticker
from transform.sentiment import get_ticker_sentiment
from load.db_operations import db_ops
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class RedditDataPipeline:
    def __init__(self):
        self.subreddits = ["investing", "wallstreetbets", "stocks"]
        self.post_limit = 10
        self.news_limit = 5
        self.stock_days = 30
        self.top_tickers_limit = 10
    
    def extract_reddit_data(self) -> List[Any]:
        """Extract Reddit data from multiple subreddits"""
        
        all_posts = []
        for subreddit in self.subreddits:
            try:
                logger.info(f"Extracting posts from r/{subreddit}")

                posts = get_subreddit_data(subreddit, self.post_limit)
                posts_list = list(posts)
                all_posts.extend(posts_list)
                logger.info(f"Extracted {len(posts_list)} posts from r/{subreddit}")
            except Exception as e:
                logger.error(f"Error extracting posts from r/{subreddit}: {str(e)}")

        logger.info(f"Extracted {len(all_posts)} posts from all subreddits")
        return all_posts

    def extract_news_data(self, tickers: Set[str]) -> Dict[str, List[Dict[str, Any]]]:
        """Extract news data for all mentioned tickers"""
        
        news_data = {}
        
        for ticker in tickers:
            try:
                logger.info(f"Extracting news for {ticker}")
                articles = get_news_for_ticker(ticker, page_size=self.news_limit)
                
                if articles:
                    news_data[ticker] = articles
                    logger.info(f"Extracted {len(articles)} news articles for {ticker}")
                else:
                    logger.warning(f"No news articles found for {ticker}")
                    
            except Exception as e:
                logger.error(f"Error extracting news for {ticker}: {str(e)}")
        
        logger.info(f"Extracted news for {len(news_data)} tickers")
        return news_data
    
    def extract_stock_data(self, tickers: Set[str]) -> Dict[str, Dict[str, Any]]:
        """Extract stock data for all mentioned tickers"""
        
        stock_data = {}
        
        for ticker in tickers:
            try:
                logger.info(f"Extracting stock data for {ticker}")
                data = get_daily_stock_data(ticker, output_size="compact")
                
                if data:
                    stock_data[ticker] = data
                    logger.info(f"Extracted stock data for {ticker} ({len(data['daily_data'])} days)")
                else:
                    logger.warning(f"No stock data found for {ticker}")
                    
            except Exception as e:
                logger.error(f"Error extracting stock data for {ticker}: {str(e)}")
        
        logger.info(f"Extracted stock data for {len(stock_data)} tickers")
        return stock_data
    
    def transform_sentiment(self, posts: List[Any]) -> tuple[List[Dict[str, Any]], Set[str]]:
        """Transform Reddit data into sentiment analysis and collect unique tickers"""

        transformed_posts = []
        all_tickers = set()

        for post in posts:
            try:
                # Combine title and body for context
                text = post.title
                if hasattr(post, 'selftext') and post.selftext:
                    text += f"\n\n{post.selftext}"
                
                # Extract tickers and sentiment
                ticker_sentiments = get_ticker_sentiment(text)
                
                if ticker_sentiments:
                    # Collect unique tickers
                    all_tickers.update(ticker_sentiments.keys())
                    
                    # Prepare post data
                    post_data = {
                        'title': post.title,
                        'body': getattr(post, 'selftext', ''),
                        'subreddit': str(post.subreddit),
                        'post_score': getattr(post, 'score', 0),
                        'comment_count': getattr(post, 'num_comments', 0),
                        'created_utc': datetime.fromtimestamp(post.created_utc),
                        'ticker_sentiments': ticker_sentiments
                    }

                    transformed_posts.append(post_data)
                    logger.info(f"Transformed post: {post.title[:50]}... with {len(ticker_sentiments)} tickers")
            
            except Exception as e:
                logger.error(f"Error transforming post {getattr(post, 'id', 'unknown')}: {str(e)}")

        logger.info(f"Transformed {len(transformed_posts)} posts with {len(all_tickers)} unique tickers")
        return transformed_posts, all_tickers
    
    def load_reddit_data(self, transformed_posts: List[Dict[str, Any]]) -> int:
        """Load transformed Reddit data and ticker mentions data to respective database tables"""

        loaded_count = 0
        
        for post in transformed_posts:
            try:
                # Extract ticker sentiments for separate insertion
                ticker_sentiments = post.pop('ticker_sentiments', {})

                # Insert post data
                post_id = db_ops.insert_reddit_data(post)

                if post_id:
                    # Insert ticker mentions
                    success = db_ops.insert_ticker_mentions(post_id, ticker_sentiments)

                    if success:
                        loaded_count += 1
                        logger.info(f"Loaded post {post_id} with {len(ticker_sentiments)} ticker mentions")
                    else:
                        logger.error(f"Failed to load ticker mentions for post {post_id}")
                else:
                    logger.error(f"Failed to insert post {post['title'][:50]}...")

            except Exception as e:
                logger.error(f"Error loading post data: {str(e)}")

        return loaded_count

    def load_news_data(self, news_data: Dict[str, List[Dict[str, Any]]]) -> int:
        """Load news data to database"""
        
        loaded_count = 0
        
        for ticker, articles in news_data.items():
            try:
                success = db_ops.insert_news_articles(ticker, articles)
                
                if success:
                    loaded_count += 1
                    logger.info(f"Loaded {len(articles)} news articles for {ticker}")
                else:
                    logger.error(f"Failed to load news articles for {ticker}")
                    
            except Exception as e:
                logger.error(f"Error loading news data for {ticker}: {str(e)}")
        
        return loaded_count

    def load_stock_data(self, stock_data: Dict[str, Dict[str, Any]]) -> int:
        """Load stock data to database"""
        
        loaded_count = 0
        
        for ticker, data in stock_data.items():
            try:
                success = db_ops.insert_stock_data(ticker, data)
                
                if success:
                    loaded_count += 1
                    logger.info(f"Loaded stock data for {ticker}")
                else:
                    logger.error(f"Failed to load stock data for {ticker}")
                    
            except Exception as e:
                logger.error(f"Error loading stock data for {ticker}: {str(e)}")
        
        return loaded_count

    
    def _test_pipeline(self):
        """Run the full ETL pipeline"""

        logger.info("Starting the Reddit data pipeline")
        start_time = time.time()

        try:
            # Extract Reddit data
            logger.info("1: Extracting Reddit data...")
            posts = self.extract_reddit_data()
            
            if not posts:
                logger.warning("No posts extracted. Pipeline stopping.")
                return

            # Transform data and extract ticker mentions
            logger.info("2: Transforming data...")
            transformed_posts, unique_tickers = self.transform_sentiment(posts)
            
            if not transformed_posts:
                logger.warning("No posts with ticker mentions found. Pipeline stopping.")
                return

            # Extract news data for all mentioned tickers
            logger.info("3: Extracting news data...")
            news_data = self.extract_news_data(unique_tickers)

            # Extract stock data for all mentioned tickers
            logger.info("4: Extracting stock data...")
            stock_data = self.extract_stock_data(unique_tickers)

            # Load all data to database
            logger.info("5: Loading to database...")
            reddit_loaded = self.load_reddit_data(transformed_posts)
            news_loaded = self.load_news_data(news_data)
            stock_loaded = self.load_stock_data(stock_data)
            
            # Refresh materialized view if Reddit data was loaded
            if reddit_loaded > 0:
                db_ops.refresh_materialized_view()
                logger.info("Refreshed materialized view")

            # Pipeline summary
            end_time = time.time()
            duration = end_time - start_time
            
            logger.info(f"Pipeline completed successfully!")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Posts processed: {len(posts)}")
            logger.info(f"Posts with tickers: {len(transformed_posts)}")
            logger.info(f"Unique tickers found: {len(unique_tickers)}")
            logger.info(f"Reddit posts loaded: {reddit_loaded}")
            logger.info(f"News articles loaded: {news_loaded}")
            logger.info(f"Stock datasets loaded: {stock_loaded}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


etl_pipeline = RedditDataPipeline()


if __name__ == "__main__":
    # Testing
    etl_pipeline._test_pipeline()