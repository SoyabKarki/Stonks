import logging
from datetime import datetime
from typing import List, Dict, Any
import time

from extract.reddit_data import get_subreddit_data
from transform.sentiment import get_ticker_sentiment
from load.db_operations import db_ops
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


class RedditDataPipeline:
    def __init__(self):
        self.subreddits = ["investing", "wallstreetbets", "stocks"]
        self.post_limit = 10
    
    def _extract_reddit_data(self) -> List[Any]:
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
    
    def _transform_sentiment(self, posts: List[Any]) -> List[Dict[str, Any]]:
        """Transform Reddit data into sentiment analysis"""

        transformed_posts = []

        for post in posts:
            try:
                # Combine title and body for context
                text = post.title
                if hasattr(post, 'selftext') and post.selftext:
                    text += f"\n\n{post.selftext}"
                
                # Extract tickers and sentiment
                ticker_sentiments = get_ticker_sentiment(text)
                
                if ticker_sentiments:
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

        logger.info(f"Transformed {len(transformed_posts)} posts")
        return transformed_posts
    
    def _load_to_database(self, transformed_posts: List[Dict[str, Any]]) -> int:
        """Load transformed data to database"""

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

        # Refresh materialized view
        if loaded_count > 0:
            db_ops.refresh_materialized_view()
            logger.info("Refreshed materialized view")

        return loaded_count
    
    def run_pipeline(self):
        """Run the full ETL pipeline"""

        logger.info("Starting the Reddit data pipeline")
        start_time = time.time()

        try:
            # Extract
            logger.info("Step 1: Extracting Reddit data...")
            posts = self._extract_reddit_data()
            
            if not posts:
                logger.warning("No posts extracted. Pipeline stopping.")
                return

            # Transform
            logger.info("Step 2: Transforming data...")
            transformed_data = self._transform_sentiment(posts)
            
            if not transformed_data:
                logger.warning("No posts with ticker mentions found. Pipeline stopping.")
                return

            # Load
            logger.info("Step 3: Loading to database...")
            loaded_count = self._load_to_database(transformed_data)
            
            logger.info(f"Pipeline completed successfully!")
            logger.info(f"Posts processed: {len(posts)}")
            logger.info(f"Posts with tickers: {len(transformed_data)}")
            logger.info(f"Posts loaded: {loaded_count}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            raise


etl_pipeline = RedditDataPipeline()


if __name__ == "__main__":
    etl_pipeline.run_pipeline()