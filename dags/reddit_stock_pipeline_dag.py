from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from dag_helper import RedditDataPipeline
from load.db_operations import db_ops

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}


with DAG(
    dag_id='reddit_data_pipeline',
    default_args=default_args,
    description='Daily ETL of Reddit posts → sentiment → news & stock data → load into Postgres',
    schedule='@daily',
    catchup=False,
    tags=['reddit', 'stocks', 'news'],
) as dag:

    pipeline = RedditDataPipeline()

    @task
    def extract_reddit():
        """Pull posts from all subreddits"""
        return pipeline.extract_reddit_data()

    @task
    def transform(posts):
        """
        Run sentiment/ticker extraction.
        Returns a tuple: (transformed_posts, unique_tickers)
        """
        return pipeline.transform_sentiment(posts)

    @task
    def extract_news(unique_tickers):
        """Fetch latest news for all tickers"""
        return pipeline.extract_news_data(unique_tickers)    

    @task
    def extract_stock(unique_tickers):
        """Fetch historical stock prices for tickers"""
        return pipeline.extract_stock_data(unique_tickers)   

    @task
    def load_reddit(transformed_posts):
        """
        Load posts + ticker mentions into the DB.
        """
        return pipeline.load_reddit_data(transformed_posts)

    @task
    def load_news(news_data):
        """Load news articles into the DB"""
        return pipeline.load_news_data(news_data)

    @task
    def load_stock(stock_data):
        """Load stock data into the DB"""
        return pipeline.load_stock_data(stock_data)

    @task
    def refresh_view(post_load_count):
        """
        Refresh the materialized view, but only if we loaded any Reddit posts.
        """
        if post_load_count and post_load_count > 0:
            db_ops.refresh_materialized_view()
        return

    @task
    def get_tickers(transformed_and_tickers):
        """Extract tickers from the transform result"""
        return transformed_and_tickers[1]

    @task
    def get_transformed_posts(transformed_and_tickers):
        """Extract transformed posts from the transform result"""
        return transformed_and_tickers[0]

    
    posts = extract_reddit()
    transformed_and_tickers = transform(posts)
        
    tickers = get_tickers(transformed_and_tickers)

    news_data  = extract_news(tickers)
    stock_data = extract_stock(tickers)

    transformed_posts = get_transformed_posts(transformed_and_tickers)
    reddit_count = load_reddit(transformed_posts)
    news_count   = load_news(news_data)
    stock_count  = load_stock(stock_data)

    refresh_view(reddit_count)