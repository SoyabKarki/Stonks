import requests
import logging
from typing import List, Dict, Any
import os
from dotenv import load_dotenv

from configs.logging_config import setup_logging

load_dotenv()

setup_logging()
logger = logging.getLogger(__name__)

api_key = os.getenv("NEWS_API_KEY")
base_url = "https://newsapi.org/v2"

if not api_key:
    raise ValueError("NEWS_API_KEY not found in environment variables")
    logger.error("NEWS_API_KEY not found in environment variables")

def get_news_for_ticker(self, ticker: str, page: int = 1, page_size: int = 5, language: str = "en") -> List[Dict[str, Any]]:
    """
    Get news articles for a specific ticker
    
    Args:
        ticker: Stock ticker symbol (e.g., 'AAPL', 'GOOGL')
        page_size: Number of articles to return (max 100)
        days_back: Number of days to look back for news
        
    Returns:
        List of news articles with metadata
    """
    try:
        query = f"{ticker} stock"

        url = f"{base_url}/everything"
        params = {
            "apiKey": api_key,
            "q": query,
            "page": page,
            "pageSize": page_size,
            "language": language,
        }

        logger.info(f"Fetching news for {ticker}")
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()
        if data['status'] == 'ok':
            articles = data.get('articles', [])
            logger.info(f"Found {len(articles)} news articles for {ticker}")

            # Process and format articles
            processed_articles = []
            for article in articles:
                processed_article = {
                    'ticker': ticker,
                    'title': article.get('title', ''),
                    'description': article.get('description', ''),
                    'url': article.get('url', ''),
                    'source': article.get('source', {}).get('name', ''),
                    'published_at': article.get('publishedAt', ''),
                    'content': article.get('content', ''),
                }
                processed_articles.append(processed_article)

            return processed_articles
        else:
            logger.error(f"News API error: {data.get('message', 'Unknown error')}")
            return []

    except Exception as e:
        logger.error(f"Error fetching news for {ticker}: {str(e)}")
        return []
    

if __name__ == "__main__":
    # Testing
    ticker = "AAPL"
    page = 1
    page_size = 5
    language = "en"
    news_data = get_news_for_ticker(ticker, page, page_size, language)
    print(news_data)