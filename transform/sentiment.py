from transformers import pipeline
import logging

from extract.reddit_data import get_subreddit_data
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# Load FinBERT sentiment analysis pipeline
pipe = pipeline("text-classification", model="ProsusAI/finbert")

# Get posts from Reddit
posts = get_subreddit_data("investing", limit=1)

def main():
    subreddit_name = "investing"
    post_limit = 1

    logger.info(f"Getting posts from {subreddit_name} with limit {post_limit}")
    posts = get_subreddit_data(subreddit_name, post_limit)

    titles = [post.title for post in posts]
    results = pipe(titles, batch_size=16)

    # Display results
    for title, result in zip(titles, results):
        sentiment = result['label']
        print(f"{title}\n\t→ Sentiment: {sentiment}")

if __name__ == "__main__":
    main()