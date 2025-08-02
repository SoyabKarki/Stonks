import logging
import time

from configs.praw_config import reddit
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def get_subreddit_data(subreddit_name: str, limit: int = 100):
    subreddit = reddit.subreddit(subreddit_name)
    return subreddit.top(limit=limit)

if __name__ == "__main__":
    subreddit_name = "python"
    limit = 10
    start = time.time() 
    subreddit_data = get_subreddit_data(subreddit_name, limit)
    for post in subreddit_data:
        print(f"{post.title} (Score: {post.score})")

    end = time.time()
    logger.info(f"Time taken: {end - start} seconds")
