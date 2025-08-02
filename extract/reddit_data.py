import logging
import time

from configs.praw_config import reddit
from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

def get_subreddit_data(subreddit_name: str, limit: int = 100):
    """
    Retrieve top posts from a subreddit.

    Parameters
    ----------
    subreddit_name : str
        Name of the subreddit (e.g., "python", "investing").
    limit : int, optional
        Maximum number of posts to retrieve (default is 100).

    Returns
    -------
    praw.models.ListingGenerator
        A generator of PRAW Submission objects representing the top posts.   
    """
    
    logger.info(f"Getting posts from {subreddit_name} with limit {limit}")
    subreddit = reddit.subreddit(subreddit_name)
    return subreddit.top(limit=limit)

if __name__ == "__main__":
    # Testing
    subreddit_name = "python"
    limit = 10
    start = time.time() 
    subreddit_data = get_subreddit_data(subreddit_name, limit)
    for post in subreddit_data:
        print(f"{post.title} (Score: {post.score})")

    end = time.time()
    logger.info(f"Time taken: {end - start} seconds")
