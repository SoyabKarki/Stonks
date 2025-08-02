from dotenv import load_dotenv
import os
import praw

load_dotenv()

# Environment variables
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_AGENT = os.getenv("CLIENT_USER_AGENT")

# Initialize PRAW
reddit = praw.Reddit(
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    user_agent=USER_AGENT
)

if __name__ == "__main__":
    # Test the connection
    print(reddit.read_only)