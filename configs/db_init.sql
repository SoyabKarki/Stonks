-- Reddit posts with sentiment analysis
CREATE TABLE IF NOT EXISTS reddit_posts (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(20) NOT NULL,
    title TEXT,
    body TEXT,
    subreddit VARCHAR(50) NOT NULL,
    post_score INTEGER,
    comment_count INTEGER,
    created_utc TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);

-- Ticker symbols mentioned in Reddit posts and their sentiment
CREATE TABLE IF NOT EXISTS ticker_mentions (
    id SERIAL PRIMARY KEY,
    post_id VARCHAR(20) REFERENCES reddit_posts(post_id),
    ticker VARCHAR(10) NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,
    sentiment_score DECIMAL(3,2),
    UNIQUE(post_id, ticker)
);

-- Create indexes for faster queries