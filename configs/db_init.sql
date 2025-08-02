-- Reddit posts with sentiment analysis
CREATE TABLE IF NOT EXISTS reddit_posts (
    id SERIAL PRIMARY KEY,
    title TEXT,
    body TEXT,
    subreddit VARCHAR(50) NOT NULL,
    post_score INTEGER,
    comment_count INTEGER,
    created_utc TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Ticker symbols mentioned in Reddit posts and their sentiment
CREATE TABLE IF NOT EXISTS ticker_mentions (
    id SERIAL PRIMARY KEY,
    post_id INTEGER REFERENCES reddit_posts(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    sentiment_label VARCHAR(20) NOT NULL,
    sentiment_score DECIMAL(3,2),
    context TEXT,
    UNIQUE(post_id, ticker)
);

-- Indexes
CREATE INDEX idx_ticker_mentions_ticker
  ON ticker_mentions(ticker);

CREATE INDEX IF NOT EXISTS idx_ticker_mentions_post_id
  ON ticker_mentions(post_id);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_subreddit
  ON reddit_posts(subreddit);

CREATE INDEX IF NOT EXISTS idx_reddit_posts_created_utc
  ON reddit_posts(created_utc);


-- Materialized view for ticker mentions
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ticker_mentions AS
SELECT
    ticker,
    COUNT(*) AS mention_count,
    ROUND(AVG(sentiment_score), 2) AS avg_sentiment_score,
    SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_count,
    SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_count,
    SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_count
FROM ticker_mentions
GROUP BY ticker
WITH NO DATA;


-- View for daily sentiment trends
CREATE VIEW IF NOT EXISTS view_daily_sentiment_trends AS
SELECT
    t.ticker,
    date_trunc('day', p.created_utc) AS day,
    COUNT(*) AS mention_count,
    ROUND(AVG(t.sentiment_score), 2) AS avg_sentiment_score
FROM ticker_mentions t
JOIN reddit_posts p ON t.post_id = p.id
GROUP BY t.ticker, day;


-- REFRESH MATERIALIZED VIEW CONCURRENTLY mv_ticker_mentions;