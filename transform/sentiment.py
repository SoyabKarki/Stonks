from transformers import pipeline
from typing import List, Dict
from collections import defaultdict
import logging
import torch
import re

from extract.reddit_data import get_subreddit_data
from extract.ticker_symbols import extract_ticker_symbols
from configs.logging_config import setup_logging

# Setup logging and device
setup_logging()
logger = logging.getLogger(__name__)

device = 0 if torch.cuda.is_available() else -1
logger.info(f"Using device: {'cuda:0' if device == 0 else 'CPU'}")

_pipe = None

def _get_pipeline():
    """Get or create the sentiment analysis pipeline (lazy loading)"""
    global _pipe
    if _pipe is None:
        logger.info("Loading FinBERT model...")
        _pipe = pipeline(
            "text-classification",
            model="ProsusAI/finbert",
            device=device
        )
        logger.info("FinBERT model loaded successfully")
    return _pipe


def _split_sentences(text: str) -> List[str]:
    """Split text into sentences using regex"""

    # Split on common sentence endings
    sentences = re.split(r'[.!?]+', text)
    sentences = [s.strip() for s in sentences if s.strip()]

    return sentences


def get_ticker_sentiment(text: str) -> Dict[str, Dict]:
    """
    For each ticker in the text, find all sentences mentioning it and
    average the sentiment scores from FinBERT.

    Returns a dict of:
        {
            "AAPL": {"label": "positive", "score": 0.87},
            ...
        }
    """
    tickers = extract_ticker_symbols(text)
    if not tickers:
        return {}

    sentences = _split_sentences(text)

    # Map tickers to the sentences that mention them
    ticker_sentences = defaultdict(list)
    for sentence in sentences:
        for ticker in tickers:
            if ticker in sentence or f"${ticker}" in sentence:
                ticker_sentences[ticker].append(sentence)
    
    # Analyze sentiment for all sentences
    all_sentences = list({sentence for sentences in ticker_sentences.values() for sentence in sentences})
    if not all_sentences:
        return {}

    pipe = _get_pipeline()
    results = pipe(all_sentences, batch_size=16)
    sent_results = {s: r for s, r in zip(all_sentences, results)}

    # Compute average sentiment for each ticker
    final = {}
    for ticker, sentences in ticker_sentences.items():
        agg = {
            "positive": 0.0, 
            "neutral": 0.0, 
            "negative": 0.0,
        }

        for sentence in sentences:
            result = sent_results[sentence]
            label = result["label"].lower()
            agg[label] += float(result["score"])
        
        count = len(sentences)
        avg = {label: agg[label] / count for label in agg}
        top = max(avg, key=avg.get)

        final[ticker] = {
            "label": top,
            "score": avg[top],
            "context": " ".join(sentences)
        }

    return final


if __name__ == "__main__":
    # Testing
    subreddit = "investing"
    limit = 2

    posts = get_subreddit_data(subreddit, limit=limit)

    for post in posts:
        text = post.title
        if hasattr(post, "selftext") and post.selftext:
            text += "\n" + post.selftext

        sentiments = get_ticker_sentiment(text)
        if not sentiments:
            continue

        print(f"\nPost: {post.title}\n")
        for ticker, info in sentiments.items():
            print(f"{ticker}: {info['label'].upper()} ({info['score']})")