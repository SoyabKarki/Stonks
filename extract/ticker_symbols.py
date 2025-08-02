import pandas as pd
import re
from typing import List, Set
import logging

from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def load_ticker_symbols() -> Set[str]:
    """Load ticker symbols from a CSV file"""
    df = pd.read_csv("extract/us_symbols.csv")
    return set(df["ticker"].str.upper())


def extract_ticker_symbols(text: str) -> Set[str]:
    """Extract ticker symbols from given text"""

    # Load all available ticker symbols
    tickers = load_ticker_symbols()

    # Patterns to match ticker symbols
    patterns = [
        r'\$([A-Z]{1,5})',  # $TICKER format
        r'\b([A-Z]{1,5})\b'  # standalone TICKER format
    ]

    found_tickers = set()

    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            if match in tickers:
                found_tickers.add(match)

    logger.info(f"Found {len(found_tickers)} ticker symbols in text")

    return found_tickers

if __name__ == "__main__":
    # Testing
    text = "I'm bullish on $AAPL and $GOOG. I also like $MSFT and $AMZN."
    tickers = extract_ticker_symbols(text)
    print(tickers)