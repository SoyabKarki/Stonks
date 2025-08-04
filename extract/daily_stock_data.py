import requests
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

from configs.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

load_dotenv()

api_key = os.getenv("ALPHA_VANTAGE_API_KEY")
base_url = "https://www.alphavantage.co/query"

def get_daily_stock_data(self, symbol: str, output_size: str = "compact") -> Optional[Dict[str, Any]]:
    """
    Get daily stock data for a symbol
    
    Args:
        symbol: Stock symbol (e.g., 'AAPL', 'IBM')
        output_size: 'compact' (last 100 days) or 'full' (last 20 years)
        
    Returns:
        Dictionary with stock data or None if error
    """

    try:
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": output_size,
            "apikey": api_key,
        }

        logger.info(f"Fetching daily stock data for {symbol}")
        response = requests.get(base_url, params=params)
        response.raise_for_status()

        data = response.json()

        # Check for API errors
        if 'Error Message' in data:
            logger.error(f"Alpha Vantage API error for {symbol}: {data['Error Message']}")
            return None
        
        if 'Note' in data:
            logger.warning(f"Alpha Vantage API limit reached: {data['Note']}")
            return None

        # Extract metadata and time series data
        meta_data = data.get('Meta Data', {})
        time_series = data.get('Time Series (Daily)', {})

        if not time_series:
            logger.warning(f"No time series data found for {symbol}")
            return None

        # Process data
        processed_data = {
            'symbol': symbol,
            'meta_data': {
                'information': meta_data.get('1. Information', ''),
                'symbol': meta_data.get('2. Symbol', ''),
                'last_refreshed': meta_data.get('3. Last Refreshed', ''),
                'output_size': meta_data.get('4. Output Size', ''),
                'time_zone': meta_data.get('5. Time Zone', '')
            },
            'daily_data': {}
        }
    
        for date, values in time_series.items():
            processed_data['daily_data'][date] = {
                'open': float(values.get('1. open', 0)),
                'high': float(values.get('2. high', 0)),
                'low': float(values.get('3. low', 0)),
                'close': float(values.get('4. close', 0)),
                'volume': int(values.get('5. volume', 0))
            }
        
        logger.info(f"Successfully fetched {len(processed_data['daily_data'])} days of data for {symbol}")
        return processed_data

    except Exception as e:
        logger.error(f"Error fetching stock data for {symbol}: {str(e)}")
        return None

if __name__ == "__main__":
    symbol = "AAPL"
    output_size = "full"
    data = get_daily_stock_data(symbol, output_size)
    print(data)
