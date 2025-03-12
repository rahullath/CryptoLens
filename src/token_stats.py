"""
Updated token statistics module for specified tokens
"""
import requests
import json
import pandas as pd
from typing import Dict, List

# API key directly in the code
API_KEY = "83c478ff-b19b-4ee5-8e9e-1b7bfcfc2436"

def fetch_token_data(symbol: str):
    """
    Fetch token data from CoinMarketCap API for a single token.
    
    Args:
        symbol: Token symbol to fetch data for
    
    Returns:
        Dictionary with token data
    """
    print(f"Fetching data for token: {symbol}")
    
    headers = {
        'X-CMC_PRO_API_KEY': API_KEY,
        'Accept': 'application/json'
    }
    
    # Get metadata
    metadata_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
    metadata_params = {'symbol': symbol}
    
    # Get quotes
    quotes_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
    quotes_params = {'symbol': symbol, 'convert': 'USD'}
    
    try:
        # Fetch metadata
        metadata_response = requests.get(metadata_url, headers=headers, params=metadata_params)
        metadata_response.raise_for_status()
        metadata = metadata_response.json()
        
        # Fetch quotes
        quotes_response = requests.get(quotes_url, headers=headers, params=quotes_params)
        quotes_response.raise_for_status()
        quotes = quotes_response.json()
        
        # Process and combine data
        if symbol in metadata.get('data', {}) and symbol in quotes.get('data', {}):
            meta = metadata['data'][symbol]
            quote = quotes['data'][symbol]
            
            return {
                'name': meta.get('name', ''),
                'symbol': symbol,
                'category': meta.get('category', ''),
                'price_usd': quote.get('quote', {}).get('USD', {}).get('price'),
                'market_cap': quote.get('quote', {}).get('USD', {}).get('market_cap'),
                'volume_24h': quote.get('quote', {}).get('USD', {}).get('volume_24h'),
                'percent_change_24h': quote.get('quote', {}).get('USD', {}).get('percent_change_24h'),
                'percent_change_7d': quote.get('quote', {}).get('USD', {}).get('percent_change_7d'),
                'circulating_supply': quote.get('circulating_supply')
            }
        else:
            print(f"No data found for token: {symbol}")
            return None
    
    except requests.exceptions.RequestException as e:
        print(f"API request error: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response: {e.response.text}")
        return None
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return None


def analyze_specified_tokens():
    """
    Analyze specified tokens and fetch their market data.
    
    Tokens: AAVE, LDO, JUP, COMP, FLUID, S, ETH, SOL, SUI, MKR, SKY
    """
    tokens = ["AAVE", "LDO", "JUP", "COMP", "FLUID", "S", "ETH", "SOL", "SUI", "MKR", "SKY"]
    token_data = {}
    
    for token in tokens:
        data = fetch_token_data(token)
        if data:
            token_data[token] = data
    
    # Save all collected data to JSON
    if token_data:
        with open('specified_token_stats.json', 'w') as f:
            json.dump(token_data, f, indent=2)
        print("Token statistics saved to 'specified_token_stats.json'")
    else:
        print("No token data available")

if __name__ == '__main__':
    analyze_specified_tokens()
