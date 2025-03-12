"""
CoinMarketCap API integration for fetching cryptocurrency market data.
"""
import os
import requests
from typing import Dict, List, Optional, Union
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class CoinMarketCapAPI:
    """
    Client for interacting with the CoinMarketCap API.
    """
    BASE_URL = "https://pro-api.coinmarketcap.com/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the CoinMarketCap API client.
        
        Args:
            api_key: CoinMarketCap API key. If not provided, will try to load from environment.
        """
        self.api_key = api_key or os.getenv("COINMARKETCAP_API_KEY")
        if not self.api_key:
            raise ValueError("CoinMarketCap API key not found. Please set COINMARKETCAP_API_KEY in .env file")
        
        self.headers = {
            'X-CMC_PRO_API_KEY': self.api_key,
            'Accept': 'application/json'
        }
    
    def get_token_metadata(self, symbols: List[str]) -> Dict:
        """
        Get metadata for specified tokens.
        
        Args:
            symbols: List of token symbols (e.g., ["BTC", "ETH"])
            
        Returns:
            Dictionary containing token metadata
        """
        endpoint = f"{self.BASE_URL}/cryptocurrency/info"
        params = {
            'symbol': ','.join(symbols)
        }
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_token_quotes(self, symbols: List[str]) -> Dict:
        """
        Get latest quotes for specified tokens.
        
        Args:
            symbols: List of token symbols (e.g., ["BTC", "ETH"])
            
        Returns:
            Dictionary containing token quotes data
        """
        endpoint = f"{self.BASE_URL}/cryptocurrency/quotes/latest"
        params = {
            'symbol': ','.join(symbols),
            'convert': 'USD'
        }
        
        response = requests.get(endpoint, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json()
    
    def get_token_stats(self, symbols: List[str]) -> Dict[str, Dict]:
        """
        Get comprehensive stats for tokens including market cap, token type, etc.
        
        Args:
            symbols: List of token symbols (e.g., ["BTC", "ETH"])
            
        Returns:
            Dictionary with token symbols as keys and their stats as values
        """
        try:
            # Get metadata (includes token type, category, etc.)
            metadata = self.get_token_metadata(symbols)
            
            # Get quotes (includes price, market cap, volume, etc.)
            quotes = self.get_token_quotes(symbols)
            
            # Combine the data
            result = {}
            for symbol in symbols:
                if symbol in metadata.get('data', {}) and symbol in quotes.get('data', {}):
                    meta = metadata['data'][symbol]
                    quote = quotes['data'][symbol]
                    
                    result[symbol] = {
                        'id': meta.get('id'),
                        'name': meta.get('name'),
                        'symbol': symbol,
                        'token_type': meta.get('category'),
                        'description': meta.get('description'),
                        'logo': meta.get('logo'),
                        'website': meta.get('urls', {}).get('website', [None])[0],
                        'explorer': meta.get('urls', {}).get('explorer', [None])[0],
                        'twitter': meta.get('urls', {}).get('twitter', [None])[0],
                        'reddit': meta.get('urls', {}).get('reddit', [None])[0],
                        'tags': meta.get('tags', []),
                        'platform': meta.get('platform'),
                        'date_added': meta.get('date_added'),
                        'price_usd': quote.get('quote', {}).get('USD', {}).get('price'),
                        'market_cap': quote.get('quote', {}).get('USD', {}).get('market_cap'),
                        'volume_24h': quote.get('quote', {}).get('USD', {}).get('volume_24h'),
                        'percent_change_24h': quote.get('quote', {}).get('USD', {}).get('percent_change_24h'),
                        'percent_change_7d': quote.get('quote', {}).get('USD', {}).get('percent_change_7d'),
                        'circulating_supply': quote.get('circulating_supply'),
                        'total_supply': quote.get('total_supply'),
                        'max_supply': quote.get('max_supply'),
                        'last_updated': quote.get('quote', {}).get('USD', {}).get('last_updated')
                    }
                
            return result
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching token stats: {e}")
            if hasattr(e, 'response') and e.response:
                print(f"Response: {e.response.text}")
            return {}
        
        except Exception as e:
            print(f"Unexpected error fetching token stats: {e}")
            return {}


def get_protocol_tokens_stats(protocol_name: str, token_symbols: List[str]) -> Dict:
    """
    Get stats for tokens associated with a specific protocol.
    
    Args:
        protocol_name: Name of the protocol (e.g., "Aave", "Lido")
        token_symbols: List of token symbols used by the protocol
        
    Returns:
        Dictionary containing token stats for the protocol
    """
    api = CoinMarketCapAPI()
    token_stats = api.get_token_stats(token_symbols)
    
    return {
        'protocol': protocol_name,
        'tokens': token_stats,
        'token_count': len(token_stats),
        'total_market_cap': sum(token.get('market_cap', 0) or 0 for token in token_stats.values()),
        'timestamp': requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC').json().get('datetime')
    }


# Example usage
if __name__ == "__main__":
    # Test with some example tokens
    aave_tokens = ["AAVE", "USDC", "USDT", "DAI", "WETH", "WBTC"]
    result = get_protocol_tokens_stats("Aave", aave_tokens)
    
    # Print results
    print(f"Protocol: {result['protocol']}")
    print(f"Token Count: {result['token_count']}")
    print(f"Total Market Cap: ${result['total_market_cap']:,.2f}")
    print("\nToken Details:")
    
    for symbol, data in result['tokens'].items():
        print(f"\n{symbol} ({data.get('name')}):")
        print(f"  Price: ${data.get('price_usd'):,.2f}")
        print(f"  Market Cap: ${data.get('market_cap'):,.2f}")
        print(f"  Token Type: {data.get('token_type')}")
        print(f"  24h Change: {data.get('percent_change_24h'):,.2f}%")
