"""
CoinMarketCap API integration for fetching cryptocurrency market data.
"""
import requests
import json
import pandas as pd
from typing import Dict, List, Optional

# Your API key directly (since there seems to be an issue with environment variables)
API_KEY = "83c478ff-b19b-4ee5-8e9e-1b7bfcfc2436"

class CoinMarketCapClient:
    """
    Client for interacting with the CoinMarketCap API.
    """
    BASE_URL = "https://pro-api.coinmarketcap.com/v1"
    
    def __init__(self):
        """Initialize the CoinMarketCap API client."""
        self.headers = {
            'X-CMC_PRO_API_KEY': API_KEY,
            'Accept': 'application/json'
        }
        print("CoinMarketCap API client initialized")
    
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
            print(f"Fetching data for tokens: {', '.join(symbols)}")
            
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
            import traceback
            traceback.print_exc()
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
    try:
        client = CoinMarketCapClient()
        token_stats = client.get_token_stats(token_symbols)
        
        # Calculate aggregate metrics
        total_market_cap = sum(token.get('market_cap', 0) or 0 for token in token_stats.values())
        avg_24h_change = sum(token.get('percent_change_24h', 0) or 0 for token in token_stats.values()) / len(token_stats) if token_stats else 0
        
        result = {
            'protocol': protocol_name,
            'tokens': token_stats,
            'token_count': len(token_stats),
            'total_market_cap': total_market_cap,
            'avg_24h_change': avg_24h_change,
            'timestamp': requests.get('http://worldtimeapi.org/api/timezone/Etc/UTC').json().get('datetime')
        }
        
        # Save to file
        output_file = f"{protocol_name.lower()}_token_stats.json"
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"Token statistics saved to '{output_file}'")
        
        # Create a summary DataFrame
        summary_data = []
        for symbol, data in token_stats.items():
            summary_data.append({
                'Symbol': symbol,
                'Name': data.get('name'),
                'Type': data.get('token_type'),
                'Price (USD)': data.get('price_usd'),
                'Market Cap (USD)': data.get('market_cap'),
                '24h Change (%)': data.get('percent_change_24h'),
                '7d Change (%)': data.get('percent_change_7d')
            })
        
        if summary_data:
            df = pd.DataFrame(summary_data)
            csv_file = f"{protocol_name.lower()}_token_summary.csv"
            df.to_csv(csv_file, index=False)
            print(f"Token summary saved to '{csv_file}'")
        
        return result
    
    except Exception as e:
        print(f"Error analyzing protocol tokens: {e}")
        import traceback
        traceback.print_exc()
        return None


def print_token_summary(stats: Dict):
    """
    Print a summary of token statistics.
    
    Args:
        stats: Dictionary containing token statistics
    """
    if not stats:
        print("No token statistics available")
        return
    
    protocol = stats.get('protocol', 'Protocol')
    print(f"\n{protocol} Token Statistics:")
    print("=" * (len(protocol) + 18))
    
    try:
        # Token stats
        print(f"Total Market Cap: ${stats.get('total_market_cap', 0):,.2f}")
        print(f"Average 24h Change: {stats.get('avg_24h_change', 0):,.2f}%")
        print(f"Number of Tokens Analyzed: {stats.get('token_count', 0)}")
        
        # Print top tokens by market cap
        tokens = stats.get('tokens', {})
        if tokens:
            sorted_tokens = sorted(tokens.items(), key=lambda x: x[1].get('market_cap', 0) or 0, reverse=True)
            print("\nTop Tokens by Market Cap:")
            for i, (symbol, data) in enumerate(sorted_tokens[:5], 1):
                print(f"{i}. {symbol}: ${data.get('market_cap', 0):,.2f} (${data.get('price_usd', 0):,.2f} per token)")
            
            print("\nToken Types:")
            token_types = {}
            for symbol, data in tokens.items():
                token_type = data.get('token_type', 'Unknown')
                if token_type in token_types:
                    token_types[token_type].append(symbol)
                else:
                    token_types[token_type] = [symbol]
            
            for token_type, symbols in token_types.items():
                print(f"- {token_type}: {', '.join(symbols)}")
    
    except Exception as e:
        print(f"Error printing token summary: {e}")


# Example usage
if __name__ == "__main__":
    # Test with Aave tokens
    aave_tokens = ["AAVE", "USDC", "USDT", "DAI", "WETH", "WBTC", "LINK", "WMATIC", "WAVAX"]
    print("Analyzing Aave tokens...")
    stats = get_protocol_tokens_stats("Aave", aave_tokens)
    
    if stats:
        print_token_summary(stats)
