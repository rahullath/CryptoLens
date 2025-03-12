"""
Lido Revenue Analyzer

This module specifically handles Lido protocol revenue analysis using Etherscan API.
It tracks actual revenue through the Execution Layer Rewards Vault.
"""

import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import requests
from dotenv import load_dotenv
import pandas as pd

# Load environment variables
load_dotenv()

class LidoAnalyzer:
    # Lido contract addresses
    EXECUTION_LAYER_REWARDS_VAULT = "0x388C818CA8B9251b393131C08a736A67ccB19297"
    
    def __init__(self):
        self.api_key = os.getenv("ETHERSCAN_API_KEY")
        if not self.api_key:
            raise ValueError("ETHERSCAN_API_KEY not found in environment variables")
        
        self.base_url = "https://api.etherscan.io/api"
        self.rate_limit_delay = 0.2  # 200ms delay between requests
        
    def _make_request(self, params: Dict) -> Optional[Dict]:
        """Make a rate-limited request to Etherscan API with exponential backoff."""
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                time.sleep(self.rate_limit_delay)
                response = requests.get(self.base_url, params=params)
                data = response.json()
                
                if data.get("status") == "1" and "result" in data:
                    return data["result"]
                elif "Max rate limit reached" in str(data.get("result", "")):
                    print(f"Rate limit hit, retrying in {retry_delay} seconds...")
                    retry_delay *= 2
                    time.sleep(retry_delay)
                    continue
                else:
                    error_msg = data.get("message", "Unknown error")
                    if "No records found" in str(error_msg):
                        print(f"No records found for params: {params}")
                        return []
                    print(f"API Error: {error_msg}")
                    return None
                    
            except Exception as e:
                print(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
                    continue
                return None
        
        return None

    def get_block_by_timestamp(self, timestamp: int) -> Optional[int]:
        """Get the closest block number for a given timestamp."""
        params = {
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": str(timestamp),
            "closest": "before",
            "apikey": self.api_key
        }
        
        result = self._make_request(params)
        return int(result) if result else None

    def get_eth_price(self) -> float:
        """Get current ETH price in USD."""
        try:
            response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
            data = response.json()
            return float(data["ethereum"]["usd"])
        except Exception as e:
            print(f"Failed to get ETH price: {e}")
            return 0.0

    def get_internal_transactions(self, address: str, start_block: int, end_block: int) -> List[Dict]:
        """Get internal transactions for a contract with pagination."""
        all_transactions = []
        page = 1
        offset = 1000  # Reduced from 10000 to avoid pagination issues
        
        while True:
            print(f"Fetching internal transactions page {page} (offset: {offset})...")
            params = {
                "module": "account",
                "action": "txlistinternal",
                "address": address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": offset,
                "sort": "asc",
                "apikey": self.api_key
            }
            
            result = self._make_request(params)
            if not result:
                break
                
            if isinstance(result, list):
                # Filter for incoming transactions only
                incoming_txs = [tx for tx in result if tx.get("to", "").lower() == address.lower()]
                all_transactions.extend(incoming_txs)
                
                if len(result) < offset:  # If we got less results than requested, we've reached the end
                    break
            else:
                print(f"Unexpected result format: {result}")
                break
                
            page += 1
            time.sleep(0.5)  # Add a small delay between pages
        
        return all_transactions
    
    def calculate_revenue(self, start_date: Optional[datetime] = None, end_date: Optional[datetime] = None) -> Dict:
        """
        Calculate Lido's revenue by tracking ETH inflows to the Execution Layer Rewards Vault.
        """
        if end_date is None:
            end_date = datetime.now()
            
        if start_date is None:
            start_date = end_date - timedelta(days=30)  # Default to last 30 days
            
        # Convert dates to block numbers
        start_block = self.get_block_by_timestamp(int(start_date.timestamp()))
        end_block = self.get_block_by_timestamp(int(end_date.timestamp()))
        
        if not start_block or not end_block:
            print("Failed to get block numbers for date range")
            return {
                "revenue_eth": 0,
                "revenue_usd": 0,
                "time_period": f"{start_date.isoformat()} to {end_date.isoformat()}"
            }
            
        print(f"\nAnalyzing period: {start_date.isoformat()} to {end_date.isoformat()}")
        print(f"Block range: {start_block} to {end_block}")
        
        # Get internal transactions (ETH inflows)
        print("\nFetching internal transactions...")
        transactions = self.get_internal_transactions(
            self.EXECUTION_LAYER_REWARDS_VAULT,
            start_block,
            end_block
        )
        
        print(f"\nFound {len(transactions)} internal transactions")
        
        # Calculate total ETH inflow
        total_eth = sum(float(tx.get("value", "0")) / 1e18 for tx in transactions)
        
        # Get current ETH price
        eth_price = self.get_eth_price()
        total_usd = total_eth * eth_price
        
        print(f"\nCalculated values:")
        print(f"Total revenue: {total_eth:.4f} ETH (${total_usd:,.2f} USD)")
        
        return {
            "revenue_eth": total_eth,
            "revenue_usd": total_usd,
            "time_period": f"{start_date.isoformat()} to {end_date.isoformat()}"
        }
    
    def analyze_revenue_periods(self) -> Dict:
        """Analyze revenue for different time periods (24h, 7d, 30d, 1y)."""
        now = datetime.now()
        
        periods = {
            "24h": timedelta(days=1),
            "7d": timedelta(days=7),
            "30d": timedelta(days=30),
            "1y": timedelta(days=365)
        }
        
        results = {}
        for period_name, delta in periods.items():
            print(f"\nAnalyzing {period_name} period...")
            start_date = now - delta
            results[period_name] = self.calculate_revenue(start_date, now)
        
        return results

def main():
    """Main function to demonstrate usage."""
    analyzer = LidoAnalyzer()
    
    print("Starting Lido Revenue Analysis...")
    print("=================================")
    
    # Analyze revenue for different periods
    revenue_analysis = analyzer.analyze_revenue_periods()
    
    # Print results
    print("\nFinal Results:")
    print("=============")
    
    for period, data in revenue_analysis.items():
        print(f"\n{period} Period ({data['time_period']}):")
        print(f"Revenue: {data['revenue_eth']:.4f} ETH (${data['revenue_usd']:,.2f} USD)")

if __name__ == "__main__":
    main() 