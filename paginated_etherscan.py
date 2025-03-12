"""
Paginated Etherscan Data Collector

This module implements pagination for the Etherscan API to retrieve complete
transaction data beyond the 10,000 transaction limit per request.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Etherscan API configuration
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY", "")

# Etherscan API base URLs for different chains
ETHERSCAN_BASE_URLS = {
    "ethereum": "https://api.etherscan.io/api",
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api.optimistic.etherscan.io/api",
    "polygon": "https://api.polygonscan.com/api",
    "base": "https://api.basescan.org/api",
    "avalanche": "https://api.snowtrace.io/api",
    "bsc": "https://api.bscscan.com/api",
    "fantom": "https://api.ftmscan.com/api",
}

class PaginatedEtherscanCollector:
    """
    A collector for Etherscan data that implements pagination to retrieve
    complete transaction data beyond the 10,000 transaction limit per request.
    """
    
    def __init__(self, api_key=ETHERSCAN_API_KEY, data_dir="data"):
        """Initialize the PaginatedEtherscanCollector."""
        self.api_key = api_key
        self.data_dir = data_dir
        self.ensure_data_dir()
        self.request_count = 0
        self.last_request_time = 0
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def _rate_limit(self):
        """
        Implement rate limiting to avoid hitting Etherscan API limits.
        Etherscan allows 5 requests per second for the free tier.
        """
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        # If we've made a request recently, wait to avoid rate limiting
        if time_since_last_request < 0.2:  # 200ms between requests (5 per second)
            sleep_time = 0.2 - time_since_last_request
            time.sleep(sleep_time)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    def get_paginated_transactions(self, contract_address, chain="ethereum", days=0, 
                                  start_block=0, end_block=99999999, page_size=10000):
        """
        Get all transactions for a contract address using pagination.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            days: Number of days to look back (0 for all time)
            start_block: Starting block number
            end_block: Ending block number
            page_size: Number of transactions per page (max 10000)
            
        Returns:
            DataFrame with all transaction data
        """
        print(f"Fetching all {chain} contract transactions for {contract_address} (past {days if days > 0 else 'all'} days)...")
        
        # Get base URL for the specified chain
        base_url = ETHERSCAN_BASE_URLS.get(chain.lower())
        if not base_url:
            print(f"Unsupported chain: {chain}")
            return pd.DataFrame()
        
        # Calculate start timestamp if days > 0
        start_timestamp = 0
        if days > 0:
            start_date = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_date.timestamp())
        
        # Initialize variables for pagination
        all_transactions = []
        page = 1
        more_pages = True
        total_fetched = 0
        
        # Fetch all pages
        while more_pages:
            print(f"Fetching page {page}...")
            
            # Apply rate limiting
            self._rate_limit()
            
            # Parameters for API call
            params = {
                "module": "account",
                "action": "txlist",
                "address": contract_address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": page_size,
                "sort": "desc",
                "apikey": self.api_key
            }
            
            try:
                response = requests.get(base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("status") == "1":
                        transactions = data.get("result", [])
                        
                        # If we got transactions, add them to our list
                        if transactions:
                            # Filter by timestamp if needed
                            if days > 0:
                                transactions = [tx for tx in transactions if int(tx.get("timeStamp", 0)) >= start_timestamp]
                            
                            all_transactions.extend(transactions)
                            total_fetched += len(transactions)
                            print(f"Fetched {len(transactions)} transactions (total: {total_fetched})")
                            
                            # If we got fewer transactions than the page size, we've reached the end
                            if len(transactions) < page_size:
                                more_pages = False
                            else:
                                page += 1
                        else:
                            more_pages = False
                    else:
                        print(f"API error: {data.get('message')}")
                        more_pages = False
                else:
                    print(f"HTTP error: {response.status_code}")
                    if response.text:
                        print(f"Response: {response.text}")
                    more_pages = False
            except Exception as e:
                print(f"Error fetching transactions: {e}")
                more_pages = False
                
            # Add a small delay between pages to be nice to the API
            time.sleep(0.5)
        
        # Convert to DataFrame
        if all_transactions:
            df = pd.DataFrame(all_transactions)
            
            # Add datetime column for easier analysis
            if "timeStamp" in df.columns:
                df["datetime"] = pd.to_datetime(df["timeStamp"].astype(int), unit='s')
            
            print(f"Total transactions fetched: {len(df)}")
            return df
        else:
            print("No transactions found")
            return pd.DataFrame()
    
    def get_paginated_internal_transactions(self, contract_address, chain="ethereum", days=0,
                                          start_block=0, end_block=99999999, page_size=10000):
        """
        Get all internal transactions for a contract address using pagination.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            days: Number of days to look back (0 for all time)
            start_block: Starting block number
            end_block: Ending block number
            page_size: Number of transactions per page (max 10000)
            
        Returns:
            DataFrame with all internal transaction data
        """
        print(f"Fetching all {chain} contract internal transactions for {contract_address} (past {days if days > 0 else 'all'} days)...")
        
        # Get base URL for the specified chain
        base_url = ETHERSCAN_BASE_URLS.get(chain.lower())
        if not base_url:
            print(f"Unsupported chain: {chain}")
            return pd.DataFrame()
        
        # Calculate start timestamp if days > 0
        start_timestamp = 0
        if days > 0:
            start_date = datetime.now() - timedelta(days=days)
            start_timestamp = int(start_date.timestamp())
        
        # Initialize variables for pagination
        all_transactions = []
        page = 1
        more_pages = True
        total_fetched = 0
        
        # Fetch all pages
        while more_pages:
            print(f"Fetching internal transactions page {page}...")
            
            # Apply rate limiting
            self._rate_limit()
            
            # Parameters for API call
            params = {
                "module": "account",
                "action": "txlistinternal",
                "address": contract_address,
                "startblock": start_block,
                "endblock": end_block,
                "page": page,
                "offset": page_size,
                "sort": "desc",
                "apikey": self.api_key
            }
            
            try:
                response = requests.get(base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("status") == "1":
                        transactions = data.get("result", [])
                        
                        # If we got transactions, add them to our list
                        if transactions:
                            # Filter by timestamp if needed
                            if days > 0:
                                transactions = [tx for tx in transactions if int(tx.get("timeStamp", 0)) >= start_timestamp]
                            
                            all_transactions.extend(transactions)
                            total_fetched += len(transactions)
                            print(f"Fetched {len(transactions)} internal transactions (total: {total_fetched})")
                            
                            # If we got fewer transactions than the page size, we've reached the end
                            if len(transactions) < page_size:
                                more_pages = False
                            else:
                                page += 1
                        else:
                            more_pages = False
                    else:
                        print(f"API error: {data.get('message')}")
                        more_pages = False
                else:
                    print(f"HTTP error: {response.status_code}")
                    if response.text:
                        print(f"Response: {response.text}")
                    more_pages = False
            except Exception as e:
                print(f"Error fetching internal transactions: {e}")
                more_pages = False
                
            # Add a small delay between pages to be nice to the API
            time.sleep(0.5)
        
        # Convert to DataFrame
        if all_transactions:
            df = pd.DataFrame(all_transactions)
            
            # Add datetime column for easier analysis
            if "timeStamp" in df.columns:
                df["datetime"] = pd.to_datetime(df["timeStamp"].astype(int), unit='s')
            
            print(f"Total internal transactions fetched: {len(df)}")
            return df
        else:
            print("No internal transactions found")
            return pd.DataFrame()
    
    def get_contract_events(self, contract_address, topic0=None, chain="ethereum", days=0,
                          start_block=0, end_block=99999999, page_size=1000):
        """
        Get events for a contract address using pagination.
        
        Args:
            contract_address: The contract address to analyze
            topic0: The event signature hash (optional)
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            days: Number of days to look back (0 for all time)
            start_block: Starting block number
            end_block: Ending block number
            page_size: Number of events per page (max 1000 for logs)
            
        Returns:
            DataFrame with event data
        """
        print(f"Fetching all {chain} contract events for {contract_address} (past {days if days > 0 else 'all'} days)...")
        
        # Get base URL for the specified chain
        base_url = ETHERSCAN_BASE_URLS.get(chain.lower())
        if not base_url:
            print(f"Unsupported chain: {chain}")
            return pd.DataFrame()
        
        # Calculate start block if days > 0
        if days > 0:
            # This is an approximation - Ethereum produces ~6500 blocks per day
            blocks_per_day = 6500
            start_block = max(start_block, end_block - (days * blocks_per_day))
        
        # Initialize variables for pagination
        all_events = []
        page = 1
        more_pages = True
        total_fetched = 0
        
        # Fetch all pages
        while more_pages:
            print(f"Fetching events page {page}...")
            
            # Apply rate limiting
            self._rate_limit()
            
            # Parameters for API call
            params = {
                "module": "logs",
                "action": "getLogs",
                "address": contract_address,
                "fromBlock": start_block,
                "toBlock": end_block,
                "page": page,
                "offset": page_size,
                "apikey": self.api_key
            }
            
            # Add topic0 if provided
            if topic0:
                params["topic0"] = topic0
            
            try:
                response = requests.get(base_url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    
                    if data.get("status") == "1":
                        events = data.get("result", [])
                        
                        # If we got events, add them to our list
                        if events:
                            all_events.extend(events)
                            total_fetched += len(events)
                            print(f"Fetched {len(events)} events (total: {total_fetched})")
                            
                            # If we got fewer events than the page size, we've reached the end
                            if len(events) < page_size:
                                more_pages = False
                            else:
                                page += 1
                        else:
                            more_pages = False
                    else:
                        print(f"API error: {data.get('message')}")
                        more_pages = False
                else:
                    print(f"HTTP error: {response.status_code}")
                    if response.text:
                        print(f"Response: {response.text}")
                    more_pages = False
            except Exception as e:
                print(f"Error fetching events: {e}")
                more_pages = False
                
            # Add a small delay between pages to be nice to the API
            time.sleep(0.5)
        
        # Convert to DataFrame
        if all_events:
            df = pd.DataFrame(all_events)
            
            # Add datetime column for easier analysis if timeStamp is available
            if "timeStamp" in df.columns:
                df["datetime"] = pd.to_datetime(df["timeStamp"].astype(int), unit='s')
            
            print(f"Total events fetched: {len(df)}")
            return df
        else:
            print("No events found")
            return pd.DataFrame()

# Example usage
if __name__ == "__main__":
    import argparse
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Paginated Etherscan Data Collector')
    parser.add_argument('--address', type=str, required=True, help='Contract address to analyze')
    parser.add_argument('--chain', type=str, default='ethereum', help='Chain to analyze (ethereum, arbitrum, optimism, etc.)')
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back (0 for all time)')
    parser.add_argument('--output-dir', type=str, default='data', help='Output directory for collected data')
    args = parser.parse_args()
    
    # Create collector
    collector = PaginatedEtherscanCollector(data_dir=args.output_dir)
    
    # Get transactions
    print(f"\nFetching transactions for {args.address} on {args.chain} (past {args.days} days)...")
    transactions = collector.get_paginated_transactions(args.address, args.chain, args.days)
    
    # Get internal transactions
    print(f"\nFetching internal transactions for {args.address} on {args.chain} (past {args.days} days)...")
    internal_txs = collector.get_paginated_internal_transactions(args.address, args.chain, args.days)
    
    # Get events (no specific topic)
    print(f"\nFetching events for {args.address} on {args.chain} (past {args.days} days)...")
    events = collector.get_contract_events(args.address, None, args.chain, args.days)
    
    # Save results
    if not transactions.empty:
        transactions.to_csv(f"{args.output_dir}/{args.chain}_{args.address}_transactions_{args.days}d.csv", index=False)
        print(f"Saved transactions to {args.output_dir}/{args.chain}_{args.address}_transactions_{args.days}d.csv")
    
    if not internal_txs.empty:
        internal_txs.to_csv(f"{args.output_dir}/{args.chain}_{args.address}_internal_txs_{args.days}d.csv", index=False)
        print(f"Saved internal transactions to {args.output_dir}/{args.chain}_{args.address}_internal_txs_{args.days}d.csv")
    
    if not events.empty:
        events.to_csv(f"{args.output_dir}/{args.chain}_{args.address}_events_{args.days}d.csv", index=False)
        print(f"Saved events to {args.output_dir}/{args.chain}_{args.address}_events_{args.days}d.csv")
