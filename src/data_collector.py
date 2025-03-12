"""
Data Collector Module for Crypto Revenue Analyzer

This module handles the collection of revenue data from various sources:
1. Direct blockchain analysis (Etherscan, Solscan)
2. DeFi Llama API (for cross-verification)
3. CoinGecko API (for market cap data)
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
import pandas as pd
from config import PROTOCOLS, NETWORKS, DEFILLAMA_BASE_URL, DEFILLAMA_PROTOCOL_URL, COINGECKO_BASE_URL, COINGECKO_COINS_URL

class DataCollector:
    def __init__(self, data_dir="../data"):
        """Initialize the DataCollector."""
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            
    def fetch_market_cap_data(self):
        """Fetch market cap data from CoinGecko API."""
        print("Fetching market cap data from CoinGecko...")
        market_cap_data = {}
        
        for protocol in PROTOCOLS:
            try:
                url = f"{COINGECKO_COINS_URL}/{protocol['slug']}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    market_cap_data[protocol['name']] = {
                        'market_cap': data.get('market_data', {}).get('market_cap', {}).get('usd', None),
                        'current_price': data.get('market_data', {}).get('current_price', {}).get('usd', None),
                        'total_supply': data.get('market_data', {}).get('total_supply', None),
                        'max_supply': data.get('market_data', {}).get('max_supply', None)
                    }
                    print(f"Successfully fetched market cap data for {protocol['name']}")
                else:
                    print(f"Failed to fetch market cap data for {protocol['name']}: {response.status_code}")
                # Respect API rate limits
                time.sleep(1)
            except Exception as e:
                print(f"Error fetching market cap data for {protocol['name']}: {e}")
                
        # Save market cap data
        self._save_data(market_cap_data, "market_cap_data.json")
        return market_cap_data
    
    def fetch_defillama_protocol_data(self):
        """Fetch protocol data from DeFi Llama API for cross-verification."""
        print("Fetching protocol data from DeFi Llama...")
        protocol_data = {}
        
        for protocol in PROTOCOLS:
            try:
                url = f"{DEFILLAMA_PROTOCOL_URL}/{protocol['defillama_id']}"
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    protocol_data[protocol['name']] = data
                    print(f"Successfully fetched DeFi Llama data for {protocol['name']}")
                else:
                    print(f"Failed to fetch DeFi Llama data for {protocol['name']}: {response.status_code}")
                # Respect API rate limits
                time.sleep(1)
            except Exception as e:
                print(f"Error fetching DeFi Llama data for {protocol['name']}: {e}")
                
        # Save DeFi Llama data
        self._save_data(protocol_data, "defillama_protocol_data.json")
        return protocol_data
    
    def fetch_etherscan_revenue_data(self, contract_address, start_date, end_date, api_key=None):
        """
        Fetch revenue data from Etherscan for Ethereum-based protocols.
        
        Args:
            contract_address: The contract address to analyze
            start_date: Start date for data collection (YYYY-MM-DD)
            end_date: End date for data collection (YYYY-MM-DD)
            api_key: Etherscan API key (optional)
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching Etherscan data for contract {contract_address}...")
        
        # Convert dates to timestamps
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        
        # Base URL for Etherscan API
        base_url = "https://api.etherscan.io/api"
        
        # Parameters for API call
        params = {
            "module": "account",
            "action": "txlist",
            "address": contract_address,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "asc"
        }
        
        # Add API key if provided
        if api_key:
            params["apikey"] = api_key
            
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data["status"] == "1":
                    # Convert to DataFrame
                    transactions = pd.DataFrame(data["result"])
                    
                    # Filter by timestamp
                    transactions["timeStamp"] = transactions["timeStamp"].astype(int)
                    transactions = transactions[
                        (transactions["timeStamp"] >= start_timestamp) & 
                        (transactions["timeStamp"] <= end_timestamp)
                    ]
                    
                    # Convert values from Wei to Ether
                    transactions["value_eth"] = transactions["value"].astype(float) / 10**18
                    
                    return transactions
                else:
                    print(f"Etherscan API error: {data.get('message', 'Unknown error')}")
            else:
                print(f"Failed to fetch Etherscan data: {response.status_code}")
        except Exception as e:
            print(f"Error fetching Etherscan data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def fetch_solscan_revenue_data(self, account_address, start_date, end_date):
        """
        Fetch revenue data from Solscan for Solana-based protocols.
        
        Args:
            account_address: The account address to analyze
            start_date: Start date for data collection (YYYY-MM-DD)
            end_date: End date for data collection (YYYY-MM-DD)
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching Solscan data for account {account_address}...")
        
        # Convert dates to timestamps
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        
        # Base URL for Solscan API
        base_url = f"https://public-api.solscan.io/account/transactions"
        
        # Parameters for API call
        params = {
            "account": account_address,
            "limit": 50  # Adjust as needed
        }
        
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame
                transactions = pd.DataFrame(data)
                
                # Filter by timestamp if available
                if "blockTime" in transactions.columns:
                    transactions = transactions[
                        (transactions["blockTime"] >= start_timestamp) & 
                        (transactions["blockTime"] <= end_timestamp)
                    ]
                    
                return transactions
            else:
                print(f"Failed to fetch Solscan data: {response.status_code}")
        except Exception as e:
            print(f"Error fetching Solscan data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def calculate_protocol_revenue(self, protocol_name, transactions_df, price_usd=None):
        """
        Calculate protocol revenue from transaction data.
        
        Args:
            protocol_name: Name of the protocol
            transactions_df: DataFrame with transaction data
            price_usd: Price in USD for conversion (if needed)
            
        Returns:
            Dictionary with revenue metrics
        """
        if transactions_df.empty:
            return {
                "protocol": protocol_name,
                "total_revenue": 0,
                "daily_avg": 0,
                "weekly_avg": 0,
                "monthly_avg": 0,
                "quarterly_avg": 0,
                "yearly_projection": 0
            }
            
        # Calculate total revenue (implementation depends on protocol)
        # This is a simplified example
        if "value_eth" in transactions_df.columns:
            total_revenue_native = transactions_df["value_eth"].sum()
        else:
            total_revenue_native = 0
            
        # Convert to USD if price is provided
        total_revenue_usd = total_revenue_native * price_usd if price_usd else total_revenue_native
        
        # Calculate time range
        if "timeStamp" in transactions_df.columns:
            timestamps = transactions_df["timeStamp"].astype(int)
        elif "blockTime" in transactions_df.columns:
            timestamps = transactions_df["blockTime"].astype(int)
        else:
            timestamps = []
            
        if len(timestamps) > 0:
            start_time = min(timestamps)
            end_time = max(timestamps)
            time_range_days = (end_time - start_time) / 86400  # Convert seconds to days
            
            # Avoid division by zero
            if time_range_days > 0:
                daily_avg = total_revenue_usd / time_range_days
                weekly_avg = daily_avg * 7
                monthly_avg = daily_avg * 30
                quarterly_avg = monthly_avg * 3
                yearly_projection = daily_avg * 365
            else:
                daily_avg = total_revenue_usd
                weekly_avg = total_revenue_usd
                monthly_avg = total_revenue_usd
                quarterly_avg = total_revenue_usd
                yearly_projection = total_revenue_usd
        else:
            daily_avg = 0
            weekly_avg = 0
            monthly_avg = 0
            quarterly_avg = 0
            yearly_projection = 0
            
        return {
            "protocol": protocol_name,
            "total_revenue": total_revenue_usd,
            "daily_avg": daily_avg,
            "weekly_avg": weekly_avg,
            "monthly_avg": monthly_avg,
            "quarterly_avg": quarterly_avg,
            "yearly_projection": yearly_projection
        }
    
    def calculate_qoq_growth(self, protocol_name, current_quarter_revenue, previous_quarter_revenue):
        """
        Calculate Quarter-over-Quarter growth for a protocol.
        
        Args:
            protocol_name: Name of the protocol
            current_quarter_revenue: Revenue for current quarter
            previous_quarter_revenue: Revenue for previous quarter
            
        Returns:
            Dictionary with QoQ growth metrics
        """
        if previous_quarter_revenue == 0:
            qoq_growth_pct = 0
        else:
            qoq_growth_pct = ((current_quarter_revenue - previous_quarter_revenue) / previous_quarter_revenue) * 100
            
        return {
            "protocol": protocol_name,
            "current_quarter_revenue": current_quarter_revenue,
            "previous_quarter_revenue": previous_quarter_revenue,
            "qoq_growth_pct": qoq_growth_pct
        }
    
    def analyze_revenue_sustainability(self, protocol_name, revenue, incentives=None):
        """
        Analyze the sustainability of protocol revenue.
        
        Args:
            protocol_name: Name of the protocol
            revenue: Revenue amount
            incentives: Incentives amount (if available)
            
        Returns:
            Dictionary with sustainability metrics
        """
        # If incentives data is not available, use a placeholder
        if incentives is None:
            sustainability_score = None
            sustainability_ratio = None
            is_sustainable = None
        else:
            # Calculate sustainability ratio (revenue / incentives)
            sustainability_ratio = revenue / incentives if incentives > 0 else float('inf')
            
            # Assign sustainability score (0-100)
            if sustainability_ratio >= 2:
                sustainability_score = 100  # Very sustainable
            elif sustainability_ratio >= 1:
                sustainability_score = 75  # Sustainable
            elif sustainability_ratio >= 0.5:
                sustainability_score = 50  # Moderately sustainable
            elif sustainability_ratio > 0:
                sustainability_score = 25  # Less sustainable
            else:
                sustainability_score = 0  # Not sustainable
                
            # Determine if sustainable (ratio > 1)
            is_sustainable = sustainability_ratio >= 1
            
        return {
            "protocol": protocol_name,
            "revenue": revenue,
            "incentives": incentives,
            "sustainability_ratio": sustainability_ratio,
            "sustainability_score": sustainability_score,
            "is_sustainable": is_sustainable
        }
    
    def _save_data(self, data, filename):
        """Save data to JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")
        
    def load_data(self, filename):
        """Load data from JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
        return None
        
    def collect_all_data(self):
        """Collect all data for analysis."""
        # Collect market cap data
        market_cap_data = self.fetch_market_cap_data()
        
        # Collect DeFi Llama data for cross-verification
        defillama_data = self.fetch_defillama_protocol_data()
        
        # Initialize results dictionary
        results = {}
        
        # Process each protocol
        for protocol in PROTOCOLS:
            protocol_name = protocol['name']
            print(f"Processing {protocol_name}...")
            
            # Initialize protocol data
            protocol_data = {
                "name": protocol_name,
                "slug": protocol['slug'],
                "token_type": protocol['token_type'],
                "chains": protocol['chains'],
                "market_cap": market_cap_data.get(protocol_name, {}).get('market_cap', None),
                "revenue": {},
                "qoq_growth": {},
                "sustainability": {}
            }
            
            # Add to results
            results[protocol_name] = protocol_data
            
        # Save collected data
        self._save_data(results, "protocol_analysis_data.json")
        print("Data collection complete!")
        return results


if __name__ == "__main__":
    collector = DataCollector()
    collector.collect_all_data()
