"""
Solscan Data Collector Module

This module uses the Solscan API to fetch accurate Solana blockchain data for revenue analysis.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from config import PROTOCOLS

# Load environment variables (for API key)
load_dotenv()

# Solscan API configuration
SOLSCAN_API_KEY = os.getenv("SOLSCAN_API_KEY", "")
SOLSCAN_BASE_URL = "https://public-api.solscan.io"

class SolscanCollector:
    def __init__(self, api_key=SOLSCAN_API_KEY, data_dir="../data"):
        """Initialize the SolscanCollector."""
        self.api_key = api_key
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_account_transactions(self, account_address, limit=100):
        """
        Get transactions for a Solana account using Solscan API.
        
        Args:
            account_address: The account address to analyze
            limit: Maximum number of transactions to retrieve
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching Solscan data for account {account_address}...")
        
        # Build URL
        url = f"{SOLSCAN_BASE_URL}/account/transactions"
        
        # Parameters for API call
        params = {
            "account": account_address,
            "limit": limit
        }
        
        # Headers with API key
        headers = {
            "token": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame
                if isinstance(data, list) and len(data) > 0:
                    transactions = pd.DataFrame(data)
                    
                    # Process transaction data
                    if "blockTime" in transactions.columns:
                        transactions["blockTime"] = transactions["blockTime"].astype(int)
                    
                    # Extract fee information if available
                    if "fee" in transactions.columns:
                        transactions["fee_lamports"] = transactions["fee"].astype(float)
                        transactions["fee_sol"] = transactions["fee_lamports"] / 10**9
                    
                    print(f"Successfully fetched {len(transactions)} transactions from Solscan")
                    return transactions
                else:
                    print(f"No transaction data found in Solscan response")
            else:
                print(f"Failed to fetch Solscan data: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching Solscan data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_token_holders(self, token_address, limit=20):
        """
        Get token holders for a Solana token.
        
        Args:
            token_address: The token address to analyze
            limit: Maximum number of holders to retrieve
            
        Returns:
            DataFrame with holder data
        """
        print(f"Fetching token holders for {token_address}...")
        
        # Build URL
        url = f"{SOLSCAN_BASE_URL}/token/holders"
        
        # Parameters for API call
        params = {
            "tokenAddress": token_address,
            "limit": limit,
            "offset": 0
        }
        
        # Headers with API key
        headers = {
            "token": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame
                if "data" in data and isinstance(data["data"], list) and len(data["data"]) > 0:
                    holders = pd.DataFrame(data["data"])
                    print(f"Successfully fetched {len(holders)} token holders from Solscan")
                    return holders
                else:
                    print(f"No holder data found in Solscan response")
            else:
                print(f"Failed to fetch Solscan data: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching Solscan data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_account_tokens(self, account_address):
        """
        Get tokens held by a Solana account.
        
        Args:
            account_address: The account address to analyze
            
        Returns:
            DataFrame with token data
        """
        print(f"Fetching tokens for account {account_address}...")
        
        # Build URL
        url = f"{SOLSCAN_BASE_URL}/account/tokens"
        
        # Parameters for API call
        params = {
            "account": account_address
        }
        
        # Headers with API key
        headers = {
            "token": self.api_key
        }
        
        try:
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                data = response.json()
                
                # Convert to DataFrame
                if isinstance(data, list) and len(data) > 0:
                    tokens = pd.DataFrame(data)
                    print(f"Successfully fetched {len(tokens)} tokens from Solscan")
                    return tokens
                else:
                    print(f"No token data found in Solscan response")
            else:
                print(f"Failed to fetch Solscan data: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching Solscan data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def collect_protocol_revenue_data(self, protocol_name, account_addresses):
        """
        Collect revenue data for a Solana protocol using Solscan.
        
        Args:
            protocol_name: Name of the protocol
            account_addresses: List of account addresses to analyze
            
        Returns:
            Dictionary with revenue data
        """
        print(f"Collecting Solana revenue data for {protocol_name}...")
        
        # Initialize results
        revenue_data = {
            "protocol": protocol_name,
            "chain": "solana",
            "accounts": {},
            "total_revenue": 0,
            "daily_avg": 0,
            "weekly_avg": 0,
            "monthly_avg": 0,
            "quarterly_avg": 0,
            "yearly_projection": 0
        }
        
        # Process each account
        for account_address in account_addresses:
            print(f"Processing account {account_address} for {protocol_name}...")
            
            # Fetch transactions
            transactions = self.get_account_transactions(account_address)
            
            # Skip if no transactions found
            if transactions.empty:
                print(f"No transactions found for account {account_address}")
                continue
            
            # Calculate revenue (implementation depends on protocol)
            account_revenue = self._calculate_account_revenue(protocol_name, account_address, transactions)
            
            # Add to results
            revenue_data["accounts"][account_address] = account_revenue
            revenue_data["total_revenue"] += account_revenue.get("total_revenue", 0)
        
        # Calculate averages
        if revenue_data["total_revenue"] > 0:
            # Assume 90 days of data for simplicity
            revenue_data["daily_avg"] = revenue_data["total_revenue"] / 90
            revenue_data["weekly_avg"] = revenue_data["daily_avg"] * 7
            revenue_data["monthly_avg"] = revenue_data["daily_avg"] * 30
            revenue_data["quarterly_avg"] = revenue_data["monthly_avg"] * 3
            revenue_data["yearly_projection"] = revenue_data["daily_avg"] * 365
        
        # Save revenue data
        self._save_data(revenue_data, f"{protocol_name.lower()}_solana_revenue_data.json")
        
        return revenue_data
    
    def _calculate_account_revenue(self, protocol_name, account_address, transactions):
        """
        Calculate account revenue from transaction data.
        This is a simplified implementation - real calculations would be protocol-specific.
        
        Args:
            protocol_name: Name of the protocol
            account_address: Account address
            transactions: DataFrame with transaction data
            
        Returns:
            Dictionary with revenue metrics
        """
        # This is a placeholder - real implementation would need protocol-specific logic
        if transactions.empty:
            return {
                "account": account_address,
                "total_revenue": 0,
                "transaction_count": 0
            }
        
        # Example calculation for Jupiter (swap fees)
        if protocol_name == "Jupiter":
            # For Jupiter, revenue comes from swap fees
            # This is a simplified example - real implementation would need more analysis
            if "fee" in transactions.columns:
                total_revenue = transactions["fee"].astype(float).sum() / 10**9  # Convert lamports to SOL
            else:
                total_revenue = 0
        
        # Default case - use transaction fees as a proxy for revenue
        else:
            if "fee" in transactions.columns:
                total_revenue = transactions["fee"].astype(float).sum() / 10**9  # Convert lamports to SOL
            else:
                total_revenue = 0
        
        return {
            "account": account_address,
            "total_revenue": total_revenue,
            "transaction_count": len(transactions)
        }
    
    def _save_data(self, data, filename):
        """Save data to JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")


if __name__ == "__main__":
    # Example usage
    collector = SolscanCollector()
    
    # Jupiter accounts (example)
    jupiter_accounts = ["JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB"]
    
    # Collect data for Jupiter
    jupiter_data = collector.collect_protocol_revenue_data("Jupiter", jupiter_accounts)
