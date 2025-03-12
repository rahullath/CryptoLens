"""
Blockchair Data Collector Module

This module uses the Blockchair API to fetch accurate blockchain data for revenue analysis.
Blockchair provides data for multiple blockchains including Ethereum, Solana, and others.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
from config import PROTOCOLS, NETWORKS

# Load environment variables (for API key)
load_dotenv()

# Blockchair API configuration
BLOCKCHAIR_API_KEY = os.getenv("BLOCKCHAIR_API_KEY", "")
BLOCKCHAIR_BASE_URL = "https://api.blockchair.com"

# Blockchain mappings for Blockchair
BLOCKCHAIR_CHAINS = {
    "ethereum": "ethereum",
    "solana": "solana",
    "bitcoin": "bitcoin",
    "polygon": "polygon",
    "arbitrum": "arbitrum-one",
    "optimism": "optimism",
    "base": "base",
    "avalanche": "avalanche",
    "sui": None,  # Not directly supported by Blockchair
}

class BlockchairCollector:
    def __init__(self, api_key=BLOCKCHAIR_API_KEY, data_dir="../data"):
        """Initialize the BlockchairCollector."""
        self.api_key = api_key
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_eth_contract_transactions(self, contract_address, start_date=None, end_date=None, limit=100):
        """
        Get transactions for an Ethereum contract address.
        
        Args:
            contract_address: The contract address to analyze
            start_date: Start date for data collection (YYYY-MM-DD)
            end_date: End date for data collection (YYYY-MM-DD)
            limit: Maximum number of transactions to retrieve
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching Blockchair data for Ethereum contract {contract_address}...")
        
        # Convert dates to timestamps if provided
        start_timestamp = None
        end_timestamp = None
        
        if start_date:
            start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp())
        if end_date:
            end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp())
        
        # Build URL
        url = f"{BLOCKCHAIR_BASE_URL}/ethereum/transactions"
        
        # Parameters for API call
        params = {
            "q": f"recipient({contract_address})",
            "limit": limit,
            "offset": 0,
        }
        
        # Add API key if available
        if self.api_key:
            params["key"] = self.api_key
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if "data" in data and len(data["data"]) > 0:
                    # Convert to DataFrame
                    transactions = pd.DataFrame(data["data"])
                    
                    # Filter by timestamp if provided
                    if start_timestamp and "time" in transactions.columns:
                        transactions = transactions[transactions["time"] >= start_timestamp]
                    if end_timestamp and "time" in transactions.columns:
                        transactions = transactions[transactions["time"] <= end_timestamp]
                    
                    # Convert values from Wei to Ether
                    if "value" in transactions.columns:
                        transactions["value_eth"] = transactions["value"].astype(float) / 10**18
                    
                    return transactions
                else:
                    print(f"No transaction data found for contract {contract_address}")
            else:
                print(f"Failed to fetch Blockchair data: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching Blockchair data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_solana_account_transactions(self, account_address, limit=100):
        """
        Get transactions for a Solana account address.
        
        Args:
            account_address: The account address to analyze
            limit: Maximum number of transactions to retrieve
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching Blockchair data for Solana account {account_address}...")
        
        # Build URL
        url = f"{BLOCKCHAIR_BASE_URL}/solana/raw/account/{account_address}"
        
        # Parameters for API call
        params = {
            "limit": limit,
        }
        
        # Add API key if available
        if self.api_key:
            params["key"] = self.api_key
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if "data" in data and account_address in data["data"]:
                    account_data = data["data"][account_address]
                    
                    # Extract transactions if available
                    if "transactions" in account_data:
                        transactions = pd.DataFrame(account_data["transactions"])
                        return transactions
                    else:
                        print(f"No transaction data found for Solana account {account_address}")
                else:
                    print(f"No data found for Solana account {account_address}")
            else:
                print(f"Failed to fetch Blockchair data: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching Blockchair data: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_protocol_contract_addresses(self, protocol_name):
        """
        Get the main contract addresses for a protocol based on its name.
        This is a simplified implementation - in a real-world scenario,
        these would be sourced from protocol documentation or community sources.
        
        Args:
            protocol_name: Name of the protocol
            
        Returns:
            Dictionary with contract addresses by chain
        """
        # This is a simplified mapping - real implementation would need more research
        contract_addresses = {
            "Aave": {
                "ethereum": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",  # Lending Pool V2
                "polygon": "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",  # Lending Pool V2 on Polygon
            },
            "LIDO": {
                "ethereum": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # Lido stETH token
            },
            "Eigen": {
                "ethereum": "0x858646372CC42E1A627fcE94aa7A7033e7CF075A",  # EigenLayer StrategyManager
            },
            "MKR": {
                "ethereum": "0x9f8F72aA9304c8B593d555F12eF6589cC3A579A2",  # MKR token
            },
            "Compound": {
                "ethereum": "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",  # Compound Comptroller
            },
            "Fluid": {
                "ethereum": "0x5E8C8A7243651DB1384C0dDfDbE39761E8e7E51a",  # Placeholder - needs verification
            },
            "Jupiter": {
                "solana": "JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB",  # Jupiter aggregator program ID
            },
            "Sonic": {
                "sui": "0x1f0d4d3ca884a1a6958fe5ba9dc6d8003d6f7913",  # Placeholder - needs verification
            }
        }
        
        return contract_addresses.get(protocol_name, {})
    
    def collect_protocol_revenue_data(self, protocol_name, start_date=None, end_date=None):
        """
        Collect revenue data for a specific protocol using Blockchair.
        
        Args:
            protocol_name: Name of the protocol
            start_date: Start date for data collection (YYYY-MM-DD)
            end_date: End date for data collection (YYYY-MM-DD)
            
        Returns:
            Dictionary with revenue data by chain
        """
        print(f"Collecting revenue data for {protocol_name}...")
        
        # Get contract addresses for the protocol
        contract_addresses = self.get_protocol_contract_addresses(protocol_name)
        
        if not contract_addresses:
            print(f"No contract addresses found for {protocol_name}")
            return {}
        
        # Initialize results
        revenue_data = {}
        
        # Process each chain
        for chain, address in contract_addresses.items():
            print(f"Processing {chain} contract {address} for {protocol_name}...")
            
            # Skip if chain not supported by Blockchair
            if chain not in BLOCKCHAIR_CHAINS or not BLOCKCHAIR_CHAINS[chain]:
                print(f"Chain {chain} not supported by Blockchair")
                continue
            
            # Fetch transactions based on chain
            if chain == "ethereum" or chain in ["polygon", "arbitrum", "optimism", "base", "avalanche"]:
                transactions = self.get_eth_contract_transactions(address, start_date, end_date)
            elif chain == "solana":
                transactions = self.get_solana_account_transactions(address)
            else:
                print(f"Chain {chain} data collection not implemented")
                continue
            
            # Skip if no transactions found
            if transactions.empty:
                print(f"No transactions found for {protocol_name} on {chain}")
                continue
            
            # Calculate revenue (implementation depends on protocol)
            # This is a simplified example - real implementation would need protocol-specific logic
            revenue = self._calculate_protocol_revenue(protocol_name, chain, transactions)
            
            # Add to results
            revenue_data[chain] = revenue
        
        # Save revenue data
        self._save_data(revenue_data, f"{protocol_name.lower()}_revenue_data.json")
        
        return revenue_data
    
    def _calculate_protocol_revenue(self, protocol_name, chain, transactions):
        """
        Calculate protocol revenue from transaction data.
        This is a simplified implementation - real calculations would be protocol-specific.
        
        Args:
            protocol_name: Name of the protocol
            chain: Blockchain name
            transactions: DataFrame with transaction data
            
        Returns:
            Dictionary with revenue metrics
        """
        # This is a placeholder - real implementation would need protocol-specific logic
        if transactions.empty:
            return {
                "protocol": protocol_name,
                "chain": chain,
                "total_revenue": 0,
                "daily_avg": 0,
                "weekly_avg": 0,
                "monthly_avg": 0,
                "quarterly_avg": 0,
                "yearly_projection": 0
            }
        
        # Example calculation for Ethereum-based protocols
        if chain == "ethereum" or chain in ["polygon", "arbitrum", "optimism", "base", "avalanche"]:
            if "value_eth" in transactions.columns:
                total_revenue = transactions["value_eth"].sum()
            elif "value" in transactions.columns:
                total_revenue = transactions["value"].astype(float).sum() / 10**18
            else:
                total_revenue = 0
        
        # Example calculation for Solana-based protocols
        elif chain == "solana":
            if "lamports" in transactions.columns:
                total_revenue = transactions["lamports"].astype(float).sum() / 10**9
            else:
                total_revenue = 0
        
        # Default case
        else:
            total_revenue = 0
        
        # Calculate time range if timestamp available
        if "time" in transactions.columns:
            timestamps = transactions["time"].astype(int)
            start_time = min(timestamps)
            end_time = max(timestamps)
            time_range_days = (end_time - start_time) / 86400  # Convert seconds to days
        else:
            time_range_days = 30  # Default to 30 days if no timestamp
        
        # Avoid division by zero
        if time_range_days > 0:
            daily_avg = total_revenue / time_range_days
            weekly_avg = daily_avg * 7
            monthly_avg = daily_avg * 30
            quarterly_avg = monthly_avg * 3
            yearly_projection = daily_avg * 365
        else:
            daily_avg = total_revenue
            weekly_avg = total_revenue
            monthly_avg = total_revenue
            quarterly_avg = total_revenue
            yearly_projection = total_revenue
        
        return {
            "protocol": protocol_name,
            "chain": chain,
            "total_revenue": total_revenue,
            "daily_avg": daily_avg,
            "weekly_avg": weekly_avg,
            "monthly_avg": monthly_avg,
            "quarterly_avg": quarterly_avg,
            "yearly_projection": yearly_projection
        }
    
    def _save_data(self, data, filename):
        """Save data to JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")
    
    def collect_all_protocols_data(self, start_date=None, end_date=None):
        """
        Collect revenue data for all protocols.
        
        Args:
            start_date: Start date for data collection (YYYY-MM-DD)
            end_date: End date for data collection (YYYY-MM-DD)
            
        Returns:
            Dictionary with revenue data by protocol
        """
        # Set default dates if not provided
        if not start_date:
            # Default to 3 months ago
            start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        if not end_date:
            # Default to today
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        print(f"Collecting revenue data for all protocols from {start_date} to {end_date}...")
        
        # Initialize results
        all_protocols_data = {}
        
        # Process each protocol
        for protocol in PROTOCOLS:
            protocol_name = protocol['name']
            revenue_data = self.collect_protocol_revenue_data(protocol_name, start_date, end_date)
            all_protocols_data[protocol_name] = revenue_data
        
        # Save all protocols data
        self._save_data(all_protocols_data, "all_protocols_revenue_data.json")
        
        return all_protocols_data


if __name__ == "__main__":
    # Example usage
    collector = BlockchairCollector()
    
    # Set date range (last 3 months)
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    
    # Collect data for all protocols
    all_data = collector.collect_all_protocols_data(start_date, end_date)
