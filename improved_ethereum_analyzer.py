"""
Improved Ethereum Revenue Analyzer

This script collects and analyzes revenue data for Ethereum DeFi protocols
using the Etherscan API with clear time frames and methodology similar to DeFi Llama.
"""

import os
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import argparse
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
}

# Protocol contract addresses with metadata
PROTOCOL_CONTRACTS = {
    "LIDO": {
        "ethereum": [
            {
                "address": "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # Lido stETH
                "description": "Lido stETH Contract",
                "fee_percentage": 0.10,  # 10% of staking rewards go to the DAO
                "revenue_type": "staking_rewards"
            },
            {
                "address": "0x442af784A788A5bd6F42A01Ebe9F287a871243fb",  # Lido DAO Treasury
                "description": "Lido DAO Treasury",
                "fee_percentage": 1.0,  # 100% of treasury funds are revenue
                "revenue_type": "treasury"
            },
            {
                "address": "0x5a98fcbea516cf06857215779fd812ca3bef1b32",  # LDO Token
                "description": "Lido DAO Token (LDO)",
                "fee_percentage": 0.0,  # Not a revenue source
                "revenue_type": "token"
            }
        ]
    },
    "Aave": {
        "ethereum": [
            {
                "address": "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",  # Aave V2 LendingPool
                "description": "Aave V2 LendingPool",
                "fee_percentage": 0.09,  # 9% of protocol fees go to the DAO
                "revenue_type": "lending_fees"
            }
        ]
    }
}

# Time periods for analysis
TIME_PERIODS = {
    "24h": 1,
    "7d": 7,
    "30d": 30,
    "1y": 365,
    "all": 0  # All time
}

class ImprovedEthereumAnalyzer:
    def __init__(self, api_key=ETHERSCAN_API_KEY, data_dir="data"):
        """Initialize the ImprovedEthereumAnalyzer."""
        self.api_key = api_key
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_contract_transactions(self, contract_address, chain="ethereum", days=0):
        """
        Get transactions for a contract address within a specific time frame.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            days: Number of days to look back (0 for all time)
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching {chain} contract transactions for {contract_address} (past {days if days > 0 else 'all'} days)...")
        
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
        
        # Parameters for API call
        params = {
            "module": "account",
            "action": "txlist",
            "address": contract_address,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "desc",
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("status") == "1":
                    transactions = pd.DataFrame(data.get("result", []))
                    
                    # Filter by timestamp if needed
                    if days > 0 and not transactions.empty and "timeStamp" in transactions.columns:
                        transactions["timeStamp"] = transactions["timeStamp"].astype(int)
                        transactions = transactions[transactions["timeStamp"] >= start_timestamp]
                    
                    print(f"Found {len(transactions)} transactions in the past {days if days > 0 else 'all'} days")
                    
                    # Add datetime column for easier analysis
                    if not transactions.empty and "timeStamp" in transactions.columns:
                        transactions["datetime"] = pd.to_datetime(transactions["timeStamp"].astype(int), unit='s')
                    
                    return transactions
                else:
                    print(f"API error: {data.get('message')}")
            else:
                print(f"HTTP error: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching contract transactions: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def get_contract_internal_transactions(self, contract_address, chain="ethereum", days=0):
        """
        Get internal transactions for a contract address within a specific time frame.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            days: Number of days to look back (0 for all time)
            
        Returns:
            DataFrame with internal transaction data
        """
        print(f"Fetching {chain} contract internal transactions for {contract_address} (past {days if days > 0 else 'all'} days)...")
        
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
        
        # Parameters for API call
        params = {
            "module": "account",
            "action": "txlistinternal",
            "address": contract_address,
            "startblock": 0,
            "endblock": 99999999,
            "sort": "desc",
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(base_url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("status") == "1":
                    transactions = pd.DataFrame(data.get("result", []))
                    
                    # Filter by timestamp if needed
                    if days > 0 and not transactions.empty and "timeStamp" in transactions.columns:
                        transactions["timeStamp"] = transactions["timeStamp"].astype(int)
                        transactions = transactions[transactions["timeStamp"] >= start_timestamp]
                    
                    print(f"Found {len(transactions)} internal transactions in the past {days if days > 0 else 'all'} days")
                    
                    # Add datetime column for easier analysis
                    if not transactions.empty and "timeStamp" in transactions.columns:
                        transactions["datetime"] = pd.to_datetime(transactions["timeStamp"].astype(int), unit='s')
                    
                    return transactions
                else:
                    print(f"API error: {data.get('message')}")
            else:
                print(f"HTTP error: {response.status_code}")
                if response.text:
                    print(f"Response: {response.text}")
        except Exception as e:
            print(f"Error fetching internal transactions: {e}")
            
        return pd.DataFrame()  # Return empty DataFrame on error
    
    def collect_protocol_data(self, protocol_name, chain="ethereum", time_period="30d"):
        """
        Collect data for a protocol on a specific chain for a specific time period.
        
        Args:
            protocol_name: Name of the protocol
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            time_period: Time period to analyze (24h, 7d, 30d, 1y, all)
            
        Returns:
            Dictionary with protocol data
        """
        print(f"Collecting {chain} data for {protocol_name} over {time_period}...")
        
        # Get days from time period
        days = TIME_PERIODS.get(time_period, 30)  # Default to 30 days
        
        # Get contract addresses for the protocol on the specified chain
        contract_configs = PROTOCOL_CONTRACTS.get(protocol_name, {}).get(chain, [])
        if not contract_configs:
            print(f"No contract addresses found for {protocol_name} on {chain}")
            return {}
        
        # Get current date and time for reference
        now = datetime.now()
        
        # Initialize results
        protocol_data = {
            "protocol": protocol_name,
            "chain": chain,
            "time_period": time_period,
            "analysis_date": now.strftime("%Y-%m-%d %H:%M:%S"),
            "contracts": {},
            "total_transactions": 0,
            "total_fees": 0,  # Total protocol fees
            "total_revenue": 0,  # Protocol revenue (portion of fees that goes to the protocol)
            "fees_by_date": {},
            "revenue_by_date": {}
        }
        
        # Process each contract
        for contract_config in contract_configs:
            contract_address = contract_config["address"]
            description = contract_config["description"]
            fee_percentage = contract_config["fee_percentage"]
            revenue_type = contract_config["revenue_type"]
            
            print(f"Processing {description} ({contract_address}) for {protocol_name} on {chain}...")
            
            # Fetch transactions
            transactions = self.get_contract_transactions(contract_address, chain, days)
            
            # Skip if no transactions found
            if transactions.empty:
                print(f"No transactions found for contract {contract_address}")
                continue
            
            # Fetch internal transactions
            internal_txs = self.get_contract_internal_transactions(contract_address, chain, days)
            
            # Calculate fees and revenue
            contract_fees, contract_revenue, daily_fees, daily_revenue = self._calculate_contract_revenue(
                protocol_name, contract_config, transactions, internal_txs
            )
            
            # Add to results
            protocol_data["contracts"][contract_address] = {
                "address": contract_address,
                "description": description,
                "transaction_count": len(transactions),
                "fees": contract_fees,
                "revenue": contract_revenue,
                "revenue_type": revenue_type,
                "fee_percentage": fee_percentage
            }
            
            protocol_data["total_transactions"] += len(transactions)
            protocol_data["total_fees"] += contract_fees
            protocol_data["total_revenue"] += contract_revenue
            
            # Merge daily data
            for date, fee in daily_fees.items():
                if date not in protocol_data["fees_by_date"]:
                    protocol_data["fees_by_date"][date] = 0
                protocol_data["fees_by_date"][date] += fee
                
            for date, revenue in daily_revenue.items():
                if date not in protocol_data["revenue_by_date"]:
                    protocol_data["revenue_by_date"][date] = 0
                protocol_data["revenue_by_date"][date] += revenue
        
        # Save protocol data
        self._save_data(protocol_data, f"{protocol_name.lower()}_{chain}_{time_period}_data.json")
        
        return protocol_data
    
    def _calculate_contract_revenue(self, protocol_name, contract_config, transactions, internal_txs):
        """
        Calculate contract fees and revenue from transaction data.
        Uses protocol-specific logic based on revenue type.
        
        Args:
            protocol_name: Name of the protocol
            contract_config: Contract configuration
            transactions: DataFrame with transaction data
            internal_txs: DataFrame with internal transaction data
            
        Returns:
            Tuple of (total_fees, total_revenue, daily_fees, daily_revenue)
        """
        contract_address = contract_config["address"]
        fee_percentage = contract_config["fee_percentage"]
        revenue_type = contract_config["revenue_type"]
        
        # Initialize results
        total_fees = 0
        total_revenue = 0
        daily_fees = {}
        daily_revenue = {}
        
        if transactions.empty:
            return total_fees, total_revenue, daily_fees, daily_revenue
        
        # Convert value from wei to ETH if the column exists
        if "value" in transactions.columns:
            transactions["value_eth"] = transactions["value"].astype(float) / 10**18
        
        # Convert gas costs if columns exist
        if "gasUsed" in transactions.columns and "gasPrice" in transactions.columns:
            transactions["gas_cost_eth"] = (
                transactions["gasUsed"].astype(float) * 
                transactions["gasPrice"].astype(float) / 10**18
            )
        
        # Group by date if datetime column exists
        if "datetime" in transactions.columns:
            transactions["date"] = transactions["datetime"].dt.date.astype(str)
            date_groups = transactions.groupby("date")
        else:
            date_groups = None
        
        # Calculate fees and revenue based on protocol and revenue type
        if protocol_name == "LIDO" and revenue_type == "staking_rewards":
            # For Lido stETH, revenue comes from staking rewards
            # Lido takes 10% of staking rewards as protocol revenue
            
            # Use transaction value as a proxy for staking activity
            if "value_eth" in transactions.columns:
                # Estimate total staking rewards based on transaction volume and APR
                # Assuming a 4% APR for staking rewards
                staking_apr = 0.04
                staking_volume = transactions["value_eth"].sum()
                
                # Estimate daily rewards
                daily_reward = staking_volume * staking_apr / 365
                
                # Total fees are the staking rewards
                total_fees = daily_reward * 30  # Assuming 30 days
                
                # Protocol revenue is 10% of staking rewards
                total_revenue = total_fees * fee_percentage
                
                # Calculate daily fees and revenue if date groups exist
                if date_groups is not None:
                    for date, group in date_groups:
                        daily_volume = group["value_eth"].sum()
                        daily_reward_estimate = daily_volume * staking_apr / 365
                        daily_fees[date] = daily_reward_estimate
                        daily_revenue[date] = daily_reward_estimate * fee_percentage
            
        elif protocol_name == "LIDO" and revenue_type == "treasury":
            # For Lido Treasury, all incoming funds are considered revenue
            if "value_eth" in transactions.columns:
                # Sum incoming transactions (where 'to' is the contract address)
                incoming_txs = transactions[transactions["to"].str.lower() == contract_address.lower()]
                total_fees = incoming_txs["value_eth"].sum()
                total_revenue = total_fees  # All treasury funds are revenue
                
                # Calculate daily fees and revenue if date groups exist
                if date_groups is not None:
                    for date, group in date_groups:
                        incoming_group = group[group["to"].str.lower() == contract_address.lower()]
                        daily_fees[date] = incoming_group["value_eth"].sum()
                        daily_revenue[date] = daily_fees[date]
        
        elif protocol_name == "Aave" and revenue_type == "lending_fees":
            # For Aave, revenue comes from lending fees
            # Aave takes a percentage of interest as protocol revenue
            
            # This is a simplified calculation - real implementation would need more data
            if "value_eth" in transactions.columns:
                # Estimate total lending volume
                lending_volume = transactions["value_eth"].sum()
                
                # Assume average interest rate of 5%
                avg_interest_rate = 0.05
                
                # Estimate total interest earned
                total_interest = lending_volume * avg_interest_rate / 12  # Monthly interest
                
                # Total fees are the interest earned
                total_fees = total_interest
                
                # Protocol revenue is a percentage of interest (reserve factor)
                total_revenue = total_fees * fee_percentage
                
                # Calculate daily fees and revenue if date groups exist
                if date_groups is not None:
                    for date, group in date_groups:
                        daily_volume = group["value_eth"].sum()
                        daily_interest = daily_volume * avg_interest_rate / 365
                        daily_fees[date] = daily_interest
                        daily_revenue[date] = daily_interest * fee_percentage
        
        else:
            # Default case - use gas fees as a proxy for activity
            # This is not accurate for revenue but gives a sense of protocol activity
            if "gas_cost_eth" in transactions.columns:
                total_fees = transactions["gas_cost_eth"].sum()
                total_revenue = total_fees * fee_percentage
                
                # Calculate daily fees and revenue if date groups exist
                if date_groups is not None:
                    for date, group in date_groups:
                        daily_fees[date] = group["gas_cost_eth"].sum()
                        daily_revenue[date] = daily_fees[date] * fee_percentage
        
        return total_fees, total_revenue, daily_fees, daily_revenue
    
    def _save_data(self, data, filename):
        """Save data to JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Improved Ethereum Revenue Analyzer')
    parser.add_argument('--protocol', type=str, help='Protocol to analyze (LIDO, Aave, Eigen, MKR, Compound)')
    parser.add_argument('--chain', type=str, default='ethereum', help='Chain to analyze (ethereum, arbitrum, optimism, etc.)')
    parser.add_argument('--time-period', type=str, default='30d', choices=TIME_PERIODS.keys(), help='Time period to analyze (24h, 7d, 30d, 1y, all)')
    parser.add_argument('--list-protocols', action='store_true', help='List supported protocols')
    parser.add_argument('--output-dir', type=str, default='data', help='Output directory for collected data')
    return parser.parse_args()


def main():
    """Main function to run the analyzer."""
    args = parse_args()
    
    # Create analyzer
    analyzer = ImprovedEthereumAnalyzer(data_dir=args.output_dir)
    
    # List supported protocols if requested
    if args.list_protocols:
        print("\nSupported protocols and contract addresses:")
        for protocol, chains in PROTOCOL_CONTRACTS.items():
            print(f"\n{protocol}:")
            for chain, contracts in chains.items():
                print(f"  {chain.upper()}:")
                for contract in contracts:
                    print(f"    - {contract['description']}: {contract['address']}")
                    print(f"      Revenue Type: {contract['revenue_type']}, Fee Percentage: {contract['fee_percentage']*100}%")
        return
    
    # Collect data for specific protocol if provided
    if args.protocol:
        if args.protocol not in PROTOCOL_CONTRACTS:
            print(f"Error: Protocol '{args.protocol}' not supported")
            print("Supported protocols:", ", ".join(PROTOCOL_CONTRACTS.keys()))
            return
            
        protocol = args.protocol
        chain = args.chain
        time_period = args.time_period
        
        if chain not in PROTOCOL_CONTRACTS[protocol]:
            print(f"Error: Chain '{chain}' not supported for protocol '{protocol}'")
            print(f"Supported chains for {protocol}:", ", ".join(PROTOCOL_CONTRACTS[protocol].keys()))
            return
        
        print(f"Collecting data for {protocol} on {chain} over {time_period}...")
        protocol_data = analyzer.collect_protocol_data(protocol, chain, time_period)
        
        if protocol_data:
            print(f"\nData collection for {protocol} on {chain} over {time_period} completed!")
            print(f"Total transactions: {protocol_data.get('total_transactions', 0)}")
            print(f"Total fees: {protocol_data.get('total_fees', 0)} ETH")
            print(f"Total revenue: {protocol_data.get('total_revenue', 0)} ETH")
            
            # Show methodology
            print("\nMethodology:")
            print("- Fees: Total protocol fees (e.g., staking rewards, lending interest)")
            print("- Revenue: Portion of fees that goes to the protocol/DAO")
            
            # Show daily breakdown if available
            if protocol_data.get("revenue_by_date"):
                print("\nDaily Revenue Breakdown:")
                for date, revenue in sorted(protocol_data["revenue_by_date"].items()):
                    print(f"  {date}: {revenue:.6f} ETH")
        else:
            print(f"No data collected for {protocol} on {chain}.")
    else:
        print("Please specify a protocol to analyze with --protocol")
        print("Available protocols:", ", ".join(PROTOCOL_CONTRACTS.keys()))


if __name__ == "__main__":
    main()
