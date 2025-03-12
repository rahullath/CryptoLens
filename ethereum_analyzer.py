"""
Ethereum Revenue Analyzer

This script collects and analyzes revenue data for Ethereum DeFi protocols
using the Etherscan V2 API.
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
    "ethereum": "https://api.etherscan.io/api",  # Using v1 API for better compatibility
    "arbitrum": "https://api.arbiscan.io/api",
    "optimism": "https://api.optimistic.etherscan.io/api",
    "polygon": "https://api.polygonscan.com/api",
    "base": "https://api.basescan.org/api",
    "avalanche": "https://api.snowtrace.io/api",
    "bsc": "https://api.bscscan.com/api",
    "fantom": "https://api.ftmscan.com/api",
}

# Protocol contract addresses
PROTOCOL_CONTRACTS = {
    "Aave": {
        "ethereum": [
            "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",  # Aave V2 LendingPool
            "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"   # Aave V3 LendingPool
        ],
        "optimism": [
            "0x794a61358D6845594F94dc1DB02A252b5b4814aD"   # Aave V3 LendingPool
        ],
        "arbitrum": [
            "0x794a61358D6845594F94dc1DB02A252b5b4814aD"   # Aave V3 LendingPool
        ],
        "polygon": [
            "0x8dFf5E27EA6b7AC08EbFdf9eB090F32ee9a30fcf",  # Aave V2 LendingPool
            "0x794a61358D6845594F94dc1DB02A252b5b4814aD"   # Aave V3 LendingPool
        ]
    },
    "LIDO": {
        "ethereum": [
            "0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84",  # Lido stETH
            "0x442af784A788A5bd6F42A01Ebe9F287a871243fb"   # Lido DAO Treasury
        ]
    },
    "Eigen": {
        "ethereum": [
            "0x858646372CC42E1A627fcE94aa7A7033e7CF075A",  # EigenLayer Strategy Manager
            "0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A"   # EigenLayer Delegation Manager
        ]
    },
    "MKR": {
        "ethereum": [
            "0x9759A6Ac90977b93B58547b4A71c78317f391A28",  # MakerDAO PSM
            "0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B"   # Maker Vat (CDP Engine)
        ]
    },
    "Compound": {
        "ethereum": [
            "0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B",  # Compound Comptroller
            "0xbEb5Fc579115071764c7423A4f12eDde41f106Ed"   # Compound cUSDC
        ],
        "polygon": [
            "0xF25212E676D1F7F89Cd72fFEe66158f541246445"   # Compound Comptroller
        ]
    }
}

class EthereumAnalyzer:
    def __init__(self, api_key=ETHERSCAN_API_KEY, data_dir="data"):
        """Initialize the EthereumAnalyzer."""
        self.api_key = api_key
        self.data_dir = data_dir
        self.ensure_data_dir()
        
    def ensure_data_dir(self):
        """Ensure the data directory exists."""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_contract_transactions(self, contract_address, chain="ethereum", start_block=0, end_block=99999999):
        """
        Get transactions for a contract address.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            DataFrame with transaction data
        """
        print(f"Fetching {chain} contract transactions for {contract_address}...")
        
        # Get base URL for the specified chain
        base_url = ETHERSCAN_BASE_URLS.get(chain.lower())
        if not base_url:
            print(f"Unsupported chain: {chain}")
            return pd.DataFrame()
        
        # Build URL
        url = base_url
        
        # Parameters for API call
        params = {
            "module": "account",
            "action": "txlist",
            "address": contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "sort": "desc",
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("status") == "1":
                    transactions = pd.DataFrame(data.get("result", []))
                    print(f"Found {len(transactions)} transactions")
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
    
    def get_contract_internal_transactions(self, contract_address, chain="ethereum", start_block=0, end_block=99999999):
        """
        Get internal transactions for a contract address.
        
        Args:
            contract_address: The contract address to analyze
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            start_block: Starting block number
            end_block: Ending block number
            
        Returns:
            DataFrame with internal transaction data
        """
        print(f"Fetching {chain} contract internal transactions for {contract_address}...")
        
        # Get base URL for the specified chain
        base_url = ETHERSCAN_BASE_URLS.get(chain.lower())
        if not base_url:
            print(f"Unsupported chain: {chain}")
            return pd.DataFrame()
        
        # Build URL
        url = base_url
        
        # Parameters for API call
        params = {
            "module": "account",
            "action": "txlistinternal",
            "address": contract_address,
            "startblock": start_block,
            "endblock": end_block,
            "sort": "desc",
            "apikey": self.api_key
        }
        
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                
                if data.get("status") == "1":
                    transactions = pd.DataFrame(data.get("result", []))
                    print(f"Found {len(transactions)} internal transactions")
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
    
    def collect_protocol_data(self, protocol_name, chain="ethereum"):
        """
        Collect data for a protocol on a specific chain.
        
        Args:
            protocol_name: Name of the protocol
            chain: The blockchain to query (ethereum, arbitrum, etc.)
            
        Returns:
            Dictionary with protocol data
        """
        print(f"Collecting {chain} data for {protocol_name}...")
        
        # Get contract addresses for the protocol on the specified chain
        contract_addresses = PROTOCOL_CONTRACTS.get(protocol_name, {}).get(chain, [])
        if not contract_addresses:
            print(f"No contract addresses found for {protocol_name} on {chain}")
            return {}
        
        # Initialize results
        protocol_data = {
            "protocol": protocol_name,
            "chain": chain,
            "contracts": {},
            "total_transactions": 0,
            "total_revenue": 0,
            "revenue_by_date": {}
        }
        
        # Process each contract
        for contract_address in contract_addresses:
            print(f"Processing contract {contract_address} for {protocol_name} on {chain}...")
            
            # Fetch transactions
            transactions = self.get_contract_transactions(contract_address, chain)
            
            # Skip if no transactions found
            if transactions.empty:
                print(f"No transactions found for contract {contract_address}")
                continue
            
            # Fetch internal transactions
            internal_txs = self.get_contract_internal_transactions(contract_address, chain)
            
            # Calculate revenue (implementation depends on protocol)
            contract_revenue = self._calculate_contract_revenue(
                protocol_name, contract_address, transactions, internal_txs
            )
            
            # Add to results
            protocol_data["contracts"][contract_address] = {
                "address": contract_address,
                "transaction_count": len(transactions),
                "revenue": contract_revenue
            }
            
            protocol_data["total_transactions"] += len(transactions)
            protocol_data["total_revenue"] += contract_revenue
        
        # Save protocol data
        self._save_data(protocol_data, f"{protocol_name.lower()}_{chain}_data.json")
        
        return protocol_data
    
    def collect_all_protocols_data(self):
        """
        Collect data for all protocols.
        
        Returns:
            Dictionary with all protocols data
        """
        print("Collecting data for all protocols...")
        
        # Initialize results
        all_protocols_data = {}
        
        # Process each protocol
        for protocol_name in PROTOCOL_CONTRACTS:
            # Process each chain for the protocol
            for chain in PROTOCOL_CONTRACTS.get(protocol_name, {}):
                protocol_data = self.collect_protocol_data(protocol_name, chain)
                
                # Add to results
                if protocol_name not in all_protocols_data:
                    all_protocols_data[protocol_name] = {}
                
                all_protocols_data[protocol_name][chain] = protocol_data
        
        # Save all protocols data
        self._save_data(all_protocols_data, "all_protocols_ethereum_data.json")
        
        return all_protocols_data
    
    def _calculate_contract_revenue(self, protocol_name, contract_address, transactions, internal_txs):
        """
        Calculate contract revenue from transaction data.
        This is a simplified implementation - real calculations would be protocol-specific.
        
        Args:
            protocol_name: Name of the protocol
            contract_address: Contract address
            transactions: DataFrame with transaction data
            internal_txs: DataFrame with internal transaction data
            
        Returns:
            Total revenue in ETH
        """
        # This is a placeholder - real implementation would need protocol-specific logic
        if transactions.empty:
            return 0
        
        # Example calculation for Aave (lending fees)
        if protocol_name == "Aave":
            # For Aave, revenue comes from lending fees
            # This is a simplified example - real implementation would need more analysis
            if "value" in transactions.columns:
                # Convert value from wei to ETH
                transactions["value_eth"] = transactions["value"].astype(float) / 10**18
                
                # Estimate revenue as a percentage of transaction value
                # This is just a placeholder - real calculation would be more complex
                revenue = transactions["value_eth"].sum() * 0.01  # Assume 1% fee
            else:
                revenue = 0
        
        # Example calculation for Lido (staking fees)
        elif protocol_name == "LIDO":
            # For Lido, revenue comes from staking fees
            if "value" in transactions.columns:
                # Convert value from wei to ETH
                transactions["value_eth"] = transactions["value"].astype(float) / 10**18
                
                # Estimate revenue as a percentage of transaction value
                # Lido takes 10% of staking rewards
                revenue = transactions["value_eth"].sum() * 0.1  # 10% fee
            else:
                revenue = 0
        
        # Default case - use gas fees as a proxy for revenue
        else:
            if "gasUsed" in transactions.columns and "gasPrice" in transactions.columns:
                # Calculate gas cost in ETH
                transactions["gas_cost_eth"] = (
                    transactions["gasUsed"].astype(float) * 
                    transactions["gasPrice"].astype(float) / 10**18
                )
                
                # Use gas cost as a proxy for revenue
                revenue = transactions["gas_cost_eth"].sum()
            else:
                revenue = 0
        
        return revenue
    
    def _save_data(self, data, filename):
        """Save data to JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to {filepath}")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Ethereum Revenue Analyzer')
    parser.add_argument('--protocol', type=str, help='Protocol to analyze (Aave, LIDO, Eigen, MKR, Compound)')
    parser.add_argument('--chain', type=str, default='ethereum', help='Chain to analyze (ethereum, arbitrum, optimism, etc.)')
    parser.add_argument('--list-protocols', action='store_true', help='List supported protocols')
    parser.add_argument('--output-dir', type=str, default='data', help='Output directory for collected data')
    return parser.parse_args()


def main():
    """Main function to run the analyzer."""
    args = parse_args()
    
    # Create analyzer
    analyzer = EthereumAnalyzer(data_dir=args.output_dir)
    
    # List supported protocols if requested
    if args.list_protocols:
        print("\nSupported protocols and contract addresses:")
        for protocol, chains in PROTOCOL_CONTRACTS.items():
            print(f"\n{protocol}:")
            for chain, addresses in chains.items():
                print(f"  {chain.upper()}:")
                for address in addresses:
                    print(f"    - {address}")
        return
    
    # Collect data for specific protocol if provided
    if args.protocol:
        if args.protocol not in PROTOCOL_CONTRACTS:
            print(f"Error: Protocol '{args.protocol}' not supported")
            print("Supported protocols:", ", ".join(PROTOCOL_CONTRACTS.keys()))
            return
            
        protocol = args.protocol
        chain = args.chain
        
        if chain not in PROTOCOL_CONTRACTS[protocol]:
            print(f"Error: Chain '{chain}' not supported for protocol '{protocol}'")
            print(f"Supported chains for {protocol}:", ", ".join(PROTOCOL_CONTRACTS[protocol].keys()))
            return
        
        print(f"Collecting data for {protocol} on {chain}...")
        protocol_data = analyzer.collect_protocol_data(protocol, chain)
        
        if protocol_data:
            print(f"\nData collection for {protocol} on {chain} completed!")
            print(f"Total transactions: {protocol_data.get('total_transactions', 0)}")
            print(f"Total revenue: {protocol_data.get('total_revenue', 0)} ETH")
        else:
            print(f"No data collected for {protocol} on {chain}.")
    else:
        # Collect data for all protocols
        print("Collecting data for all Ethereum protocols...")
        all_data = analyzer.collect_all_protocols_data()
        
        if all_data:
            print("\nData collection for all protocols completed!")
            
            # Print summary
            print("\nSummary:")
            for protocol, chains in all_data.items():
                for chain, data in chains.items():
                    print(f"  - {protocol} ({chain}):")
                    print(f"    - Transactions: {data.get('total_transactions', 0)}")
                    print(f"    - Revenue: {data.get('total_revenue', 0)} ETH")
        else:
            print("No data collected.")


if __name__ == "__main__":
    main()
