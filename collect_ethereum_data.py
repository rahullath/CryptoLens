"""
Ethereum Data Collection Script

This script collects revenue data for Ethereum protocols using the Etherscan V2 API.
"""

import os
import sys
import json
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from src.etherscan_collector import EtherscanCollector

# Load environment variables
load_dotenv()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Ethereum Data Collector')
    parser.add_argument('--protocol', type=str, help='Protocol to collect data for (Aave, LIDO, Eigen, MKR, Compound)')
    parser.add_argument('--chain', type=str, default='ethereum', help='Chain to collect data from (ethereum, arbitrum, optimism, etc.)')
    parser.add_argument('--list-chains', action='store_true', help='List supported chains')
    parser.add_argument('--output-dir', type=str, default='data', help='Output directory for collected data')
    return parser.parse_args()

def main():
    """Main function to run the Ethereum data collector."""
    args = parse_args()
    
    # Create collector
    collector = EtherscanCollector(data_dir=args.output_dir)
    
    # Ensure output directory exists
    if not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    # List supported chains if requested
    if args.list_chains:
        print("Fetching supported chains...")
        chains = collector.get_supported_chains()
        
        if chains:
            print("\nSupported chains:")
            for chain in chains:
                print(f"  - {chain.get('chainName', 'Unknown')} (ID: {chain.get('chainId', 'Unknown')})")
        else:
            print("No chains found or API error occurred.")
        
        return
    
    # Collect data for specific protocol if provided
    if args.protocol:
        protocol = args.protocol
        chain = args.chain
        
        print(f"Collecting data for {protocol} on {chain}...")
        protocol_data = collector.collect_protocol_data(protocol, chain)
        
        if protocol_data:
            print(f"\nData collection for {protocol} on {chain} completed!")
            print(f"Total transactions: {protocol_data.get('total_transactions', 0)}")
            print(f"Total revenue: {protocol_data.get('total_revenue', 0)} ETH")
        else:
            print(f"No data collected for {protocol} on {chain}.")
    else:
        # Collect data for all protocols
        print("Collecting data for all Ethereum protocols...")
        all_data = collector.collect_all_protocols_data()
        
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
