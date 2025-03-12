"""
Main Module for Crypto Revenue Analyzer

This module orchestrates the entire process:
1. Collecting data from blockchain explorers
2. Analyzing the data
3. Creating visualizations
"""

import os
import json
import argparse
from datetime import datetime, timedelta
from dotenv import load_dotenv
from data_collector import DataCollector
from blockchair_collector import BlockchairCollector
from solscan_collector import SolscanCollector
from visualizer import Visualizer

# Load environment variables
load_dotenv()

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Crypto Revenue Analyzer')
    parser.add_argument('--start-date', type=str, help='Start date for data collection (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date for data collection (YYYY-MM-DD)')
    parser.add_argument('--skip-collection', action='store_true', help='Skip data collection and use existing data')
    parser.add_argument('--skip-visualization', action='store_true', help='Skip visualization creation')
    parser.add_argument('--solana-only', action='store_true', help='Only collect Solana data using Solscan')
    return parser.parse_args()

def main():
    """Main function to run the analyzer."""
    args = parse_args()
    
    # Set default dates if not provided
    if not args.start_date:
        # Default to 3 months ago
        args.start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    if not args.end_date:
        # Default to today
        args.end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Crypto Revenue Analyzer")
    print(f"Start Date: {args.start_date}")
    print(f"End Date: {args.end_date}")
    
    # Data collection
    if not args.skip_collection:
        print("\n=== Collecting Data ===")
        
        # Collect general protocol data
        print("\n--- Collecting General Protocol Data ---")
        data_collector = DataCollector()
        protocol_data = data_collector.collect_all_data()
        
        if not args.solana_only:
            # Collect blockchain data using Blockchair
            print("\n--- Collecting Blockchain Data (Blockchair) ---")
            blockchair_collector = BlockchairCollector()
            blockchain_data = blockchair_collector.collect_all_protocols_data(args.start_date, args.end_date)
        
        # Collect Solana data using Solscan
        print("\n--- Collecting Solana Data (Solscan) ---")
        solscan_collector = SolscanCollector()
        
        # Jupiter accounts (example)
        jupiter_accounts = ["JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB"]
        jupiter_data = solscan_collector.collect_protocol_revenue_data("Jupiter", jupiter_accounts)
        
        # Fluid accounts (example)
        fluid_accounts = ["FLUIDRdWNMiTJmJuXyiFLpb5p9NVPQcC5U9FJUNTmRQ"]
        fluid_data = solscan_collector.collect_protocol_revenue_data("Fluid", fluid_accounts)
        
        print("\nData collection complete!")
    else:
        print("\nSkipping data collection, using existing data.")
    
    # Visualization
    if not args.skip_visualization:
        print("\n=== Creating Visualizations ===")
        visualizer = Visualizer()
        visualizer.create_all_visualizations()
        print("\nVisualization creation complete!")
    else:
        print("\nSkipping visualization creation.")
    
    print("\nCrypto Revenue Analyzer completed successfully!")

if __name__ == "__main__":
    main()
