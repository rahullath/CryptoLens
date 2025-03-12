"""
Lido Revenue Analyzer using Dune Analytics

This module fetches Lido protocol revenue data directly from Dune Analytics dashboards.
Uses the Execution Layer Rewards Vault query to track ETH inflows.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Optional
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.types import QueryParameter
import pandas as pd
import requests

# Load environment variables
load_dotenv()

class LidoDuneAnalyzer:
    # Dune Analytics Query IDs for Lido revenue tracking
    REVENUE_QUERY = """
    WITH blocks AS (
        SELECT
            number,
            blocks.time,
            blocks.base_fee_per_gas,
            blocks.gas_used,
            (blocks.base_fee_per_gas/1e18) * (blocks.gas_used / 1e18) AS total_burn
        FROM ethereum.blocks
        WHERE miner = 0x388c818ca8b9251b393131c08a736a67ccb19297
    ),
    eth_tx AS (
        SELECT
            block_time,
            block_number,
            gas_used,
            (gas_used / 1e18) * (gas_price / 1e18) AS fee
        FROM ethereum.transactions
        WHERE block_number IN (SELECT DISTINCT number FROM blocks)
    ),
    eth_tx_agg AS (
        SELECT
            block_number,
            MAX(block_time) AS block_time,
            SUM(gas_used) AS block_gas_used,
            SUM(fee) AS fee
        FROM eth_tx
        GROUP BY block_number
    ),
    blocks_rewards AS (
        SELECT
            block_number,
            block_time,
            block_gas_used,
            fee - b.total_burn AS block_reward
        FROM eth_tx_agg AS t
        LEFT JOIN blocks AS b ON t.block_number = b.number
        ORDER BY block_number DESC
    ),
    transfers AS (
        SELECT
            block_time AS time,
            block_number,
            SUM(CAST(value AS DOUBLE)) / 1e18 AS amount
        FROM ethereum.traces
        WHERE "to" = 0x388c818ca8b9251b393131c08a736a67ccb19297
        AND (NOT LOWER(call_type) IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
        AND tx_success
        AND success
        AND block_number >= 15537393
        GROUP BY 1, 2
    ),
    aggr_data AS (
        SELECT
            block_time,
            block_number,
            block_reward AS amount
        FROM blocks_rewards
        UNION
        SELECT
            time,
            block_number,
            CAST(amount AS DOUBLE)
        FROM transfers
    ),
    eth_received_daily AS (
        SELECT
            DATE_TRUNC('day', block_time) AS day,
            SUM(CAST(amount AS DOUBLE)) AS daily_amount
        FROM aggr_data
        GROUP BY 1
    )
    SELECT
        day,
        CAST(daily_amount AS DOUBLE) AS eth_received,
        SUM(CAST(daily_amount AS DOUBLE)) OVER (ORDER BY day NULLS FIRST) AS eth_received_cumul
    FROM eth_received_daily
    WHERE day >= CURRENT_DATE - INTERVAL ':days' day
    ORDER BY day DESC
    """
    
    def __init__(self):
        self.api_key = os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")
        
        self.client = DuneClient(self.api_key)
        
    def get_revenue_data(self, days: int = 30) -> Dict:
        """
        Get Lido revenue data from Dune Analytics.
        
        Args:
            days: Number of days to analyze (default: 30)
            
        Returns:
            Dictionary containing revenue data
        """
        try:
            # Create a new query with parameters
            print(f"\nFetching Lido revenue data for the last {days} days...")
            
            # Replace the days parameter in the query
            query = self.REVENUE_QUERY.replace(":days", str(days))
            
            # Execute the query
            results = self.client.sql_query(
                name="lido_revenue",
                query=query,
                description=f"Lido revenue analysis for last {days} days"
            )
            
            if not results or not hasattr(results, 'result'):
                print("No data returned from Dune Analytics")
                return {
                    "success": False,
                    "error": "No data returned"
                }
            
            # Convert result to DataFrame
            df = pd.DataFrame(results.result.rows)
            if df.empty:
                return {
                    "success": False,
                    "error": "No data found for the specified period"
                }
            
            # Rename columns to match expected format
            df.columns = ['date', 'daily_eth', 'cumulative_eth']
            
            # Calculate key metrics
            total_eth = float(df['daily_eth'].sum())
            latest_cumulative = float(df['cumulative_eth'].iloc[0])
            daily_avg = total_eth / min(days, len(df))
            
            # Get current ETH price from CoinGecko
            try:
                response = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd")
                eth_price = float(response.json()["ethereum"]["usd"])
            except:
                eth_price = 1900  # Fallback price if API fails
            
            return {
                "success": True,
                "period_days": days,
                "total_revenue_eth": total_eth,
                "total_revenue_usd": total_eth * eth_price,
                "daily_average_eth": daily_avg,
                "daily_average_usd": daily_avg * eth_price,
                "cumulative_eth": latest_cumulative,
                "cumulative_usd": latest_cumulative * eth_price,
                "eth_price": eth_price,
                "raw_data": df.to_dict('records')
            }
            
        except Exception as e:
            print(f"Error fetching data from Dune: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def analyze_revenue_periods(self) -> Dict:
        """Analyze revenue for different time periods (24h, 7d, 30d, 1y)."""
        periods = {
            "24h": 1,
            "7d": 7,
            "30d": 30,
            "1y": 365
        }
        
        results = {}
        for period_name, days in periods.items():
            print(f"\nAnalyzing {period_name} period...")
            results[period_name] = self.get_revenue_data(days)
        
        return results

def main():
    """Main function to demonstrate usage."""
    analyzer = LidoDuneAnalyzer()
    
    print("Starting Lido Revenue Analysis (Dune Analytics)...")
    print("===============================================")
    
    # Analyze revenue for different periods
    revenue_analysis = analyzer.analyze_revenue_periods()
    
    # Print results
    print("\nFinal Results:")
    print("=============")
    
    for period, data in revenue_analysis.items():
        if data["success"]:
            print(f"\n{period} Period:")
            print(f"Total Revenue: {data['total_revenue_eth']:.4f} ETH (${data['total_revenue_usd']:,.2f} USD)")
            print(f"Daily Average: {data['daily_average_eth']:.4f} ETH (${data['daily_average_usd']:,.2f} USD)")
            print(f"Cumulative Revenue: {data['cumulative_eth']:.4f} ETH (${data['cumulative_usd']:,.2f} USD)")
            print(f"Current ETH Price: ${data['eth_price']:.2f}")
        else:
            print(f"\n{period} Period: Error - {data.get('error', 'Unknown error')}")

if __name__ == "__main__":
    main() 