"""
Compound V3 Revenue and Yield Earners Analyzer using Dune Analytics

- Fetches revenue data (Query ID: 2693782).
- Fetches yield earners data (Query ID: 2694202).
- Summarizes both datasets individually and combines for total Compound V3 revenue.
"""

import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas as pd

# Load environment variables
load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY")

# Your Dune Query IDs
REVENUE_QUERY_ID = 2693782
YIELD_EARNERS_QUERY_ID = 2694202

def fetch_dune_query(query_id):
    if not DUNE_API_KEY:
        raise ValueError("DUNE_API_KEY not found in environment variables")

    client = DuneClient(DUNE_API_KEY)
    query = QueryBase(query_id=query_id)
    return client.run_query_dataframe(query)

def summarize_monthly_revenue(revenue_df):
    revenue_df['Timestamp'] = pd.to_datetime(revenue_df['Timestamp'])
    revenue_df['Year-Month'] = revenue_df['Timestamp'].dt.to_period('M')
    monthly_summary = revenue_df.groupby('Year-Month')['Amount (USDC)'].sum().reset_index()
    monthly_summary['Amount (USDC)'] = monthly_summary['Amount (USDC)'].round(2)
    return monthly_summary

def main():
    try:
        # Fetch Compound V3 Revenue Data
        revenue_df = fetch_dune_query(REVENUE_QUERY_ID)[['ts', 'amount']]
        revenue_df.columns = ['Timestamp', 'Amount (USDC)']
        
        # Summarize Monthly Revenue
        monthly_revenue = summarize_monthly_revenue(revenue_df)

        total_revenue = monthly_revenue['Amount (USDC)'].sum()
        avg_monthly_revenue = monthly_revenue['Amount (USDC)'].mean()

        print("\nCompound V3 Monthly Revenue Summary:")
        print("="*40)
        print(f"Total Revenue: ${total_revenue:,.2f}")
        print(f"Average Monthly Revenue: ${avg_monthly_revenue:,.2f}")
        monthly_revenue.to_csv('compound_v3_monthly_revenue.csv', index=False)
        print("Monthly revenue details saved to 'compound_v3_monthly_revenue.csv'")

        # Fetch Compound V3 Yield Earners Data
        earners_df = fetch_dune_query(YIELD_EARNERS_QUERY_ID)
        earners_df.columns = ['Wallet', 'Total Accrual Amount (USDC)']

        total_earners_accrual = earners_df['Total Accrual Amount (USDC)'].sum().round(2)
        total_unique_wallets = earners_df['Wallet'].nunique()

        print("\nCompound V3 Yield Earners Summary:")
        print("="*40)
        print(f"Total Yield Earners: {total_unique_wallets}")
        print(f"Total Accrued Amount: ${total_earners_accrual:,.2f}")
        earners_df.to_csv('compound_v3_yield_earners.csv', index=False)
        print("Yield earners details saved to 'compound_v3_yield_earners.csv'")

        # Combined Total
        combined_total = total_revenue + total_earners_accrual
        print("\nCombined Total Revenue (Revenue + Yield Earners):")
        print("="*40)
        print(f"Combined Total: ${combined_total:,.2f}")

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
