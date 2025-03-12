"""
Compound Finance V3 Monthly Revenue Analyzer using Dune Analytics

Fetches Compound V3 revenue data and summarizes it monthly.
"""

import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas as pd

# Load environment variables
load_dotenv()

DUNE_API_KEY = os.getenv("DUNE_API_KEY")
QUERY_ID = 2693782  # Your Dune query ID for Compound V3 Accruals

def fetch_compound_revenue():
    if not DUNE_API_KEY:
        raise ValueError("DUNE_API_KEY not found in environment variables")

    client = DuneClient(DUNE_API_KEY)
    query = QueryBase(query_id=QUERY_ID)
    revenue_data = client.run_query_dataframe(query)

    # Ensure expected columns are present
    expected_cols = ['ts', 'amount', 'transaction', 'ledger']
    if not all(col in revenue_data.columns for col in expected_cols):
        raise ValueError(f"Missing columns in query results: {expected_cols}")

    revenue_data = revenue_data[['ts', 'amount', 'transaction', 'ledger']]

    revenue_data.columns = ['Timestamp', 'Amount (USDC)', 'Transaction ID', 'Ledger Type']
    revenue_data['Timestamp'] = pd.to_datetime(revenue_data['Timestamp'])

    return revenue_data

def summarize_monthly_revenue(revenue_data):
    revenue_data['Year-Month'] = revenue_data['Timestamp'].dt.to_period('M')
    monthly_revenue = revenue_data.groupby('Year-Month')['Amount (USDC)'].sum().reset_index()
    monthly_revenue['Amount (USDC)'] = monthly_revenue['Amount (USDC)'].round(2)

    return monthly_revenue

def main():
    try:
        revenue_data = fetch_compound_revenue()
        
        if revenue_data.empty:
            print("No revenue data fetched.")
            return

        monthly_summary = summarize_monthly_revenue(revenue_data)

        total_revenue = monthly_summary['Amount (USDC)'].sum()
        avg_monthly_revenue = monthly_summary['Amount (USDC)'].mean()
        period_start = revenue_data['Timestamp'].min().date()
        period_end = revenue_data['Timestamp'].max().date()
        total_months = monthly_summary.shape[0]

        # Summary
        print("\nCompound V3 Revenue Summary:")
        print("="*30)
        print(f"Period Analyzed: {period_start} to {period_end}")
        print(f"Total Months: {total_months}")
        print(f"Total Revenue: ${total_revenue:,.2f}")
        print(f"Average Monthly Revenue: ${avg_monthly_revenue:,.2f}")

        # Save detailed monthly data
        monthly_summary.to_csv('compound_v3_monthly_revenue.csv', index=False)
        print("\nDetailed monthly data saved to 'compound_v3_monthly_revenue.csv'")

    except Exception as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    main()
