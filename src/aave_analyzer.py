"""
Aave Revenue Analyzer using Dune Analytics

This module fetches Aave protocol revenue data from Dune Analytics and calculates revenue metrics.
Uses borrow volume and APR data to compute monthly revenue.
"""

import os
from datetime import datetime, timedelta
from typing import Dict, Optional
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
from dune_client.types import QueryParameter
import pandas as pd

# Load environment variables
load_dotenv()

class AaveAnalyzer:
    AAVE_QUERY_ID = 3237115  # Dune query ID for Aave metrics
    AVERAGE_BORROW_APR = 0.0952  # 9.52% average borrow APR
    
    def __init__(self):
        self.api_key = os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")
        
        # Initialize Dune client with a 5-minute timeout
        self.client = DuneClient(
            api_key=self.api_key,
            request_timeout=300
        )
    
    def get_revenue_data(self, time_interval: str = 'day', trading_days: int = 30, performance: str = "medium") -> Dict:
        """
        Get Aave revenue data from Dune Analytics and calculate metrics.
        
        Args:
            time_interval: Time interval for data aggregation ('day', 'week', 'month')
            trading_days: Number of days to look back for trading data
            performance: Query execution performance ('medium' or 'large')
        
        Returns:
            Dictionary containing revenue metrics and raw data
        """
        try:
            print(f"\nFetching Aave borrow volume data for the last {trading_days} days...")
            
            # Create query object with parameters
            query = QueryBase(
                query_id=self.AAVE_QUERY_ID,
                params=[
                    QueryParameter.text_type(name="Time interval", value=time_interval),
                    QueryParameter.number_type(name="Trading Num Days", value=trading_days)
                ]
            )
            
            # Execute query and get results
            print(f"Executing Dune query {self.AAVE_QUERY_ID} with parameters...")
            results = self.client.run_query_dataframe(
                query=query,
                performance=performance
            )
            
            if results.empty:
                return {
                    "success": False,
                    "error": "No data returned from Dune Analytics"
                }
            
            # Print column names for debugging
            print("\nColumns in result data:", results.columns.tolist())
            
            # Process the data
            processed_data = self.process_revenue_data(results)
            
            return {
                "success": True,
                "data": processed_data
            }
            
        except Exception as e:
            print(f"Error fetching Aave data: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def process_revenue_data(self, df: pd.DataFrame) -> Dict:
        """
        Process raw borrow volume data and calculate revenue metrics.
        
        Args:
            df: DataFrame containing borrow volume data
            
        Returns:
            Dictionary containing processed metrics
        """
        try:
            # Ensure we have the expected columns
            expected_columns = ['day', 'token', 'Deposits_volume', 'Borrow_volume']
            for col in expected_columns:
                if col not in df.columns:
                    raise ValueError(f"Expected column '{col}' not found in data. Available columns: {df.columns.tolist()}")
            
            # Print sample data for debugging
            print("\nSample data from Dune query:")
            print(df.head())
            print("\nData types:")
            print(df.dtypes)
            
            # Rename day column to date for consistency
            df = df.rename(columns={'day': 'date'})
            
            # Ensure date column is datetime
            df['date'] = pd.to_datetime(df['date'])
            
            # Convert volume columns to numeric, handling any non-numeric values
            df['Deposits_volume'] = pd.to_numeric(df['Deposits_volume'], errors='coerce').fillna(0)
            df['Borrow_volume'] = pd.to_numeric(df['Borrow_volume'], errors='coerce').fillna(0)
            
            # Sort by date
            df = df.sort_values('date')
            
            # Create monthly aggregations
            monthly_data = df.groupby([df['date'].dt.to_period('M'), 'token']).agg({
                'Deposits_volume': 'sum',
                'Borrow_volume': 'sum'
            }).reset_index()
            
            # Calculate monthly revenue (based on borrow volume)
            monthly_data['revenue'] = monthly_data['Borrow_volume'] * (self.AVERAGE_BORROW_APR / 12)
            
            # Calculate cumulative metrics
            monthly_data['cumulative_volume'] = monthly_data.groupby('token')['Borrow_volume'].cumsum()
            monthly_data['cumulative_revenue'] = monthly_data.groupby('token')['revenue'].cumsum()
            
            # Convert period to string for better serialization
            monthly_data['date'] = monthly_data['date'].astype(str)
            
            # Calculate totals across all tokens
            total_volume = float(monthly_data['Borrow_volume'].sum())
            total_revenue = float(monthly_data['revenue'].sum())
            avg_monthly_revenue = float(monthly_data.groupby('date')['revenue'].sum().mean())
            
            return {
                'monthly_metrics': monthly_data.to_dict('records'),
                'total_volume': total_volume,
                'total_revenue': total_revenue,
                'avg_monthly_revenue': avg_monthly_revenue,
                'months_analyzed': len(monthly_data['date'].unique()),
                'tokens_analyzed': len(monthly_data['token'].unique()),
                'date_range': {
                    'start': df['date'].min().strftime('%Y-%m-%d'),
                    'end': df['date'].max().strftime('%Y-%m-%d')
                }
            }
            
        except Exception as e:
            print(f"Error processing revenue data: {e}")
            import traceback
            traceback.print_exc()
            return None

def main():
    """Example usage of the AaveAnalyzer."""
    try:
        # Initialize analyzer
        analyzer = AaveAnalyzer()
        
        # Get revenue data with parameters
        print("Starting Aave Revenue Analysis...")
        print("================================")
        
        # Define parameters
        time_interval = 'day'  # Options: 'day', 'week', 'month'
        trading_days = 90      # Last 90 days of data
        
        results = analyzer.get_revenue_data(
            time_interval=time_interval,
            trading_days=trading_days
        )
        
        if results["success"]:
            data = results["data"]
            
            # Print summary
            print("\nAave Revenue Summary:")
            print("====================")
            print(f"Period Analyzed: {data['date_range']['start']} to {data['date_range']['end']}")
            print(f"Total Months: {data['months_analyzed']}")
            print(f"Total Tokens: {data['tokens_analyzed']}")
            print(f"Total Borrow Volume: ${data['total_volume']:,.2f}")
            print(f"Total Revenue: ${data['total_revenue']:,.2f}")
            print(f"Average Monthly Revenue: ${data['avg_monthly_revenue']:,.2f}")
            
            # Save detailed results to CSV
            monthly_df = pd.DataFrame(data['monthly_metrics'])
            monthly_df.to_csv('aave_monthly_revenue.csv', index=False)
            print("\nDetailed monthly data saved to 'aave_monthly_revenue.csv'")
            
        else:
            print(f"\nError: {results['error']}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()