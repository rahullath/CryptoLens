"""
Dune Analytics Query Implementation
This module handles fetching and processing data from Dune Analytics queries.
"""

import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import pandas as pd

# Load environment variables
load_dotenv()

class DuneQueryManager:
    def __init__(self):
        self.api_key = os.getenv("DUNE_API_KEY")
        if not self.api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")
        
        # Initialize Dune client with a 5-minute timeout
        self.client = DuneClient(
            api_key=self.api_key,
            request_timeout=300  # 5 minutes timeout
        )
    
    def get_fresh_results(self, query_id: int, performance: str = "medium"):
        """
        Execute query and get fresh results from Dune Analytics.
        
        Args:
            query_id: The Dune query ID to execute
            performance: Query execution performance ('medium' or 'large')
            
        Returns:
            DataFrame containing the query results
        """
        try:
            print(f"Executing Dune query {query_id}...")
            
            # Create query object
            query = QueryBase(query_id=query_id)
            
            # Execute query and get results as DataFrame
            results = self.client.run_query_dataframe(
                query=query,
                performance=performance
            )
            
            # Rename columns to match our expected format
            if 'ETH received' in results.columns and 'ETH received cumul' in results.columns:
                results = results.rename(columns={
                    'ETH received': 'eth_received',
                    'ETH received cumul': 'eth_received_cumul'
                })
            
            return {
                "success": True,
                "data": results
            }
            
        except Exception as e:
            print(f"Error executing Dune query: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    def get_cached_results(self, query_id: int):
        """
        Get latest cached results from Dune Analytics without executing the query.
        
        Args:
            query_id: The Dune query ID to fetch results for
            
        Returns:
            DataFrame containing the query results
        """
        try:
            print(f"Fetching latest cached results for query {query_id}...")
            
            # Get latest results as DataFrame
            results = self.client.get_latest_result_dataframe(query_id)
            
            # Rename columns to match our expected format
            if 'ETH received' in results.columns and 'ETH received cumul' in results.columns:
                results = results.rename(columns={
                    'ETH received': 'eth_received',
                    'ETH received cumul': 'eth_received_cumul'
                })
            
            return {
                "success": True,
                "data": results
            }
            
        except Exception as e:
            print(f"Error fetching cached results: {e}")
            return {
                "success": False,
                "error": str(e)
            }

    def process_time_series_data(self, df):
        """
        Process time series data and create monthly, quarterly, and yearly aggregations.
        
        Args:
            df: DataFrame with 'day' column and numerical columns to aggregate
            
        Returns:
            Dictionary containing different time period aggregations
        """
        try:
            # Print column names for debugging
            print("\nAvailable columns:", df.columns.tolist())
            
            # Ensure day column is datetime
            df['day'] = pd.to_datetime(df['day'])
            
            # Sort by date
            df = df.sort_values('day')
            
            # Create time period columns
            df['month'] = df['day'].dt.to_period('M')
            df['quarter'] = df['day'].dt.to_period('Q')
            df['year'] = df['day'].dt.to_period('Y')
            
            # Check if we need to rename columns
            eth_received_col = 'eth_received' if 'eth_received' in df.columns else 'ETH received'
            eth_cumul_col = 'eth_received_cumul' if 'eth_received_cumul' in df.columns else 'ETH received cumul'
            
            # Monthly aggregation
            monthly_data = df.groupby('month').agg({
                eth_received_col: 'sum',
                eth_cumul_col: 'last'
            }).reset_index()
            monthly_data['month'] = monthly_data['month'].astype(str)
            
            # Quarterly aggregation
            quarterly_data = df.groupby('quarter').agg({
                eth_received_col: 'sum',
                eth_cumul_col: 'last'
            }).reset_index()
            quarterly_data['quarter'] = quarterly_data['quarter'].astype(str)
            
            # Yearly aggregation
            yearly_data = df.groupby('year').agg({
                eth_received_col: 'sum',
                eth_cumul_col: 'last'
            }).reset_index()
            yearly_data['year'] = yearly_data['year'].astype(str)
            
            # Rename columns in aggregated data for consistency
            monthly_data = monthly_data.rename(columns={
                eth_received_col: 'eth_received',
                eth_cumul_col: 'eth_received_cumul'
            })
            quarterly_data = quarterly_data.rename(columns={
                eth_received_col: 'eth_received',
                eth_cumul_col: 'eth_received_cumul'
            })
            yearly_data = yearly_data.rename(columns={
                eth_received_col: 'eth_received',
                eth_cumul_col: 'eth_received_cumul'
            })
            
            return {
                'daily': df,
                'monthly': monthly_data,
                'quarterly': quarterly_data,
                'yearly': yearly_data
            }
            
        except Exception as e:
            print(f"Error processing time series data: {e}")
            return None

def main():
    """Example usage of the DuneQueryManager."""
    try:
        # Initialize the query manager
        dune_manager = DuneQueryManager()
        
        # Your query ID
        QUERY_ID = 1273933
        
        # Get fresh results
        print("\nGetting fresh results...")
        fresh_results = dune_manager.get_fresh_results(QUERY_ID)
        
        if fresh_results["success"]:
            df = fresh_results["data"]
            print("\nProcessing all data rows...")
            
            # Process time series data
            results = dune_manager.process_time_series_data(df)
            
            if results:
                # Print daily data summary
                print("\nDaily Data Summary:")
                print("==================")
                print(f"Total number of days: {len(results['daily'])}")
                print(f"Date range: {results['daily']['day'].min()} to {results['daily']['day'].max()}")
                print(f"Total ETH received: {results['daily']['eth_received'].sum():.4f}")
                
                # Print monthly summary
                print("\nMonthly Summary:")
                print("===============")
                print(results['monthly'].to_string(index=False))
                
                # Print quarterly summary
                print("\nQuarterly Summary:")
                print("=================")
                print(results['quarterly'].to_string(index=False))
                
                # Print yearly summary
                print("\nYearly Summary:")
                print("==============")
                print(results['yearly'].to_string(index=False))
                
                # Save results to CSV files
                print("\nSaving results to CSV files...")
                results['daily'].to_csv('daily_revenue.csv', index=False)
                results['monthly'].to_csv('monthly_revenue.csv', index=False)
                results['quarterly'].to_csv('quarterly_revenue.csv', index=False)
                results['yearly'].to_csv('yearly_revenue.csv', index=False)
                print("Data saved successfully!")
                
            else:
                print("Error processing time series data")
        else:
            print(f"\nError getting fresh results: {fresh_results['error']}")
            
            # Fallback to cached results
            print("\nFalling back to cached results...")
            cached_results = dune_manager.get_cached_results(QUERY_ID)
            
            if cached_results["success"]:
                df = cached_results["data"]
                results = dune_manager.process_time_series_data(df)
                # ... (same processing as above)
            else:
                print(f"\nError getting cached results: {cached_results['error']}")
        
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main() 