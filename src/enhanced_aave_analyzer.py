"""
Enhanced Aave Analyzer with CoinMarketCap token statistics integration.
This script combines the functionality of aave_analyzer.py with token market data.
"""
import os
import pandas as pd
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dotenv import load_dotenv
from dune_client.types import QueryParameter
from dune_client.client import DuneClient
from dune_client.query import QueryBase
import token_stats

# Load environment variables
load_dotenv()

class EnhancedAaveAnalyzer:
    """
    Enhanced analyzer for Aave protocol revenue data with token market statistics.
    """
    AAVE_QUERY_ID = 3237115
    AVERAGE_BORROW_APR = 0.03  # 3% average borrow APR
    
    # Common Aave tokens
    AAVE_TOKENS = ["AAVE", "USDC", "USDT", "DAI", "WETH", "WBTC", "LINK", "WMATIC", "WAVAX"]
    
    def __init__(self):
        """
        Initialize the Aave analyzer.
        """
        self.dune_api_key = os.getenv("DUNE_API_KEY")
        if not self.dune_api_key:
            raise ValueError("DUNE_API_KEY not found in environment variables")
        
        self.dune = DuneClient(self.dune_api_key)
    
    def get_revenue_data(self, time_interval: str, trading_days: int) -> pd.DataFrame:
        """
        Fetch Aave borrow volume data from Dune Analytics.
        
        Args:
            time_interval: Time interval for query (e.g., "90 days", "1 year")
            trading_days: Number of trading days to analyze
            
        Returns:
            DataFrame containing borrow volume data
        """
        try:
            print(f"Executing Dune query {self.AAVE_QUERY_ID} with parameters...")
            
            # Create query object with parameters
            # Use "90d" instead of "90 days" for the Time interval parameter
            # Common enum values are likely "30d", "90d", "180d", "1y", etc.
            time_interval_value = "90d"  # Default to 90 days
            
            if time_interval == "30 days":
                time_interval_value = "30d"
            elif time_interval == "180 days" or time_interval == "6 months":
                time_interval_value = "180d"
            elif time_interval == "1 year" or time_interval == "365 days":
                time_interval_value = "1y"
            
            print(f"Using time interval value: {time_interval_value}")
            
            query = QueryBase(
                query_id=self.AAVE_QUERY_ID,
                params=[
                    QueryParameter.text_type(name="Time interval", value=time_interval_value),
                    QueryParameter.number_type(name="Trading Num Days", value=trading_days)
                ]
            )
            
            # Execute query and get results
            results = self.dune.run_query(query)
            
            if not results or not hasattr(results, 'result') or not results.result:
                print("No results returned from Dune query")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(results.result.rows)
            return df
            
        except Exception as e:
            print(f"Error fetching revenue data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
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
    
    def print_summary(self, results: Dict):
        """
        Print a summary of the analysis results.
        
        Args:
            results: Dictionary containing analysis results
        """
        print("\nAave Revenue Summary:")
        print("====================")
        
        if not results:
            print("Unexpected error: No results available")
            return
        
        try:
            # Revenue metrics
            date_range = results.get('date_range', {})
            print(f"Period Analyzed: {date_range.get('start')} to {date_range.get('end')}")
            print(f"Total Months: {results.get('months_analyzed')}")
            print(f"Total Tokens: {results.get('tokens_analyzed')}")
            print(f"Total Borrow Volume: ${results.get('total_volume', 0):,.2f}")
            print(f"Total Revenue: ${results.get('total_revenue', 0):,.2f}")
            print(f"Average Monthly Revenue: ${results.get('avg_monthly_revenue', 0):,.2f}")
        
        except Exception as e:
            print(f"Error printing summary: {e}")
    
    def analyze_aave_revenue(self, time_interval: str = "90 days", trading_days: int = 90) -> Dict:
        """
        Analyze Aave revenue data and token market statistics.
        
        Args:
            time_interval: Time interval for analysis (e.g., "90 days", "1 year")
            trading_days: Number of trading days to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        print("Starting Enhanced Aave Revenue Analysis...")
        print("=========================================")
        
        print(f"Fetching Aave borrow volume data for the last {time_interval}...")
        df = self.get_revenue_data(time_interval, trading_days)
        
        if df is None or df.empty:
            print("No data available for analysis")
            return None
        
        # Process revenue data
        results = self.process_revenue_data(df)
        
        if results:
            # Print revenue summary
            self.print_summary(results)
            
            # Save monthly data to CSV
            if 'monthly_metrics' in results:
                pd.DataFrame(results['monthly_metrics']).to_csv('aave_monthly_revenue.csv', index=False)
                print("Detailed monthly data saved to 'aave_monthly_revenue.csv'")
            
            # Fetch token market data
            print("\nFetching token market data from CoinMarketCap...")
            token_stats_result = token_stats.analyze_protocol_tokens("Aave", self.AAVE_TOKENS)
            
            if token_stats_result:
                # Add token stats to results
                results['token_stats'] = token_stats_result
                
                # Create a combined analysis report
                self.create_combined_report(results)
        
        return results
    
    def create_combined_report(self, results: Dict):
        """
        Create a combined analysis report with revenue and token statistics.
        
        Args:
            results: Dictionary containing analysis results
        """
        try:
            # Extract data
            revenue_data = {
                'total_volume': results.get('total_volume', 0),
                'total_revenue': results.get('total_revenue', 0),
                'avg_monthly_revenue': results.get('avg_monthly_revenue', 0),
                'months_analyzed': results.get('months_analyzed', 0),
                'tokens_analyzed': results.get('tokens_analyzed', 0),
                'date_range': results.get('date_range', {})
            }
            
            token_stats_data = results.get('token_stats', {})
            
            # Create combined report
            combined_report = {
                'revenue_analysis': revenue_data,
                'token_analysis': token_stats_data,
                'timestamp': datetime.now().isoformat(),
                'protocol': 'Aave'
            }
            
            # Save to file
            with open('aave_combined_analysis.json', 'w') as f:
                json.dump(combined_report, f, indent=2)
            print("Combined analysis report saved to 'aave_combined_analysis.json'")
            
            # Create a summary DataFrame for the report
            if 'monthly_metrics' in results and token_stats_data and 'tokens' in token_stats_data:
                # Get the most recent month's data
                monthly_df = pd.DataFrame(results['monthly_metrics'])
                monthly_df['date'] = pd.to_datetime(monthly_df['date'].astype(str))
                latest_month = monthly_df['date'].max()
                latest_data = monthly_df[monthly_df['date'] == latest_month]
                
                # Get token market data
                token_market_data = token_stats_data['tokens']
                
                # Create combined data
                combined_data = []
                for _, row in latest_data.iterrows():
                    token = row['token']
                    market_data = token_market_data.get(token, {})
                    
                    combined_data.append({
                        'Token': token,
                        'Borrow Volume': row['Borrow_volume'],
                        'Monthly Revenue': row['revenue'],
                        'Market Cap': market_data.get('market_cap', 0),
                        'Price (USD)': market_data.get('price_usd', 0),
                        '24h Change (%)': market_data.get('percent_change_24h', 0),
                        'Revenue to Market Cap Ratio': row['revenue'] / market_data.get('market_cap', 1) * 100 if market_data.get('market_cap', 0) > 0 else 0
                    })
                
                # Create and save DataFrame
                combined_df = pd.DataFrame(combined_data)
                combined_df.to_csv('aave_token_revenue_metrics.csv', index=False)
                print("Token revenue metrics saved to 'aave_token_revenue_metrics.csv'")
                
                # Print top tokens by revenue to market cap ratio
                print("\nTop Tokens by Revenue to Market Cap Ratio:")
                sorted_data = sorted(combined_data, key=lambda x: x['Revenue to Market Cap Ratio'], reverse=True)
                for i, data in enumerate(sorted_data[:5], 1):
                    print(f"{i}. {data['Token']}: {data['Revenue to Market Cap Ratio']:.6f}% (Revenue: ${data['Monthly Revenue']:,.2f}, Market Cap: ${data['Market Cap']:,.2f})")
        
        except Exception as e:
            print(f"Error creating combined report: {e}")
            import traceback
            traceback.print_exc()


def main():
    """
    Main function to run the enhanced Aave analyzer.
    """
    analyzer = EnhancedAaveAnalyzer()
    analyzer.analyze_aave_revenue()


if __name__ == "__main__":
    main()
