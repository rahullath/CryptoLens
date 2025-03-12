"""
Script to check the parameters for the Aave Dune query.
"""
import os
from dotenv import load_dotenv
from dune_client.client import DuneClient
from dune_client.query import QueryBase

# Load environment variables
load_dotenv()

def check_query_parameters():
    """
    Check the parameters for the Aave Dune query.
    """
    # Get API key
    dune_api_key = os.getenv("DUNE_API_KEY")
    if not dune_api_key:
        print("DUNE_API_KEY not found in environment variables")
        return
    
    # Initialize client
    dune = DuneClient(dune_api_key)
    
    # Query ID for Aave
    query_id = 3237115
    
    try:
        # Get query details
        print(f"Fetching details for Dune query {query_id}...")
        query = dune.get_query(query_id)
        
        # Print query information
        print(f"\nQuery Name: {query.name}")
        print(f"Query Description: {query.description}")
        
        # Check parameters
        print("\nParameters:")
        for param in query.parameters:
            print(f"- {param.name} (Type: {param.type})")
            if hasattr(param, 'options') and param.options:
                print(f"  Options: {', '.join(param.options)}")
            if hasattr(param, 'default') and param.default:
                print(f"  Default: {param.default}")
    
    except Exception as e:
        print(f"Error checking query parameters: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_query_parameters()
