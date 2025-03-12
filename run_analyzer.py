"""
Run Script for Crypto Revenue Analyzer

This script provides a simplified way to run the crypto revenue analyzer
with improved error handling and configuration options.
"""

import os
import sys
import time
import subprocess
from datetime import datetime, timedelta

def main():
    """Main function to run the analyzer with improved error handling."""
    print("Crypto Revenue Analyzer Runner")
    print("==============================")
    
    # Check if .env file exists
    if not os.path.exists(".env"):
        print("Error: .env file not found. Please create it from .env.example")
        print("Creating .env file from .env.example...")
        try:
            with open(".env.example", "r") as src:
                with open(".env", "w") as dst:
                    dst.write(src.read())
            print("Created .env file. Please edit it with your API keys.")
        except Exception as e:
            print(f"Error creating .env file: {e}")
            return
    
    # Create necessary directories
    for directory in ["data", "visualizations"]:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f"Created {directory} directory")
    
    # Ask for collection mode
    print("\nCollection Mode:")
    print("1. Full collection (all sources)")
    print("2. Solana only (using Solscan)")
    print("3. Skip collection (use existing data)")
    
    mode = input("Select mode (1-3) [default: 1]: ").strip() or "1"
    
    # Set command arguments
    cmd_args = [sys.executable, "src/main.py"]
    
    if mode == "2":
        cmd_args.append("--solana-only")
    elif mode == "3":
        cmd_args.append("--skip-collection")
    
    # Ask for date range
    use_custom_dates = input("\nUse custom date range? (y/n) [default: n]: ").lower().strip() == "y"
    
    if use_custom_dates:
        default_start = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
        default_end = datetime.now().strftime("%Y-%m-%d")
        
        start_date = input(f"Start date (YYYY-MM-DD) [default: {default_start}]: ").strip() or default_start
        end_date = input(f"End date (YYYY-MM-DD) [default: {default_end}]: ").strip() or default_end
        
        cmd_args.extend(["--start-date", start_date, "--end-date", end_date])
    
    # Run the command
    print("\nRunning crypto revenue analyzer...")
    print(f"Command: {' '.join(cmd_args)}")
    print("\n" + "="*50 + "\n")
    
    try:
        process = subprocess.Popen(
            cmd_args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Print output in real-time
        for line in iter(process.stdout.readline, ''):
            print(line, end='')
        
        process.stdout.close()
        return_code = process.wait()
        
        if return_code != 0:
            print(f"\nAnalyzer exited with error code {return_code}")
        else:
            print("\nAnalyzer completed successfully!")
            
            # Check if visualization files exist
            vis_files = [
                "visualizations/protocol_comparison.html",
                "visualizations/revenue_bubble_map.html"
            ]
            
            missing_files = [f for f in vis_files if not os.path.exists(f)]
            
            if missing_files:
                print("\nWarning: Some visualization files were not created:")
                for f in missing_files:
                    print(f"  - {f}")
                print("\nThis might be due to missing or incomplete data.")
            else:
                print("\nAll visualization files were created successfully!")
                
    except Exception as e:
        print(f"\nError running analyzer: {e}")

if __name__ == "__main__":
    main()
