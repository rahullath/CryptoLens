# Crypto Revenue Analyzer - Usage Guide

This guide explains how to use the Crypto Revenue Analyzer to collect and analyze revenue data for DeFi protocols.

## Setup

1. **Install Dependencies**

   ```
   pip install -r requirements.txt
   ```

2. **Configure API Keys**

   - Copy `.env.example` to `.env`
   - Add your Blockchair API key to the `.env` file:
     ```
     BLOCKCHAIR_API_KEY=your_blockchair_api_key_here
     ```

## Running the Analyzer

### Basic Usage

Run the main script to collect data and generate visualizations:

```
python src/main.py
```

This will:
1. Collect protocol data from CoinGecko and DeFi Llama
2. Collect blockchain data using Blockchair API
3. Generate protocol comparison table and revenue bubble map

### Advanced Options

You can customize the analysis with command-line arguments:

```
python src/main.py --start-date 2024-01-01 --end-date 2024-03-01
```

Available options:
- `--start-date`: Start date for data collection (YYYY-MM-DD)
- `--end-date`: End date for data collection (YYYY-MM-DD)
- `--skip-collection`: Skip data collection and use existing data
- `--skip-visualization`: Skip visualization creation

## Understanding the Results

### Protocol Comparison Table

The protocol comparison table (`visualizations/protocol_comparison.html`) shows:
- Market Cap
- Annual Revenue
- QoQ Revenue Growth
- Sustainability Score
- Token Type
- Overall Rating

### Revenue Bubble Map

The revenue bubble map (`visualizations/revenue_bubble_map.html`) visualizes:
- Which protocols contribute to revenue on each blockchain
- The size of each bubble represents the annual revenue
- Hovering over a bubble shows the exact revenue amount and percentage contribution

## Direct Blockchain Analysis

For the most accurate data, the analyzer uses direct blockchain analysis through Blockchair:

1. **Ethereum Protocols (Lido, Aave, Compound, Maker, EigenLayer)**
   - Analyzes contract transactions and events
   - Calculates revenue based on protocol-specific logic

2. **Solana Protocols (Jupiter, Fluid)**
   - Analyzes account transactions
   - Calculates revenue from swap fees and other sources

3. **Sui Protocols (Sonic)**
   - Currently limited support through Blockchair
   - May require manual data input for the most accurate results

## Customizing the Analysis

To add or modify protocols, edit the `PROTOCOLS` list in `src/config.py`.

To change how revenue is calculated for a specific protocol, modify the `_calculate_protocol_revenue` method in `src/blockchair_collector.py`.

## Troubleshooting

If you encounter rate limiting with the Blockchair API, try:
1. Reducing the date range
2. Using the `--skip-collection` flag to work with existing data
3. Upgrading to a higher tier Blockchair API plan

## Data Verification

Always verify the collected data against multiple sources:
1. Protocol documentation
2. Community sources
3. Official financial reports (if available)
