# CryptoLens 🔍

A powerful analytics tool for tracking and analyzing cryptocurrency revenue data using 

1) Etherscan.io API
2) Solscan.io API
3) CoinGecko API
4) Blockchair API
5) Dune Analytics API
6) Coinmarketcap API
7) DefiLlama API
   

## Features

- Fetch and analyze cryptocurrency revenue data from Dune Analytics
- Process time-series data with daily, monthly, quarterly, and yearly aggregations
- Export results to CSV files for further analysis
- Automatic data caching and error handling
- Support for both fresh and cached query results

## Setup

1. Clone the repository:
```bash
git clone https://github.com/yourusername/CryptoLens.git
cd CryptoLens
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root with your Etherscan API key:
```
ETHERSCAN_API_KEY=your_api_key_here
```

## Usage

Run the main script:
```bash
python src/dune_query.py
```

The script will:
1. Fetch data from Dune Analytics
2. Process and aggregate the data by different time periods
3. Save results to CSV files:
   - `daily_revenue.csv`
   - `monthly_revenue.csv`
   - `quarterly_revenue.csv`
   - `yearly_revenue.csv`

## Output Files

The script generates four CSV files:
- `daily_revenue.csv`: Daily revenue data
- `monthly_revenue.csv`: Monthly aggregated data
- `quarterly_revenue.csv`: Quarterly aggregated data
- `yearly_revenue.csv`: Yearly aggregated data

## Requirements

- Python 3.7+
- dune-client
- pandas
- python-dotenv

## License

MIT License - feel free to use this project for any purpose.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
