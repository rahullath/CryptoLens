# Configuration for the Crypto Revenue Analyzer

# List of protocols to analyze
PROTOCOLS = [
    {
        "name": "Aave",
        "slug": "aave",
        "defillama_id": "aave",
        "chains": ["ethereum", "polygon", "avalanche", "optimism", "arbitrum"],
        "token_type": "governance"
    },
    {
        "name": "LIDO",
        "slug": "lido",
        "defillama_id": "lido",
        "chains": ["ethereum"],
        "token_type": "governance"
    },
    {
        "name": "Eigen",
        "slug": "eigen",
        "defillama_id": "eigenlayer",
        "chains": ["ethereum"],
        "token_type": "governance"
    },
    {
        "name": "MKR",
        "slug": "maker",
        "defillama_id": "makerdao",
        "chains": ["ethereum"],
        "token_type": "governance"
    },
    {
        "name": "Compound",
        "slug": "compound",
        "defillama_id": "compound",
        "chains": ["ethereum"],
        "token_type": "governance"
    },
    {
        "name": "Fluid",
        "slug": "fluid",
        "defillama_id": "fluid", 
        "chains": ["ethereum"],
        "token_type": "utility"
    },
    {
        "name": "Jupiter",
        "slug": "jupiter",
        "defillama_id": "jupiter",
        "chains": ["solana"],
        "token_type": "governance"
    },
    {
        "name": "Sonic",
        "slug": "sonic",
        "defillama_id": "sonic",
        "chains": ["sui"],
        "token_type": "governance"
    }
]

# Blockchain networks to track for revenue contributions
NETWORKS = ["ethereum", "solana", "sui"]

# API Endpoints
DEFILLAMA_BASE_URL = "https://api.llama.fi"
DEFILLAMA_FEES_URL = f"{DEFILLAMA_BASE_URL}/overview/fees"
DEFILLAMA_PROTOCOL_URL = f"{DEFILLAMA_BASE_URL}/protocol"

# CoinGecko API for market cap data
COINGECKO_BASE_URL = "https://api.coingecko.com/api/v3"
COINGECKO_COINS_URL = f"{COINGECKO_BASE_URL}/coins"

# Time periods for data collection
TIME_PERIODS = {
    "daily": 86400,
    "weekly": 604800,
    "monthly": 2592000,
    "quarterly": 7776000,
    "yearly": 31536000
}
