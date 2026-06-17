import requests
import logging
from datetime import datetime, timezone

# Set up logging — this is how we'll see what's happening
# when the script runs without print statements everywhere
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# The coins we want to track — these are CoinGecko's coin IDs
COIN_IDS = [
    "bitcoin", "ethereum", "solana", "cardano", "ripple",
    "polkadot", "dogecoin", "avalanche-2", "chainlink", "polygon"
]

def fetch_crypto_data(api_url: str) -> list[dict]:
    """
    Fetches current market data for all tracked coins from CoinGecko.
    
    Returns a list of coin records, each enriched with a fetch timestamp.
    Returns an empty list if the API call fails — the producer handles this
    gracefully rather than crashing.
    """
    params = {
        'vs_currency' : 'usd',
        'ids' : ','.join(COIN_IDS),
        'order' : 'market_cap_desc',
        'per_page' : 50,
        'sparkline' : False,
        'price_change_percentage' : '1h,24h'
    }
    
    try:
        response = requests.get(
            api_url,
            params=params,
            timeout=10  # Don't wait more than 10s for a response
        )
        
        if not response.status_code == 200:
            logger.warning(f"CoinGecko API returned status code: {response.status_code}")
            return []
        
        coins = response.json()
        
        # Enrich each record with a fetch timestamp
        # This tells us exactly when WE fetched this data,
        # separate from CoinGecko's own last_updated field
        fetch_time = datetime.now(timezone.utc).isoformat()
        
        enriched = []
        for coin in coins:
            coin_dict = {
                'id': coin['id'],
                'symbol': coin['symbol'],
                'name': coin['name'],
                'current_price': coin['current_price'],
                'market_cap': coin['market_cap'],
                'market_cap_rank': coin['market_cap_rank'],
                'total_volume': coin['total_volume'],
                'high_24h': coin['high_24h'],
                'low_24h': coin['low_24h'],
                'price_change_24h': coin['price_change_24h'],
                'price_change_percentage_24h': coin['price_change_percentage_24h'],
                'price_change_percentage_1h': coin.get('price_change_percentage_1h_in_currency'),
                'circulating_supply' : coin['circulating_supply'],
                'last_updated' : coin['last_updated'],
                'fetch_timestamp' : fetch_time
            }
            enriched.append(coin_dict)
        
        logger.info(f"Fetched {len(enriched)} coins from CoinGecko")
        return enriched
        
    except requests.exceptions.Timeout:
        logger.warning("CoinGecko API request timed out")
        return []
    except requests.exceptions.ConnectionError:
        logger.warning("Failed to connect to CoinGecko API")
        return []
    except Exception as e:
        logger.error(f"Unexpected error fetching data: {e}")
        return []