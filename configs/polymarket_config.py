"""
Configuration file for Polymarket data collection.
"""

# Keywords to filter crypto-related events
CRYPTO_KEYWORDS = [
    # Major cryptocurrencies
    'bitcoin', 'btc',
    'ethereum', 'eth',
    'solana', 'sol',
    'cardano', 'ada',
    'polkadot', 'dot',
    'avalanche', 'avax',
    'polygon', 'matic',
    'chainlink', 'link',
    'uniswap', 'uni',
    'ripple', 'xrp',
    'dogecoin', 'doge',
    'shiba', 'shib',
    'litecoin', 'ltc',
    'cosmos', 'atom',
    'algorand', 'algo',
    'tezos', 'xtz',
    'monero', 'xmr',
    'stellar', 'xlm',
    
    # General crypto terms
    'crypto', 'cryptocurrency',
    'defi', 'nft', 'nfts',
    'blockchain',
    'token', 'coin',
    'altcoin',
    'stablecoin',
    'memecoin',
    
    # Platforms and protocols
    'binance', 'bnb',
    'coinbase',
    'ftx',
    'opensea',
    'metamask',
    'uniswap',
    'aave',
    'compound',
    'synthetix',
]

# Slug prefixes for 15-minute events
FIFTEEN_MINUTE_EVENTS_SLUG_PREFIXES = [
    "btc-updown-15m-",  # Bitcoin Up or Down
    "eth-updown-15m-",  # Ethereum Up or Down
    "sol-updown-15m-",  # Solana Up or Down
    "xrp-updown-15m-",  # XRP Up or Down
]

# Slug prefixes to crypto name dictionary
SLUG_PREFIX_TO_CRYPTO_NAME = {
    "btc-updown-15m-": "BTC",
    "eth-updown-15m-": "ETH",
    "sol-updown-15m-": "SOL",
    "xrp-updown-15m-": "XRP"
}

# Gamma API configuration
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"
DEFAULT_LIMIT = 50
DEFAULT_MAX_EVENTS = None  # None means fetch all

# CLOB API configuration
CLOB_API_BASE_URL = "https://clob.polymarket.com"
MARKET_CHANNEL = "market"
USER_CHANNEL = "user"
WEBSOCKET_URL = "wss://ws-subscriptions-clob.polymarket.com"
WEBSOCKET_PING_INTERVAL = 5  # seconds
