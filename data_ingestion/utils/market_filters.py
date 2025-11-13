from typing import Dict, Any
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from configs.polymarket_config import CRYPTO_KEYWORDS


def is_crypto_market(market: Dict[str, Any]) -> bool:
    """
    Determine if a given market is related to cryptocurrency.
    
    Args:
        market (dict): A market event dictionary from Polymarket API.
        
    Returns:
        bool: True if the market is crypto-related, False otherwise.
    """
    title = market.get('title', '').lower()
    return any(keyword in title for keyword in CRYPTO_KEYWORDS)
