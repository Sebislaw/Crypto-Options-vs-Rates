import requests
from typing import Dict, Any, List
import sys
from pathlib import Path
import json

parent_dir = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(parent_dir))

from configs.polymarket_config import (
    DEFAULT_LIMIT,
    GAMMA_API_BASE_URL,
    FIFTEEN_MINUTE_EVENTS_SLUG_PREFIXES
)
from data_ingestion.utils.time_utils import current_quarter_timestamp_et


def get_polymarket_events(
    limit: int = DEFAULT_LIMIT,
    closed: bool = None,
    max_events: int = None,
    paginate: bool = True
) -> List[Dict[str, Any]]:
    """
    Fetch Polymarket events from the Gamma API.
    
    Args:
        limit: Number of events to fetch per request (default: from config)
        closed: Filter by closed markets (True/False/None for all)
        max_events: Maximum number of events to fetch (None for all)
        paginate: If True, fetch all pages; if False, fetch only first page
    
    Returns:
        list: List of events dictionaries
    """
    url = f"{GAMMA_API_BASE_URL}/events"
    all_events = []
    offset = 0
    
    while True:
        params = {
            'limit': limit,
            'offset': offset
        }
        
        if closed is not None:
            params['closed'] = str(closed).lower()
        
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error for bad status codes
        
        events = response.json()
        
        if not events:
            break
        
        all_events.extend(events)
        
        if max_events and len(all_events) >= max_events:
            all_events = all_events[:max_events]
            break
        
        # If paginate is False, only fetch the first page
        if not paginate:
            break
        
        # If we got fewer events than the limit, we've reached the end
        if len(events) < limit:
            break
        
        offset += limit
        print(f"Fetched {len(all_events)} events so far...")
    
    return all_events


def get_current_15m_events() -> List[Dict[str, Any]]:
    """
    Fetch current 15-minute crypto events from Polymarket.
    
    Returns:
        list: List of current 15-minute event dictionaries.
    """

    url_base = GAMMA_API_BASE_URL + "/events/slug/"
    result = {}
    for slug_prefix in FIFTEEN_MINUTE_EVENTS_SLUG_PREFIXES:
        url = url_base + slug_prefix + str(current_quarter_timestamp_et())
        response = requests.get(url)
        result[slug_prefix[0:3]] = response.json()
    return result


def get_club_token_ids_from_15m_events() -> List[str]:
    """
    Extract CLOB token IDs from current 15-minute events.
    
    Returns:
        dict: Dictionary mapping crypto symbol to list of CLOB token IDs.
    """
    fifteen_minute_events = get_current_15m_events()
    clob_token_ids = {}
    
    for crypto in 'btc', 'eth', 'sol', 'xrp':
        # Parse the JSON string to get actual list
        token_ids_str = fifteen_minute_events[crypto]['markets'][0]['clobTokenIds']
        clob_token_ids[crypto] = json.loads(token_ids_str)
    return clob_token_ids
