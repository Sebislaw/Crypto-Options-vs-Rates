# Polymarket 15-Minute Crypto Options WebSocket Streamer

## Overview

This module provides **real-time WebSocket streaming of Polymarket's 15-minute crypto "Up or Down" options markets**. The main functionality is a self-reconnecting WebSocket client that automatically tracks the current 15-minute window and updates when new markets become available.

### Supported Cryptocurrencies
- **BTC** (Bitcoin)
- **ETH** (Ethereum)
- **SOL** (Solana)
- **XRP** (Ripple)

---

## Quick Start

### Run the WebSocket streamer for a specific crypto:

```python
from ingestion_layer.polymarket.polymarket_clob import start_market_websocket_connection

# Start streaming BTC market data
start_market_websocket_connection('btc')
```

Or from command line:
```bash
python ingestion_layer/polymarket/polymarket_clob.py
```

**What it does:**
1. Determines the current 15-minute timestamp (e.g., 12:00 PM, 12:15 PM, 12:30 PM, etc.)
2. Fetches the current 15-minute event for your chosen crypto from Polymarket's Gamma API
3. Extracts the CLOB token IDs needed to subscribe to the WebSocket
4. Connects to Polymarket's CLOB WebSocket and streams live orderbook updates
5. **Automatically disconnects and reconnects when the next 15-minute window starts** with new market data

---

## How It Works

### Architecture Flow

```
User starts connection for 'btc'
        ↓
current_quarter_timestamp_et() → Calculate current 15-min timestamp (e.g., 1767200400)
        ↓
get_current_15m_events() → Fetch event from Gamma API using slug "btc-updown-15m-1767200400"
        ↓
get_club_token_ids_from_15m_events() → Extract CLOB token IDs from event
        ↓
WebSocketOrderBook → Connect to CLOB WebSocket with token IDs
        ↓
Stream orderbook updates until 15-min window ends
        ↓
Auto-disconnect and restart loop with new timestamp
```

### Key Components

#### 1. **`polymarket_clob.py`** (Main Entry Point)
- **`start_market_websocket_connection(crypto_name)`**: Main function that runs the continuous streaming loop
- **`WebSocketOrderBook`**: WebSocket client that:
  - Subscribes to market updates for specific token IDs
  - Sends periodic pings to keep connection alive
  - Monitors the current 15-minute window and auto-reconnects when it changes

#### 2. **`polymarket_gamma.py`** (Market Data Fetching)
- **`get_current_15m_events()`**: Fetches the current 15-minute event for all supported cryptos
- **`get_club_token_ids_from_15m_events()`**: Extracts and parses CLOB token IDs from events
- **`get_polymarket_events()`**: General function to fetch any Polymarket events (used for exploration)

#### 3. **`utils/time_utils.py`** (Timestamp Management)
- **`current_quarter_timestamp_et()`**: Returns Unix timestamp for the start of the current 15-minute block (aligned to Eastern Time)
  - Examples: If current time is 12:07 PM ET → returns timestamp for 12:00 PM
  - If current time is 12:18 PM ET → returns timestamp for 12:15 PM

#### 4. **`configs/polymarket_config.py`** (Configuration)
- API endpoints, WebSocket URLs, supported crypto slugs, and connection settings

---

## Understanding 15-Minute Events

Polymarket offers "Up or Down" binary options for crypto prices in 15-minute windows:
- **Question**: "Will Bitcoin be UP or DOWN from 12:00 PM to 12:15 PM ET?"
- **Outcomes**: ["Up", "Down"]
- **Resolution**: Based on Chainlink BTC/USD price feed

Each event has:
- A unique **slug** (e.g., `btc-updown-15m-1767200400`)
- **CLOB token IDs** that identify the specific orderbook for trading
- Market metadata (volume, liquidity, current prices, etc.)

---

### 1. Stream Live Orderbook Data (Main Use Case)

```python
from ingestion_layer.polymarket.polymarket_clob import start_market_websocket_connection

# Stream Ethereum market
start_market_websocket_connection('eth')
```

**Output:**
```
============================================================
Fetching current 15-minute event tokens for ETH...
Found 1 token IDs for ETH
============================================================

Connected to market with 1 token IDs at timestamp 1767200400
Next 15-minute window starts in 874 seconds
{"asset_id": "37283594079074919903126935096138570848920072247779153380455704888017339871157", "event_type": "price_change", "price": "0.52", ...}
...
```

### 2. Get Current 15-Minute Timestamp

```python
from ingestion_layer.polymarket.utils.time_utils import current_quarter_timestamp_et

timestamp = current_quarter_timestamp_et()
print(timestamp)  # e.g., 1767200400 (represents 12:00 PM ET as Unix timestamp)
```

### 3. Fetch Current 15-Minute Events for All Cryptos

```python
from ingestion_layer.polymarket.polymarket_gamma import get_current_15m_events

events = get_current_15m_events()
print(events.keys())  # dict_keys(['btc', 'eth', 'sol', 'xrp'])

# Access BTC event details
btc_event = events['btc']
print(btc_event['title'])      # "Bitcoin Up or Down - December 31, 12:00PM-12:15PM ET"
print(btc_event['slug'])       # "btc-updown-15m-1767200400"
print(btc_event['volume'])     # "92509.470055"
print(btc_event['liquidity'])  # "20579.7452"
```

### 4. Extract CLOB Token IDs

```python
from ingestion_layer.polymarket.polymarket_gamma import get_club_token_ids_from_15m_events

token_ids = get_club_token_ids_from_15m_events()
print(token_ids)
```

**Output:**
```python
{
  'btc': ['8851848546962572193739008211889046036776198486765606333032192644001246174440', 
          '10181859819235412241349111952383458602722012323104397550162049365057126390549'],
  'eth': ['37283594079074919903126935096138570848920072247779153380455704888017339871157', 
          '16136687713760634223563054086318388951167402190497275532808513193239467005933'],
  'sol': [...],
  'xrp': [...]
}
```

### 5. Explore Other Polymarket Events (Optional)

```python
from ingestion_layer.polymarket.polymarket_gamma import get_polymarket_events

# Fetch first 100 active events
events = get_polymarket_events(limit=100, closed=False, paginate=False)
for event in events[:5]:
    print(f"{event['title']} - Volume: {event['volume']}")
```

---

## API Reference

### Main Functions

#### `start_market_websocket_connection(crypto_name: str)`
**Location:** `polymarket_clob.py`

Starts a self-reconnecting WebSocket stream for the specified crypto.

- **Parameters:**
  - `crypto_name` (str): One of `'btc'`, `'eth'`, `'sol'`, `'xrp'`
- **Behavior:** Runs indefinitely, auto-reconnecting every 15 minutes

---

#### `get_current_15m_events() -> Dict[str, Any]`
**Location:** `polymarket_gamma.py`

Fetches the current 15-minute "Up or Down" events for all supported cryptos from the Gamma API.

- **Returns:** Dictionary with crypto keys (`'btc'`, `'eth'`, `'sol'`, `'xrp'`) mapping to event objects
- **Event Object Fields:** `id`, `slug`, `title`, `description`, `volume`, `liquidity`, `markets`, `resolutionSource`, etc.

---

#### `get_club_token_ids_from_15m_events() -> Dict[str, List[str]]`
**Location:** `polymarket_gamma.py`

Extracts CLOB token IDs from the current 15-minute events.

- **Returns:** Dictionary mapping crypto keys to lists of CLOB token ID strings
- **Note:** Token IDs are extracted from the `clobTokenIds` field (which is a JSON-encoded string in the API response)

---

#### `current_quarter_timestamp_et() -> int`
**Location:** `utils/time_utils.py`

Returns the Unix timestamp for the start of the current 15-minute block (in Eastern Time).

- **Returns:** Integer Unix timestamp (UTC)
- **Example:** If current time is 12:07 PM ET, returns timestamp for 12:00 PM ET

---

#### `get_polymarket_events(...) -> List[Dict[str, Any]]`
**Location:** `polymarket_gamma.py`

General-purpose function to fetch Polymarket events with pagination support.

- **Parameters:**
  - `limit` (int): Events per page (default: 50)
  - `closed` (bool | None): Filter by market status
  - `max_events` (int | None): Maximum total events to fetch
  - `paginate` (bool): Whether to fetch all pages (default: True)
- **Returns:** List of event dictionaries

---

## Technical Notes

### Why CLOB Token IDs?
Polymarket's CLOB (Central Limit Order Book) requires specific token IDs to subscribe to orderbook updates. Each 15-minute event has two token IDs representing the two outcomes ("Up" and "Down"). The WebSocket subscribes to the "Up" token (first in the list) to monitor market activity.

### Auto-Reconnection Logic
The `WebSocketOrderBook` class includes a `monitor_timestamp()` thread that:
1. Calculates when the next 15-minute window starts (current timestamp + 15 minutes)
2. Sleeps until that time
3. Closes the WebSocket connection
4. Triggers `start_market_websocket_connection()` to restart with fresh market data

### Event Slug Format
Event slugs follow the pattern: `{crypto}-updown-15m-{unix_timestamp}`
- Example: `btc-updown-15m-1767200400`

### Data Fields
- **`clobTokenIds`**: JSON-encoded string (needs `json.loads()` to parse)
- **CLOB-specific fields**: `volumeClob`, `liquidityClob`, `volume24hrClob`, etc. are numeric values specific to the orderbook engine

---

## Configuration

Key settings in [configs/polymarket_config.py](configs/polymarket_config.py):
- **`GAMMA_API_BASE_URL`**: `https://gamma-api.polymarket.com`
- **`WEBSOCKET_URL`**: `wss://ws-subscriptions-clob.polymarket.com`
- **`WEBSOCKET_PING_INTERVAL`**: 5 seconds
- **`SUPPORTED_CRYPTOS`**: `['btc', 'eth', 'sol', 'xrp']`
- **`FIFTEEN_MINUTE_EVENTS_SLUG_PREFIXES`**: Slug patterns for each crypto

---

## Sample Event Data

<details>
<summary>Click to expand: Full BTC 15-minute event JSON</summary>

```json
{
  "id": "1066851",
  "question": "Bitcoin Up or Down - December 31, 12:00PM-12:15PM ET",
  "slug": "btc-updown-15m-1767200400",
  "title": "Bitcoin Up or Down",
  "resolutionSource": "https://data.chain.link/streams/btc-usd",
  "outcomes": "[\"Up\", \"Down\"]",
  "outcomePrices": "[\"0.585\", \"0.415\"]",
  "volume": "92509.470055",
  "liquidity": "20579.7452",
  "clobTokenIds": "[\"8851848546962572193739008211889046036776198486765606333032192644001246174440\", \"10181859819235412241349111952383458602722012323104397550162049365057126390549\"]",
  "active": true,
  "closed": false,
  "enableOrderBook": true,
  "lastTradePrice": 0.58,
  "bestBid": 0.58,
  "bestAsk": 0.59,
  "spread": 0.01
}
```

</details>
