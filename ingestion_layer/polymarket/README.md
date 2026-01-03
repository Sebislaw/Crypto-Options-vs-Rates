Polymarket data — commands and example outputs

What data you can get
- 15-minute event objects from the Gamma API (via `get_current_15m_events()` / `get_polymarket_events()`). Each event includes metadata keys such as `id`, `slug`, `title`, `markets`, `liquidity`, `volume`, and many others.
- Per-event `markets` list: each market includes `id`, `question`, `outcomes`, `volume`, and `clobTokenIds` (see note below).
- CLOB token IDs mapping: `get_club_token_ids_from_15m_events()` parses `clobTokenIds` and returns a dict mapping crypto keys (`'btc'`, `'eth'`, `'sol'`, `'xrp'`) to lists of CLOB token ID strings.
- WebSocket order book feed: `WebSocketOrderBook` (in `polymarket_clob.py`) connects to Polymarket's CLOB websocket, sends an `assets_ids` subscription and prints raw messages from the orderbook channel.
- Utility: `current_quarter_timestamp_et()` returns the UNIX timestamp (UTC seconds) for the start of the current 15-minute ET block.

Commands and example outputs
- List public symbols in `polymarket_clob`:

  Command:
  ```powershell
  python -c "from ingestion_layer.polymarket import polymarket_clob; print([n for n in dir(polymarket_clob) if not n.startswith('_')])"
  ```

  Example output (truncated):
  ```text
  ["List", "MARKET_CHANNEL", "Path", "WEBSOCKET_PING_INTERVAL", "WEBSOCKET_URL", "WebSocketApp\n", "WebSocketOrderBook", "current_quarter_timestamp_et", "get_club_token_ids_from_15m_events", "json", "parent_dir", "start_market_websocket_connection", "sys", "threading", "time"]
  ```

- Inspect current 15-minute events (show top-level keys and market sample keys):

  Command:
  ```powershell
  python -c "from ingestion_layer.polymarket.polymarket_gamma import get_current_15m_events; d=get_current_15m_events(); ks=list(d.keys()); print('top_keys:', ks); first=ks[0]; print('first_keys:', list(d[first].keys())); print('markets_sample_keys:', list(d[first]['markets'][0].keys()))"
  ```

  Example output (truncated):
  ```text
  top_keys: ['btc', 'eth', 'sol', 'xrp']
  first_keys: ['id', 'ticker', 'slug', 'title', 'description', 'resolutionSource', 'startDate', 'creationDate', 'endDate', 'image', 'icon', 'active', 'closed', 'archived', 'new', 'featured', 'restricted', 'liquidity', 'volume', 'openInterest', 'createdAt', 'updatedAt', 'competitive', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr', 'enableOrderBook', 'liquidityClob', 'negRisk', 'commentCount', 'markets', 'series', 'tags', 'cyom', 'showAllOutcomes', 'showMarketImages', 'enableNegRisk', 'automaticallyActive', 'startTime', 'seriesSlug', 'negRiskAugmented', 'pendingDeployment', 'deploying', 'requiresTranslation']
  markets_sample_keys: ['id', 'question', 'conditionId', 'slug', 'resolutionSource', 'endDate', 'liquidity', 'startDate', 'image', 'icon', 'description', 'outcomes', 'outcomePrices', 'volume', 'active', 'closed', 'marketMakerAddress', 'createdAt', 'updatedAt', 'new', 'featured', 'archived', 'restricted', 'groupItemThreshold', 'questionID', 'enableOrderBook', 'orderPriceMinTickSize', 'orderMinSize', 'volumeNum', 'liquidityNum', 'endDateIso', 'startDateIso', 'hasReviewedDates', 'volume24hr', 'volume1wk', 'volume1mo', 'volume1yr', 'clobTokenIds', 'volume24hrClob', 'volume1wkClob', 'volume1moClob', 'volume1yrClob', 'volumeClob', 'liquidityClob', 'acceptingOrders', 'negRisk', 'ready', 'funded', 'acceptingOrdersTimestamp', 'cyom', 'competitive', 'pagerDutyNotificationEnabled', 'approved', 'rewardsMinSize', 'rewardsMaxSpread', 'spread', 'lastTradePrice', 'bestBid', 'bestAsk', 'automaticallyActive', 'clearBookOnStart', 'showGmpSeries', 'showGmpOutcome', 'manualActivation', 'negRiskOther', 'umaResolutionStatuses', 'pendingDeployment', 'deploying', 'rfqEnabled', 'eventStartTime', 'holdingRewardsEnabled', 'feesEnabled', 'requiresTranslation']

  Full sample market JSON (first BTC market, single-line):

  ```text
  {"id": "1066851", "question": "Bitcoin Up or Down - December 31, 12:00PM-12:15PM ET", "conditionId": "0x2e6af11e4b853b05ba7135239b21d77e808889b6ce1e735de856c94c4e5b5fce", "slug": "btc-updown-15m-1767200400", "resolutionSource": "https://data.chain.link/streams/btc-usd", "endDate": "2025-12-31T17:15:00Z", "liquidity": "20579.7452", "startDate": "2025-12-30T17:10:15.861305Z", "image": "https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png", "icon": "https://polymarket-upload.s3.us-east-2.amazonaws.com/BTC+fullsize.png", "description": "This market will resolve to \"Up\" if the Bitcoin price at the end of the time range specified in the title is greater than or equal to the price at the beginning of that range. Otherwise, it will resolve to \"Down\".\nThe resolution source for this market is information from Chainlink, specifically the BTC/USD data stream available at https://data.chain.link/streams/btc-usd.\nPlease note that this market is about the price according to Chainlink data stream BTC/USD, not according to other sources or spot markets.", "outcomes": "[\"Up\", \"Down\"]", "outcomePrices": "[\"0.585\", \"0.415\"]", "volume": "92509.470055", "active": true, "closed": false, "marketMakerAddress": "", "createdAt": "2025-12-30T17:01:49.027357Z", "updatedAt": "2025-12-31T17:07:53.929803Z", "new": false, "featured": false, "archived": false, "restricted": true, "groupItemThreshold": "0", "questionID": "0xf69e4e6eb29d29bd1852208e2a27f652e16053fdfdad35ce6953e08f0fb08424", "enableOrderBook": true, "orderPriceMinTickSize": 0.01, "orderMinSize": 5, "volumeNum": 92509.470055, "liquidityNum": 20579.7452, "endDateIso": "2025-12-31", "startDateIso": "2025-12-30", "hasReviewedDates": true, "volume24hr": 815.1150849999999, "volume1wk": 815.1150849999999, "volume1mo": 815.1150849999999, "volume1yr": 815.1150849999999, "clobTokenIds": "[\"8851848546962572193739008211889046036776198486765606333032192644001246174440\", \"10181859819235412241349111952383458602722012323104397550162049365057126390549\"]", "volume24hrClob": 815.1150849999999, "volume1wkClob": 815.1150849999999, "volume1moClob": 815.1150849999999, "volume1yrClob": 815.1150849999999, "volumeClob": 92509.470055, "liquidityClob": 20579.7452, "acceptingOrders": true, "negRisk": false, "ready": false, "funded": false, "acceptingOrdersTimestamp": "2025-12-30T17:09:53Z", "cyom": false, "competitive": 0.9928268261808434, "pagerDutyNotificationEnabled": false, "approved": true, "rewardsMinSize": 0, "rewardsMaxSpread": 0, "spread": 0.01, "lastTradePrice": 0.58, "bestBid": 0.58, "bestAsk": 0.59, "automaticallyActive": true, "clearBookOnStart": false, "showGmpSeries": false, "showGmpOutcome": false, "manualActivation": false, "negRiskOther": false, "umaResolutionStatuses": "[]", "pendingDeployment": false, "deploying": false, "rfqEnabled": false, "eventStartTime": "2025-12-31T17:00:00Z", "holdingRewardsEnabled": false, "feesEnabled": false, "requiresTranslation": false}
  ```
  ```

- Get parsed CLOB token IDs mapping (decoded JSON from `clobTokenIds`):

  Command:
  ```powershell
  python -c "from ingestion_layer.polymarket.polymarket_gamma import get_club_token_ids_from_15m_events; import json; print(get_club_token_ids_from_15m_events())"
  ```

  Example output:
  ```text
  {"btc": ["8851848546962572193739008211889046036776198486765606333032192644001246174440", "10181859819235412241349111952383458602722012323104397550162049365057126390549"], "eth": ["37283594079074919903126935096138570848920072247779153380455704888017339871157", "16136687713760634223563054086318388951167402190497275532808513193239467005933"], "sol": ["93602383906756530404250452182523938962913165356156639921731486380789417773643", "84927622167566286524912028297335672109479033056884227873841717675364594807856"], "xrp": ["82889833286003340162384985575443597850577731446058256304717407903585705938381", "78879235013957887446233685248607168866886346364832354128806412869023858396787"]}
  ```

- Show current quarter timestamp (utility):

  Command:
  ```powershell
  python -c "from ingestion_layer.polymarket.utils.time_utils import current_quarter_timestamp_et; print(current_quarter_timestamp_et())"
  ```

  Example output:
  ```text
  1767200400
  ```

Notes on data formats
- The `markets[*]['clobTokenIds']` field returned by the Gamma events API is a JSON-encoded string (i.e. a string containing a JSON array). `get_club_token_ids_from_15m_events()` decodes that string with `json.loads(...)` and returns a Python `list` of token-id strings.
- The events include both human-friendly fields and CLOB-specific numeric fields with the `Clob` suffix (e.g. `volumeClob`, `liquidityClob`) which are numeric metrics directly related to the CLOB engine.
- The WebSocket orderbook handler (`WebSocketOrderBook`) prints the raw messages it receives; messages are sent as JSON strings by the websocket server and will contain orderbook updates keyed by typical CLOB fields (prices, sizes, bids/asks) depending on the feed.
