# Merge Logic (Lambda Architecture)

This module implements the merge stage of the Lambda Architecture, combining batch and speed layer views.

## Strategy

The merge logic follows these principles:

1. **Batch layer is authoritative** for completed 15-minute windows
2. **Speed layer fills gaps** for the most recent incomplete window
3. **Timestamp-based precedence** ensures newest data is used where overlap exists

## Usage

### Command Line

```bash
# Get merged view for a symbol
python serving_layer/merge/merge_views.py BTC

# Get current state (most recent data point)
python serving_layer/merge/merge_views.py BTC --live

# Get prediction accuracy statistics
python serving_layer/merge/merge_views.py BTC --accuracy
```

### Python API

```python
from serving_layer.merge.merge_views import MergeEngine

with MergeEngine() as engine:
    # Full merged view
    result = engine.get_merged_view('BTC', include_live=True)

    # Current state only
    current = engine.get_current_state('BTC')

    # Prediction accuracy
    accuracy = engine.get_prediction_accuracy('BTC', window_count=96)
```

## Output Format

```json
{
  "symbol": "BTC",
  "analytics": [...],
  "live": [...],
  "merged_timeline": [
    {"timestamp": 1767201300, "source": "batch", "type": "window_summary", ...},
    {"timestamp": 1767201250, "source": "speed", "type": "live_point", ...}
  ]
}
```
