# Historical Data WebSocket API

This WebSocket endpoint allows frontend applications to access stored historical cricket and market data.

## WebSocket URL
```
ws://localhost:8000/ws/bet-signals/
```

## Message Types

### 1. Get Cricket History
Request historical cricket match data from CSV files.

**Request:**
```json
{
  "type": "get_cricket_history",
  "match_id": "151807",
  "source_match_id": "151807",
  "limit": 100
}
```

**Response:**
```json
{
  "type": "cricket_history_response",
  "match_id": "151807",
  "source_match_id": "151807",
  "cricket_snapshots": [
    {
      "type": "cricket_snapshot",
      "timestamp": "2024-01-15T14:30:00",
      "match_id": "151807",
      "source_match_id": "151807",
      "status": "live",
      "state": "in_progress",
      "innings": 1,
      "batting_team": "Mumbai Indians",
      "bowling_team": "Chennai Super Kings",
      "score": 145,
      "wickets": 2,
      "overs": 18.3,
      "target": null,
      "current_run_rate": 7.83,
      "required_run_rate": null
    }
  ],
  "cricket_commentary": [
    {
      "type": "cricket_commentary",
      "timestamp": "2024-01-15T14:30:00",
      "match_id": "151807",
      "source_match_id": "151807",
      "innings": 1,
      "over_number": 18,
      "ball_number": 4,
      "batter_name": "Rohit Sharma",
      "bowler_name": "Deepak Chahar",
      "non_striker_name": "Suryakumar Yadav",
      "runs_batter": 4,
      "runs_extras": 0,
      "runs_total": 4,
      "is_wicket": false,
      "wicket_kind": null,
      "player_out_name": null,
      "commentary": "FOUR! Rohit Sharma smashes it to the boundary"
    }
  ],
  "total_snapshots": 45,
  "total_commentary": 234
}
```

### 2. Get Market History
Request historical market price data from database.

**Request:**
```json
{
  "type": "get_market_history",
  "market_id": "1.256856948",
  "runner_id": "67868736",
  "limit": 100,
  "hours_back": 24
}
```

**Response:**
```json
{
  "type": "market_history_response",
  "market_id": "1.256856948",
  "runner_id": "67868736",
  "market_ticks": [
    {
      "type": "market_tick",
      "tick_time": "2024-01-15T14:30:00",
      "market_id": "1.256856948",
      "runner_id": "67868736",
      "runner_name": "Mumbai Indians",
      "ltp": 2.34,
      "win_prob": 0.428,
      "traded_volume": 125000.50,
      "phase": "in_play",
      "snapshot": "market_ws_async"
    }
  ],
  "total_ticks": 89,
  "hours_back": 24
}
```

### 3. Get Combined History
Request synchronized cricket and market data for the same time period.

**Request:**
```json
{
  "type": "get_combined_history",
  "match_id": "151807",
  "source_match_id": "151807",
  "market_id": "1.256856948",
  "runner_id": "67868736",
  "limit": 50
}
```

**Response:**
```json
{
  "type": "combined_history_response",
  "match_id": "151807",
  "source_match_id": "151807",
  "market_id": "1.256856948",
  "runner_id": "67868736",
  "cricket_snapshots": [
    {
      "timestamp": "2024-01-15T14:30:00",
      "score": 145,
      "wickets": 2,
      "overs": 18.3,
      "crr": 7.83
    }
  ],
  "market_ticks": [
    {
      "tick_time": "2024-01-15T14:30:05",
      "ltp": 2.34,
      "traded_volume": 125000.50
    }
  ],
  "total_cricket": 45,
  "total_market": 89
}
```

## Error Handling

All error responses follow this format:
```json
{
  "type": "error",
  "message": "Description of the error"
}
```

## Frontend Integration

### JavaScript Example
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/bet-signals/');

ws.onopen = function(event) {
    console.log('Connected to historical data WebSocket');

    // Request cricket history
    ws.send(JSON.stringify({
        type: 'get_cricket_history',
        match_id: '151807',
        source_match_id: '151807',
        limit: 50
    }));

    // Request market history
    ws.send(JSON.stringify({
        type: 'get_market_history',
        market_id: '1.256856948',
        runner_id: '67868736',
        limit: 50,
        hours_back: 2
    }));
};

ws.onmessage = function(event) {
    const data = JSON.parse(event.data);

    if (data.type === 'cricket_history_response') {
        // Handle cricket data
        updateCricketChart(data.cricket_snapshots);
    } else if (data.type === 'market_history_response') {
        // Handle market data
        updateMarketChart(data.market_ticks);
    } else if (data.type === 'error') {
        console.error('WebSocket error:', data.message);
    }
};
```

## Testing

Use the management command to test the WebSocket:

```bash
# Test cricket data
python manage.py test_historical_ws --test-type cricket

# Test market data
python manage.py test_historical_ws --test-type market

# Test combined data
python manage.py test_historical_ws --test-type combined
```

## Data Sources

- **Cricket Data**: Read from CSV files in `live_match_data/` directory
- **Market Data**: Read from `PriceTick` model in database
- **Combined Data**: Synchronized cricket snapshots with market ticks from the same time period

## Notes

- Cricket data is stored in CSV format during live polling
- Market data is stored in the database when cricket data is available
- Combined history aligns cricket snapshots with market ticks by timestamp
- All data is returned in chronological order (oldest first)