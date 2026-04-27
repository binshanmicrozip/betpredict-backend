from betapp.history_store import get_combined_history
from betapp.redis_price import get_all_market_prices


def build_signal_ws_payload(
    source_match_id: str,
    market_id: str,
    runner_id: str,
    cricket: dict,
    price: dict,
    prediction: dict,
    ball_key: str | None = None,
    history: dict | None = None,
):
    runner_name = price.get("runner_name") or str(runner_id)

    # Build runners list: fetch all runners for this market from Redis
    # so both team names appear in every ball's price payload
    all_prices = get_all_market_prices(market_id)
    runners = []
    for p in all_prices:
        rid = str(p.get("runner_id") or "")
        rname = p.get("runner_name") or rid
        runners.append({
            "runner_id": rid,
            "runner_name": rname,
            "ltp": p.get("ltp"),
            "tv": p.get("tv"),
        })

    return {
        "type": "bet_signal",
        "status": cricket.get("status", "live"),
        "source_match_id": str(source_match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "runner_name": runner_name,
        "ball_key": ball_key,
        "cricket": cricket,
        "price": {
            **price,
            "runner_name": runner_name,
            "runners": runners,
        },
        "prediction": prediction,
        "history": history or {
            "balls": [],
            "market": [],
            "patterns": [],
        },
    }
