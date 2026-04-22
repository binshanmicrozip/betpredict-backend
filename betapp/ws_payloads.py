from betapp.history_store import get_combined_history

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
    return {
        "type": "bet_signal",
        "status": cricket.get("status", "live"),
        "source_match_id": str(source_match_id),
        "market_id": str(market_id),
        "runner_id": str(runner_id),
        "ball_key": ball_key,
        "cricket": cricket,
        "price": price,
        "prediction": prediction,
        "history": history or {
            "balls": [],
            "market": [],
            "patterns": [],
        },
    }