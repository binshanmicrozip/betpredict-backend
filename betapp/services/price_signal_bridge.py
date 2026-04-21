from betapp.services.signal_engine import run_signal_engine


def on_price_tick(match_id: str, market_id: str, runner_id: str, price_data: dict):
    return run_signal_engine(
        match_id=str(match_id),
        market_id=str(market_id),
        runner_id=str(runner_id),
        price_data=price_data,
    )
