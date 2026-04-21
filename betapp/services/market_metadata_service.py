from betapp.models import Market, Runner


def get_market_metadata(market_id: str, runner_id: str | None = None) -> dict:
    market = Market.objects.filter(market_id=str(market_id)).first()
    runner = None

    if runner_id is not None:
        runner = Runner.objects.filter(
            market_id=str(market_id),
            selection_id=str(runner_id)
        ).first()

        if not runner:
            try:
                runner = Runner.objects.filter(
                    market_id=str(market_id),
                    selection_id=int(runner_id)
                ).first()
            except Exception:
                runner = None

    return {
        "event_id": getattr(market, "event_id", None) if market else None,
        "event_name": getattr(market, "event_name", None) if market else None,
        "market_type": getattr(market, "market_type", None) if market else None,
        "market_name": getattr(market, "market_name", None) if market else None,
        "market_time": getattr(market, "market_start_time", None) if market else None,
        "runner_name": getattr(runner, "runner_name", None) if runner else None,
    }