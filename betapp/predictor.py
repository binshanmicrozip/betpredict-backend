import os
import pickle
import numpy as np


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH = os.path.join(BASE_DIR, "betpredict_model.pkl")



with open(MODEL_PATH, "rb") as f:
    pkg = pickle.load(f)

model = pkg["model"]
feat_cols = pkg["feature_cols"]

THRESHOLD = 0.65


def parse_score(score_str):
    parts = str(score_str).split("/")
    runs = int(parts[0]) if parts and str(parts[0]).isdigit() else 0
    wickets = int(parts[1]) if len(parts) > 1 and str(parts[1]).isdigit() else 0
    return runs, wickets


def parse_over(over_str):
    parts = str(over_str).split(".")
    over = int(parts[0]) if parts and str(parts[0]).isdigit() else 0
    ball = int(parts[1]) if len(parts) > 1 and str(parts[1]).isdigit() else 0
    return over, ball


def parse_recent(recent_str):
    balls = str(recent_str).strip().split()
    if not balls:
        return 0, 0, 0, 0, 0, 0

    last = balls[-1]
    wicket = 1 if "W" in last.upper() else 0
    runs = int(last) if last.isdigit() else 0
    four = 1 if runs == 4 else 0
    six = 1 if runs == 6 else 0
    boundary = 1 if runs in [4, 6] else 0
    dot = 1 if runs == 0 and wicket == 0 else 0

    return runs, wicket, boundary, dot, six, four


def build_features(cricket: dict, price: dict) -> dict:
    score, wickets = parse_score(cricket.get("score", "0/0"))
    over, ball = parse_over(cricket.get("overs", "0.0"))

    crr = float(cricket.get("crr", 0) or 0)
    rrr = float(cricket.get("rrr", 0) or 0)
    inn = int(cricket.get("innings", 1) or 1)
    last3 = float(cricket.get("last3_runs", 0) or 0)
    last5 = float(cricket.get("last5_runs", 0) or 0)
    wkts5 = float(cricket.get("last5_wkts", 0) or 0)
    tgt = int(cricket.get("target", 0) or 0)

    lr, lw, lb, ld, ls, lf = parse_recent(cricket.get("recent", ""))

    ltp = float(price.get("ltp", 2.0) or 2.0)
    prev_ltp = float(price.get("prev_ltp", ltp) or ltp)
    pdiff = round(ltp - prev_ltp, 4)

    balls_used = over * 6 + ball
    balls_remaining = max(120 - balls_used, 1)

    runs_remaining = max(tgt - score - 1, 0) if inn == 2 else 0

    if inn == 2 and crr > 0:
        run_rate_pressure = min(rrr / max(crr, 0.1), 5.0)
    else:
        run_rate_pressure = crr / 8.0

    features = {
        "innings": inn,
        "over": over,
        "over_norm": over / 19.0,
        "ball_in_over": ball,
        "ball_in_over_norm": ball / 5.0,
        "ball_number": balls_used,
        "balls_used_pct": balls_used / 120.0,
        "is_innings_2": 1 if inn == 2 else 0,
        "score_before": score,
        "wickets_before": wickets,
        "wickets_in_hand": 10 - wickets,
        "wicket_pressure": wickets / 10.0,
        "runs_remaining": runs_remaining,
        "balls_remaining": balls_remaining,
        "current_run_rate": crr,
        "required_rate": rrr,
        "run_rate_pressure": run_rate_pressure,
        "runs_per_ball_needed": runs_remaining / balls_remaining if inn == 2 else 0,
        "batting_momentum": (last3 - 3.9) / 3.9,
        "is_powerplay": 1 if over <= 5 else 0,
        "is_middle": 1 if 6 <= over <= 14 else 0,
        "is_death": 1 if over >= 15 else 0,
        "last_ball_runs": lr,
        "last_ball_wicket": lw,
        "last_ball_boundary": lb,
        "last_ball_six": ls,
        "last_ball_four": lf,
        "last_ball_dot": ld,
        "rolling_3_runs": last3,
        "rolling_6_runs": last5,
        "rolling_3_wickets": wkts5,
        "rolling_6_dots": 0,
        "rolling_6_boundaries": 0,
        "pressure_index": (
            (wickets / 10.0) * 0.4
            + (min(run_rate_pressure, 3) / 3) * 0.3
            + (min(wkts5 / 3, 1)) * 0.3
        ),
        "price": ltp,
        "implied_prob": 1 / max(ltp, 1.01),
        "price_favorite": 1 if ltp < 1.5 else 0,
        "price_even": 1 if 1.5 <= ltp <= 2.5 else 0,
        "price_underdog": 1 if ltp > 2.5 else 0,
        "price_change": pdiff,
        "price_change_pct": pdiff / max(prev_ltp, 0.01),
        "price_volatility": 0,
        "price_momentum": 1 if pdiff > 0 else (-1 if pdiff < 0 else 0),
        "price_trend_3": pdiff,
        "batting_won_toss": 0,
    }

    return features


def predict(cricket: dict, price: dict) -> dict:
    features = build_features(cricket, price)

    row = {col: features.get(col, 0) for col in feat_cols}
    X = np.array([[row[col] for col in feat_cols]])

    probs = model.predict_proba(X)[0]
    p_up, p_down = probs[0], probs[1]

    if p_down >= THRESHOLD:
        signal = "BACK"
        confidence = p_down
        reason = "Price likely to DROP - team batting well"
    elif p_up >= THRESHOLD:
        signal = "LAY"
        confidence = p_up
        reason = "Price likely to RISE - team under pressure"
    else:
        signal = "WAIT"
        confidence = max(p_up, p_down)
        reason = "Not confident enough to signal"

    return {
        "signal": signal,
        "confidence": round(float(confidence), 4),
        "p_back": round(float(p_down), 4),
        "p_lay": round(float(p_up), 4),
        "reason": reason,
        "features_used": row,
    }
 