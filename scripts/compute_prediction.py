from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, time as dtime

LATEST_PATH = Path("data/latest.csv")
TRENDS_PATH = Path("data/trends.csv")
VELOCITY_PATH = Path("data/velocity.csv")
OUTPUT_PATH = Path("data/predictions.csv")


def clamp(x, low=-1.5, high=1.5):
    return max(low, min(high, x))


def deadline_weight():
    now = datetime.utcnow().time()

    if dtime(0, 0) <= now <= dtime(2, 30):
        return 1.35
    elif dtime(22, 0) <= now <= dtime(23, 59):
        return 1.15
    else:
        return 1.0


def main():
    for p in [LATEST_PATH, TRENDS_PATH, VELOCITY_PATH]:
        if not p.exists():
            print("ℹ️ Missing data — skipping predictions")
            return

    latest = pd.read_csv(LATEST_PATH)
    trends = pd.read_csv(TRENDS_PATH)
    velocity = (
        pd.read_csv(VELOCITY_PATH)
        .sort_values("timestamp")
        .groupby("player_id")
        .tail(1)
    )

    df = (
        latest
        .merge(trends, on="player_id")
        .merge(velocity, on="player_id")
    )

    time_factor = deadline_weight()
    predictions = []

    for _, row in df.iterrows():
        # -------------------------
        # 1. HUB CORE: threshold pressure
        # -------------------------
        est_threshold = max(15000, 0.1 * row["selected_by_percent"] * 1000)
        pressure = row["net_now"] / est_threshold

        # -------------------------
        # 2. Modifiers
        # -------------------------
        velocity_boost = np.tanh(row["velocity"] / 5000)
        trend_boost = row["trend_score"]

        raw_score = (
            0.65 * pressure +
            0.20 * velocity_boost +
            0.15 * trend_boost
        )

        score = clamp(raw_score * time_factor)

        direction = "rise" if score >= 0 else "fall"
        confidence = round(abs(score), 3)

        if confidence >= 1.0:
            alert = "imminent"
        elif confidence >= 0.7:
            alert = "warming"
        else:
            alert = "cooling"

        predictions.append({
            "player_id": row["player_id"],
            "name": row["web_name"],
            "prediction_score": round(score, 4),
            "direction": direction,
            "confidence": confidence,
            "alert_level": alert,
            "threshold_pressure": round(pressure, 3),
            "velocity": int(row["velocity"]),
            "net_transfers": int(row["net_now"]),
            "ownership": round(row["selected_by_percent"], 2),
        })

    result = pd.DataFrame(predictions)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"🔮 Hub-style predictions generated: {len(result)} players")
    print(f"⏱️ Time factor: {time_factor}")


if __name__ == "__main__":
    main()
