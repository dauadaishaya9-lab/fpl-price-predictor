from pathlib import Path
import pandas as pd
import numpy as np
import json
from datetime import datetime, time as dtime

# =====================
# Paths
# =====================
LATEST_PATH = Path("data/latest.csv")
TRENDS_PATH = Path("data/trends.csv")
VELOCITY_PATH = Path("data/velocity.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")
OUTPUT_PATH = Path("data/predictions.csv")


# =====================
# Helpers
# =====================
def clamp(x, low=-1, high=1):
    return max(low, min(high, x))


def deadline_weight():
    now = datetime.utcnow().time()
    if dtime(0, 0) <= now <= dtime(2, 30):
        return 1.30
    elif dtime(22, 0) <= now <= dtime(23, 59):
        return 1.15
    else:
        return 0.85


def ownership_bucket(ownership):
    if ownership < 5:
        return "low"
    elif ownership < 15:
        return "mid_low"
    elif ownership < 30:
        return "mid_high"
    else:
        return "high"


def load_thresholds():
    if THRESHOLDS_PATH.exists():
        return json.loads(THRESHOLDS_PATH.read_text())

    return {
        "low": {"rise": {"imminent": 0.6, "warming": 0.35},
                "fall": {"imminent": 0.7, "warming": 0.45}},
        "mid_low": {"rise": {"imminent": 0.65, "warming": 0.4},
                    "fall": {"imminent": 0.72, "warming": 0.48}},
        "mid_high": {"rise": {"imminent": 0.7, "warming": 0.45},
                     "fall": {"imminent": 0.75, "warming": 0.5}},
        "high": {"rise": {"imminent": 0.78, "warming": 0.55},
                 "fall": {"imminent": 0.8, "warming": 0.6}},
    }


# =====================
# Main
# =====================
def main():
    for path in [LATEST_PATH, TRENDS_PATH, VELOCITY_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping prediction")
            return

    latest = pd.read_csv(LATEST_PATH)
    trends = pd.read_csv(TRENDS_PATH)
    velocity = pd.read_csv(VELOCITY_PATH)

    velocity = (
        velocity.sort_values("timestamp")
        .groupby("player_id")
        .tail(1)
    )

    df = (
        latest.merge(trends, on="player_id", how="inner")
        .merge(velocity, on="player_id", how="inner")
    )

    if df.empty:
        print("ℹ️ No data to predict")
        return

    v_mean = df["velocity"].abs().mean() or 1
    n_mean = df["net_now"].abs().mean() or 1

    time_factor = deadline_weight()
    thresholds = load_thresholds()

    predictions = []

    for _, row in df.iterrows():
        velocity_norm = row["velocity"] / v_mean
        net_norm = row["net_now"] / n_mean
        ownership = row["selected_by_percent"]

        raw_score = (
            0.45 * row["trend_score"] +
            0.35 * velocity_norm +
            0.15 * net_norm +
            0.05 * (1 - min(ownership / 50, 1))
        )

        score = clamp(raw_score * time_factor)
        direction = "rise" if score > 0 else "fall"
        confidence = abs(score)

        # =====================
        # 🔒 FALSE POSITIVE SUPPRESSION
        # =====================

        # Gate 1: spike detection
        if abs(row["velocity"]) > 2.5 * (abs(row["trend_score"]) + 0.05):
            confidence *= 0.75

        # Gate 2: net confirmation
        if direction == "rise" and row["net_now"] <= 0:
            confidence *= 0.6
        if direction == "fall" and row["net_now"] >= 0:
            confidence *= 0.6

        # Gate 3: ownership inertia
        if ownership > 30:
            confidence *= 0.85
        if ownership > 50:
            confidence *= 0.7

        confidence = round(confidence, 3)

        bucket = ownership_bucket(ownership)
        t = thresholds[bucket][direction]

        if confidence >= t["imminent"]:
            alert_level = "imminent"
        elif confidence >= t["warming"]:
            alert_level = "warming"
        else:
            alert_level = "cooling"

        predictions.append({
            "player_id": row["player_id"],
            "name": row["web_name"],
            "prediction_score": round(score, 4),
            "direction": direction,
            "confidence": confidence,
            "alert_level": alert_level,
            "trend_score": round(row["trend_score"], 4),
            "velocity": int(row["velocity"]),
            "net_transfers": int(row["net_now"]),
            "ownership": round(ownership, 2),
            "ownership_bucket": bucket,
        })

    result = pd.DataFrame(predictions)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"🔮 Predictions updated: {len(result)} players")
    print("🛡️ False-positive suppression active")


if __name__ == "__main__":
    main()
