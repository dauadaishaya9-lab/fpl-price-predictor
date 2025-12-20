from pathlib import Path
import pandas as pd
import numpy as np

# Paths
LATEST_PATH = Path("data/latest.csv")
TRENDS_PATH = Path("data/trends.csv")
VELOCITY_PATH = Path("data/velocity.csv")
OUTPUT_PATH = Path("data/predictions.csv")


def clamp(x, low=-1, high=1):
    return max(low, min(high, x))


def main():
    for path in [LATEST_PATH, TRENDS_PATH, VELOCITY_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping prediction")
            return

    latest = pd.read_csv(LATEST_PATH)
    trends = pd.read_csv(TRENDS_PATH)
    velocity = pd.read_csv(VELOCITY_PATH)

    # Keep ONLY latest velocity per player
    velocity = (
        velocity.sort_values("timestamp")
        .groupby("player_id")
        .tail(1)
    )

    # Required columns
    if not {"player_id", "selected_by_percent", "web_name"}.issubset(latest.columns):
        print("⚠️ latest.csv missing required columns")
        return

    if not {"player_id", "trend_score"}.issubset(trends.columns):
        print("⚠️ trends.csv missing required columns")
        return

    if not {"player_id", "velocity", "net_now"}.issubset(velocity.columns):
        print("⚠️ velocity.csv missing required columns")
        return

    df = (
        latest.merge(trends, on="player_id", how="inner")
        .merge(velocity, on="player_id", how="inner")
    )

    # Dynamic normalization factors
    v_mean = df["velocity"].abs().mean() or 1
    n_mean = df["net_now"].abs().mean() or 1

    predictions = []

    for _, row in df.iterrows():
        velocity_norm = row["velocity"] / v_mean
        net_norm = row["net_now"] / n_mean
        ownership_factor = min(row["selected_by_percent"] / 50, 1)

        raw_score = (
            0.45 * row["trend_score"] +
            0.35 * velocity_norm +
            0.15 * net_norm +
            0.05 * (1 - ownership_factor)
        )

        score = clamp(raw_score)

        direction = "rise" if score > 0 else "fall"
        confidence = round(abs(score), 3)

        if confidence >= 0.7:
            alert_level = "imminent"
        elif confidence >= 0.4:
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
            "ownership": round(row["selected_by_percent"], 2),
        })

    result = pd.DataFrame(predictions)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"🔮 Predictions updated: {len(result)} players")


if __name__ == "__main__":
    main()
