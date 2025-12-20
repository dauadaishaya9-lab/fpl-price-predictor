from pathlib import Path
import pandas as pd

# Paths
LATEST_PATH = Path("data/latest.csv")
TRENDS_PATH = Path("data/trends.csv")
VELOCITY_PATH = Path("data/velocity.csv")
OUTPUT_PATH = Path("data/predictions.csv")


def prediction_score(trend, velocity, net_transfers, ownership):
    """
    Combines multiple signals into a single prediction score.
    All inputs are assumed numeric.
    """

    # Normalize components safely
    velocity_norm = velocity / 10000 if velocity else 0
    net_norm = net_transfers / 50000 if net_transfers else 0
    ownership_factor = min(ownership / 50, 1) if ownership else 0

    score = (
        0.45 * trend +
        0.35 * velocity_norm +
        0.15 * net_norm +
        0.05 * (1 - ownership_factor)
    )

    return round(score, 4)


def main():
    # Required files
    for path in [LATEST_PATH, TRENDS_PATH, VELOCITY_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping prediction")
            return

    latest = pd.read_csv(LATEST_PATH)
    trends = pd.read_csv(TRENDS_PATH)
    velocity = pd.read_csv(VELOCITY_PATH)

    # Ensure required columns exist
    required_latest = {"player_id", "web_name", "selected_by_percent"}
    required_trends = {"player_id", "trend_score"}
    required_velocity = {"player_id", "velocity", "net_now"}

    if not required_latest.issubset(latest.columns):
        print("⚠️ latest.csv missing required columns")
        return

    if not required_trends.issubset(trends.columns):
        print("⚠️ trends.csv missing required columns")
        return

    if not required_velocity.issubset(velocity.columns):
        print("⚠️ velocity.csv missing required columns")
        return

    # Merge all signals by player_id
    df = latest.merge(trends, on="player_id", how="inner")
    df = df.merge(velocity, on="player_id", how="inner")

    predictions = []

    for _, row in df.iterrows():
        score = prediction_score(
            trend=row["trend_score"],
            velocity=row["velocity"],
            net_transfers=row["net_now"],
            ownership=row["selected_by_percent"],
        )

        predictions.append({
            "player_id": row["player_id"],
            "name": row["web_name"],
            "prediction_score": score,
            "trend_score": row["trend_score"],
            "velocity": row["velocity"],
            "net_transfers": row["net_now"],
            "ownership": row["selected_by_percent"],
        })

    result = pd.DataFrame(predictions)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print(f"🔮 Prediction file created: {OUTPUT_PATH}")
    print(f"📊 Players scored: {len(result)}")


if __name__ == "__main__":
    main()
