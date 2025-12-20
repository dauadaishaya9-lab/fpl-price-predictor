from pathlib import Path
import pandas as pd

VELOCITY_PATH = Path("data/velocity.csv")
TRENDS_PATH = Path("data/trends.csv")

ROLLING_WINDOW = 4  # 4 snapshots ≈ 1 day if run every 6h


def main():
    if not VELOCITY_PATH.exists():
        print("ℹ️ velocity.csv not found — skipping trends")
        return

    df = pd.read_csv(VELOCITY_PATH)

    if df.empty or "velocity" not in df.columns:
        print("ℹ️ velocity data invalid — skipping trends")
        return

    # Ensure correct types
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0)

    # Compute rolling trend per player
    df = df.sort_values(["player_id", "timestamp"])

    df["trend_score"] = (
        df.groupby("player_id")["velocity"]
        .rolling(ROLLING_WINDOW, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Save only latest trend per player
    latest_trends = (
        df.groupby("player_id")
        .tail(1)[["player_id", "trend_score"]]
    )

    TRENDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    latest_trends.to_csv(TRENDS_PATH, index=False)

    print(f"📈 Trend scores computed for {len(latest_trends)} players")


if __name__ == "__main__":
    main()
