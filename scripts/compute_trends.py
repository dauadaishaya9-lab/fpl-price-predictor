from pathlib import Path
import pandas as pd
import numpy as np

VELOCITY_PATH = Path("data/velocity.csv")
TRENDS_PATH = Path("data/trends.csv")

ROLLING_WINDOW = 4  # 4 snapshots ≈ 1 day (if run every 6h)


def normalize(series: pd.Series) -> pd.Series:
    """
    Normalize a series to -1 → +1 using tanh.
    Always returns a Series (safe for transform).
    """
    mean_abs = series.abs().mean()
    if mean_abs == 0 or pd.isna(mean_abs):
        return pd.Series(0, index=series.index)

    return np.tanh(series / mean_abs)


def main():
    if not VELOCITY_PATH.exists():
        print("ℹ️ velocity.csv not found — skipping trends")
        return

    df = pd.read_csv(VELOCITY_PATH)

    required_cols = {"player_id", "velocity"}
    if not required_cols.issubset(df.columns):
        print("⚠️ velocity.csv missing required columns")
        return

    if df.empty:
        print("ℹ️ velocity data empty — skipping trends")
        return

    # Ensure numeric
    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0)

    # Ensure timestamp exists
    if "timestamp" not in df.columns:
        df["timestamp"] = range(len(df))

    # Sort correctly for rolling window
    df = df.sort_values(["player_id", "timestamp"])

    # Rolling mean of velocity
    df["rolling_velocity"] = (
        df.groupby("player_id")["velocity"]
        .rolling(ROLLING_WINDOW, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Normalized trend score (-1 → +1)
    df["trend_score"] = (
        df.groupby("player_id")["rolling_velocity"]
        .transform(normalize)
        .fillna(0)
    )

    # Keep only latest trend per player
    latest_trends = (
        df.groupby("player_id")
        .tail(1)[["player_id", "trend_score"]]
    )

    TRENDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    latest_trends.to_csv(TRENDS_PATH, index=False)

    print(f"📈 Trend scores computed for {len(latest_trends)} players")


if __name__ == "__main__":
    main()
