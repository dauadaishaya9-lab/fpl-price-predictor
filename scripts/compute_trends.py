from pathlib import Path
import pandas as pd
import numpy as np

VELOCITY_PATH = Path("data/velocity.csv")
TRENDS_PATH = Path("data/trends.csv")

ROLLING_WINDOW = 4  # ≈ 1 day if run every 6h
RECENCY_WEIGHT = 0.7  # higher = more deadline-sensitive


def normalize_global(series: pd.Series) -> pd.Series:
    """
    Normalize across ALL players to -1 → +1 using tanh.
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

    required_cols = {"player_id", "velocity", "timestamp"}
    if not required_cols.issubset(df.columns):
        print("⚠️ velocity.csv missing required columns")
        return

    if df.empty:
        print("ℹ️ velocity data empty — skipping trends")
        return

    df["velocity"] = pd.to_numeric(df["velocity"], errors="coerce").fillna(0)

    # Ensure correct order
    df = df.sort_values(["player_id", "timestamp"])

    # Rolling mean
    df["rolling_velocity"] = (
        df.groupby("player_id")["velocity"]
        .rolling(ROLLING_WINDOW, min_periods=1)
        .mean()
        .reset_index(level=0, drop=True)
    )

    # Recency weighting (deadline sensitivity)
    df["weighted_velocity"] = (
        df.groupby("player_id")["rolling_velocity"]
        .apply(
            lambda x: x * np.linspace(1 - RECENCY_WEIGHT, 1, len(x))
        )
        .reset_index(level=0, drop=True)
    )

    # Global normalization (Hub-style)
    df["trend_score"] = normalize_global(df["weighted_velocity"]).fillna(0)

    # Keep only latest snapshot per player
    latest_trends = (
        df.groupby("player_id")
        .tail(1)[["player_id", "trend_score"]]
    )

    TRENDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    latest_trends.to_csv(TRENDS_PATH, index=False)

    print(f"📈 Trend scores computed for {len(latest_trends)} players")


if __name__ == "__main__":
    main()
