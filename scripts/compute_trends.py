from pathlib import Path
import pandas as pd
import numpy as np


INPUT_PATH = Path("data/latest.csv")
OUTPUT_PATH = Path("data/trends.csv")


def main():
    if not INPUT_PATH.exists():
        print("ℹ️ No latest.csv found — skipping trends")
        return

    df = pd.read_csv(INPUT_PATH)

    required_cols = {
        "id",
        "name",
        "price",
        "net_transfers_delta",
        "ownership",
        "minutes",
        "status",
    }

    if not required_cols.issubset(df.columns):
        raise RuntimeError("latest.csv missing required columns")

    # --- Direction ---
    df["direction"] = np.where(
        df["net_transfers_delta"] > 0,
        "up",
        np.where(df["net_transfers_delta"] < 0, "down", "flat"),
    )

    # --- Momentum score ---
    # Normalize by ownership to avoid bias
    df["ownership"] = df["ownership"].replace(0, np.nan)

    df["momentum"] = (
        df["net_transfers_delta"] / df["ownership"]
    ).replace([np.inf, -np.inf], 0).fillna(0)

    # --- Strength buckets ---
    df["strength"] = pd.cut(
        df["momentum"],
        bins=[-1e9, -200, -50, 50, 200, 1e9],
        labels=["strong_down", "weak_down", "neutral", "weak_up", "strong_up"],
    )

    # --- Rule-based flags ---
    df["rise_flag"] = (
        (df["direction"] == "up")
        & (df["momentum"] > 100)
        & (df["minutes"] > 0)
        & (df["status"] == "a")
    )

    df["fall_flag"] = (
        (df["direction"] == "down")
        & (df["momentum"] < -50)
        & (df["minutes"] < 60)
    )

    output_cols = [
        "id",
        "name",
        "price",
        "net_transfers_delta",
        "direction",
        "momentum",
        "strength",
        "rise_flag",
        "fall_flag",
    ]

    df[output_cols].sort_values(
        by="momentum", ascending=False
    ).to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Trends file created: {OUTPUT_PATH}")
    print(f"📈 Rise candidates: {df['rise_flag'].sum()}")
    print(f"📉 Fall candidates: {df['fall_flag'].sum()}")


if __name__ == "__main__":
    main()
