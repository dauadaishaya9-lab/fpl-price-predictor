from pathlib import Path
import pandas as pd
import numpy as np

VELOCITY_PATH = Path("data/velocity.csv")
VOLATILITY_PATH = Path("data/player_volatility.csv")

MIN_SAMPLES = 5


def main():
    if not VELOCITY_PATH.exists():
        print("ℹ️ velocity.csv missing — skipping volatility update")
        return

    v = pd.read_csv(VELOCITY_PATH)

    if "player_id" not in v.columns or "velocity" not in v.columns:
        print("⚠️ velocity.csv missing required columns")
        return

    v["abs_velocity"] = v["velocity"].abs()

    grouped = v.groupby("player_id")["abs_velocity"]

    stats = grouped.agg(
        avg_abs_velocity="mean",
        velocity_std="std",
        samples="count",
    ).reset_index()

    stats["velocity_std"] = stats["velocity_std"].fillna(0)

    if VOLATILITY_PATH.exists():
        old = pd.read_csv(VOLATILITY_PATH)
        stats = (
            pd.concat([old, stats])
            .groupby("player_id", as_index=False)
            .agg({
                "avg_abs_velocity": "mean",
                "velocity_std": "mean",
                "samples": "sum",
            })
        )

    stats.to_csv(VOLATILITY_PATH, index=False)
    print(f"🧠 Player volatility memory updated ({len(stats)} players)")


if __name__ == "__main__":
    main()
