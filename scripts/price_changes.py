from pathlib import Path
import pandas as pd
from datetime import datetime

SNAPSHOT_DIR = Path("data/snapshots")
OUT_PATH = Path("data/price_changes.csv")


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots to record price changes")
        return

    prev_path = snapshots[-2]
    curr_path = snapshots[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    required = {"player_id", "price"}
    if not required.issubset(prev.columns) or not required.issubset(curr.columns):
        print("⚠️ Missing required columns in snapshots")
        return

    merged = curr.merge(
        prev[["player_id", "price"]],
        on="player_id",
        suffixes=("", "_prev"),
        how="inner",
    )

    # ---------------------
    # Detect price change
    # ---------------------
    changed = merged[merged["price"] != merged["price_prev"]].copy()

    if changed.empty:
        print("ℹ️ No price changes detected")
        return

    changed["actual_change"] = changed.apply(
        lambda r: "rise" if r["price"] > r["price_prev"] else "fall",
        axis=1,
    )

    # ---------------------
    # Date from snapshot filename
    # ---------------------
    date = datetime.strptime(
        curr_path.stem.replace("snapshot_", ""),
        "%Y-%m-%d_%H-%M-%S"
    ).date()

    changed["date"] = date

    out = changed[["player_id", "date", "actual_change"]]

    # ---------------------
    # Append (never overwrite)
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUT_PATH.exists():
        out.to_csv(OUT_PATH, mode="a", header=False, index=False)
    else:
        out.to_csv(OUT_PATH, index=False)

    print(f"💾 Recorded {len(out)} real price changes")


if __name__ == "__main__":
    main()
