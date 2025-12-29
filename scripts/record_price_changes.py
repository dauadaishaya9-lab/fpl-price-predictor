from pathlib import Path
import pandas as pd
from datetime import datetime

SNAPSHOT_DIR = Path("data/snapshots")
OUT_PATH = Path("data/price_changes.csv")


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots to detect price changes")
        return

    # ---------------------
    # Load latest snapshot
    # ---------------------
    curr_path = snapshots[-1]
    curr = pd.read_csv(curr_path)

    if {"player_id", "price"}.issubset(curr.columns) is False:
        print("⚠️ Latest snapshot missing required columns")
        return

    # ---------------------
    # Find previous snapshot with DIFFERENT prices
    # ---------------------
    prev = None
    prev_path = None

    for path in reversed(snapshots[:-1]):
        temp = pd.read_csv(path)
        merged = curr.merge(
            temp[["player_id", "price"]],
            on="player_id",
            suffixes=("", "_prev"),
            how="inner",
        )

        if (merged["price"] != merged["price_prev"]).any():
            prev = temp
            prev_path = path
            break

    if prev is None:
        print("ℹ️ No previous snapshot with price differences found")
        return

    # ---------------------
    # Detect price changes
    # ---------------------
    merged = curr.merge(
        prev[["player_id", "price"]],
        on="player_id",
        suffixes=("", "_prev"),
        how="inner",
    )

    changed = merged[merged["price"] != merged["price_prev"]].copy()

    if changed.empty:
        print("ℹ️ No price changes detected")
        return

    changed["actual_change"] = changed.apply(
        lambda r: "rise" if r["price"] > r["price_prev"] else "fall",
        axis=1,
    )

    # ---------------------
    # Date from CURRENT snapshot filename
    # ---------------------
    date = datetime.strptime(
        curr_path.stem.replace("snapshot_", ""),
        "%Y-%m-%d_%H-%M-%S",
    ).date()

    changed["date"] = date

    out = changed[["player_id", "date", "actual_change"]]

    # ---------------------
    # De-duplicate (player_id + date)
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUT_PATH.exists():
        existing = pd.read_csv(OUT_PATH)

        out = out.merge(
            existing[["player_id", "date"]],
            on=["player_id", "date"],
            how="left",
            indicator=True,
        )

        out = out[out["_merge"] == "left_only"].drop(columns=["_merge"])

    if out.empty:
        print("ℹ️ Price changes already recorded")
        return

    # ---------------------
    # Append
    # ---------------------
    if OUT_PATH.exists():
        out.to_csv(OUT_PATH, mode="a", header=False, index=False)
    else:
        out.to_csv(OUT_PATH, index=False)

    print(f"💾 Recorded {len(out)} real price changes")
    print(f"🔍 Compared: {prev_path.name} → {curr_path.name}")


if __name__ == "__main__":
    main()
