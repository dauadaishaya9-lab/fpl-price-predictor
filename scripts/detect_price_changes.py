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

    prev_path = snapshots[-2]
    curr_path = snapshots[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    required = {"player_id", "price"}
    if not required.issubset(prev.columns) or not required.issubset(curr.columns):
        print("⚠️ Snapshots missing price column")
        return

    merged = curr.merge(
        prev[["player_id", "price"]],
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner"
    )

    merged["price_delta"] = merged["price_curr"] - merged["price_prev"]

    today = datetime.utcnow().date().isoformat()

    changes = merged[merged["price_delta"] != 0]

    if changes.empty:
        print("ℹ️ No price changes detected")
        return

    out = changes[["player_id", "price_delta"]].copy()
    out["date"] = today
    out["actual_change"] = out["price_delta"].apply(
        lambda x: "rise" if x > 0 else "fall"
    )

    if OUT_PATH.exists() and OUT_PATH.stat().st_size > 0:
        history = pd.read_csv(OUT_PATH)
        out = pd.concat([history, out], ignore_index=True)

    out.drop_duplicates(
        subset=["player_id", "date"],
        keep="last",
        inplace=True
    )

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUT_PATH, index=False)

    print(f"📈📉 Logged {len(out)} price changes")


if __name__ == "__main__":
    main()
