from pathlib import Path
import pandas as pd
from datetime import datetime

SNAPSHOT_DIR = Path("data/snapshots")
OUTCOMES_PATH = Path("data/price_changes.csv")


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots to detect price changes")
        return

    prev_path = snapshots[-2]
    curr_path = snapshots[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    required = {"player_id", "now_cost"}
    if not required.issubset(prev.columns) or not required.issubset(curr.columns):
        print("⚠️ Snapshots missing required columns")
        return

    merged = curr.merge(
        prev,
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner"
    )

    merged["price_delta"] = merged["now_cost_curr"] - merged["now_cost_prev"]

    changes = merged[merged["price_delta"] != 0]

    if changes.empty:
        print("ℹ️ No price changes detected")
        return

    today = datetime.utcnow().date().isoformat()

    rows = []
    for _, r in changes.iterrows():
        rows.append({
            "player_id": r["player_id"],
            "date": today,
            "price_delta": r["price_delta"],
            "actual_change": "rise" if r["price_delta"] > 0 else "fall"
        })

    new = pd.DataFrame(rows)

    if OUTCOMES_PATH.exists():
        history = pd.read_csv(OUTCOMES_PATH)
        combined = pd.concat([history, new], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["player_id", "date"],
            keep="last"
        )
    else:
        combined = new

    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTCOMES_PATH, index=False)

    print(f"📉📈 Logged {len(new)} price changes")


if __name__ == "__main__":
    main()
