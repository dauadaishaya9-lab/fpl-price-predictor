from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots for deltas")
        return

    prev_path = snapshots[-2]
    curr_path = snapshots[-1]

    prev = safe_read_csv(prev_path)
    curr = safe_read_csv(curr_path)

    required = {
        "player_id",
        "transfers_in_event",
        "transfers_out_event",
        "price",
    }

    if not required.issubset(prev.columns) or not required.issubset(curr.columns):
        print("⚠️ Missing required columns for delta computation")
        return

    merged = curr.merge(
        prev[[
            "player_id",
            "transfers_in_event",
            "transfers_out_event",
            "price",
        ]],
        on="player_id",
        suffixes=("", "_prev"),
        how="left",
    )

    merged["net_transfers_delta"] = (
        merged["transfers_in_event"]
        - merged["transfers_out_event"]
        - merged["transfers_in_event_prev"]
        + merged["transfers_out_event_prev"]
    )

    merged["price_change"] = merged["price"] - merged["price_prev"]

    merged.drop(
        columns=[
            "transfers_in_event_prev",
            "transfers_out_event_prev",
            "price_prev",
        ],
        inplace=True,
    )

    merged.to_csv(curr_path, index=False)

    print("✅ Deltas added to snapshot")


if __name__ == "__main__":
    main()
