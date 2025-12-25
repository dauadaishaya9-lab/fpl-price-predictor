from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")
DELTA_DIR = Path("data/deltas")
DELTA_DIR.mkdir(parents=True, exist_ok=True)


def extract_timestamp(path: Path) -> str:
    # snapshot_YYYY-MM-DD_HH-MM-SS.csv → YYYY-MM-DD_HH-MM-SS
    return path.stem.replace("snapshot_", "")


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots for deltas")
        return

    prev_path = snapshots[-2]
    curr_path = snapshots[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    required_cols = {
        "player_id",
        "transfers_in_event",
        "transfers_out_event",
        "price",
    }

    if not required_cols.issubset(prev.columns) or not required_cols.issubset(curr.columns):
        print("⚠️ Snapshot files missing required columns")
        return

    merged = curr.merge(
        prev,
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    # ---------------------
    # Transfer delta
    # ---------------------
    merged["net_transfers_delta"] = (
        merged["transfers_in_event_curr"]
        - merged["transfers_out_event_curr"]
        - merged["transfers_in_event_prev"]
        + merged["transfers_out_event_prev"]
    )

    # ---------------------
    # PRICE CHANGE (🔑 FIX)
    # ---------------------
    merged["price_change"] = (
        merged["price_curr"] - merged["price_prev"]
    )

    deltas = merged[
        [
            "player_id",
            "net_transfers_delta",
            "price_change",
        ]
    ].copy()

    deltas["timestamp"] = extract_timestamp(curr_path)

    out_path = DELTA_DIR / f"delta_{extract_timestamp(curr_path)}.csv"
    deltas.to_csv(out_path, index=False)

    print(f"✅ Delta created: {out_path}")
    print(f"📊 Players processed: {len(deltas)}")
    print(
        f"💰 Price moves detected: "
        f"{(deltas['price_change'] != 0).sum()}"
    )


if __name__ == "__main__":
    main()
