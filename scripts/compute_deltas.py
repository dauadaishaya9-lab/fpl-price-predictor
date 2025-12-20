from pathlib import Path
from datetime import datetime
import pandas as pd

LATEST_PATH = Path("data/latest.csv")
SNAPSHOTS_DIR = Path("data/snapshots")
DELTAS_DIR = Path("data/deltas")


def main():
    # Ensure required files/folders exist
    if not LATEST_PATH.exists():
        print("ℹ️ data/latest.csv not found — skipping deltas")
        return

    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    DELTAS_DIR.mkdir(parents=True, exist_ok=True)

    # Timestamp for files
    today = datetime.utcnow().strftime("%Y-%m-%d")

    snapshot_path = SNAPSHOTS_DIR / f"{today}.csv"
    delta_path = DELTAS_DIR / f"{today}.csv"

    latest = pd.read_csv(LATEST_PATH)

    # First run: save snapshot only
    if not any(SNAPSHOTS_DIR.glob("*.csv")):
        latest.to_csv(snapshot_path, index=False)
        print(f"📸 First snapshot saved: {snapshot_path}")
        return

    # Get previous snapshot
    snapshot_files = sorted(SNAPSHOTS_DIR.glob("*.csv"))
    prev_snapshot_path = snapshot_files[-1]
    prev = pd.read_csv(prev_snapshot_path)

    # Compute deltas
    merged = latest.merge(
        prev,
        on="name",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    merged["transfers_in_delta"] = (
        merged["transfers_in_event_curr"]
        - merged["transfers_in_event_prev"]
    )

    merged["transfers_out_delta"] = (
        merged["transfers_out_event_curr"]
        - merged["transfers_out_event_prev"]
    )

    merged["net_transfers_delta"] = (
        merged["transfers_in_delta"]
        - merged["transfers_out_delta"]
    )

    deltas = merged[
        [
            "name",
            "transfers_in_delta",
            "transfers_out_delta",
            "net_transfers_delta",
        ]
    ]

    # Save outputs
    deltas.to_csv(delta_path, index=False)
    latest.to_csv(snapshot_path, index=False)

    print(f"✅ Delta file created: {delta_path}")
    print(f"📸 Snapshot updated: {snapshot_path}")
    print(f"📊 Players processed: {len(deltas)}")


if __name__ == "__main__":
    main()
