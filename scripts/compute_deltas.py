from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")
DELTA_DIR = Path("data/deltas")
DELTA_DIR.mkdir(parents=True, exist_ok=True)


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots for deltas")
        return

    prev = pd.read_csv(snapshots[-2])
    curr = pd.read_csv(snapshots[-1])

    merged = curr.merge(
        prev,
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    merged["net_transfers_delta"] = (
        merged["transfers_in_event_curr"]
        - merged["transfers_out_event_curr"]
        - merged["transfers_in_event_prev"]
        + merged["transfers_out_event_prev"]
    )

    deltas = merged[[
        "player_id",
        "net_transfers_delta"
    ]]

    out = DELTA_DIR / snapshots[-1].name.replace("snapshot", "delta")
    deltas.to_csv(out, index=False)

    print(f"✅ Delta created: {out}")


if __name__ == "__main__":
    main()
