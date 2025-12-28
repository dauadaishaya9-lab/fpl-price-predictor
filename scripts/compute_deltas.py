from pathlib import Path
import pandas as pd
from datetime import datetime

SNAPSHOT_DIR = Path("data/snapshots")
PROTECTION_PATH = Path("data/protection_status.csv")

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
        "status",
    }

    if not required.issubset(prev.columns) or not required.issubset(curr.columns):
        print("⚠️ Missing required columns for delta computation")
        return

    merged = curr.merge(
        prev[
            [
                "player_id",
                "transfers_in_event",
                "transfers_out_event",
                "price",
                "status",
            ]
        ],
        on="player_id",
        suffixes=("", "_prev"),
        how="left",
    )

    # ---------------------
    # Base delta
    # ---------------------
    merged["net_transfers_delta"] = (
        merged["transfers_in_event"]
        - merged["transfers_out_event"]
        - merged["transfers_in_event_prev"]
        + merged["transfers_out_event_prev"]
    )

    merged["price_change"] = merged["price"] - merged["price_prev"]

    # =====================================================
    # 🔴 PROTECTION A — ACTIVE INJURY / SUSPENSION
    # =====================================================
    injured = merged["status"].isin(["i", "s"])
    merged.loc[injured, "net_transfers_delta"] = 0
    merged.loc[injured, "price_change"] = 0

    # =====================================================
    # 🟢 PROTECTION B — POST-RECOVERY LOCK
    # =====================================================
    if PROTECTION_PATH.exists():
        prot = pd.read_csv(PROTECTION_PATH)
        prot["lock_until"] = pd.to_datetime(prot["lock_until"]).dt.date

        today = pd.to_datetime(
            curr_path.stem.replace("snapshot_", ""),
            format="%Y-%m-%d_%H-%M-%S"
        ).date()

        locked_ids = prot.loc[
            prot["lock_until"] >= today,
            "player_id"
        ]

        locked = merged["player_id"].isin(locked_ids)
        merged.loc[locked, "net_transfers_delta"] = 0

    # ---------------------
    # Cleanup
    # ---------------------
    merged.drop(
        columns=[
            "transfers_in_event_prev",
            "transfers_out_event_prev",
            "price_prev",
            "status_prev",
        ],
        inplace=True,
        errors="ignore",
    )

    merged.to_csv(curr_path, index=False)
    print("✅ Deltas updated with injury + recovery protection")

if __name__ == "__main__":
    main()
