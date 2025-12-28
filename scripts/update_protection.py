from pathlib import Path
import pandas as pd
from datetime import timedelta

SNAPSHOT_DIR = Path("data/snapshots")
PROTECTION_PATH = Path("data/protection_status.csv")


def main():
    snaps = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    if len(snaps) < 2:
        print("ℹ️ Not enough snapshots for protection tracking")
        return

    prev = pd.read_csv(snaps[-2])
    curr = pd.read_csv(snaps[-1])

    # ✅ SAFER: read date from snapshot content
    if "snapshot_date" not in curr.columns:
        print("⚠️ snapshot_date missing — protection skipped")
        return

    today = pd.to_datetime(curr["snapshot_date"].iloc[0]).date()

    # ---------------------
    # Load existing protection
    # ---------------------
    if PROTECTION_PATH.exists():
        prot = pd.read_csv(PROTECTION_PATH)
        prot["lock_until"] = pd.to_datetime(prot["lock_until"]).dt.date
    else:
        prot = pd.DataFrame(columns=["player_id", "lock_until"])

    # ---------------------
    # Detect recovery (red → available)
    # ---------------------
    prev_red = prev[prev["status"].isin(["i", "s"])][["player_id"]]
    curr_green = curr[curr["status"].isin(["a", "d"])][["player_id"]]

    recovered = prev_red.merge(curr_green, on="player_id")

    if not recovered.empty:
        recovered = recovered.copy()
        recovered["lock_until"] = today + timedelta(days=8)

        prot = (
            pd.concat([prot, recovered], ignore_index=True)
            .sort_values("lock_until")
            .drop_duplicates("player_id", keep="last")
        )

    prot.to_csv(PROTECTION_PATH, index=False)
    print(f"🛑 Protection updated: {len(recovered)} players")


if __name__ == "__main__":
    main()
