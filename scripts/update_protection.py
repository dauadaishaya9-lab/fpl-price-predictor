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

    today = pd.to_datetime(
        snaps[-1].stem.replace("snapshot_", ""),
        format="%Y-%m-%d_%H-%M-%S"
    ).date()

    if PROTECTION_PATH.exists():
        prot = pd.read_csv(PROTECTION_PATH)
        prot["lock_until"] = pd.to_datetime(prot["lock_until"]).dt.date
    else:
        prot = pd.DataFrame(columns=["player_id", "lock_until"])

    # 🔑 Recovery detection: red → green
    prev_red = prev[prev["status"].isin(["i", "s"])][["player_id"]]
    curr_green = curr[curr["status"].isin(["a", "d"])][["player_id"]]

    recovered = prev_red.merge(curr_green, on="player_id")

    if not recovered.empty:
        recovered["lock_until"] = today + timedelta(days=8)

        prot = (
            pd.concat([prot, recovered], ignore_index=True)
            .sort_values("lock_until")
            .drop_duplicates("player_id", keep="last")
        )

    prot.to_csv(PROTECTION_PATH, index=False)
    print(f"🛡️ Recovery protection active: {len(recovered)} players")

if __name__ == "__main__":
    main()
