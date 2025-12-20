from pathlib import Path
import requests
import pandas as pd
from datetime import datetime
import sys

FPL_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

DATA_DIR = Path("data")
SNAPSHOT_DIR = DATA_DIR / "snapshots"
LATEST_PATH = DATA_DIR / "latest.csv"

SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    try:
        r = requests.get(FPL_URL, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"❌ FPL fetch failed: {e}")
        sys.exit(1)

    data = r.json()
    teams = {t["id"]: t["name"] for t in data["teams"]}

    rows = []
    for p in data["elements"]:
        rows.append({
            "player_id": p["id"],
            "name": f'{p["first_name"]} {p["second_name"]}',
            "web_name": p["web_name"],
            "team": teams.get(p["team"], ""),
            "price": p["now_cost"] / 10,
            "ownership": float(p["selected_by_percent"]),
            "transfers_in_event": p["transfers_in_event"],
            "transfers_out_event": p["transfers_out_event"],
            "form": float(p["form"]) if p["form"] else 0.0,
            "minutes": p["minutes"],
            "status": p["status"],
        })

    df = pd.DataFrame(rows)

    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    snapshot_path = SNAPSHOT_DIR / f"snapshot_{ts}.csv"

    df.to_csv(snapshot_path, index=False)
    df.to_csv(LATEST_PATH, index=False)

    print(f"📸 Snapshot saved: {snapshot_path}")
    print(f"🆕 latest.csv updated ({len(df)} players)")


if __name__ == "__main__":
    main()
