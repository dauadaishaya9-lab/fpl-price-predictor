from pathlib import Path
import requests
import pandas as pd
import datetime
import sys

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
SNAPSHOT_DIR = Path("data") / "snapshots"

# Ensure snapshots path is a directory
if SNAPSHOT_DIR.exists() and not SNAPSHOT_DIR.is_dir():
    raise RuntimeError("data/snapshots exists but is not a directory")

SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

# Fetch FPL data
try:
    response = requests.get(FPL_BOOTSTRAP_URL, timeout=30)
    response.raise_for_status()
except Exception as e:
    print(f"❌ Failed to fetch FPL data: {e}")
    sys.exit(1)

data = response.json()

players = data.get("elements", [])
teams = {t["id"]: t["name"] for t in data.get("teams", [])}

rows = []
for p in players:
    rows.append({
        "player_id": p["id"],
        "web_name": p["web_name"],
        "first_name": p["first_name"],
        "second_name": p["second_name"],
        "team": teams.get(p["team"], "Unknown"),
        "now_cost": p["now_cost"] / 10,
        "selected_by_percent": float(p["selected_by_percent"]),
        "transfers_in_event": p["transfers_in_event"],
        "transfers_out_event": p["transfers_out_event"],
        "total_points": p["total_points"],
        "form": float(p["form"]) if p["form"] else 0.0,
        "minutes": p["minutes"],
        "status": p["status"],
    })

df = pd.DataFrame(rows)

timestamp = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
snapshot_path = SNAPSHOT_DIR / f"snapshot_{timestamp}.csv"

df.to_csv(snapshot_path, index=False)

print(f"✅ Snapshot saved: {snapshot_path}")
print(f"📊 Players captured: {len(df)}")
