import requests
import json
from pathlib import Path
from datetime import datetime

SNAPSHOT_DIR = Path("data/snapshots")
SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

def main():
    response = requests.get(URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    now = datetime.utcnow()
    suffix = "AM" if now.hour < 12 else "PM"
    filename = SNAPSHOT_DIR / f"{now.date()}_{suffix}.json"

    with open(filename, "w") as f:
        json.dump(data, f)

    print(f"✅ Snapshot saved to {filename.resolve()}")

if __name__ == "__main__":
    main()
