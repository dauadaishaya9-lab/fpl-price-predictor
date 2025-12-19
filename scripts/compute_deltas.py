import json
import pandas as pd
from pathlib import Path
from unicodedata import normalize
import re

SNAPSHOT_DIR = Path("data/snapshots")
DELTA_DIR = Path("data/deltas")
DELTA_DIR.mkdir(parents=True, exist_ok=True)


def normalize_name(name: str) -> str:
    name = normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = name.lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def load_latest_snapshots():
    files = sorted(SNAPSHOT_DIR.glob("*.json"))
    if len(files) < 2:
        raise RuntimeError("Not enough snapshots to compute deltas")

    return files[-2], files[-1]


def snapshot_to_df(path: Path) -> pd.DataFrame:
    with open(path, "r") as f:
        data = json.load(f)

    players = []
    for p in data["elements"]:
        display_name = f"{p['first_name']} {p['second_name']}"
        name_key = normalize_name(display_name)

        players.append({
            "element_id": p["id"],
            "display_name": display_name,
            "name_key": name_key,
            "team": p["team"],
            "price": p["now_cost"] / 10,
            "selected_by": float(p["selected_by_percent"]),
            "transfers_in": p["transfers_in_event"],
            "transfers_out": p["transfers_out_event"],
            "status": p["status"]
        })

    return pd.DataFrame(players)


def main():
    prev_path, curr_path = load_latest_snapshots()

    prev_df = snapshot_to_df(prev_path)
    curr_df = snapshot_to_df(curr_path)

    merged = prev_df.merge(
        curr_df,
        on=["name_key", "team"],
        suffixes=("_prev", "_curr"),
        how="inner"
    )

    merged["price_delta"] = merged["price_curr"] - merged["price_prev"]
    merged["transfers_in_delta"] = merged["transfers_in_curr"] - merged["transfers_in_prev"]
    merged["transfers_out_delta"] = merged["transfers_out_curr"] - merged["transfers_out_prev"]
    merged["net_transfers"] = merged["transfers_in_delta"] - merged["transfers_out_delta"]
    merged["ownership_delta"] = merged["selected_by_curr"] - merged["selected_by_prev"]

    output_cols = [
        "display_name_curr",
        "team",
        "price_prev",
        "price_curr",
        "price_delta",
        "net_transfers",
        "ownership_delta",
        "status_curr",
        "element_id_curr"
    ]

    merged[output_cols].rename(columns={
        "display_name_curr": "player",
        "status_curr": "status",
        "element_id_curr": "element_id"
    }).to_csv(DELTA_DIR / "latest.csv", index=False)

    print("Delta file saved to data/deltas/latest.csv")


if __name__ == "__main__":
    main()
