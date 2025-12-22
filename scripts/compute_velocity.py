from pathlib import Path
import pandas as pd

DELTAS_DIR = Path("data/deltas")
OUTPUT_PATH = Path("data/velocity.csv")


def extract_timestamp(path: Path) -> str:
    # delta_YYYY-MM-DD_HH-MM-SS.csv → YYYY-MM-DD_HH-MM-SS
    return path.stem.replace("delta_", "")


def main():
    if not DELTAS_DIR.exists():
        print("ℹ️ data/deltas not found — skipping velocity")
        return

    delta_files = sorted(DELTAS_DIR.glob("delta_*.csv"))

    if len(delta_files) < 2:
        print("ℹ️ Not enough delta files to compute velocity")
        return

    prev_path = delta_files[-2]
    curr_path = delta_files[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    required_cols = {"player_id", "net_transfers_delta"}
    if not required_cols.issubset(prev.columns) or not required_cols.issubset(curr.columns):
        print("ℹ️ Delta files missing required columns — skipping velocity")
        return

    merged = curr.merge(
        prev,
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    # ✅ FIX: invert sign so positive = accelerating transfers (risers)
    merged["velocity"] = (
        merged["net_transfers_delta_prev"]
        - merged["net_transfers_delta_curr"]
    )

    merged["timestamp"] = extract_timestamp(curr_path)

    result = merged[
        [
            "player_id",
            "timestamp",
            "net_transfers_delta_curr",
            "net_transfers_delta_prev",
            "velocity",
        ]
    ].rename(
        columns={
            "net_transfers_delta_curr": "net_now",
            "net_transfers_delta_prev": "net_prev",
        }
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH)
        result = pd.concat([existing, result], ignore_index=True)
        result = result.drop_duplicates(
            subset=["player_id", "timestamp"],
            keep="last",
        )

    result.to_csv(OUTPUT_PATH, index=False)

    print(f"🚀 Velocity updated ({len(result)} total rows)")


if __name__ == "__main__":
    main()
