from pathlib import Path
import pandas as pd

DELTA_DIR = Path("data/deltas")
OUTPUT = Path("data/velocity.csv")


def main():
    files = sorted(DELTA_DIR.glob("delta_*.csv"))

    if len(files) < 2:
        print("ℹ️ Not enough delta files for velocity")
        return

    prev = pd.read_csv(files[-2])
    curr = pd.read_csv(files[-1])

    merged = curr.merge(prev, on="player_id", suffixes=("_curr", "_prev"))
    merged["velocity"] = (
        merged["net_transfers_delta_curr"]
        - merged["net_transfers_delta_prev"]
    )

    merged[["player_id", "velocity"]].to_csv(OUTPUT, index=False)
    print("📈 Velocity updated")


if __name__ == "__main__":
    main()        columns={
            "net_transfers_delta_curr": "net_now",
            "net_transfers_delta_prev": "net_prev",
        }
    )

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUTPUT_PATH, index=False)

    print("✅ Velocity computed:", OUTPUT_PATH)


if __name__ == "__main__":
    main()
