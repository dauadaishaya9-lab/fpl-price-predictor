from pathlib import Path
import pandas as pd

DELTAS_DIR = Path("data/deltas")
OUTPUT_PATH = Path("data/velocity.csv")


def main():
    if not DELTAS_DIR.exists():
        return

    delta_files = sorted(DELTAS_DIR.glob("*.csv"))

    if len(delta_files) < 2:
        return

    prev_path = delta_files[-2]
    curr_path = delta_files[-1]

    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    merged = curr.merge(
        prev,
        on="name",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    merged["velocity"] = (
        merged["net_transfers_delta_curr"]
        - merged["net_transfers_delta_prev"]
    )

    result = merged[
        [
            "name",
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
    result.to_csv(OUTPUT_PATH, index=False)


if __name__ == "__main__":
    main()