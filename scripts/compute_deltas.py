from pathlib import Path
import pandas as pd


SNAPSHOT_DIR = Path("data/snapshots")
OUTPUT_PATH = Path("data/latest.csv")


def load_latest_snapshots():
    """
    Load the two most recent snapshot CSV files.
    Returns (previous_path, current_path) or (None, None) if not enough data.
    """
    if not SNAPSHOT_DIR.exists():
        print("ℹ️ No snapshots directory yet — skipping deltas")
        return None, None

    snapshots = sorted(SNAPSHOT_DIR.glob("*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Only one snapshot available — skipping delta computation")
        return None, None

    return snapshots[-2], snapshots[-1]


def compute_deltas(prev_path: Path, curr_path: Path) -> pd.DataFrame:
    """
    Compute player-level deltas between two snapshots.
    """
    prev = pd.read_csv(prev_path)
    curr = pd.read_csv(curr_path)

    # Ensure required columns exist
    required_cols = {
        "id",
        "web_name",
        "now_cost",
        "total_transfers_in",
        "total_transfers_out",
    }

    if not required_cols.issubset(prev.columns) or not required_cols.issubset(
        curr.columns
    ):
        raise RuntimeError("Snapshot files missing required columns")

    merged = curr.merge(
        prev,
        on="id",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    merged["price_change"] = (
        merged["now_cost_curr"] - merged["now_cost_prev"]
    ) / 10.0

    merged["transfers_in_delta"] = (
        merged["total_transfers_in_curr"]
        - merged["total_transfers_in_prev"]
    )

    merged["transfers_out_delta"] = (
        merged["total_transfers_out_curr"]
        - merged["total_transfers_out_prev"]
    )

    merged["net_transfers_delta"] = (
        merged["transfers_in_delta"] - merged["transfers_out_delta"]
    )

    result = merged[
        [
            "id",
            "web_name_curr",
            "now_cost_curr",
            "price_change",
            "transfers_in_delta",
            "transfers_out_delta",
            "net_transfers_delta",
        ]
    ].rename(
        columns={
            "web_name_curr": "name",
            "now_cost_curr": "price",
        }
    )

    return result.sort_values(
        by="net_transfers_delta", ascending=False
    ).reset_index(drop=True)


def main():
    prev_path, curr_path = load_latest_snapshots()

    if prev_path is None or curr_path is None:
        return

    deltas = compute_deltas(prev_path, curr_path)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    deltas.to_csv(OUTPUT_PATH, index=False)

    print(f"✅ Delta file created: {OUTPUT_PATH}")
    print(f"📊 Players processed: {len(deltas)}")


if __name__ == "__main__":
    main()
