from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
SNAPSHOT_DIR = Path("data/snapshots")
DELTA_DIR = Path("data/deltas")
DELTA_DIR.mkdir(parents=True, exist_ok=True)


# =====================
# Helpers
# =====================
def extract_timestamp(path: Path) -> str:
    # snapshot_YYYY-MM-DD_HH-MM-SS.csv → YYYY-MM-DD_HH-MM-SS
    return path.stem.replace("snapshot_", "")


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def find_previous_price_snapshot(
    snapshots: list[Path],
    latest_df: pd.DataFrame
) -> tuple[Path | None, pd.DataFrame]:
    """
    Walk backwards through snapshots until we find one
    where at least ONE player's price differs.
    """
    latest_prices = latest_df[["player_id", "price"]]

    for path in reversed(snapshots[:-1]):
        df = safe_read_csv(path)
        if df.empty:
            continue

        merged = latest_prices.merge(
            df[["player_id", "price"]],
            on="player_id",
            suffixes=("_latest", "_prev"),
            how="inner",
        )

        if (merged["price_latest"] != merged["price_prev"]).any():
            return path, df

    return None, pd.DataFrame()


# =====================
# Main
# =====================
def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots for deltas")
        return

    curr_path = snapshots[-1]
    curr = safe_read_csv(curr_path)

    required_cols = {
        "player_id",
        "transfers_in_event",
        "transfers_out_event",
        "price",
    }

    if not required_cols.issubset(curr.columns):
        print("⚠️ Latest snapshot missing required columns")
        return

    prev_path, prev = find_previous_price_snapshot(snapshots, curr)

    if prev_path is None or prev.empty:
        print("ℹ️ No earlier snapshot with different prices found")
        return

    merged = curr.merge(
        prev,
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner",
    )

    # ---------------------
    # Transfer delta
    # ---------------------
    merged["net_transfers_delta"] = (
        merged["transfers_in_event_curr"]
        - merged["transfers_out_event_curr"]
        - merged["transfers_in_event_prev"]
        + merged["transfers_out_event_prev"]
    )

    # ---------------------
    # PRICE CHANGE (STATE-BASED ✅)
    # ---------------------
    merged["price_change"] = (
        merged["price_curr"] - merged["price_prev"]
    )

    deltas = merged[
        [
            "player_id",
            "net_transfers_delta",
            "price_change",
        ]
    ].copy()

    deltas["timestamp"] = extract_timestamp(curr_path)

    out_path = DELTA_DIR / f"delta_{extract_timestamp(curr_path)}.csv"
    deltas.to_csv(out_path, index=False)

    print(f"✅ Delta created: {out_path.name}")
    print(f"📊 Players processed: {len(deltas)}")
    print(
        f"💰 Price moves detected: "
        f"{(deltas['price_change'] != 0).sum()}"
    )


if __name__ == "__main__":
    main()
