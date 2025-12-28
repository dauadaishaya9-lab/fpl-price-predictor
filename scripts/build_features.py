from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")
FEATURE_DIR = Path("data/features")
FEATURE_DIR.mkdir(parents=True, exist_ok=True)

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def main():
    snaps = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snaps) < 2:
        print("ℹ️ Not enough snapshots to build features")
        return

    curr = safe_read_csv(snaps[-1])
    prev = safe_read_csv(snaps[-2])

    required = {
        "player_id",
        "price",
        "ownership",
        "transfers_in_event",
        "transfers_out_event",
        "web_name",
    }

    if not required.issubset(curr.columns):
        print("⚠️ Snapshot missing required columns")
        return

    df = curr.merge(
        prev[
            [
                "player_id",
                "price",
                "transfers_in_event",
                "transfers_out_event",
            ]
        ],
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="left",
    )

    # ---------------------
    # FEATURES
    # ---------------------
    df["net_transfers_delta"] = (
        df["transfers_in_event_curr"]
        - df["transfers_out_event_curr"]
        - df["transfers_in_event_prev"].fillna(0)
        + df["transfers_out_event_prev"].fillna(0)
    )

    df["velocity"] = (
        df["net_transfers_delta"]
        / df["ownership"].clip(lower=0.1)
    )

    df["trend_score"] = (
        df["price_curr"] - df["price_prev"].fillna(df["price_curr"])
    )

    out_cols = [
        "player_id",
        "web_name",
        "price_curr",
        "ownership",
        "net_transfers_delta",
        "velocity",
        "trend_score",
    ]

    features = df[out_cols].rename(columns={"price_curr": "price"})

    out_path = FEATURE_DIR / f"features_{snaps[-1].stem.replace('snapshot_', '')}.csv"
    features.to_csv(out_path, index=False)

    print(f"🧠 Features built: {out_path.name}")

if __name__ == "__main__":
    main()
