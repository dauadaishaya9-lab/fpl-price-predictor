from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")

def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    if not snapshots:
        print("ℹ️ No snapshots found")
        return

    path = snapshots[-1]
    df = pd.read_csv(path)

    if "net_transfers_delta" not in df.columns:
        print("⚠️ net_transfers_delta missing")
        return

    df["velocity"] = df["net_transfers_delta"].rolling(
        window=3, min_periods=1
    ).mean()

    df.to_csv(path, index=False)

    print("✅ Velocity added to snapshot")


if __name__ == "__main__":
    main()
