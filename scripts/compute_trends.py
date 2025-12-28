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

    if "velocity" not in df.columns:
        print("⚠️ velocity missing")
        return

    df["trend_score"] = df["velocity"].rolling(
        window=5, min_periods=1
    ).mean()

    df.to_csv(path, index=False)

    print("✅ Trend score added to snapshot")


if __name__ == "__main__":
    main()
