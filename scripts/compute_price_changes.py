from pathlib import Path
import pandas as pd

SNAPSHOT_DIR = Path("data/snapshots")
OUTPUT_PATH = Path("data/price_changes.csv")


def extract_date(path: Path):
    # snapshot_YYYY-MM-DD_HH-MM-SS.csv → YYYY-MM-DD
    return path.stem.split("_")[1]


def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if len(snapshots) < 2:
        print("ℹ️ Not enough snapshots to compute price changes")
        return

    prev = pd.read_csv(snapshots[-2])
    curr = pd.read_csv(snapshots[-1])

    date = extract_date(snapshots[-1])

    df = curr.merge(
        prev[["player_id", "price"]],
        on="player_id",
        suffixes=("_curr", "_prev"),
        how="inner"
    )

    def classify(row):
        if row["price_curr"] > row["price_prev"]:
            return "rise"
        elif row["price_curr"] < row["price_prev"]:
            return "fall"
        else:
            return "none"

    df["actual_change"] = df.apply(classify, axis=1)
    df["date"] = date

    result = df[["player_id", "date", "actual_change"]]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUTPUT_PATH.exists():
        existing = pd.read_csv(OUTPUT_PATH)
        result = pd.concat([existing, result], ignore_index=True)
        result = result.drop_duplicates(
            subset=["player_id", "date"],
            keep="last"
        )

    result.to_csv(OUTPUT_PATH, index=False)

    print(f"📉 Price changes recorded for {date}")


if __name__ == "__main__":
    main()
