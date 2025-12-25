from pathlib import Path
import pandas as pd
import json

HISTORY_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")

def ownership_bucket(o):
    if o < 5:
        return "low"
    elif o < 15:
        return "mid_low"
    elif o < 30:
        return "mid_high"
    return "high"

def main():
    if not HISTORY_PATH.exists() or HISTORY_PATH.stat().st_size == 0:
        print("ℹ️ No prediction history yet — skipping learning")
        return

    if not OUTCOMES_PATH.exists() or OUTCOMES_PATH.stat().st_size == 0:
        print("ℹ️ No price outcomes yet — skipping learning")
        return

    preds = pd.read_csv(HISTORY_PATH)
    actuals = pd.read_csv(OUTCOMES_PATH)

    df = preds.merge(
        actuals,
        on=["player_id", "date"],
        how="inner"
    )

    if df.empty:
        print("ℹ️ No matched prediction/outcome rows — skipping learning")
        return

    df["bucket"] = df["ownership"].apply(ownership_bucket)
    df["correct"] = df["direction"] == df["actual_change"]

    thresholds = {}

    for bucket in df["bucket"].unique():
        thresholds[bucket] = {}

        for direction in ["rise", "fall"]:
            subset = df[
                (df["bucket"] == bucket) &
                (df["direction"] == direction) &
                (df["correct"])
            ]

            if subset.empty:
                thresholds[bucket][direction] = {
                    "imminent": 0.75,
                    "warming": 0.45
                }
                continue

            thresholds[bucket][direction] = {
                "imminent": round(subset["confidence"].quantile(0.75), 3),
                "warming": round(subset["confidence"].quantile(0.40), 3),
            }

    THRESHOLDS_PATH.write_text(json.dumps(thresholds, indent=2))
    print("🧠 Thresholds learned and saved")

if __name__ == "__main__":
    main()
