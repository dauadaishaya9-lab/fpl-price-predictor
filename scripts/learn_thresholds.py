from pathlib import Path
import pandas as pd
import json

PREDS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")

def main():
    if not PREDS_PATH.exists() or not OUTCOMES_PATH.exists():
        print("ℹ️ Not enough data to learn thresholds")
        return

    preds = pd.read_csv(PREDS_PATH)
    actuals = pd.read_csv(OUTCOMES_PATH)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to learn thresholds")
        return

    preds["date"] = pd.to_datetime(preds["date"]).dt.date
    actuals["date"] = pd.to_datetime(actuals["date"]).dt.date

    merged = preds.merge(
        actuals,
        on="player_id",
        suffixes=("_pred", "_actual")
    )

    merged = merged[merged["date_actual"] > merged["date_pred"]]

    if merged.empty:
        print("ℹ️ No prediction → outcome pairs yet")
        return

    merged["hit"] = merged["direction"] == merged["actual_change"]

    thresholds = {}
    for (bucket, direction), g in merged.groupby(["ownership_bucket", "direction"]):
        thresholds.setdefault(bucket, {})[direction] = round(g["hit"].mean(), 2)

    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(THRESHOLDS_PATH, "w") as f:
        json.dump(thresholds, f, indent=2)

    print("🧠 Threshold learning complete")

if __name__ == "__main__":
    main()
