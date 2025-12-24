from pathlib import Path
import pandas as pd
import json

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")  # actual rises/falls
THRESHOLDS_PATH = Path("data/thresholds.json")

# =====================
# Bucketing
# =====================
def ownership_bucket(ownership):
    if ownership < 5:
        return "low"
    elif ownership < 15:
        return "mid_low"
    elif ownership < 30:
        return "mid_high"
    else:
        return "high"


# =====================
# Main
# =====================
def main():
    if not PREDICTIONS_PATH.exists() or not OUTCOMES_PATH.exists():
        print("ℹ️ Missing history or outcomes — skipping learning")
        return

    preds = pd.read_csv(PREDICTIONS_PATH)
    actuals = pd.read_csv(OUTCOMES_PATH)

    df = preds.merge(
        actuals,
        on=["player_id", "date"],
        how="inner"
    )

    df["bucket"] = df["ownership"].apply(ownership_bucket)
    df["correct"] = df["direction"] == df["actual_change"]

    thresholds = {}

    for bucket in df["bucket"].unique():
        thresholds[bucket] = {}

        for direction in ["rise", "fall"]:
            subset = df[
                (df["bucket"] == bucket) &
                (df["direction"] == direction)
            ]

            if subset.empty:
                thresholds[bucket][direction] = {
                    "imminent": 0.7,
                    "warming": 0.4
                }
                continue

            # Successful predictions only
            correct = subset[subset["correct"]]

            imminent = correct["confidence"].quantile(0.75)
            warming = correct["confidence"].quantile(0.40)

            thresholds[bucket][direction] = {
                "imminent": round(float(imminent), 3),
                "warming": round(float(warming), 3),
            }

    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    THRESHOLDS_PATH.write_text(json.dumps(thresholds, indent=2))

    print("🧠 Ownership-aware thresholds learned")
    print(json.dumps(thresholds, indent=2))


if __name__ == "__main__":
    main()
