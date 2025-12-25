from pathlib import Path
import pandas as pd
import json

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")


# =====================
# Helpers
# =====================
def safe_read_csv(path: Path):
    if not path.exists():
        return pd.DataFrame()
    if path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


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
    preds = safe_read_csv(PREDICTIONS_PATH)
    actuals = safe_read_csv(OUTCOMES_PATH)

    # ---- Cold start protection ----
    if preds.empty:
        print("ℹ️ No prediction history yet — learning skipped")
        return

    if actuals.empty:
        print("ℹ️ No price outcomes yet — learning skipped")
        return

    df = preds.merge(
        actuals,
        on=["player_id", "date"],
        how="inner"
    )

    if df.empty:
        print("ℹ️ No matched predictions/outcomes yet — learning skipped")
        return

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
                    "warming": 0.4,
                }
                continue

            correct = subset[subset["correct"]]

            if correct.empty:
                thresholds[bucket][direction] = {
                    "imminent": 0.75,
                    "warming": 0.45,
                }
                continue

            imminent = correct["confidence"].quantile(0.75)
            warming = correct["confidence"].quantile(0.40)

            thresholds[bucket][direction] = {
                "imminent": round(float(imminent), 3),
                "warming": round(float(warming), 3),
            }

    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    THRESHOLDS_PATH.write_text(json.dumps(thresholds, indent=2))

    print("🧠 Threshold learning complete")
    print(json.dumps(thresholds, indent=2))


if __name__ == "__main__":
    main()
