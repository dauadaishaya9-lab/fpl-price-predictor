from pathlib import Path
import pandas as pd
import json

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions/stored_predictions.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")


# =====================
# Helpers
# =====================
def safe_read_csv(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


# =====================
# Main
# =====================
def main():
    preds = safe_read_csv(PREDICTIONS_PATH)
    actuals = safe_read_csv(OUTCOMES_PATH)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to learn thresholds")
        return

    # ---- REQUIRED COLUMNS ----
    pred_cols = {"player_id", "date", "bucket", "signal", "direction"}
    act_cols = {"player_id", "date", "actual_change"}

    if not pred_cols.issubset(preds.columns):
        print("⚠️ Predictions missing required columns")
        return

    if not act_cols.issubset(actuals.columns):
        print("⚠️ Outcomes missing required columns")
        return

    # ---- DATE PARSING ----
    preds["date"] = pd.to_datetime(preds["date"]).dt.date
    actuals["date"] = pd.to_datetime(actuals["date"]).dt.date

    # ---- STRICT SEPARATION ----
    # Only learn when: outcome_date > prediction_date
    merged = preds.merge(
        actuals,
        on="player_id",
        how="inner",
        suffixes=("_pred", "_actual")
    )

    merged = merged[
        merged["date_actual"] > merged["date_pred"]
    ]

    if merged.empty:
        print("ℹ️ No valid prediction → outcome pairs yet")
        return

    # ---- HIT FLAG ----
    merged["hit"] = merged["direction"] == merged["actual_change"]

    # =====================
    # LEARNING
    # =====================
    thresholds = {}

    grouped = merged.groupby(["bucket", "direction", "signal"])

    for (bucket, direction, signal), group in grouped:
        hit_rate = group["hit"].mean()

        thresholds.setdefault(bucket, {})
        thresholds[bucket].setdefault(direction, {})
        thresholds[bucket][direction][signal] = round(hit_rate, 2)

    # =====================
    # SAVE
    # =====================
    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(THRESHOLDS_PATH, "w") as f:
        json.dump(thresholds, f, indent=2)

    print("🧠 Threshold learning complete")
    print(json.dumps(thresholds, indent=2))


if __name__ == "__main__":
    main()
