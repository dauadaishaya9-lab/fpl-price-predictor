from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions/stored_predictions.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
OUT_PATH = Path("data/accuracy.csv")


def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def main():
    preds = safe_read_csv(PREDICTIONS_PATH)
    actuals = safe_read_csv(OUTCOMES_PATH)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to compute accuracy")
        return

    # ---------------------
    # Required columns
    # ---------------------
    pred_cols = {"player_id", "date", "direction"}
    act_cols = {"player_id", "date", "actual_change"}

    if not pred_cols.issubset(preds.columns):
        print("⚠️ Predictions missing required columns")
        return

    if not act_cols.issubset(actuals.columns):
        print("⚠️ Outcomes missing required columns")
        return

    # ---------------------
    # Date parsing
    # ---------------------
    preds["date"] = pd.to_datetime(preds["date"]).dt.date
    actuals["date"] = pd.to_datetime(actuals["date"]).dt.date

    # ---------------------
    # STRICT separation
    # prediction_date < outcome_date
    # ---------------------
    merged = preds.merge(
        actuals,
        on="player_id",
        how="inner",
        suffixes=("_pred", "_actual"),
    )

    merged = merged[
        merged["date_actual"] > merged["date_pred"]
    ]

    if merged.empty:
        print("ℹ️ No valid prediction → outcome pairs yet")
        return

    # ---------------------
    # Accuracy
    # ---------------------
    merged["correct"] = (
        merged["direction"] == merged["actual_change"]
    )

    accuracy = (
        merged
        .groupby("date_pred")["correct"]
        .agg(
            total_predictions="count",
            correct_predictions="sum",
        )
        .reset_index()
    )

    accuracy["accuracy"] = (
        accuracy["correct_predictions"]
        / accuracy["total_predictions"]
    ).round(3)

    # ---------------------
    # Save
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    accuracy.to_csv(OUT_PATH, index=False)

    print("📈 Accuracy report updated")
    print(accuracy.tail())


if __name__ == "__main__":
    main()
