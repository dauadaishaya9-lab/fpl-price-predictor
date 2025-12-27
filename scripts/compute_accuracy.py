from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
OUT_PATH = Path("data/accuracy.csv")


def safe_read_csv(path: Path) -> pd.DataFrame:
    """
    Read CSV safely.
    Returns empty DataFrame if file is missing, empty, or unreadable.
    """
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
        print("⚠️ predictions_history.csv missing required columns")
        return

    if not act_cols.issubset(actuals.columns):
        print("⚠️ price_changes.csv missing required columns")
        return

    # ---------------------
    # Date parsing
    # ---------------------
    preds["date"] = pd.to_datetime(preds["date"], errors="coerce").dt.date
    actuals["date"] = pd.to_datetime(actuals["date"], errors="coerce").dt.date

    preds = preds.dropna(subset=["date"])
    actuals = actuals.dropna(subset=["date"])

    # ---------------------
    # Join predictions → outcomes
    # STRICT: prediction must come BEFORE price change
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
    # Accuracy calculation
    # ---------------------
    merged["correct"] = (
        merged["direction"] == merged["actual_change"]
    )

    accuracy = (
        merged
        .groupby("date_pred")
        .agg(
            total_predictions=("correct", "count"),
            correct_predictions=("correct", "sum"),
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
