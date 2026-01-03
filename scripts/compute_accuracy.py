from pathlib import Path
import pandas as pd
from datetime import timedelta

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions_history.csv")
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
    pred_cols = {"player_id", "date", "direction", "alert_level"}
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
    # ONLY REAL PREDICTIONS
    # ---------------------
    preds = preds[
        (preds["alert_level"] == "imminent") &
        (preds["direction"].isin(["rise", "fall"]))
    ]

    if preds.empty:
        print("ℹ️ No imminent predictions to score")
        return

    # ---------------------
    # One prediction per player per day
    # ---------------------
    preds = (
        preds
        .sort_values("confidence", ascending=False)
        .drop_duplicates(["player_id", "date"])
    )

    # ---------------------
    # Shift outcomes to D+1
    # ---------------------
    actuals = actuals.rename(columns={"date": "outcome_date"})
    preds["outcome_date"] = preds["date"] + timedelta(days=1)

    # ---------------------
    # Join: prediction day D → outcome day D+1
    # ---------------------
    merged = preds.merge(
        actuals,
        on=["player_id", "outcome_date"],
        how="left",
    )

    # ---------------------
    # Score correctness
    # ---------------------
    merged["correct"] = (
        merged["direction"] == merged["actual_change"]
    )

    # ---------------------
    # Accuracy per prediction day
    # ---------------------
    accuracy = (
        merged
        .groupby("date")
        .agg(
            predicted=("player_id", "count"),
            correct=("correct", "sum"),
        )
        .reset_index()
        .rename(columns={"date": "date_pred"})
    )

    accuracy["accuracy"] = (
        accuracy["correct"] / accuracy["predicted"]
    ).round(3)

    # ---------------------
    # Save
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    accuracy.to_csv(OUT_PATH, index=False)

    print("📈 Accuracy report updated (D → D+1 strict)")
    print(accuracy.tail())


if __name__ == "__main__":
    main()
