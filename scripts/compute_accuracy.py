from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
PROTECTION_PATH = Path("data/protection_status.csv")
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
    prot = safe_read_csv(PROTECTION_PATH)

    if preds.empty:
        print("ℹ️ No predictions to score")
        return

    # ---------------------
    # Required columns
    # ---------------------
    pred_cols = {"player_id", "date", "direction", "confidence"}
    act_cols = {"player_id", "date", "actual_change"}

    if not pred_cols.issubset(preds.columns):
        print("⚠️ predictions_history.csv missing required columns")
        return

    if actuals.empty or not act_cols.issubset(actuals.columns):
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
    # One prediction per player per day (highest confidence)
    # ---------------------
    preds = (
        preds
        .sort_values("confidence", ascending=False)
        .drop_duplicates(["player_id", "date"])
    )

    # ---------------------
    # Remove neutral / invalid
    # ---------------------
    preds = preds[preds["direction"].isin(["rise", "fall"])]
    actuals = actuals[actuals["actual_change"].isin(["rise", "fall"])]

    if preds.empty:
        print("ℹ️ No valid predictions after filtering")
        return

    # ---------------------
    # Exclude protected players
    # ---------------------
    if not prot.empty and {"player_id", "lock_until"}.issubset(prot.columns):
        prot["lock_until"] = pd.to_datetime(
            prot["lock_until"], errors="coerce"
        ).dt.date

        preds = preds.merge(prot, on="player_id", how="left")

        preds = preds[
            preds["lock_until"].isna()
            | (preds["date"] > preds["lock_until"])
        ]

        preds = preds.drop(columns=["lock_until"], errors="ignore")

    if preds.empty:
        print("ℹ️ No valid predictions after protection filtering")
        return

    # ---------------------
    # Total predictions per day (DENOMINATOR)
    # ---------------------
    totals = (
        preds
        .groupby("date")
        .size()
        .reset_index(name="predicted")
        .rename(columns={"date": "date_pred"})
    )

    # ---------------------
    # Match predictions to outcomes
    # STRICT: outcome must be D+1 or later
    # ---------------------
    merged = preds.merge(
        actuals,
        on="player_id",
        how="left",
        suffixes=("_pred", "_actual"),
    )

    merged = merged[
        merged["date_actual"] >= (merged["date_pred"] + pd.Timedelta(days=1))
    ]

    # ---------------------
    # Correctness
    # ---------------------
    merged["correct"] = (
        merged["direction"] == merged["actual_change"]
    )

    # One success per prediction max
    resolved = (
        merged[merged["correct"]]
        .drop_duplicates(["player_id", "date_pred"])
        .groupby("date_pred")
        .size()
        .reset_index(name="correct")
    )

    # ---------------------
    # Final accuracy table
    # ---------------------
    accuracy = totals.merge(resolved, on="date_pred", how="left")
    accuracy["correct"] = accuracy["correct"].fillna(0).astype(int)

    accuracy["accuracy"] = (
        accuracy["correct"] / accuracy["predicted"]
    ).round(3)

    # ---------------------
    # Save
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    accuracy.to_csv(OUT_PATH, index=False)

    print("📈 Accuracy report updated (precision-based)")
    print(accuracy.tail())


if __name__ == "__main__":
    main()
