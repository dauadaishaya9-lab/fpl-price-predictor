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
    # 🔑 ONE PREDICTION PER PLAYER PER DAY
    # ---------------------
    preds = (
        preds
        .sort_values("confidence", ascending=False)
        .drop_duplicates(["player_id", "date"])
    )

    # ---------------------
    # 🛑 REMOVE NEUTRAL / INVALID
    # ---------------------
    preds = preds[preds["direction"].isin(["rise", "fall"])]
    actuals = actuals[actuals["actual_change"].isin(["rise", "fall"])]

    # ---------------------
    # 🛡️ EXCLUDE PROTECTED PLAYERS
    # ---------------------
    if not prot.empty and {"player_id", "lock_until"}.issubset(prot.columns):
        prot["lock_until"] = pd.to_datetime(prot["lock_until"], errors="coerce").dt.date

        preds = preds.merge(
            prot,
            on="player_id",
            how="left",
        )

        preds = preds[
            preds["lock_until"].isna()
            | (preds["date"] > preds["lock_until"])
        ]

        preds = preds.drop(columns=["lock_until"], errors="ignore")

    if preds.empty:
        print("ℹ️ No valid predictions after filtering")
        return

    # ---------------------
    # Join predictions → outcomes
    # STRICT: outcome must be NEXT DAY OR LATER
    # ---------------------
    merged = preds.merge(
        actuals,
        on="player_id",
        how="inner",
        suffixes=("_pred", "_actual"),
    )

    merged = merged[
        merged["date_actual"] >= (merged["date_pred"] + pd.Timedelta(days=1))
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
