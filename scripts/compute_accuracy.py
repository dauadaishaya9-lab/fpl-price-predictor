from pathlib import Path
import pandas as pd

PRED_HISTORY = Path("data/predictions_history.csv")
PRICE_CHANGES = Path("data/price_changes.csv")
OUTPUT_PATH = Path("data/accuracy_report.csv")


def main():
    if not PRED_HISTORY.exists() or not PRICE_CHANGES.exists():
        print("ℹ️ Missing history or price changes — skipping accuracy")
        return

    preds = pd.read_csv(PRED_HISTORY)
    actuals = pd.read_csv(PRICE_CHANGES)

    df = preds.merge(
        actuals,
        on=["player_id", "date"],
        how="inner"
    )

    if df.empty:
        print("ℹ️ No overlapping data — skipping accuracy")
        return

    df["correct"] = df["direction"] == df["actual_change"]

    # ---- Daily accuracy ----
    daily = (
        df.groupby("date")
        .agg(
            total_predictions=("correct", "count"),
            correct_predictions=("correct", "sum"),
        )
        .reset_index()
    )

    daily["accuracy"] = (
        daily["correct_predictions"] / daily["total_predictions"]
    ).round(3)

    # ---- Overall ----
    overall_accuracy = round(df["correct"].mean(), 3)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    daily.to_csv(OUTPUT_PATH, index=False)

    print("📊 Accuracy report updated")
    print(f"Overall accuracy: {overall_accuracy}")


if __name__ == "__main__":
    main()
