from pathlib import Path
import pandas as pd
from datetime import date

LATEST_PATH = Path("data/latest.csv")
PREDICTIONS_PATH = Path("data/predictions.csv")
OUTCOMES_PATH = Path("data/outcomes.csv")

def main():
    if not LATEST_PATH.exists() or not PREDICTIONS_PATH.exists():
        print("ℹ️ Missing data — skipping outcomes")
        return

    latest = pd.read_csv(LATEST_PATH)
    preds = pd.read_csv(PREDICTIONS_PATH)

    df = latest.merge(preds, on="player_id", how="inner")

    if "price_change" not in latest.columns:
        print("⚠️ latest.csv must contain price_change")
        return

    df["actual_change"] = df["price_change"].apply(
        lambda x: 1 if x > 0 else -1 if x < 0 else 0
    )

    outcomes = df[[
        "player_id",
        "direction",
        "confidence",
        "prediction_score",
        "actual_change"
    ]].copy()

    outcomes["date"] = date.today().isoformat()
    outcomes.rename(columns={"prediction_score": "score"}, inplace=True)

    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)

    if OUTCOMES_PATH.exists():
        existing = pd.read_csv(OUTCOMES_PATH)
        outcomes = pd.concat([existing, outcomes], ignore_index=True)

    OUTCOMES_PATH.write_text(outcomes.to_csv(index=False))
    print(f"🧠 Outcomes logged ({len(outcomes)} rows)")

if __name__ == "__main__":
    main()
