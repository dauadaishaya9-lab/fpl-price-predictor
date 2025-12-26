from pathlib import Path
import pandas as pd
from datetime import date

HISTORY_PATH = Path("data/predictions_history.csv")

def main():
    if not HISTORY_PATH.exists():
        raise RuntimeError("❌ predictions_history.csv missing")

    df = pd.read_csv(HISTORY_PATH)

    if df.empty:
        raise RuntimeError("❌ predictions_history.csv is empty")

    today = date.today().isoformat()
    today_preds = df[df["date"] == today]

    if today_preds.empty:
        raise RuntimeError("❌ No predictions for today")

    print(f"📊 Daily summary: {len(today_preds)} predictions")

    # Telegram send logic here

if __name__ == "__main__":
    main()
