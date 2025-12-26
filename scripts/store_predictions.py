from pathlib import Path
import pandas as pd
from datetime import datetime

PREDICTIONS_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")

REQUIRED_COLUMNS = {
    "player_id",
    "direction",
    "confidence",
    "ownership_bucket",
}

def main():
    if not PREDICTIONS_PATH.exists():
        raise RuntimeError("❌ data/predictions.csv does not exist")

    preds = pd.read_csv(PREDICTIONS_PATH)

    if preds.empty:
        raise RuntimeError("❌ predictions.csv is empty")

    missing = REQUIRED_COLUMNS - set(preds.columns)
    if missing:
        raise RuntimeError(f"❌ predictions.csv missing columns: {missing}")

    preds = preds.copy()
    preds["date"] = datetime.utcnow().date().isoformat()

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    if HISTORY_PATH.exists():
        history = pd.read_csv(HISTORY_PATH)
        combined = pd.concat([history, preds], ignore_index=True)
    else:
        combined = preds

    combined.to_csv(HISTORY_PATH, index=False)

    print(f"🧠 Stored {len(preds)} predictions")

if __name__ == "__main__":
    main()
