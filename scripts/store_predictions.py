from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")

def main():
    if not PREDICTIONS_PATH.exists():
        print("ℹ️ predictions.csv missing — nothing to store")
        return

    preds = pd.read_csv(PREDICTIONS_PATH)

    if preds.empty:
        print("ℹ️ predictions.csv empty — nothing to store")
        return

    # ---- REQUIRED COLUMNS (MATCH REAL FILE) ----
    required = {
        "player_id",
        "web_name",
        "prediction_score",
        "direction",
        "confidence",
        "alert_level",
        "trend_score",
        "velocity",
        "net_transfers",
        "ownership",
        "ownership_bucket",
    }

    missing = required - set(preds.columns)
    if missing:
        print(f"⚠️ predictions.csv missing columns: {missing}")
        return

    preds = preds.copy()
    preds["date"] = datetime.utcnow().date().isoformat()

    preds = preds[
        [
            "date",
            "player_id",
            "web_name",
            "direction",
            "confidence",
            "prediction_score",
            "alert_level",
            "trend_score",
            "velocity",
            "net_transfers",
            "ownership",
            "ownership_bucket",
        ]
    ]

    # ---- SAFE HISTORY LOAD ----
    if HISTORY_PATH.exists() and HISTORY_PATH.stat().st_size > 0:
        history = pd.read_csv(HISTORY_PATH)
        history = pd.concat([history, preds], ignore_index=True)
    else:
        history = preds

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"🗃️ Stored {len(preds)} predictions")
    print(f"📈 History rows: {len(history)}")

if __name__ == "__main__":
    main()
