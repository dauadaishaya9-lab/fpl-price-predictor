from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")

# =====================
# Main
# =====================
def main():
    if not PREDICTIONS_PATH.exists():
        print("❌ predictions.csv not found")
        return

    preds = pd.read_csv(PREDICTIONS_PATH)

    if preds.empty:
        print("ℹ️ predictions.csv is empty — nothing to store")
        return

    # Required columns (match YOUR schema)
    required_cols = [
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
    ]

    missing = [c for c in required_cols if c not in preds.columns]
    if missing:
        raise ValueError(f"predictions.csv missing columns: {missing}")

    # Keep only required columns
    preds = preds[required_cols].copy()

    # Add date
    preds["date"] = datetime.utcnow().date().isoformat()

    # Append to history
    if HISTORY_PATH.exists():
        history = pd.read_csv(HISTORY_PATH)
        history = pd.concat([history, preds], ignore_index=True)
    else:
        history = preds

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"✅ Stored {len(preds)} predictions into predictions_history.csv")

if __name__ == "__main__":
    main()
