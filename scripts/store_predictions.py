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
    # ---- Guard: predictions must exist ----
    if not PREDICTIONS_PATH.exists():
        print("ℹ️ predictions.csv missing — nothing to store")
        return

    preds = pd.read_csv(PREDICTIONS_PATH)

    if preds.empty:
        print("ℹ️ predictions.csv empty — nothing to store")
        return

    # ---- Required columns (matches your actual file) ----
    required_cols = {
        "player_id",
        "name",
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

    missing = required_cols - set(preds.columns)
    if missing:
        print(f"⚠️ predictions.csv missing columns: {missing}")
        return

    # ---- Add date stamp ----
    preds = preds.copy()
    preds["date"] = datetime.utcnow().date().isoformat()

    # ---- Column order (stable history schema) ----
    keep_cols = [
        "date",
        "player_id",
        "name",
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
    preds = preds[keep_cols]

    # ---- Load existing history SAFELY ----
    if HISTORY_PATH.exists() and HISTORY_PATH.stat().st_size > 0:
        history = pd.read_csv(HISTORY_PATH)
        history = pd.concat([history, preds], ignore_index=True)
    else:
        history = preds

    # ---- Save ----
    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"🗃️ Stored {len(preds)} predictions")
    print(f"📈 Total history rows: {len(history)}")


if __name__ == "__main__":
    main()
