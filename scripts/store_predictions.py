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

    # ---- Timestamp for this run ----
    run_time = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    preds["run_timestamp"] = run_time

    # ---- Keep only stable columns ----
    keep_cols = [
        "player_id",
        "name",
        "direction",
        "confidence",
        "prediction_score",
        "ownership",
        "run_timestamp",
    ]

    preds = preds[keep_cols]

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)

    # ---- Append mode (never overwrite) ----
    if HISTORY_PATH.exists() and HISTORY_PATH.stat().st_size > 0:
        history = pd.read_csv(HISTORY_PATH)
        history = pd.concat([history, preds], ignore_index=True)
    else:
        history = preds

    history.to_csv(HISTORY_PATH, index=False)

    print(f"🧠 Stored {len(preds)} prediction rows")


if __name__ == "__main__":
    main()
