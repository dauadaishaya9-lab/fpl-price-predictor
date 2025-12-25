from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")


def safe_read_csv(path: Path):
    """
    Safely read CSV.
    Returns empty DataFrame if file is missing or empty.
    """
    if not path.exists():
        return pd.DataFrame()

    if path.stat().st_size == 0:
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()


def main():
    # ---- Load predictions ----
    preds = safe_read_csv(PREDICTIONS_PATH)

    if preds.empty:
        print("ℹ️ predictions.csv empty — nothing to store")
        return

    required_cols = {
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

    missing = required_cols - set(preds.columns)
    if missing:
        print(f"⚠️ predictions.csv missing columns: {missing}")
        return

    # ---- Prepare today's snapshot ----
    today = datetime.utcnow().date().isoformat()
    preds = preds.copy()
    preds["date"] = today

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

    # ---- Load history safely ----
    history = safe_read_csv(HISTORY_PATH)

    if history.empty:
        combined = preds
    else:
        combined = pd.concat([history, preds], ignore_index=True)

    HISTORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(HISTORY_PATH, index=False)

    print(f"🗃️ Stored {len(preds)} predictions")
    print(f"📈 Total history rows: {len(combined)}")


if __name__ == "__main__":
    main()
