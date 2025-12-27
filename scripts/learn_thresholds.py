from pathlib import Path
import pandas as pd
import json

PREDS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")

def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()

def main():
    preds = safe_read_csv(PREDS_PATH)
    actuals = safe_read_csv(OUTCOMES_PATH)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to learn thresholds")
        return

    required_preds = {
        "player_id",
        "date",
        "direction",
        "signal",
        "ownership_bucket",
    }

    required_actuals = {
        "player_id",
        "date",
        "actual_change",
    }

    if not required_preds.issubset(preds.columns):
        print("⚠️ Predictions missing required columns")
        return

    if not required_actuals.issubset(actuals.columns):
        print("⚠️ Outcomes missing required columns")
        return

    preds["date"] = pd.to_datetime(preds["date"]).dt.date
    actuals["date"] = pd.to_datetime(actuals["date"]).dt.date

    merged = preds.merge(
        actuals,
        on="player_id",
        suffixes=("_pred", "_actual"),
        how="inner",
    )

    # 🔒 STRICT CAUSALITY
    merged = merged[
        merged["date_actual"] > merged["date_pred"]
    ]

    if merged.empty:
        print("ℹ️ No valid prediction → outcome pairs yet")
        return

    merged["hit"] = merged["direction"] == merged["actual_change"]

    thresholds = {}

    grouped = merged.groupby(
        ["ownership_bucket", "direction", "signal"]
    )

    for (bucket, direction, signal), g in grouped:
        hit_rate = round(g["hit"].mean(), 2)
        thresholds.setdefault(bucket, {}) \
                  .setdefault(direction, {})[signal] = hit_rate

    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(THRESHOLDS_PATH, "w") as f:
        json.dump(thresholds, f, indent=2)

    print("🧠 Threshold learning complete")
    print(json.dumps(thresholds, indent=2))

if __name__ == "__main__":
    main()
