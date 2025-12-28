from pathlib import Path
import pandas as pd
import json

# =====================
# Paths
# =====================
PREDS_PATH = Path("data/predictions_history.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")


# =====================
# Helpers
# =====================
def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


# =====================
# Main
# =====================
def main():
    preds = safe_read_csv(PREDS_PATH)
    actuals = safe_read_csv(OUTCOMES_PATH)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to learn thresholds")
        return

    # ---------------------
    # Required columns
    # ---------------------
    pred_cols = {
        "player_id",
        "date",
        "direction",
        "alert_level",        # ✅ correct column
        "ownership_bucket",
    }

    act_cols = {
        "player_id",
        "date",
        "actual_change",
    }

    if not pred_cols.issubset(preds.columns):
        print("⚠️ Predictions missing required columns")
        print("Found:", list(preds.columns))
        return

    if not act_cols.issubset(actuals.columns):
        print("⚠️ Outcomes missing required columns")
        return

    # ---------------------
    # Date parsing
    # ---------------------
    preds["date"] = pd.to_datetime(preds["date"], errors="coerce").dt.date
    actuals["date"] = pd.to_datetime(actuals["date"], errors="coerce").dt.date

    preds = preds.dropna(subset=["date"])
    actuals = actuals.dropna(subset=["date"])

    # ---------------------
    # STRICT CAUSAL JOIN
    # prediction_date < outcome_date
    # ---------------------
    merged = preds.merge(
        actuals,
        on="player_id",
        how="inner",
        suffixes=("_pred", "_actual"),
    )

    merged = merged[
        merged["date_actual"] > merged["date_pred"]
    ]

    if merged.empty:
        print("ℹ️ No valid prediction → outcome pairs yet")
        return

    # ---------------------
    # Hit / Miss
    # ---------------------
    merged["hit"] = (
        merged["direction"] == merged["actual_change"]
    )

    # ---------------------
    # LEARN THRESHOLDS
    # ---------------------
    thresholds = {}

    grouped = merged.groupby(
        ["ownership_bucket", "direction", "alert_level"]
    )

    for (bucket, direction, alert), g in grouped:
        hit_rate = round(g["hit"].mean(), 2)

        thresholds \
            .setdefault(bucket, {}) \
            .setdefault(direction, {})[alert] = hit_rate

    # ---------------------
    # Save
    # ---------------------
    THRESHOLDS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(THRESHOLDS_PATH, "w") as f:
        json.dump(thresholds, f, indent=2)

    print("🧠 Threshold learning complete")
    print(json.dumps(thresholds, indent=2))


if __name__ == "__main__":
    main()
