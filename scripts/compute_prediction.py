from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
FEATURE_DIR = Path("data/features")
OUT_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")

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

def latest_feature_file() -> Path | None:
    files = sorted(FEATURE_DIR.glob("features_*.csv"))
    return files[-1] if files else None

# =====================
# Main
# =====================
def main():
    feature_path = latest_feature_file()

    if feature_path is None:
        print("⚠️ No feature files found. Did build_features.py run?")
        return

    df = safe_read_csv(feature_path)

    if df.empty:
        print("⚠️ Feature file is empty")
        return

    # ---------------------
    # Required feature set
    # ---------------------
    required = {
        "player_id",
        "web_name",
        "price",
        "ownership",
        "net_transfers_delta",
        "velocity",
        "trend_score",
    }

    if not required.issubset(df.columns):
        print("⚠️ Feature table missing required columns")
        return

    # ---------------------
    # 🔑 TRANSFER PRESSURE
    # ---------------------
    df["transfer_pressure"] = (
        df["net_transfers_delta"]
        / df["ownership"].clip(lower=0.1)
    )

    # ---------------------
    # PREDICTION SCORE
    # ---------------------
    df["prediction_score"] = (
        0.6 * df["transfer_pressure"]
        + 0.25 * df["velocity"]
        + 0.15 * df["trend_score"]
    )

    # ---------------------
    # DIRECTION
    # ---------------------
    df["direction"] = "none"
    df.loc[df["prediction_score"] > 0, "direction"] = "rise"
    df.loc[df["prediction_score"] < 0, "direction"] = "fall"

    # ---------------------
    # SIGNAL STRENGTH
    # ---------------------
    df["signal"] = "weak"
    df.loc[df["prediction_score"].abs() > 1.0, "signal"] = "building"
    df.loc[df["prediction_score"].abs() > 2.0, "signal"] = "strong"
    df.loc[df["prediction_score"].abs() > 3.0, "signal"] = "imminent"

    # ---------------------
    # CONFIDENCE
    # ---------------------
    df["confidence"] = df["prediction_score"].abs().clip(0, 5).round(2)

    # ---------------------
    # OWNERSHIP BUCKET
    # ---------------------
    df["ownership_bucket"] = pd.cut(
        df["ownership"],
        bins=[0, 2, 5, 10, 20, 100],
        labels=["0-2%", "2-5%", "5-10%", "10-20%", "20%+"],
    )

    # ---------------------
    # OUTPUT
    # ---------------------
    today = datetime.utcnow().date().isoformat()
    df["date"] = today

    out_cols = [
        "date",
        "player_id",
        "web_name",
        "direction",
        "confidence",
        "prediction_score",
        "signal",
        "velocity",
        "net_transfers_delta",
        "transfer_pressure",
        "ownership",
        "ownership_bucket",
    ]

    predictions = df[out_cols].copy()

    # ---------------------
    # SAVE
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUT_PATH, index=False)

    history = safe_read_csv(HISTORY_PATH)
    history = pd.concat([history, predictions], ignore_index=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"🔮 Predictions generated from features: {feature_path.name}")
    print(f"📊 Players scored: {len(predictions)}")

if __name__ == "__main__":
    main()
