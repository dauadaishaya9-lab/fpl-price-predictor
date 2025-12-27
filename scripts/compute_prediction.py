from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
SNAPSHOT_DIR = Path("data/snapshots")
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

# =====================
# Main
# =====================
def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))

    if not snapshots:
        print("ℹ️ No snapshots found")
        return

    snap = safe_read_csv(snapshots[-1])

    required = {
        "player_id",
        "web_name",
        "price",
        "ownership",
        "net_transfers_delta",
        "velocity",
        "trend_score",
    }

    if not required.issubset(snap.columns):
        print("⚠️ Snapshot missing required columns")
        return

    df = snap.copy()

    # ---------------------
    # 🔑 CORE FIX: TRANSFER PRESSURE
    # ---------------------
    df["transfer_pressure"] = (
        df["net_transfers_delta"]
        / df["ownership"].clip(lower=0.1)
    )

    # ---------------------
    # SCORE
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
    # SIGNAL
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

    predictions = df[out_cols]

    # ---------------------
    # SAVE DAILY + HISTORY
    # ---------------------
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUT_PATH, index=False)

    history = safe_read_csv(HISTORY_PATH)
    history = pd.concat([history, predictions], ignore_index=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"🔮 Predictions updated: {len(predictions)} players")

if __name__ == "__main__":
    main()
