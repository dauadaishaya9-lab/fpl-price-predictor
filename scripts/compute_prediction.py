from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
SNAPSHOT_DIR = Path("data/snapshots")
OUT_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")
PRICE_CHANGES_PATH = Path("data/price_changes.csv")

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

def detect_market_bias(price_changes: pd.DataFrame) -> str:
    """
    Determine overall daily market regime.
    """
    if price_changes.empty:
        return "neutral"

    counts = price_changes["actual_change"].value_counts(normalize=True)

    rises = counts.get("rise", 0)
    falls = counts.get("fall", 0)

    if rises >= 0.65:
        return "bullish"
    if falls >= 0.65:
        return "bearish"

    return "neutral"

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

    # =====================
    # MARKET REGIME
    # =====================
    price_changes = safe_read_csv(PRICE_CHANGES_PATH)
    market_bias = detect_market_bias(price_changes)

    print(f"🌍 Market regime detected: {market_bias}")

    # =====================
    # TRANSFER PRESSURE
    # =====================
    df["transfer_pressure"] = (
        df["net_transfers_delta"]
        / df["ownership"].clip(lower=0.1)
    )

    # =====================
    # RAW SCORE
    # =====================
    df["prediction_score"] = (
        0.55 * df["transfer_pressure"]
        + 0.30 * df["velocity"]
        + 0.15 * df["trend_score"]
    )

    # =====================
    # REGIME ADJUSTMENT (🔑 FIX)
    # =====================
    if market_bias == "bullish":
        df.loc[df["prediction_score"] < 0, "prediction_score"] *= 0.4
    elif market_bias == "bearish":
        df.loc[df["prediction_score"] > 0, "prediction_score"] *= 0.4
    else:
        df["prediction_score"] *= 0.7  # neutral = cautious

    # =====================
    # DIRECTION
    # =====================
    df["direction"] = "none"
    df.loc[df["prediction_score"] > 0.5, "direction"] = "rise"
    df.loc[df["prediction_score"] < -0.5, "direction"] = "fall"

    # =====================
    # CONFIDENCE
    # =====================
    df["confidence"] = df["prediction_score"].abs().clip(0, 5).round(2)

    # =====================
    # OWNERSHIP BUCKET
    # =====================
    df["ownership_bucket"] = pd.cut(
        df["ownership"],
        bins=[0, 2, 5, 10, 20, 100],
        labels=["0-2%", "2-5%", "5-10%", "10-20%", "20%+"],
    )

    today = datetime.utcnow().date().isoformat()
    df["date"] = today
    df["market_bias"] = market_bias

    out_cols = [
        "date",
        "player_id",
        "web_name",
        "direction",
        "confidence",
        "prediction_score",
        "velocity",
        "net_transfers_delta",
        "transfer_pressure",
        "ownership",
        "ownership_bucket",
        "market_bias",
    ]

    predictions = df[out_cols]

    # =====================
    # SAVE
    # =====================
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUT_PATH, index=False)

    history = safe_read_csv(HISTORY_PATH)
    history = pd.concat([history, predictions], ignore_index=True)
    history.to_csv(HISTORY_PATH, index=False)

    rises = (predictions["direction"] == "rise").sum()
    falls = (predictions["direction"] == "fall").sum()

    print(f"🔮 Predictions generated: {len(predictions)} players")
    print(f"📈 Rises: {rises} | 📉 Falls: {falls}")

if __name__ == "__main__":
    main()
