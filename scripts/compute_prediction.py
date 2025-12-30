from pathlib import Path
import pandas as pd
from datetime import datetime
import json

# =====================
# Paths
# =====================
SNAPSHOT_DIR = Path("data/snapshots")
PROTECTION_PATH = Path("data/protection_status.csv")
OUT_PATH = Path("data/predictions.csv")
HISTORY_PATH = Path("data/predictions_history.csv")
PRICE_CHANGES_PATH = Path("data/price_changes.csv")
THRESHOLD_PATH = Path("data/thresholds.json")

# =====================
# Canonical history schema (CRITICAL)
# =====================
HISTORY_COLUMNS = [
    "date",
    "player_id",
    "web_name",
    "direction",
    "alert_level",
    "confidence",
    "raw_score",
    "prediction_score",
    "velocity",
    "net_transfers_delta",
    "transfer_pressure",
    "ownership",
    "ownership_bucket",
    "market_bias",
    "rise_threshold",
    "fall_threshold",
]

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
    if price_changes.empty:
        return "neutral"

    counts = price_changes["actual_change"].value_counts(normalize=True)
    if counts.get("rise", 0) >= 0.65:
        return "bullish"
    if counts.get("fall", 0) >= 0.65:
        return "bearish"
    return "neutral"

def load_threshold_quantiles():
    if not THRESHOLD_PATH.exists():
        return 0.95, 0.05, False
    try:
        with open(THRESHOLD_PATH) as f:
            data = json.load(f)
        return (
            data.get("rise_quantile", 0.95),
            data.get("fall_quantile", 0.05),
            True,
        )
    except Exception:
        return 0.95, 0.05, False

def normalize_history_schema(df: pd.DataFrame) -> pd.DataFrame:
    for col in HISTORY_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[HISTORY_COLUMNS]

# =====================
# Main
# =====================
def main():
    snapshots = sorted(SNAPSHOT_DIR.glob("snapshot_*.csv"))
    if not snapshots:
        print("ℹ️ No snapshots found")
        return

    snap_path = snapshots[-1]
    df = safe_read_csv(snap_path)

    required = {
        "player_id",
        "web_name",
        "price",
        "ownership",
        "net_transfers_delta",
        "velocity",
        "trend_score",
        "status",
    }

    if not required.issubset(df.columns):
        print("⚠️ Snapshot missing required columns")
        return

    # ---------------------
    # Market regime
    # ---------------------
    price_changes = safe_read_csv(PRICE_CHANGES_PATH)
    market_bias = detect_market_bias(price_changes)
    print(f"🌍 Market regime detected: {market_bias}")

    # ---------------------
    # Raw signal
    # ---------------------
    df["transfer_pressure"] = (
        df["net_transfers_delta"] / df["ownership"].clip(lower=0.1)
    )

    df["raw_score"] = (
        0.55 * df["transfer_pressure"]
        + 0.30 * df["velocity"]
        + 0.15 * df["trend_score"]
    )

    df["prediction_score"] = df["raw_score"]

    if market_bias == "bullish":
        df.loc[df["prediction_score"] < 0, "prediction_score"] *= 0.4
    elif market_bias == "bearish":
        df.loc[df["prediction_score"] > 0, "prediction_score"] *= 0.4
    else:
        df["prediction_score"] *= 0.7

    # ---------------------
    # Protections
    # ---------------------
    injured = df["status"].isin(["i", "s"])
    df.loc[injured, ["prediction_score", "raw_score"]] = 0

    if PROTECTION_PATH.exists():
        prot = pd.read_csv(PROTECTION_PATH)
        prot["lock_until"] = pd.to_datetime(prot["lock_until"]).dt.date

        today = pd.to_datetime(
            snap_path.stem.replace("snapshot_", ""),
            format="%Y-%m-%d_%H-%M-%S",
        ).date()

        locked_ids = prot.loc[prot["lock_until"] >= today, "player_id"]
        df.loc[df["player_id"].isin(locked_ids), ["prediction_score", "raw_score"]] = 0

    # ---------------------
    # Learned thresholds
    # ---------------------
    rise_q, fall_q, learned = load_threshold_quantiles()
    active = df["prediction_score"] != 0

    if active.sum() >= 50:
        rise_threshold = df.loc[active, "prediction_score"].quantile(rise_q)
        fall_threshold = df.loc[active, "prediction_score"].quantile(fall_q)
    else:
        rise_threshold = float("inf")
        fall_threshold = float("-inf")

    print(
        f"🎯 Thresholds — rise_q={rise_q}, fall_q={fall_q}, "
        f"source={'learned' if learned else 'default'}"
    )

    # ---------------------
    # Direction
    # ---------------------
    df["direction"] = "none"
    df.loc[df["prediction_score"] >= rise_threshold, "direction"] = "rise"
    df.loc[df["prediction_score"] <= fall_threshold, "direction"] = "fall"

    # ---------------------
    # Confidence
    # ---------------------
    scale = df.loc[active, "prediction_score"].abs().quantile(0.95)
    scale = scale if scale > 0 else 1

    df["confidence"] = (
        (df["prediction_score"].abs() / scale) * 5
    ).clip(0, 5).round(2)

    # ---------------------
    # Alert level (CRITICAL FIX)
    # ---------------------
    df["alert_level"] = "none"

    df.loc[
        (df["direction"] != "none") & (df["confidence"] >= 4),
        "alert_level"
    ] = "imminent"

    df.loc[
        (df["direction"] != "none") & (df["confidence"].between(2.5, 4)),
        "alert_level"
    ] = "warming"

    # ---------------------
    # Ownership bucket
    # ---------------------
    df["ownership_bucket"] = pd.cut(
        df["ownership"],
        bins=[0, 2, 5, 10, 20, 100],
        labels=["0-2%", "2-5%", "5-10%", "10-20%", "20%+"],
    )

    # ---------------------
    # Output
    # ---------------------
    today = datetime.utcnow().date().isoformat()
    df["date"] = today
    df["market_bias"] = market_bias
    df["rise_threshold"] = rise_threshold
    df["fall_threshold"] = fall_threshold

    predictions = df[HISTORY_COLUMNS]

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    predictions.to_csv(OUT_PATH, index=False)

    history = safe_read_csv(HISTORY_PATH)
    history = normalize_history_schema(history)

    history = pd.concat([history, predictions], ignore_index=True)
    history.to_csv(HISTORY_PATH, index=False)

    print(f"🔮 Predictions generated: {len(predictions)} players")
    print(
        f"📈 Rises: {(predictions['direction'] == 'rise').sum()} | "
        f"📉 Falls: {(predictions['direction'] == 'fall').sum()}"
    )

if __name__ == "__main__":
    main()
