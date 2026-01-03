from pathlib import Path
import os
import pandas as pd
import requests
from datetime import datetime

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions.csv")
WATCHLIST_PATH = Path("data/watchlist.csv")
SNAPSHOT_PATH = Path("data/bootstrap_static.csv")

# =====================
# Telegram config
# =====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# =====================
# Telegram sender
# =====================
def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ℹ️ Telegram credentials not set — skipping alerts")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
        print("📨 Telegram alert sent")
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")

# =====================
# Main
# =====================
def main():
    for path in [PREDICTIONS_PATH, WATCHLIST_PATH, SNAPSHOT_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping alerts")
            return

    predictions = pd.read_csv(PREDICTIONS_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)
    snapshot = pd.read_csv(SNAPSHOT_PATH)

    if "name" not in watchlist.columns:
        print("⚠️ watchlist.csv must contain a 'name' column")
        return

    # =====================================================
    # VALID FPL PLAYERS (TODAY)
    # =====================================================
    valid_players = snapshot[
        (snapshot["status"] == "a") &
        (snapshot["selected_by_percent"].astype(float) > 0)
    ][["id", "web_name"]].rename(columns={"id": "player_id"})

    # =====================================================
    # TODAY ONLY
    # =====================================================
    today = datetime.utcnow().date().isoformat()
    predictions = predictions[predictions["date"] == today]

    if predictions.empty:
        print("ℹ️ No predictions for today")
        return

    # =====================================================
    # FILTER TO VALID FPL PLAYERS
    # =====================================================
    predictions = predictions.merge(
        valid_players,
        on="player_id",
        how="inner"
    )

    if predictions.empty:
        print("ℹ️ No valid FPL players after snapshot filtering")
        return

    # =====================================================
    # ONE PLAYER = ONE ALERT
    # =====================================================
    predictions = (
        predictions
        .sort_values("confidence", ascending=False)
        .drop_duplicates("player_id")
    )

    # =====================================================
    # IGNORE NEUTRAL
    # =====================================================
    predictions = predictions[predictions["direction"].isin(["rise", "fall"])]

    if predictions.empty:
        print("ℹ️ No actionable predictions")
        return

    # =====================================================
    # NAME RESOLUTION (TRUST SNAPSHOT)
    # =====================================================
    predictions["player_name"] = predictions["web_name"].astype(str)

    # =====================================================
    # WATCHLIST FILTER (NAME-BASED, SAFE NOW)
    # =====================================================
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = predictions[
        predictions["player_name"].str.lower().isin(watch_names)
    ]

    if df.empty:
        print("ℹ️ No watchlist players matched")
        return

    # =====================================================
    # ALERT LEVEL (RECOMPUTED)
    # =====================================================
    def classify_alert(row):
        if row["confidence"] >= 4:
            return "imminent"
        if row["confidence"] >= 2.5:
            return "warming"
        return None

    df["alert_level"] = df.apply(classify_alert, axis=1)
    df = df[df["alert_level"].notna()]

    if df.empty:
        print("ℹ️ No alert-worthy watchlist players")
        return

    # =====================================================
    # BUILD ALERTS
    # =====================================================
    alerts = []

    for _, row in df.iterrows():
        emoji = "📈" if row["direction"] == "rise" else "📉"

        title = (
            "Imminent Riser" if row["direction"] == "rise" and row["alert_level"] == "imminent"
            else "Imminent Faller" if row["direction"] == "fall" and row["alert_level"] == "imminent"
            else "Price Move Building"
        )

        alerts.append(
            f"{emoji} *{title}*\n"
            f"{row['player_name']}\n"
            f"Score: {row.get('prediction_score', 0):.2f}\n"
            f"Confidence: {row['confidence']:.2f}"
        )

    if not alerts:
        print("ℹ️ Nothing to alert")
        return

    send_telegram(
        "🚨 *FPL Price Prediction Alerts*\n\n" + "\n\n".join(alerts)
    )

if __name__ == "__main__":
    main()
