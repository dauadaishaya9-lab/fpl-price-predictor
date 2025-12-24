from pathlib import Path
import os
import pandas as pd
import requests

# =====================
# Paths
# =====================
LATEST_PATH = Path("data/latest.csv")
PREDICTIONS_PATH = Path("data/predictions.csv")
WATCHLIST_PATH = Path("data/watchlist.csv")

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
    for path in [LATEST_PATH, PREDICTIONS_PATH, WATCHLIST_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping alerts")
            return

    latest = pd.read_csv(LATEST_PATH)
    predictions = pd.read_csv(PREDICTIONS_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)

    if "name" not in watchlist.columns:
        print("⚠️ watchlist.csv must contain a 'name' column")
        return

    # ---- Merge ----
    df = latest.merge(predictions, on="player_id", how="inner")

    if df.empty:
        print("ℹ️ No merged alert data")
        return

    # =====================
    # 🔑 NAME RESOLUTION (FIX)
    # =====================
    name_col = None
    for col in [
        "web_name",
        "name",
        "web_name_x",
        "web_name_y",
        "name_x",
        "name_y",
    ]:
        if col in df.columns:
            name_col = col
            break

    if name_col is None:
        raise ValueError("❌ No player name column found after merge")

    df["player_name"] = df[name_col].astype(str)

    # =====================
    # Ownership resolution
    # =====================
    if "ownership" in df.columns:
        df["ownership_final"] = df["ownership"]
    elif "selected_by_percent" in df.columns:
        df["ownership_final"] = df["selected_by_percent"]
    elif "ownership_x" in df.columns:
        df["ownership_final"] = df["ownership_x"]
    else:
        print("⚠️ Ownership column missing — skipping alerts")
        return

    # =====================
    # Watchlist filter
    # =====================
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = df[df["player_name"].str.lower().isin(watch_names)]

    if df.empty:
        print("ℹ️ No watchlist players matched")
        return

    alerts = []

    for _, row in df.iterrows():
        if row["alert_level"] not in {"imminent", "warming"}:
            continue

        direction = row["direction"]
        emoji = "📈" if direction == "rise" else "📉"

        title = (
            "Imminent Riser" if direction == "rise"
            else "Imminent Faller"
            if row["alert_level"] == "imminent"
            else "Price Movement Building"
        )

        alerts.append(
            f"{emoji} *{title}*\n"
            f"{row['player_name']} (£{row['price']})\n"
            f"Ownership: {row['ownership_final']:.1f}%\n"
            f"Score: {row['prediction_score']:.2f}\n"
            f"Confidence: {row['confidence']:.2f}"
        )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram(
        "🚨 *FPL Price Prediction Alerts*\n\n" + "\n\n".join(alerts)
    )

if __name__ == "__main__":
    main()
