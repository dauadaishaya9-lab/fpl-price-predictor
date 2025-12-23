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
# Main logic
# =====================
def main():
    # ---- Safety gates ----
    for path in [LATEST_PATH, PREDICTIONS_PATH, WATCHLIST_PATH]:
        if not path.exists():
            print(f"ℹ️ Missing {path} — skipping alerts")
            return

    latest = pd.read_csv(LATEST_PATH)
    predictions = pd.read_csv(PREDICTIONS_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)

    # ---- Merge snapshot + predictions ----
    df = latest.merge(predictions, on="player_id", how="inner")

    # ---- Resolve ownership ----
    if "ownership_x" in df.columns:
        df["ownership_final"] = df["ownership_x"]
    elif "ownership" in df.columns:
        df["ownership_final"] = df["ownership"]
    else:
        print("⚠️ Ownership column missing — skipping alerts")
        return

    # ---- Watchlist filter ----
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = df[df["web_name"].str.lower().isin(watch_names)]

    if df.empty:
        print("ℹ️ No watchlist players found in predictions")
        return

    alerts = []

    for _, row in df.iterrows():
        score = row["prediction_score"]
        confidence = row["confidence"]

        # 🔑 Truth source: score sign
        direction = "rise" if score > 0 else "fall"

        # ---- Thresholds (asymmetric) ----
        if direction == "rise":
            if confidence < 0.60:
                continue
        else:  # fall
            if confidence < 0.70:
                continue

        name = row["web_name"]
        price = row["price"]
        ownership = row["ownership_final"]
        alert_level = row["alert_level"]

        # ---- Formatting ----
        if alert_level == "imminent":
            emoji = "📈" if direction == "rise" else "📉"
            title = "TONIGHT: Price Rise" if direction == "rise" else "TONIGHT: Price Fall"
        else:
            emoji = "⚠️"
            title = "Building Rise Risk" if direction == "rise" else "Building Fall Risk"

        alerts.append(
            f"{emoji} *{title}*\n"
            f"{name} (£{price})\n"
            f"Ownership: {ownership:.1f}%\n"
            f"Score: {score:.2f}\n"
            f"Confidence: {confidence:.2f}"
        )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram(
        "🚨 *FPL Price Alerts*\n\n" + "\n\n".join(alerts)
    )


if __name__ == "__main__":
    main()
