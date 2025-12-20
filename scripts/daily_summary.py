from pathlib import Path
import os
import pandas as pd
import requests

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions.csv")

# =====================
# Telegram config
# =====================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ℹ️ Telegram credentials not set — skipping summary")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
        print("📨 Daily summary sent")
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


def main():
    if not PREDICTIONS_PATH.exists():
        print("ℹ️ predictions.csv missing — skipping summary")
        return

    df = pd.read_csv(PREDICTIONS_PATH)

    required = {"name", "direction", "confidence", "alert_level"}
    if not required.issubset(df.columns):
        print("⚠️ predictions.csv missing required columns")
        return

    # Only meaningful signals
    df = df[df["alert_level"].isin(["warming", "imminent"])]

    if df.empty:
        print("ℹ️ No meaningful predictions today")
        return

    risers = (
        df[df["direction"] == "rise"]
        .sort_values("confidence", ascending=False)
        .head(15)
    )

    fallers = (
        df[df["direction"] == "fall"]
        .sort_values("confidence", ascending=False)
        .head(15)
    )

    message = "📊 *FPL Daily Price Prediction Summary*\n\n"

    if not risers.empty:
        message += "📈 *Potential Risers*\n"
        for _, r in risers.iterrows():
            message += f"• {r['name']} ({r['confidence']:.2f})\n"
        message += "\n"

    if not fallers.empty:
        message += "📉 *Potential Fallers*\n"
        for _, r in fallers.iterrows():
            message += f"• {r['name']} ({r['confidence']:.2f})\n"

    send_telegram(message)


if __name__ == "__main__":
    main()
