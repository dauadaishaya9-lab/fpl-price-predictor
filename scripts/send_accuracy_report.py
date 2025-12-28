from pathlib import Path
import pandas as pd
import requests
import os

ACCURACY_PATH = Path("data/accuracy.csv")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def main():
    if not ACCURACY_PATH.exists():
        print("ℹ️ No accuracy report to send")
        return

    df = pd.read_csv(ACCURACY_PATH)

    if df.empty:
        print("ℹ️ Accuracy file empty")
        return

    last = df.iloc[-1]

    msg = (
        "📊 *FPL Prediction Accuracy*\n\n"
        f"📅 Date: `{last['date_pred']}`\n"
        f"🎯 Total predictions: `{int(last['total_predictions'])}`\n"
        f"✅ Correct: `{int(last['correct_predictions'])}`\n"
        f"📈 Accuracy: `{last['accuracy'] * 100:.1f}%`"
    )

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown",
    }

    r = requests.post(url, json=payload, timeout=15)

    if r.status_code != 200:
        print("❌ Telegram send failed:", r.text)
    else:
        print("📬 Accuracy report sent to Telegram")


if __name__ == "__main__":
    main()
