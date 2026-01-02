from pathlib import Path
import pandas as pd
import requests
import os

# =====================
# Paths & Telegram
# =====================
ACCURACY_PATH = Path("data/accuracy.csv")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing")
        return

    if not ACCURACY_PATH.exists():
        print("ℹ️ No accuracy report to send")
        return

    df = pd.read_csv(ACCURACY_PATH)

    if df.empty:
        print("ℹ️ Accuracy file empty")
        return

    last = df.iloc[-1]

    # ---------------------
    # Column-safe extraction
    # ---------------------
    date = (
        last.get("date_pred")
        or last.get("date")
        or "unknown"
    )

    total = (
        last.get("total_predictions")
        or last.get("predictions")
        or last.get("n_predictions")
        or 0
    )

    correct = (
        last.get("correct_predictions")
        or last.get("correct")
        or 0
    )

    # Accuracy may be precomputed or derived
    if "accuracy" in last and pd.notna(last["accuracy"]):
        accuracy_pct = last["accuracy"] * 100
    else:
        accuracy_pct = (correct / total * 100) if total else 0.0

    # ---------------------
    # Message
    # ---------------------
    msg = (
        "📊 *FPL Prediction Accuracy*\n\n"
        f"📅 Date: `{date}`\n"
        f"🎯 Predictions: `{int(correct)}/{int(total)}`\n"
        f"📈 Accuracy: `{accuracy_pct:.1f}%`"
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
