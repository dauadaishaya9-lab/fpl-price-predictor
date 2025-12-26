from pathlib import Path
import pandas as pd
import requests
from datetime import date
import os

# =====================
# Paths
# =====================
HISTORY_PATH = Path("data/predictions_history.csv")

# =====================
# Telegram
# =====================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

TELEGRAM_URL = (
    f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    if BOT_TOKEN and CHAT_ID else None
)

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


def send_telegram(message: str):
    if not TELEGRAM_URL:
        print("⚠️ Telegram not configured — printing summary only")
        print(message)
        return

    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }

    try:
        requests.post(TELEGRAM_URL, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


# =====================
# Main
# =====================
def main():
    df = safe_read_csv(HISTORY_PATH)

    if df.empty:
        print("ℹ️ No stored predictions — skipping daily summary")
        return

    today = date.today().isoformat()
    today_preds = df[df["date"] == today]

    if today_preds.empty:
        print("ℹ️ No predictions for today — skipping daily summary")
        return

    # ---- BASIC METRICS ----
    total = len(today_preds)

    imminent_rise = len(
        today_preds[
            (today_preds["direction"] == "rise") &
            (today_preds["signal"] == "imminent")
        ]
    )

    imminent_fall = len(
        today_preds[
            (today_preds["direction"] == "fall") &
            (today_preds["signal"] == "imminent")
        ]
    )

    warming = len(today_preds[today_preds["signal"] == "warming"])

    # ---- TOP CONFIDENCE ----
    top = (
        today_preds
        .sort_values("confidence", ascending=False)
        .head(5)
    )

    # =====================
    # MESSAGE
    # =====================
    message = (
        f"📊 *FPL Daily Summary*\n"
        f"🗓 Date: {today}\n\n"
        f"🔢 Total predictions: {total}\n"
        f"📈 Imminent rises: {imminent_rise}\n"
        f"📉 Imminent falls: {imminent_fall}\n"
        f"🌡 Warming signals: {warming}\n\n"
        f"🔥 *Top Confidence Picks*\n"
    )

    for _, row in top.iterrows():
        message += (
            f"• {row['web_name']} → "
            f"{row['direction']} ({row['confidence']:.2f})\n"
        )

    send_telegram(message)
    print("📨 Daily summary sent")


if __name__ == "__main__":
    main()
