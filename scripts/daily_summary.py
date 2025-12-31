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


def send_telegram(message: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegram credentials missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")


# =====================
# Alert classification
# =====================
def classify_alert(row):
    if row["confidence"] >= 4.0:
        return "imminent"
    if row["confidence"] >= 2.5:
        return "warming"
    return "normal"


# =====================
# Main
# =====================
def main():
    if not HISTORY_PATH.exists():
        print("⚠️ predictions_history.csv missing")
        return

    df = pd.read_csv(HISTORY_PATH)

    if df.empty:
        print("⚠️ predictions_history.csv is empty")
        return

    today = date.today().isoformat()

    # ---------------------
    # Accuracy-aligned filter
    # ---------------------
    today_preds = df[
        (df["date"] == today) &
        (df["direction"].isin(["rise", "fall"]))
    ].copy()

    if today_preds.empty:
        print("ℹ️ No active predictions today")
        return

    # ---------------------
    # ONE prediction per player (latest wins)
    # ---------------------
    today_preds = (
        today_preds
        .sort_index()
        .drop_duplicates(subset=["player_id"], keep="last")
    )

    # ---------------------
    # Alert levels
    # ---------------------
    today_preds["alert_level"] = today_preds.apply(classify_alert, axis=1)

    # ---------------------
    # Counts
    # ---------------------
    total = len(today_preds)
    imminent = today_preds[today_preds["alert_level"] == "imminent"]
    warming = today_preds[today_preds["alert_level"] == "warming"]
    rises = today_preds[today_preds["direction"] == "rise"]
    falls = today_preds[today_preds["direction"] == "fall"]

    lines = [
        "📊 *FPL Daily Prediction Summary*",
        f"📅 {today}",
        "",
        f"🎯 Total predictions: *{total}*",
        f"🚀 Imminent: *{len(imminent)}*",
        f"🔥 Warming: *{len(warming)}*",
        f"📈 Rises: *{len(rises)}*",
        f"📉 Falls: *{len(falls)}*",
        "",
        "📋 *All Predictions*",
    ]

    # ---------------------
    # Full list (same ones accuracy sees)
    # ---------------------
    today_preds = today_preds.sort_values("confidence", ascending=False)

    for _, row in today_preds.iterrows():
        arrow = "⬆️" if row["direction"] == "rise" else "⬇️"
        badge = "🚀" if row["alert_level"] == "imminent" else "🔥"
        lines.append(
            f"{badge} {arrow} {row['web_name']} "
            f"({row['direction']}, {row['confidence']:.2f})"
        )

    message = "\n".join(lines)

    print(f"📊 Daily summary sent: {total} predictions")
    send_telegram(message)


if __name__ == "__main__":
    main()
