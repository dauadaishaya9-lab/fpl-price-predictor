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
    """
    Derive alert level from confidence.
    This keeps history clean and logic flexible.
    """
    if row["direction"] == "none":
        return "none"
    if row["confidence"] >= 4.0:
        return "imminent"
    if row["confidence"] >= 2.5:
        return "warming"
    return "none"


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
    today_preds = df[df["date"] == today].copy()

    if today_preds.empty:
        print("⚠️ No predictions for today")
        return

    # ---- REQUIRED COLUMNS ----
    required = {
        "web_name",
        "direction",
        "confidence",
    }

    if not required.issubset(today_preds.columns):
        print("⚠️ predictions_history.csv missing required columns")
        return

    # =====================
    # Derive alert levels
    # =====================
    today_preds["alert_level"] = today_preds.apply(classify_alert, axis=1)

    # =====================
    # BUILD SUMMARY
    # =====================
    total = len(today_preds)

    imminent = today_preds[today_preds["alert_level"] == "imminent"]
    warming = today_preds[today_preds["alert_level"] == "warming"]

    rises = today_preds[today_preds["direction"] == "rise"]
    falls = today_preds[today_preds["direction"] == "fall"]

    lines = [
        "📊 *FPL Daily Prediction Summary*",
        f"📅 {today}",
        "",
        f"🔢 Total predictions: *{total}*",
        f"🚀 Imminent: *{len(imminent)}*",
        f"🔥 Warming: *{len(warming)}*",
        f"📈 Rises: *{len(rises)}*",
        f"📉 Falls: *{len(falls)}*",
        "",
    ]

    # =====================
    # Top imminent picks
    # =====================
    if not imminent.empty:
        top = imminent.sort_values("confidence", ascending=False).head(5)
        lines.append("⭐ *Top Imminent Picks*")
        for _, row in top.iterrows():
            arrow = "⬆️" if row["direction"] == "rise" else "⬇️"
            lines.append(
                f"{arrow} {row['web_name']} "
                f"({row['direction']}, {row['confidence']:.2f})"
            )

    message = "\n".join(lines)

    print(f"📊 Daily summary sent: {total} predictions")
    send_telegram(message)


if __name__ == "__main__":
    main()
