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

    required = {"web_name", "direction", "confidence"}
    if not required.issubset(today_preds.columns):
        print("⚠️ predictions_history.csv missing required columns")
        return

    # =====================
    # Derive alert levels
    # =====================
    today_preds["alert_level"] = today_preds.apply(classify_alert, axis=1)

    # Only real predictions
    active = today_preds[today_preds["direction"] != "none"]

    if active.empty:
        print("ℹ️ No active predictions today")
        return

    # =====================
    # Summary counts
    # =====================
    imminent = active[active["alert_level"] == "imminent"]
    warming = active[active["alert_level"] == "warming"]
    rises = active[active["direction"] == "rise"]
    falls = active[active["direction"] == "fall"]

    lines = [
        "📊 *FPL Daily Prediction Summary*",
        f"📅 {today}",
        "",
        f"🔢 Total predictions: *{len(active)}*",
        f"🚀 Imminent: *{len(imminent)}*",
        f"🔥 Warming: *{len(warming)}*",
        f"📈 Rises: *{len(rises)}*",
        f"📉 Falls: *{len(falls)}*",
        "",
    ]

    # =====================
    # FULL LIST — this is the fix
    # =====================
    lines.append("📋 *All Predictions*")

    active = active.sort_values("confidence", ascending=False)

    for _, row in active.iterrows():
        arrow = "⬆️" if row["direction"] == "rise" else "⬇️"
        badge = "🚀" if row["alert_level"] == "imminent" else "🔥"
        lines.append(
            f"{badge} {arrow} {row['web_name']} "
            f"({row['direction']}, {row['confidence']:.2f})"
        )

    message = "\n".join(lines)

    print(f"📊 Daily summary sent: {len(active)} predictions")
    send_telegram(message)


if __name__ == "__main__":
    main()
