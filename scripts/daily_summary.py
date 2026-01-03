from pathlib import Path
import pandas as pd
import requests
from datetime import date
import os

# =====================
# Paths
# =====================
HISTORY_PATH = Path("data/predictions_history.csv")
SNAPSHOT_PATH = Path("data/bootstrap_static.csv")  # latest snapshot

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
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            print("⚠️ Telegram send failed:", r.text)
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")

# =====================
# Main
# =====================
def main():
    if not HISTORY_PATH.exists():
        print("⚠️ predictions_history.csv missing")
        return

    if not SNAPSHOT_PATH.exists():
        print("⚠️ bootstrap_static.csv missing")
        return

    df = pd.read_csv(HISTORY_PATH)
    if df.empty:
        print("⚠️ predictions_history.csv empty")
        return

    snapshot = pd.read_csv(SNAPSHOT_PATH)

    # ---------------------
    # Build VALID player universe (TODAY)
    # ---------------------
    valid_players = snapshot[
        (snapshot["status"] == "a") &              # available
        (snapshot["selected_by_percent"].astype(float) > 0)
    ][["id", "web_name"]].rename(columns={"id": "player_id"})

    # ---------------------
    # Today only, imminent only
    # ---------------------
    today = date.today().isoformat()

    today_preds = df[
        (df["date"] == today) &
        (df["alert_level"] == "imminent") &
        (df["direction"].isin(["rise", "fall"]))
    ].copy()

    if today_preds.empty:
        print("ℹ️ No imminent predictions today")
        return

    # ---------------------
    # Re-validate against TODAY snapshot
    # ---------------------
    today_preds = today_preds.merge(
        valid_players,
        on="player_id",
        how="inner"
    )

    if today_preds.empty:
        print("ℹ️ No valid FPL players after snapshot filtering")
        return

    # ---------------------
    # ONE per player (highest confidence)
    # ---------------------
    today_preds = (
        today_preds
        .sort_values("confidence", ascending=False)
        .drop_duplicates(subset=["player_id"], keep="first")
    )

    rises = today_preds[today_preds["direction"] == "rise"]
    falls = today_preds[today_preds["direction"] == "fall"]

    # ---------------------
    # Message
    # ---------------------
    lines = [
        "📊 *FPL Daily Prediction Summary*",
        f"📅 {today}",
        "",
        f"🚨 Imminent predictions: *{len(today_preds)}*",
        f"📈 Rises: *{len(rises)}*",
        f"📉 Falls: *{len(falls)}*",
        "",
        "📋 *Imminent Only (Valid FPL Players)*",
    ]

    for _, row in today_preds.iterrows():
        arrow = "⬆️" if row["direction"] == "rise" else "⬇️"
        lines.append(
            f"{arrow} *{row['web_name']}* — {row['confidence']:.2f}"
        )

    message = "\n".join(lines)

    print(f"📊 Daily summary sent: {len(today_preds)} imminent predictions")
    send_telegram(message)

if __name__ == "__main__":
    main()
