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

    if "name" not in watchlist.columns:
        print("⚠️ watchlist.csv must contain a 'name' column")
        return

    # ---- Merge snapshot + predictions (ID-safe) ----
    df = latest.merge(predictions, on="player_id", how="inner")

    # ---- Resolve ownership safely ----
    if "ownership_x" in df.columns:
        df["ownership_final"] = df["ownership_x"]
    elif "ownership" in df.columns:
        df["ownership_final"] = df["ownership"]
    else:
        print("⚠️ Ownership column missing — skipping alerts")
        return

    # ---- Required columns ----
    required_cols = {
        "web_name",
        "price",
        "ownership_final",
        "prediction_score",
        "direction",
        "confidence",
        "alert_level",
    }

    if not required_cols.issubset(df.columns):
        print("⚠️ Missing required columns after merge — skipping alerts")
        return

    # ---- Watchlist filter (external names only) ----
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = df[df["web_name"].str.lower().isin(watch_names)]

    if df.empty:
        print("ℹ️ No watchlist players found in predictions")
        return

    alerts = []

    for _, row in df.iterrows():
        name = row["web_name"]
        price = row["price"]
        ownership = row["ownership_final"]

        direction = row["direction"]
        alert_level = row["alert_level"]
        confidence = row["confidence"]
        score = row["prediction_score"]

        # ---- Only alert on predicted movement ----
        if alert_level not in {"imminent", "warming"}:
            continue

        # ---- Formatting ----
        if alert_level == "imminent":
            emoji = "📈" if direction == "rise" else "📉"
            title = "Imminent Riser" if direction == "rise" else "Imminent Faller"
        else:
            emoji = "⚠️"
            title = "Warming Up" if direction == "rise" else "Fall Risk Building"

        alerts.append(
            f"{emoji} *{title}*\n"
            f"{name} (£{price})\n"
            f"Ownership: {ownership:.1f}%\n"
            f"Prediction score: {score:.2f}\n"
            f"Confidence: {confidence:.2f}"
        )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram(
        "🚨 *FPL Price Prediction Alerts*\n\n" + "\n\n".join(alerts)
    )


if __name__ == "__main__":
    main()
    # ---- Required columns ----
    required_cols = {
        "web_name",
        "price",
        "ownership_final",
        "prediction_score",
        "direction",
        "confidence",
        "alert_level",
    }

    if not required_cols.issubset(df.columns):
        print("⚠️ Missing required columns after merge — skipping alerts")
        return

    # ---- Watchlist filter (external names only) ----
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = df[df["web_name"].str.lower().isin(watch_names)]

    if df.empty:
        print("ℹ️ No watchlist players found in predictions")
        return

    alerts = []

    for _, row in df.iterrows():
        name = row["web_name"]
        price = row["price"]
        ownership = row["ownership_final"]

        direction = row["direction"]
        alert_level = row["alert_level"]
        confidence = row["confidence"]
        score = row["prediction_score"]

        # ---- Only alert on predicted movement ----
        if alert_level not in {"imminent", "warming"}:
            continue

        # ---- Formatting ----
        if alert_level == "imminent":
            emoji = "📈" if direction == "rise" else "📉"
            title = "Imminent Riser" if direction == "rise" else "Imminent Faller"
        else:
            emoji = "⚠️"
            title = "Warming Up" if direction == "rise" else "Fall Risk Building"

        alerts.append(
            f"{emoji} *{title}*\n"
            f"{name} (£{price})\n"
            f"Ownership: {ownership:.1f}%\n"
            f"Prediction score: {score:.2f}\n"
            f"Confidence: {confidence:.2f}"
        )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram(
        "🚨 *FPL Price Prediction Alerts*\n\n" + "\n\n".join(alerts)
    )


if __name__ == "__main__":
    main()
