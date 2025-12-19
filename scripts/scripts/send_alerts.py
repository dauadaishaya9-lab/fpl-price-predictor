import os
import pandas as pd
import requests

LATEST_PATH = "data/latest.csv"
TRENDS_PATH = "data/trends.csv"
WATCHLIST_PATH = "data/watchlist.csv"

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ℹ️ Telegram credentials not set — skipping alerts")
        return

    url = f"https://api.telegram.org/bot{8244832361:AAE_2pQgyy31EvbTwndy8eEPKYc6e5QBrwA}/sendMessage"
    payload = {
        "chat_id": 7030202884,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        requests.post(url, json=payload, timeout=10).raise_for_status()
        print("📨 Telegram alert sent")
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


def main():
    # Required files
    for path in [LATEST_PATH, WATCHLIST_PATH]:
        if not os.path.exists(path):
            print(f"ℹ️ Missing {path} — skipping alerts")
            return

    latest = pd.read_csv(LATEST_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)

    if "name" not in watchlist.columns:
        print("⚠️ watchlist.csv must contain a 'name' column")
        return

    # Filter to watchlist players only
    watch_names = set(watchlist["name"].astype(str))
    latest = latest[latest["name"].isin(watch_names)]

    if latest.empty:
        print("ℹ️ No watchlist players found in latest data")
        return

    # Load trends if available
    if os.path.exists(TRENDS_PATH):
        trends = pd.read_csv(TRENDS_PATH)
        latest = latest.merge(trends, on="name", how="left")
    else:
        latest["trend_score"] = 0

    alerts = []

    for _, row in latest.iterrows():
        name = row["name"]
        price = row.get("price", "?")
        trend = row.get("trend_score", 0)
        price_change = row.get("price_change", 0)

        # Imminent rise
        if trend >= 0.70 and price_change == 0:
            alerts.append(
                f"📈 *Riser Alert*\n"
                f"{name} (£{price})\n"
                f"Trend score: {trend:.2f}"
            )

        # Imminent fall
        elif trend <= -0.70 and price_change == 0:
            alerts.append(
                f"📉 *Faller Alert*\n"
                f"{name} (£{price})\n"
                f"Trend score: {trend:.2f}"
            )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    message = "🚨 *FPL Watchlist Alert*\n\n" + "\n\n".join(alerts)
    send_telegram(message)


if __name__ == "__main__":
    main()
