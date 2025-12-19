import os
import pandas as pd
import requests

LATEST_PATH = "data/latest.csv"
TRENDS_PATH = "data/trends.csv"
WATCHLIST_PATH = "data/watchlist.csv"


def send_telegram(message: str):
    token = os.getenv("AAE_2pQgyy31EvbTwndy8eEPKYc6e5QBrwA
    chat_id = os.getenv("7030202884")

    if not token or not chat_id:
        print("ℹ️ Telegram credentials not set — skipping alerts")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "Markdown",
    }

    try:
        requests.post(url, json=payload, timeout=10)
        print("📨 Telegram alert sent")
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}")


def main():
    # Check required files
    for path in [LATEST_PATH, WATCHLIST_PATH]:
        if not os.path.exists(path):
            print(f"ℹ️ Missing {path} — skipping alerts")
            return

    latest = pd.read_csv(LATEST_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)

    if "name" not in watchlist.columns:
        print("⚠️ watchlist.csv must contain a 'name' column")
        return

    watch_names = set(watchlist["name"].astype(str))

    latest = latest[latest["name"].isin(watch_names)]

    if latest.empty:
        print("ℹ️ No watchlist players in latest snapshot")
        return

    # Try loading trends if available
    if os.path.exists(TRENDS_PATH):
        trends = pd.read_csv(TRENDS_PATH)
        latest = latest.merge(trends, on="name", how="left")
    else:
        latest["trend_score"] = 0

    messages = []

    for _, row in latest.iterrows():
        net = row.get("net_transfers_delta", 0)
        trend = row.get("trend_score", 0)
        price_change = row.get("price_change", 0)

        # Balanced rise alert
        if net >= 15000 and trend >= 5000 and price_change == 0:
            messages.append(
                f"🔼 *{row['name']}*\n"
                f"Net transfers: +{int(net):,}\n"
                f"Trend strength: rising\n"
                f"Likely price rise in 24–48h"
            )

        # Balanced fall alert
        elif net <= -12000 and trend <= -4000 and price_change == 0:
            messages.append(
                f"🔽 *{row['name']}*\n"
                f"Net transfers: {int(net):,}\n"
                f"Trend strength: falling\n"
                f"Fall risk increasing"
            )

    if not messages:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram("⚠️ *FPL Price Alerts*\n\n" + "\n\n".join(messages))


if __name__ == "__main__":
    main()
