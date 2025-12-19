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


def adaptive_threshold(ownership: float) -> float:
    if ownership >= 20:
        return 0.85
    elif ownership >= 5:
        return 0.70
    else:
        return 0.55


def main():
    # Required files check
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
        print("ℹ️ No watchlist players found")
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
        trend = float(row.get("trend_score", 0))
        ownership = float(row.get("selected_by_percent", 0))
        price_change = float(row.get("price_change", 0))

        threshold = adaptive_threshold(ownership)

        # 📈 Imminent rise
        if trend >= threshold and price_change == 0:
            alerts.append(
                f"📈 *Imminent Riser*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Trend: {trend:.2f} / {threshold}"
            )

        # ⚠️ Warming up (rise)
        elif trend >= threshold * 0.6 and price_change == 0:
            alerts.append(
                f"⚠️ *Warming Up*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Trend building: {trend:.2f} / {threshold}"
            )

        # 📉 Imminent fall
        elif trend <= -threshold and price_change == 0:
            alerts.append(
                f"📉 *Imminent Faller*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Trend: {trend:.2f} / {threshold}"
            )

        # ⚠️ Warming up (fall)
        elif trend <= -threshold * 0.6 and price_change == 0:
            alerts.append(
                f"⚠️ *Fall Risk Building*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Trend weakening: {trend:.2f} / {threshold}"
            )

    if not alerts:
        print("ℹ️ No alert-worthy watchlist players")
        return

    send_telegram("🚨 *FPL Price Watch*\n\n" + "\n\n".join(alerts))


if __name__ == "__main__":
    main()