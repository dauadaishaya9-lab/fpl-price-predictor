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
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }

    try:
        requests.post(url, json=payload, timeout=10)
    except Exception:
        pass


def main():
    # Required files
    if not os.path.exists(LATEST_PATH):
        return

    if not os.path.exists(WATCHLIST_PATH):
        return

    latest = pd.read_csv(LATEST_PATH)
    watchlist = pd.read_csv(WATCHLIST_PATH)

    if "name" not in watchlist.columns:
        return

    watch_names = set(watchlist["name"].astype(str).str.strip())
    latest = latest[latest["name"].isin(watch_names)]

    if latest.empty:
        return

    # Load trends if present
    if os.path.exists(TRENDS_PATH):
        trends = pd.read_csv(TRENDS_PATH)
        latest = latest.merge(trends, on="name", how="left")
    else:
        latest["trend_score"] = 0

    warnings = []
    imminents = []

    for _, row in latest.iterrows():
        name = row["name"]
        price = row.get("price", "?")
        net = row.get("net_transfers_delta", 0)
        trend = row.get("trend_score", 0)
        price_change = row.get("price_change", 0)

        # Ignore players who already changed price
        if price_change != 0:
            continue

        # 🟡 EARLY WARNING
        if 8000 <= net < 15000 and 3000 <= trend < 7000:
            warnings.append(
                f"🟡 *Warming Up*\n"
                f"{name} (£{price})\n"
                f"Transfers gaining momentum"
            )

        elif -15000 < net <= -8000 and -7000 < trend <= -3000:
            warnings.append(
                f"🟡 *Cooling Down*\n"
                f"{name} (£{price})\n"
                f"Sales pressure increasing"
            )

        # 🚨 IMMINENT MOVE
        elif net >= 15000 and trend >= 7000:
            imminents.append(
                f"🔼 *Imminent Price Rise*\n"
                f"{name} (£{price})\n"
                f"Strong sustained buying"
            )

        elif net <= -15000 and trend <= -7000:
            imminents.append(
                f"🔽 *Imminent Price Fall*\n"
                f"{name} (£{price})\n"
                f"Heavy sustained selling"
            )

    if not warnings and not imminents:
        return

    message_parts = []

    if imminents:
        message_parts.append(
            "🚨 *IMMINENT FPL PRICE MOVES*\n\n" + "\n\n".join(imminents)
        )

    if warnings:
        message_parts.append(
            "⚠️ *Players to Watch*\n\n" + "\n\n".join(warnings)
        )

    final_message = "\n\n".join(message_parts)
    send_telegram(final_message)


if __name__ == "__main__":
    main()
