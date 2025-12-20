from pathlib import Path
import pandas as pd
import os
import requests

LATEST = Path("data/latest.csv")
VELOCITY = Path("data/velocity.csv")
WATCHLIST = Path("data/watchlist.csv")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")


def send_telegram(message: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("ℹ️ Telegram credentials not set — skipping alerts")
        return

    requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "Markdown",
        },
        timeout=10,
    )


def adaptive_threshold(ownership: float) -> float:
    if ownership >= 20:
        return 0.85
    elif ownership >= 5:
        return 0.70
    else:
        return 0.55


def main():
    if not LATEST.exists() or not WATCHLIST.exists():
        print("ℹ️ Required files missing — skipping alerts")
        return

    latest = pd.read_csv(LATEST)
    watchlist = pd.read_csv(WATCHLIST)

    # Map watchlist names → IDs
    watch = latest[latest["name"].isin(watchlist["name"])]
    if watch.empty:
        print("ℹ️ No watchlist players found in latest snapshot")
        return

    if VELOCITY.exists():
        velocity = pd.read_csv(VELOCITY)
        watch = watch.merge(velocity, on="player_id", how="left")
    else:
        watch["velocity"] = 0

    watch["velocity"] = watch["velocity"].fillna(0)

    alerts = []

    for _, r in watch.iterrows():
        name = r["name"]
        team = r["team"]
        price = r["price"]
        ownership = float(r["ownership"])
        velocity = float(r["velocity"])

        threshold = adaptive_threshold(ownership)

        # 📈 Imminent rise
        if velocity >= threshold:
            alerts.append(
                f"📈 *Imminent Riser*\n"
                f"{name} ({team}) £{price}\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Velocity: {velocity:.2f} / {threshold}"
            )

        # ⚠️ Warming up (rise)
        elif velocity >= threshold * 0.6:
            alerts.append(
                f"⚠️ *Warming Up*\n"
                f"{name} ({team}) £{price}\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Velocity building: {velocity:.2f} / {threshold}"
            )

        # 📉 Imminent fall
        elif velocity <= -threshold:
            alerts.append(
                f"📉 *Imminent Faller*\n"
                f"{name} ({team}) £{price}\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Velocity: {velocity:.2f} / {threshold}"
            )

        # ⚠️ Fall risk building
        elif velocity <= -threshold * 0.6:
            alerts.append(
                f"⚠️ *Fall Risk Building*\n"
                f"{name} ({team}) £{price}\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Velocity weakening: {velocity:.2f} / {threshold}"
            )

    if not alerts:
        print("ℹ️ No alert-worthy players")
        return

    send_telegram("🚨 *FPL Price Watch*\n\n" + "\n\n".join(alerts))
    print("📨 Alerts sent")


if __name__ == "__main__":
    main()
