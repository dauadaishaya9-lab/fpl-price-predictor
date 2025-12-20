from pathlib import Path
import os
import pandas as pd
import requests

LATEST_PATH = Path("data/latest.csv")
PREDICTIONS_PATH = Path("data/predictions.csv")
WATCHLIST_PATH = Path("data/watchlist.csv")

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


def adaptive_threshold(ownership: float):
    if ownership >= 30:
        return 0.75
    elif ownership >= 10:
        return 0.65
    else:
        return 0.55


def main():
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

    # 🔗 Merge predictions into latest (ID-safe)
    df = latest.merge(predictions, on="player_id", how="left")
    df["prediction_score"] = df["prediction_score"].fillna(0)

    # 🔍 Filter by watchlist (names externally only)
    watch_names = set(watchlist["name"].astype(str).str.lower())
    df = df[df["web_name"].str.lower().isin(watch_names)]

    if df.empty:
        print("ℹ️ No watchlist players found in latest snapshot")
        return

    alerts = []

    for _, row in df.iterrows():
        name = row["web_name"]
        price = row["now_cost"]
        ownership = row["selected_by_percent"]
        score = row["prediction_score"]
        price_change = row.get("price_change", 0)

        threshold = adaptive_threshold(ownership)

        # 🚀 Imminent Rise
        if score >= threshold and price_change == 0:
            alerts.append(
                f"📈 *Imminent Riser*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Prediction: {score:.2f} / {threshold}"
            )

        # ⚠️ Warming Up
        elif score >= threshold * 0.7 and price_change == 0:
            alerts.append(
                f"⚠️ *Warming Up*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Prediction building: {score:.2f}"
            )

        # 📉 Imminent Fall
        elif score <= -threshold and price_change == 0:
            alerts.append(
                f"📉 *Imminent Faller*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Prediction: {score:.2f} / -{threshold}"
            )

        # ⚠️ Fall Risk
        elif score <= -threshold * 0.7 and price_change == 0:
            alerts.append(
                f"⚠️ *Fall Risk Building*\n"
                f"{name} (£{price})\n"
                f"Ownership: {ownership:.1f}%\n"
                f"Prediction weakening: {score:.2f}"
            )

    if not alerts:
        print("ℹ️ No alert-worthy players")
        return

    send_telegram("🚨 *FPL Price Prediction Alerts*\n\n" + "\n\n".join(alerts))


if __name__ == "__main__":
    main()
