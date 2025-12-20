from pathlib import Path
import os
import requests
import pandas as pd

WATCHLIST_PATH = Path("data/watchlist.csv")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OWNER_ID = os.getenv("TELEGRAM_OWNER_ID")

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"


def send(chat_id, text):
    requests.post(
        f"{API_URL}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10,
    )


def main():
    if not BOT_TOKEN or not OWNER_ID:
        print("Missing Telegram credentials")
        return

    OWNER_ID_INT = int(OWNER_ID)

    updates = requests.get(f"{API_URL}/getUpdates", timeout=10).json()

    if not updates.get("ok"):
        return

    for update in updates["result"]:
        msg = update.get("message")
        if not msg:
            continue

        chat_id = msg["chat"]["id"]
        user_id = msg["from"]["id"]
        text = msg.get("text", "").strip().lower()

        # 🔒 Owner-only
        if user_id != OWNER_ID_INT:
            send(chat_id, "⛔ You are not authorized to edit the watchlist.")
            continue

        # Ensure file exists
        WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
        if not WATCHLIST_PATH.exists():
            pd.DataFrame(columns=["name"]).to_csv(WATCHLIST_PATH, index=False)

        df = pd.read_csv(WATCHLIST_PATH)

        if text.startswith("/add "):
            name = text.replace("/add ", "").strip()

            if not name:
                send(chat_id, "⚠️ Usage: /add playername")
                continue

            if name in df["name"].astype(str).str.lower().values:
                send(chat_id, f"ℹ️ {name} is already in your watchlist.")
            else:
                df.loc[len(df)] = name
                df.to_csv(WATCHLIST_PATH, index=False)
                send(chat_id, f"✅ Added *{name}* to watchlist.")

        elif text.startswith("/remove "):
            name = text.replace("/remove ", "").strip()

            if name not in df["name"].astype(str).str.lower().values:
                send(chat_id, f"ℹ️ {name} not found in watchlist.")
            else:
                df = df[df["name"].str.lower() != name]
                df.to_csv(WATCHLIST_PATH, index=False)
                send(chat_id, f"🗑️ Removed *{name}* from watchlist.")

        elif text == "/list":
            if df.empty:
                send(chat_id, "📭 Watchlist is empty.")
            else:
                players = "\n".join(f"• {n}" for n in df["name"])
                send(chat_id, f"📋 *Your Watchlist*\n\n{players}")


if __name__ == "__main__":
    main()
