from pathlib import Path
import os
import requests
import pandas as pd

# =====================
# Paths
# =====================
WATCHLIST_PATH = Path("data/watchlist.csv")
OFFSET_PATH = Path("data/telegram_offset.txt")

# =====================
# Telegram config
# =====================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OWNER_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

API_URL = f"https://api.telegram.org/bot{TOKEN}"

# =====================
# Telegram helpers
# =====================
def send_message(chat_id: str, text: str):
    requests.post(
        f"{API_URL}/sendMessage",
        json={"chat_id": chat_id, "text": text},
        timeout=10,
    )


def get_updates(offset=None):
    """
    IMPORTANT:
    - timeout=0 prevents long polling
    - Required for GitHub Actions stability
    """
    params = {
        "timeout": 0,
        "limit": 10,
    }
    if offset:
        params["offset"] = offset

    r = requests.get(
        f"{API_URL}/getUpdates",
        params=params,
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("result", [])


# =====================
# Persistence
# =====================
def load_offset():
    if OFFSET_PATH.exists():
        return int(OFFSET_PATH.read_text().strip())
    return None


def save_offset(offset: int):
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_PATH.write_text(str(offset))


def load_watchlist():
    if WATCHLIST_PATH.exists():
        return pd.read_csv(WATCHLIST_PATH)

    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    return pd.DataFrame(columns=["name"])


def save_watchlist(df: pd.DataFrame):
    df.drop_duplicates().to_csv(WATCHLIST_PATH, index=False)


# =====================
# Main logic
# =====================
def main():
    if not TOKEN or not OWNER_CHAT_ID:
        print("ℹ️ Telegram credentials not set — skipping watchlist editor")
        return

    offset = load_offset()
    updates = get_updates(offset)

    if not updates:
        print("ℹ️ No new Telegram commands")
        return

    watchlist = load_watchlist()
    watch_names = set(watchlist["name"].str.lower())

    latest_update_id = None
    changed = False

    for update in updates:
        latest_update_id = update["update_id"] + 1

        message = update.get("message", {})
        text = message.get("text", "").strip()
        chat_id = str(message.get("chat", {}).get("id"))

        # 🔒 Owner-only protection
        if chat_id != OWNER_CHAT_ID:
            send_message(chat_id, "🚫 You are not authorized to manage this watchlist.")
            continue

        # =====================
        # Commands
        # =====================
        if text.startswith("/add "):
            name = text.replace("/add ", "").strip().lower()
            if not name:
                send_message(chat_id, "⚠️ Usage: /add player_name")
                continue

            if name in watch_names:
                send_message(chat_id, f"ℹ️ *{name}* is already in your watchlist.")
            else:
                watchlist.loc[len(watchlist)] = {"name": name}
                watch_names.add(name)
                changed = True
                send_message(chat_id, f"➕ *{name}* added to watchlist.")

        elif text.startswith("/remove "):
            name = text.replace("/remove ", "").strip().lower()
            if name not in watch_names:
                send_message(chat_id, f"ℹ️ *{name}* is not in your watchlist.")
            else:
                watchlist = watchlist[watchlist["name"].str.lower() != name]
                watch_names.remove(name)
                changed = True
                send_message(chat_id, f"➖ *{name}* removed from watchlist.")

        elif text == "/list":
            if not watch_names:
                send_message(chat_id, "📭 Watchlist is empty.")
            else:
                players = "\n".join(sorted(watch_names))
                send_message(chat_id, f"📋 *Your Watchlist:*\n{players}")

        else:
            send_message(
                chat_id,
                "🤖 Commands:\n"
                "/add player_name\n"
                "/remove player_name\n"
                "/list",
            )

    if latest_update_id:
        save_offset(latest_update_id)

    if changed:
        save_watchlist(watchlist)
        print("✅ Watchlist updated and saved")
    else:
        print("ℹ️ No watchlist changes")


if __name__ == "__main__":
    main()
