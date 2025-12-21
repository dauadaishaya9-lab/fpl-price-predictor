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
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

API_URL = f"https://api.telegram.org/bot{TOKEN}"

# =====================
# Telegram helpers
# =====================
def send_message(text: str):
    url = f"{API_URL}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    requests.post(url, json=payload, timeout=10)


def get_updates(offset=None):
    params = {"timeout": 30}
    if offset:
        params["offset"] = offset

    r = requests.get(f"{API_URL}/getUpdates", params=params, timeout=30)
    r.raise_for_status()
    return r.json()["result"]


def load_offset():
    if OFFSET_PATH.exists():
        return int(OFFSET_PATH.read_text().strip())
    return None


def save_offset(offset):
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_PATH.write_text(str(offset))


# =====================
# Watchlist helpers
# =====================
def load_watchlist():
    if WATCHLIST_PATH.exists():
        return pd.read_csv(WATCHLIST_PATH)

    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    return pd.DataFrame(columns=["name"])


def save_watchlist(df):
    df.drop_duplicates().to_csv(WATCHLIST_PATH, index=False)


# =====================
# Main logic
# =====================
def main():
    if not TOKEN or not CHAT_ID:
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
        if chat_id != CHAT_ID:
            continue

        # =====================
        # /add
        # =====================
        if text.startswith("/add "):
            name = text.replace("/add ", "").strip().lower()

            if not name:
                send_message("⚠️ Usage: `/add playername`")
                continue

            if name in watch_names:
                send_message(f"ℹ️ *{name.title()}* is already in your watchlist")
                continue

            watchlist.loc[len(watchlist)] = {"name": name}
            watch_names.add(name)
            changed = True

            send_message(f"✅ *{name.title()}* added to watchlist")

        # =====================
        # /remove
        # =====================
        elif text.startswith("/remove "):
            name = text.replace("/remove ", "").strip().lower()

            if name not in watch_names:
                send_message(f"ℹ️ *{name.title()}* is not in your watchlist")
                continue

            watchlist = watchlist[watchlist["name"].str.lower() != name]
            watch_names.remove(name)
            changed = True

            send_message(f"🗑️ *{name.title()}* removed from watchlist")

        # =====================
        # /list
        # =====================
        elif text == "/list":
            if watchlist.empty:
                send_message("📭 Your watchlist is empty")
            else:
                players = "\n".join(
                    f"• {n.title()}" for n in sorted(watchlist["name"])
                )
                send_message(f"📋 *Your Watchlist*\n\n{players}")

        # =====================
        # Unknown command
        # =====================
        elif text.startswith("/"):
            send_message(
                "🤖 *Available commands*\n\n"
                "• `/add playername`\n"
                "• `/remove playername`\n"
                "• `/list`"
            )

    if latest_update_id:
        save_offset(latest_update_id)

    if changed:
        save_watchlist(watchlist)
        print("✅ Watchlist updated & saved")
    else:
        print("ℹ️ No watchlist changes")


if __name__ == "__main__":
    main()
