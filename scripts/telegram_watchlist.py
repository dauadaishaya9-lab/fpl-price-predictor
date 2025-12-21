from pathlib import Path
import os
import requests
import pandas as pd
from requests.exceptions import ReadTimeout

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
# Helpers
# =====================
def get_updates(offset=None):
    params = {
        "timeout": 5,   # short polling
        "limit": 10,
    }
    if offset is not None:
        params["offset"] = offset

    r = requests.get(
        f"{API_URL}/getUpdates",
        params=params,
        timeout=10,
    )
    r.raise_for_status()
    return r.json().get("result", [])


def send_message(text: str):
    requests.post(
        f"{API_URL}/sendMessage",
        json={
            "chat_id": OWNER_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        },
        timeout=10,
    )


def load_offset():
    if OFFSET_PATH.exists():
        return int(OFFSET_PATH.read_text().strip())
    return None


def save_offset(offset):
    OFFSET_PATH.parent.mkdir(parents=True, exist_ok=True)
    OFFSET_PATH.write_text(str(offset))


def load_watchlist():
    if WATCHLIST_PATH.exists():
        return pd.read_csv(WATCHLIST_PATH)

    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    return pd.DataFrame(columns=["name"])


def save_watchlist(df):
    df.drop_duplicates().to_csv(WATCHLIST_PATH, index=False)


def parse_names(text: str, prefix: str):
    raw = text[len(prefix):]
    return [n.strip().lower() for n in raw.split(",") if n.strip()]


# =====================
# Main
# =====================
def main():
    if not TOKEN or not OWNER_CHAT_ID:
        print("ℹ️ Telegram credentials missing")
        return

    offset = load_offset()

    try:
        updates = get_updates(offset)
    except ReadTimeout:
        print("ℹ️ Telegram timeout — no updates")
        return
    except Exception as e:
        print(f"⚠️ Telegram error: {e}")
        return

    if not updates:
        print("ℹ️ No new Telegram commands")
        return

    watchlist = load_watchlist()
    watch_names = set(watchlist["name"].str.lower())

    latest_offset = None
    changed = False
    replies = []

    for update in updates:
        latest_offset = update["update_id"] + 1

        msg = update.get("message", {})
        text = msg.get("text", "").strip()
        chat_id = str(msg.get("chat", {}).get("id"))

        # 🔒 Owner only
        if chat_id != OWNER_CHAT_ID:
            continue

        # 📋 List
        if text == "/list":
            if watch_names:
                replies.append(
                    "📋 *Your Watchlist*\n" +
                    "\n".join(f"• {n}" for n in sorted(watch_names))
                )
            else:
                replies.append("📭 *Your watchlist is empty*")

        # ➕ Add multiple
        elif text.startswith("/add "):
            names = parse_names(text, "/add ")
            added, skipped = [], []

            for name in names:
                if name not in watch_names:
                    watchlist.loc[len(watchlist)] = {"name": name}
                    watch_names.add(name)
                    added.append(name)
                    changed = True
                else:
                    skipped.append(name)

            if added:
                replies.append(f"➕ *Added:* {', '.join(added)}")
            if skipped:
                replies.append(f"ℹ️ *Already in watchlist:* {', '.join(skipped)}")

        # ➖ Remove multiple
        elif text.startswith("/remove "):
            names = parse_names(text, "/remove ")
            removed, missing = [], []

            for name in names:
                if name in watch_names:
                    watchlist = watchlist[watchlist["name"].str.lower() != name]
                    watch_names.remove(name)
                    removed.append(name)
                    changed = True
                else:
                    missing.append(name)

            if removed:
                replies.append(f"➖ *Removed:* {', '.join(removed)}")
            if missing:
                replies.append(f"ℹ️ *Not in watchlist:* {', '.join(missing)}")

    if latest_offset is not None:
        save_offset(latest_offset)

    if changed:
        save_watchlist(watchlist)

    for reply in replies:
        send_message(reply)

    print("✅ Telegram watchlist processed")


if __name__ == "__main__":
    main()
