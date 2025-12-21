from pathlib import Path
import os
import time
import shutil
import requests
import pandas as pd
from requests.exceptions import ReadTimeout

# =====================
# Paths
# =====================
DATA_DIR = Path("data")
WATCHLIST_PATH = DATA_DIR / "watchlist.csv"
OFFSET_PATH = DATA_DIR / "telegram_offset.txt"
RESET_FLAG = DATA_DIR / ".reset_pending"

SNAPSHOTS_DIR = DATA_DIR / "snapshots"
DELTAS_DIR = DATA_DIR / "deltas"

FILES_TO_DELETE = [
    DATA_DIR / "velocity.csv",
    DATA_DIR / "trends.csv",
    DATA_DIR / "predictions.csv",
]

# =====================
# Telegram config
# =====================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
OWNER_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_URL = f"https://api.telegram.org/bot{TOKEN}"

RESET_TIMEOUT = 60  # seconds

# =====================
# Telegram helpers
# =====================
def get_updates(offset=None):
    params = {"timeout": 5, "limit": 10}
    if offset is not None:
        params["offset"] = offset

    return requests.get(
        f"{API_URL}/getUpdates",
        params=params,
        timeout=10
    ).json()["result"]


def send_message(text):
    requests.post(
        f"{API_URL}/sendMessage",
        json={
            "chat_id": OWNER_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
        },
        timeout=10,
    )

# =====================
# File helpers
# =====================
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
    return pd.DataFrame(columns=["name"])


def save_watchlist(df):
    WATCHLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.drop_duplicates().to_csv(WATCHLIST_PATH, index=False)

# =====================
# Reset helpers
# =====================
def mark_reset_pending():
    RESET_FLAG.write_text(str(int(time.time())))


def reset_pending_valid():
    if not RESET_FLAG.exists():
        return False
    ts = int(RESET_FLAG.read_text())
    return (time.time() - ts) <= RESET_TIMEOUT


def clear_reset_flag():
    if RESET_FLAG.exists():
        RESET_FLAG.unlink()


def perform_reset():
    if SNAPSHOTS_DIR.exists():
        shutil.rmtree(SNAPSHOTS_DIR)
    if DELTAS_DIR.exists():
        shutil.rmtree(DELTAS_DIR)

    for f in FILES_TO_DELETE:
        if f.exists():
            f.unlink()

    if OFFSET_PATH.exists():
        OFFSET_PATH.unlink()

    clear_reset_flag()

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
        print("ℹ️ Telegram timeout — no messages")
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

    for update in updates:
        latest_offset = update["update_id"] + 1

        msg = update.get("message", {})
        text = msg.get("text", "").strip()
        chat_id = str(msg.get("chat", {}).get("id"))

        # 🔒 Owner only
        if chat_id != OWNER_CHAT_ID:
            continue

        # ---------- WATCHLIST ----------
        if text == "/list":
            if watch_names:
                send_message(
                    "📋 *Your Watchlist*\n" +
                    "\n".join(f"• {n}" for n in sorted(watch_names))
                )
            else:
                send_message("📭 *Your watchlist is empty*")

        elif text.startswith("/add "):
            names = text.replace("/add ", "").lower().split()
            added = []
            for name in names:
                if name not in watch_names:
                    watchlist.loc[len(watchlist)] = {"name": name}
                    watch_names.add(name)
                    added.append(name)

            if added:
                changed = True
                send_message(f"➕ Added: {', '.join(added)}")
            else:
                send_message("ℹ️ No new players added")

        elif text.startswith("/remove "):
            names = text.replace("/remove ", "").lower().split()
            removed = []

            for name in names:
                if name in watch_names:
                    watchlist = watchlist[watchlist["name"].str.lower() != name]
                    watch_names.remove(name)
                    removed.append(name)

            if removed:
                changed = True
                send_message(f"➖ Removed: {', '.join(removed)}")
            else:
                send_message("ℹ️ No matching players found")

        # ---------- RESET ----------
        elif text == "/reset":
            mark_reset_pending()
            send_message(
                "⚠️ *Pre-season reset requested*\n\n"
                "This will delete:\n"
                "• snapshots\n"
                "• deltas\n"
                "• velocity / trends / predictions\n"
                "• telegram offset\n\n"
                f"Type `/confirm_reset` within {RESET_TIMEOUT}s to proceed."
            )

        elif text == "/confirm_reset":
            if reset_pending_valid():
                perform_reset()
                send_message("✅ *Pre-season reset completed.*\nSystem will rebuild automatically.")
            else:
                send_message("❌ Reset expired or not requested.")

    if latest_offset:
        save_offset(latest_offset)

    if changed:
        save_watchlist(watchlist)

    print("✅ Telegram watchlist processed")


if __name__ == "__main__":
    main()
