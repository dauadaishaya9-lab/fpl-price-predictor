from pathlib import Path
import requests
import json
import time
from datetime import datetime, timedelta, timezone
import os
import sys

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DATA_DIR = Path("data")
RESET_PATH = DATA_DIR / "reset_request.json"

RESET_EXPIRY_MINUTES = 60

TELEGRAM_API = f"https://api.telegram.org/bot{BOT_TOKEN}"

# =========================
# HELPERS
# =========================
def now_utc():
    return datetime.now(timezone.utc)


def send(msg):
    requests.post(
        f"{TELEGRAM_API}/sendMessage",
        json={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        },
        timeout=15,
    )


def load_reset():
    if not RESET_PATH.exists():
        return None
    try:
        with open(RESET_PATH) as f:
            return json.load(f)
    except Exception:
        return None


def save_reset(data):
    RESET_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(RESET_PATH, "w") as f:
        json.dump(data, f, indent=2)


def delete_reset():
    if RESET_PATH.exists():
        RESET_PATH.unlink()


def reset_expired(reset):
    created = datetime.fromisoformat(reset["created_at"])
    return now_utc() > created + timedelta(minutes=RESET_EXPIRY_MINUTES)


def cleanup_if_expired(reset):
    if reset and reset_expired(reset):
        delete_reset()
        send("⌛ Reset request expired and was cleaned up.")
        return True
    return False


def get_latest_command():
    r = requests.get(
        f"{TELEGRAM_API}/getUpdates",
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()

    if not data["result"]:
        return None

    # Only care about the latest message
    msg = data["result"][-1].get("message")
    if not msg:
        return None

    if str(msg["chat"]["id"]) != str(CHAT_ID):
        return None

    return msg.get("text", "").strip()


# =========================
# MAIN
# =========================
def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("❌ Telegram env vars missing")
        sys.exit(1)

    command = get_latest_command()
    if not command:
        return  # nothing to do

    reset = load_reset()

    # 🔥 CLEANUP FIRST
    if reset and cleanup_if_expired(reset):
        reset = None

    # =========================
    # /reset
    # =========================
    if command == "/reset":
        if reset:
            send("⏳ Reset already requested. Use /confirm_reset or /cancel_reset.")
            return

        save_reset({
            "status": "pending",
            "confirmed": False,
            "created_at": now_utc().isoformat(),
        })

        send(
            "⚠️ *Season reset requested*\n\n"
            "This will wipe all learned data.\n\n"
            "Use `/confirm_reset` within 1 hour to proceed,\n"
            "or `/cancel_reset` to abort."
        )
        return

    # =========================
    # /confirm_reset
    # =========================
    if command == "/confirm_reset":
        if not reset:
            send("❌ No reset request found.")
            return

        if reset.get("confirmed"):
            send("✅ Reset already confirmed. Awaiting execution.")
            return

        reset["confirmed"] = True
        reset["confirmed_at"] = now_utc().isoformat()
        save_reset(reset)

        send(
            "🔥 *Reset confirmed*\n\n"
            "Season reset will execute on the next workflow run."
        )
        return

    # =========================
    # /cancel_reset
    # =========================
    if command == "/cancel_reset":
        if reset:
            delete_reset()
            send("❌ Reset cancelled.")
        else:
            send("ℹ️ No reset to cancel.")
        return


if __name__ == "__main__":
    main()
