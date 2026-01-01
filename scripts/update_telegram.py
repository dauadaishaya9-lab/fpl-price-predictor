from pathlib import Path
import requests
import json
from datetime import datetime, timedelta
import os
import shutil

# =====================
# CONFIG
# =====================
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

RESET_PATH = Path("data/reset_request.json")
DATA_DIR = Path("data")

RESET_TTL_MINUTES = 60  # 1 hour confirmation window

RESET_TARGETS = [
    "predictions_history.csv",
    "price_changes.csv",
    "accuracy.csv",
    "thresholds.json",
    "protection_status.csv",
    "latest.csv",
    "snapshots",
]

# =====================
# TELEGRAM HELPERS
# =====================
def send(msg: str):
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegram creds missing")
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(
        url,
        json={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown",
        },
        timeout=15,
    )


def get_updates(offset=None):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"timeout": 10}
    if offset:
        params["offset"] = offset
    r = requests.get(url, params=params, timeout=15)
    r.raise_for_status()
    return r.json()["result"]


# =====================
# RESET STATE
# =====================
def utcnow_naive():
    """Always return naive UTC datetime"""
    return datetime.utcnow().replace(tzinfo=None)


def load_reset():
    if not RESET_PATH.exists():
        return None
    try:
        data = json.loads(RESET_PATH.read_text())
        # normalize old / broken files
        if "created_at" not in data:
            return None
        return data
    except Exception:
        return None


def save_reset(data):
    RESET_PATH.parent.mkdir(parents=True, exist_ok=True)
    RESET_PATH.write_text(json.dumps(data, indent=2))


def clear_reset():
    if RESET_PATH.exists():
        RESET_PATH.unlink()


def reset_expired(reset):
    try:
        created = datetime.fromisoformat(reset["created_at"]).replace(tzinfo=None)
    except Exception:
        return True  # corrupted timestamp → expire it

    return utcnow_naive() > created + timedelta(minutes=RESET_TTL_MINUTES)


# =====================
# RESET EXECUTION
# =====================
def execute_seasonal_reset():
    for target in RESET_TARGETS:
        path = DATA_DIR / target

        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        elif path.exists():
            path.unlink()

    send(
        "🧹 *Seasonal reset completed*\n"
        "All learning, history and thresholds wiped.\n"
        "Fresh season activated."
    )


# =====================
# COMMAND HANDLER
# =====================
def handle_command(cmd: str):
    reset = load_reset()

    # Auto-clean expired reset
    if reset and reset_expired(reset):
        clear_reset()
        reset = None

    # ---------------------
    # /reset
    # ---------------------
    if cmd == "/reset":
        if reset:
            send("⏳ Reset already requested.\nUse /confirm_reset or /cancel_reset.")
            return

        save_reset({
            "created_at": utcnow_naive().isoformat(),
            "status": "pending",
        })

        send(
            "⚠️ *Seasonal reset requested*\n\n"
            "This will wipe all learning, history and thresholds.\n\n"
            "⏳ You have *1 hour* to confirm:\n"
            "/confirm_reset\n\n"
            "Or cancel with:\n"
            "/cancel_reset"
        )
        return

    # ---------------------
    # /confirm_reset
    # ---------------------
    if cmd == "/confirm_reset":
        if not reset:
            send("ℹ️ No active reset request.")
            return

        if reset_expired(reset):
            clear_reset()
            send("⌛ Reset request expired. Send /reset again.")
            return

        execute_seasonal_reset()
        clear_reset()
        return

    # ---------------------
    # /cancel_reset
    # ---------------------
    if cmd == "/cancel_reset":
        if not reset:
            send("ℹ️ No active reset request.")
            return

        clear_reset()
        send("❌ Reset request cancelled.")
        return


# =====================
# MAIN
# =====================
def main():
    if not BOT_TOKEN or not CHAT_ID:
        print("⚠️ Telegram not configured")
        return

    updates = get_updates()
    if not updates:
        return

    last_update_id = None

    for u in updates:
        last_update_id = u["update_id"]

        msg = u.get("message", {})
        text = msg.get("text", "")
        chat_id = msg.get("chat", {}).get("id")

        if str(chat_id) != str(CHAT_ID):
            continue

        if text.startswith("/"):
            handle_command(text.strip())

    if last_update_id is not None:
        get_updates(offset=last_update_id + 1)


if __name__ == "__main__":
    main()
