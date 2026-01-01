from pathlib import Path
import requests
import json
from datetime import datetime, timedelta
import os

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

DATA_DIR = Path("data")
RESET_REQUEST = DATA_DIR / "reset_request.json"
RESET_LOCK = DATA_DIR / "reset_lock.json"

DATA_DIR.mkdir(exist_ok=True)

# -------------------------
# Helpers
# -------------------------

def tg(method, payload):
    r = requests.post(f"{API_URL}/{method}", json=payload, timeout=20)
    r.raise_for_status()
    return r.json()

def send(msg):
    tg("sendMessage", {
        "chat_id": CHAT_ID,
        "text": msg
    })

def now():
    return datetime.utcnow()

# -------------------------
# Load last message
# -------------------------

def get_latest_command():
    updates = tg("getUpdates", {"limit": 1})
    if not updates.get("result"):
        return None

    msg = updates["result"][-1].get("message")
    if not msg:
        return None

    return msg.get("text", "").strip()

# -------------------------
# Command handlers
# -------------------------

def handle_reset_request():
    if RESET_REQUEST.exists():
        send("⏳ Reset already requested. Use /confirm_reset or /cancel_reset.")
        return

    expires = now() + timedelta(hours=1)
    RESET_REQUEST.write_text(json.dumps({
        "requested_at": now().isoformat() + "Z",
        "expires_at": expires.isoformat() + "Z"
    }, indent=2))

    send(
        "⚠️ *RESET REQUESTED*\n\n"
        "This will wipe learning data.\n\n"
        "⏳ You have 1 hour.\n"
        "Send /confirm_reset to proceed or /cancel_reset to abort."
    )

def handle_confirm_reset():
    if not RESET_REQUEST.exists():
        send("❌ No reset request found.")
        return

    req = json.loads(RESET_REQUEST.read_text())
    if now() > datetime.fromisoformat(req["expires_at"].replace("Z", "")):
        RESET_REQUEST.unlink(missing_ok=True)
        send("⌛ Reset request expired.")
        return

    RESET_LOCK.write_text(json.dumps({
        "confirmed_at": now().isoformat() + "Z"
    }, indent=2))

    RESET_REQUEST.unlink(missing_ok=True)
    send("🔥 Reset CONFIRMED. Will execute on next run.")

def handle_cancel_reset():
    if RESET_REQUEST.exists():
        RESET_REQUEST.unlink()
        send("🟢 Reset cancelled.")
    else:
        send("Nothing to cancel.")

def handle_status():
    msgs = ["📊 *System status*"]

    if RESET_REQUEST.exists():
        req = json.loads(RESET_REQUEST.read_text())
        msgs.append(f"⚠️ Reset pending (expires {req['expires_at']})")
    elif RESET_LOCK.exists():
        msgs.append("🔥 Reset confirmed (pending execution)")
    else:
        msgs.append("🟢 No reset pending")

    send("\n".join(msgs))

def handle_help():
    send(
        "/status – system state\n"
        "/reset – request season reset\n"
        "/confirm_reset – confirm reset\n"
        "/cancel_reset – cancel reset"
    )

# -------------------------
# Main
# -------------------------

def main():
    cmd = get_latest_command()
    if not cmd:
        return

    if cmd == "/reset":
        handle_reset_request()
    elif cmd == "/confirm_reset":
        handle_confirm_reset()
    elif cmd == "/cancel_reset":
        handle_cancel_reset()
    elif cmd == "/status":
        handle_status()
    elif cmd == "/help":
        handle_help()

if __name__ == "__main__":
    main()
