from pathlib import Path
import pandas as pd
import requests
from datetime import datetime

# =====================
# Paths
# =====================
PREDICTIONS_PATH = Path("data/predictions/stored_predictions.csv")

# =====================
# Telegram
# =====================
TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"


# =====================
# Helpers
# =====================
def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


def send_telegram_message(token: str, chat_id: str, text: str):
    url = TELEGRAM_API.format(token=token)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }
    requests.post(url, json=payload, timeout=10)


# =====================
# Main
# =====================
def main():
    preds = safe_read_csv(PREDICTIONS_PATH)

    if preds.empty:
        print("ℹ️ No stored predictions — skipping daily summary")
        return

    required = {
        "player_id",
        "date",
        "bucket",
        "signal",
        "direction",
        "confidence",
        "web_name",
    }

    if not required.issubset(preds.columns):
        print("⚠️ stored_predictions.csv missing required columns")
        return

    # ---- TODAY FILTER ----
    today = datetime.utcnow().date()
    preds["date"] = pd.to_datetime(preds["date"]).dt.date
    today_preds = preds[preds["date"] == today]

    if today_preds.empty:
        print("ℹ️ No predictions for today")
        return

    # ---- SORT BY CONFIDENCE ----
    today_preds = today_preds.sort_values("confidence", ascending=False)

    # ---- BUILD MESSAGE ----
    lines = ["📊 *FPL Daily Price Watch*\n"]

    for _, row in today_preds.iterrows():
        emoji = "📈" if row["direction"] == "rise" else "📉"
        lines.append(
            f"{emoji} *{row['web_name']}* — {row['direction'].upper()} "
            f"({row['signal']}, {row['bucket']}, {int(row['confidence']*100)}%)"
        )

    message = "\n".join(lines)

    token = str(Path().resolve().joinpath("").env.get("TELEGRAM_BOT_TOKEN", ""))
    chat_id = str(Path().resolve().joinpath("").env.get("TELEGRAM_CHAT_ID", ""))

    if not token or not chat_id:
        print("⚠️ Telegram credentials missing — summary not sent")
        return

    send_telegram_message(token, chat_id, message)
    print("✅ Daily summary sent")


if __name__ == "__main__":
    main()
