from pathlib import Path
import pandas as pd
from datetime import datetime

# =====================
# Paths
# =====================
DELTAS_PATH = Path("data/deltas/latest_deltas.csv")
OUTCOMES_PATH = Path("data/price_changes.csv")


# =====================
# Helpers
# =====================
def safe_read_csv(path: Path):
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    try:
        return pd.read_csv(path)
    except Exception:
        return pd.DataFrame()


# =====================
# Main
# =====================
def main():
    deltas = safe_read_csv(DELTAS_PATH)

    if deltas.empty:
        print("ℹ️ No deltas available — skipping outcome logging")
        return

    required = {"player_id", "price_delta"}
    if not required.issubset(deltas.columns):
        print("⚠️ latest_deltas.csv missing required columns")
        return

    today = datetime.utcnow().date().isoformat()

    outcomes = []

    for _, row in deltas.iterrows():
        delta = row["price_delta"]

        if delta > 0:
            change = "rise"
        elif delta < 0:
            change = "fall"
        else:
            continue  # ignore no-change players

        outcomes.append({
            "player_id": row["player_id"],
            "date": today,
            "actual_change": change,
        })

    if not outcomes:
        print("ℹ️ No price changes detected")
        return

    new = pd.DataFrame(outcomes)

    history = safe_read_csv(OUTCOMES_PATH)

    combined = pd.concat([history, new], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["player_id", "date"],
        keep="last"
    )

    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTCOMES_PATH, index=False)

    print(f"📉📈 Logged {len(new)} price changes")


if __name__ == "__main__":
    main()
