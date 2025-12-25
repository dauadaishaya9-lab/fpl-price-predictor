from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
DELTA_DIR = Path("data/deltas")
OUTCOMES_PATH = Path("data/price_changes.csv")


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


def latest_delta_file() -> Path | None:
    files = sorted(DELTA_DIR.glob("delta_*.csv"))
    return files[-1] if files else None


# =====================
# Main
# =====================
def main():
    delta_path = latest_delta_file()

    if delta_path is None:
        print("ℹ️ No delta files found — skipping outcome logging")
        return

    deltas = safe_read_csv(delta_path)

    if deltas.empty:
        print("ℹ️ Delta file empty — skipping outcome logging")
        return

    required = {"player_id", "price_change", "timestamp"}
    if not required.issubset(deltas.columns):
        print("⚠️ Delta file missing required columns")
        return

    outcomes = []

    for _, row in deltas.iterrows():
        change = row["price_change"]

        if change > 0:
            actual = "rise"
        elif change < 0:
            actual = "fall"
        else:
            continue  # ignore no-change players

        outcomes.append({
            "player_id": row["player_id"],
            "date": row["timestamp"].split("_")[0],
            "actual_change": actual,
        })

    if not outcomes:
        print("ℹ️ No price changes detected in latest delta")
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

    print(f"📉📈 Logged {len(new)} price changes from {delta_path.name}")


if __name__ == "__main__":
    main()
