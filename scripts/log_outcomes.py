from pathlib import Path
import pandas as pd

DELTA_DIR = Path("data/deltas")
OUTCOMES_PATH = Path("data/price_changes.csv")

def main():
    files = sorted(DELTA_DIR.glob("delta_*.csv"))
    if not files:
        print("ℹ️ No delta files found")
        return

    deltas = pd.read_csv(files[-1])

    required = {"player_id", "price_change", "timestamp"}
    if not required.issubset(deltas.columns):
        raise RuntimeError("❌ Delta file missing required columns")

    rows = []
    for _, r in deltas.iterrows():
        if r["price_change"] == 0:
            continue

        rows.append({
            "player_id": r["player_id"],
            "date": r["timestamp"].split("_")[0],
            "actual_change": "rise" if r["price_change"] > 0 else "fall"
        })

    if not rows:
        print("ℹ️ No price changes detected")
        return

    new = pd.DataFrame(rows)

    if OUTCOMES_PATH.exists():
        old = pd.read_csv(OUTCOMES_PATH)
        combined = pd.concat([old, new], ignore_index=True)
    else:
        combined = new

    combined.drop_duplicates(
        subset=["player_id", "date"],
        keep="last",
        inplace=True
    )

    OUTCOMES_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(OUTCOMES_PATH, index=False)

    print(f"📉📈 Logged {len(new)} price changes")

if __name__ == "__main__":
    main()
