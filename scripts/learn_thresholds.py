from pathlib import Path
import pandas as pd

OUTCOMES_PATH = Path("data/outcomes.csv")
THRESHOLDS_PATH = Path("data/thresholds.json")

def main():
    if not OUTCOMES_PATH.exists():
        print("ℹ️ No outcomes yet — skipping learning")
        return

    df = pd.read_csv(OUTCOMES_PATH)

    learned = {}

    for direction in ["rise", "fall"]:
        subset = df[
            (df["direction"] == direction) &
            (df["actual_change"] == (1 if direction == "rise" else -1))
        ]

        if subset.empty:
            continue

        learned[direction] = {
            "imminent": round(subset["confidence"].quantile(0.75), 3),
            "warming": round(subset["confidence"].quantile(0.50), 3),
        }

    THRESHOLDS_PATH.write_text(pd.Series(learned).to_json())
    print("📐 Thresholds learned:", learned)

if __name__ == "__main__":
    main()
