from pathlib import Path
import pandas as pd
import json

# =====================
# Paths
# =====================
PRED_HISTORY = Path("data/predictions_history.csv")
PRICE_CHANGES = Path("data/price_changes.csv")
THRESHOLD_PATH = Path("data/thresholds.json")

MIN_SAMPLES = 4

# =====================
# Helpers
# =====================
def safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists() or path.stat().st_size == 0:
        return pd.DataFrame()
    return pd.read_csv(path)

# =====================
# Main
# =====================
def main():
    preds = safe_read_csv(PRED_HISTORY)
    actuals = safe_read_csv(PRICE_CHANGES)

    if preds.empty or actuals.empty:
        print("ℹ️ Not enough data to tune thresholds")
        return

    preds["date"] = pd.to_datetime(preds["date"])
    actuals["date"] = pd.to_datetime(actuals["date"])

    # ---------------------
    # Only imminent predictions
    # ---------------------
    preds = preds[
        (preds["alert_level"] == "imminent") &
        (preds["direction"].isin(["rise", "fall"]))
    ]

    if preds.empty:
        print("ℹ️ No imminent predictions to learn from yet")
        return

    # ---------------------
    # STRICT D+1 merge
    # ---------------------
    preds["target_date"] = preds["date"] + pd.Timedelta(days=1)

    merged = preds.merge(
        actuals,
        left_on=["player_id", "target_date"],
        right_on=["player_id", "date"],
        how="inner",
    )

    if len(merged) < MIN_SAMPLES:
        print("ℹ️ Not enough resolved predictions yet")
        return

    # ---------------------
    # Candidate thresholds
    # ---------------------
    candidates = [
        (0.90, 0.10),
        (0.92, 0.08),
        (0.94, 0.06),
        (0.95, 0.05),
        (0.96, 0.04),
        (0.97, 0.03),
    ]

    best = {
        "accuracy": 0,
        "rise_q": None,
        "fall_q": None,
        "samples": 0,
    }

    for rise_q, fall_q in candidates:
        rise_thr = merged["prediction_score"].quantile(rise_q)
        fall_thr = merged["prediction_score"].quantile(fall_q)

        test = merged.copy()
        test["predicted"] = "none"
        test.loc[test["prediction_score"] >= rise_thr, "predicted"] = "rise"
        test.loc[test["prediction_score"] <= fall_thr, "predicted"] = "fall"

        test = test[test["predicted"] != "none"]

        if len(test) < MIN_SAMPLES:
            continue

        acc = (test["predicted"] == test["actual_change"]).mean()

        if acc > best["accuracy"] or (
            acc == best["accuracy"] and len(test) > best["samples"]
        ):
            best = {
                "accuracy": round(float(acc), 3),
                "rise_q": rise_q,
                "fall_q": fall_q,
                "samples": len(test),
            }

    if best["rise_q"] is None:
        print("⚠️ No viable threshold configuration yet")
        return

    # ---------------------
    # Save thresholds
    # ---------------------
    THRESHOLD_PATH.parent.mkdir(parents=True, exist_ok=True)

    with open(THRESHOLD_PATH, "w") as f:
        json.dump(
            {
                "rise_quantile": best["rise_q"],
                "fall_quantile": best["fall_q"],
                "accuracy": best["accuracy"],
                "samples": best["samples"],
                "scope": "imminent_only",
                "horizon": "D+1",
            },
            f,
            indent=2,
        )

    print("🧠 Thresholds tuned (strict D+1, leak-free)")
    print(best)

if __name__ == "__main__":
    main()
