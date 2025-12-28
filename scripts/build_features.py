from pathlib import Path
import pandas as pd

# =====================
# Paths
# =====================
SNAPSHOT_DIR = Path("data/snapshots")
DELTA_DIR = Path("data/deltas")
VELOCITY_DIR = Path("data/velocity")
TRENDS_DIR = Path("data/trends")
FEATURE_DIR = Path("data/features")

FEATURE_DIR.mkdir(parents=True, exist_ok=True)

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

def latest(dir: Path, pattern: str) -> Path | None:
    files = sorted(dir.glob(pattern))
    return files[-1] if files else None

# =====================
# Main
# =====================
def main():
    snap_path = latest(SNAPSHOT_DIR, "snapshot_*.csv")
    delta_path = latest(DELTA_DIR, "delta_*.csv")
    vel_path = latest(VELOCITY_DIR, "velocity_*.csv")
    trend_path = latest(TRENDS_DIR, "trends_*.csv")

    if not all([snap_path, delta_path, vel_path, trend_path]):
        print("⚠️ Missing inputs for feature building")
        return

    snap = safe_read_csv(snap_path)
    delta = safe_read_csv(delta_path)
    vel = safe_read_csv(vel_path)
    trend = safe_read_csv(trend_path)

    if snap.empty or delta.empty or vel.empty or trend.empty:
        print("⚠️ One or more input files empty")
        return

    # ---------------------
    # Merge everything
    # ---------------------
    df = snap.merge(delta, on="player_id", how="left")
    df = df.merge(vel, on="player_id", how="left")
    df = df.merge(trend, on="player_id", how="left")

    # ---------------------
    # Fill safe defaults
    # ---------------------
    df["net_transfers_delta"] = df["net_transfers_delta"].fillna(0)
    df["velocity"] = df["velocity"].fillna(0)
    df["trend_score"] = df["trend_score"].fillna(0)

    out_path = FEATURE_DIR / f"features_{snap_path.stem.replace('snapshot_', '')}.csv"
    df.to_csv(out_path, index=False)

    print(f"🧩 Features built: {out_path.name}")
    print(f"📊 Players: {len(df)}")

if __name__ == "__main__":
    main()
