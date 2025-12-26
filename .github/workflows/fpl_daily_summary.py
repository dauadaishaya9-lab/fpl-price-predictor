name: FPL Daily Summary

on:
  schedule:
    # Evening summary after all pipelines & learning are done
    - cron: "0 20 * * *"   # 20:00 UTC
  workflow_dispatch:

jobs:
  summary:
    runs-on: ubuntu-latest

    steps:
      # -------------------------
      # Checkout repository
      # -------------------------
      - name: Checkout repo
        uses: actions/checkout@v4

      # -------------------------
      # Setup Python
      # -------------------------
      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      # -------------------------
      # Install dependencies
      # -------------------------
      - name: Install dependencies
        run: |
          pip install pandas requests

      # -------------------------
      # Send daily summary
      # -------------------------
      - name: Send daily summary
        env:
          TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
        run: |
          python scripts/daily_summary.py
