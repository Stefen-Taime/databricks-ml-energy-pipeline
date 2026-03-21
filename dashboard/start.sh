#!/bin/bash
# Launcher script for Databricks Apps
# Uses DATABRICKS_APP_PORT if set, otherwise defaults to 8501
PORT="${DATABRICKS_APP_PORT:-8501}"
echo "Starting Streamlit on port $PORT"
exec streamlit run app.py \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  --server.headless true \
  --browser.gatherUsageStats false \
  --theme.base dark \
  --theme.primaryColor "#6C63FF" \
  --theme.backgroundColor "#0E1117" \
  --theme.secondaryBackgroundColor "#1A1D29" \
  --theme.textColor "#FAFAFA"
