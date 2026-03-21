import streamlit as st
import sys
import os

st.set_page_config(page_title="Debug", layout="wide")
st.title("Energy Insights - Diagnostic")

st.write(f"Python version: {sys.version}")
st.write(f"Working directory: {os.getcwd()}")
st.write(f"Files in cwd: {os.listdir('.')}")

# Test imports one by one
imports_status = {}

for mod_name in ["plotly", "plotly.express", "plotly.graph_objects", "pandas", "numpy", "snowflake.connector"]:
    try:
        __import__(mod_name)
        imports_status[mod_name] = "OK"
    except Exception as e:
        imports_status[mod_name] = f"FAIL: {e}"

st.subheader("Import Status")
for mod, status in imports_status.items():
    if "OK" in status:
        st.success(f"{mod}: {status}")
    else:
        st.error(f"{mod}: {status}")

# Test Snowflake connection
st.subheader("Snowflake Connection Test")
try:
    import snowflake.connector

    # Get credentials from environment
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD")

    # Validate required credentials
    if not all([account, user, password]):
        missing = []
        if not account: missing.append("SNOWFLAKE_ACCOUNT")
        if not user: missing.append("SNOWFLAKE_USER")
        if not password: missing.append("SNOWFLAKE_PASSWORD")
        st.error(f"❌ Missing required environment variables: {', '.join(missing)}")
        st.info("💡 Please set these variables in your .env file")
    else:
        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ML_ENERGY_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "ML_ENERGY_DB"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        )
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_TIMESTAMP(), CURRENT_DATABASE(), CURRENT_WAREHOUSE()")
        row = cur.fetchone()
        st.success(f"Connected! Time={row[0]}, DB={row[1]}, WH={row[2]}")

        cur.execute("SELECT COUNT(*) FROM PREDICTIONS.ENERGY_PREDICTIONS")
        pred = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM PREDICTIONS.ENERGY_ACTUALS")
        act = cur.fetchone()[0]
        st.info(f"Predictions: {pred} rows, Actuals: {act} rows")
        conn.close()
except Exception as e:
    st.error(f"Snowflake connection failed: {e}")

st.subheader("Environment Variables")
for key in ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]:
    val = os.environ.get(key, "NOT SET")
    if key == "SNOWFLAKE_PASSWORD" and val != "NOT SET":
        val = "***SET***"
    st.write(f"{key}: {val}")
