# =============================================================================
# Energy Consumption Dashboard — Streamlit + Plotly
# Modern, professional UI connecting to Snowflake analytics
# =============================================================================

import os
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import snowflake.connector

# =============================================================================
# Page config
# =============================================================================
st.set_page_config(
    page_title="Energy Insights",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =============================================================================
# Custom CSS — Modern dark glassmorphism theme
# =============================================================================
st.markdown("""
<style>
    /* ---------- Global ---------- */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    .stApp {
        background: linear-gradient(135deg, #0E1117 0%, #1A1D29 50%, #0E1117 100%);
    }

    /* ---------- Sidebar ---------- */
    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #141720 0%, #1A1D29 100%);
        border-right: 1px solid rgba(108, 99, 255, 0.15);
    }

    section[data-testid="stSidebar"] .stSelectbox label,
    section[data-testid="stSidebar"] .stRadio label {
        color: #A0AEC0 !important;
        font-weight: 500;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* ---------- Metric cards ---------- */
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, rgba(108, 99, 255, 0.08) 0%, rgba(108, 99, 255, 0.02) 100%);
        border: 1px solid rgba(108, 99, 255, 0.15);
        border-radius: 16px;
        padding: 20px 24px;
        backdrop-filter: blur(10px);
        transition: all 0.3s ease;
    }

    div[data-testid="stMetric"]:hover {
        border-color: rgba(108, 99, 255, 0.4);
        box-shadow: 0 8px 32px rgba(108, 99, 255, 0.12);
        transform: translateY(-2px);
    }

    div[data-testid="stMetric"] label {
        color: #8B95A5 !important;
        font-weight: 500;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }

    div[data-testid="stMetric"] [data-testid="stMetricValue"] {
        color: #FAFAFA !important;
        font-weight: 700;
        font-size: 1.8rem;
    }

    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        font-weight: 600;
    }

    /* ---------- Tabs ---------- */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: rgba(26, 29, 41, 0.6);
        border-radius: 12px;
        padding: 4px;
        border: 1px solid rgba(108, 99, 255, 0.1);
    }

    .stTabs [data-baseweb="tab"] {
        border-radius: 10px;
        padding: 10px 24px;
        font-weight: 500;
        color: #8B95A5;
        transition: all 0.25s ease;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #6C63FF 0%, #5A52E0 100%) !important;
        color: white !important;
        font-weight: 600;
        box-shadow: 0 4px 16px rgba(108, 99, 255, 0.3);
    }

    /* ---------- Plotly chart containers ---------- */
    .stPlotlyChart {
        background: rgba(26, 29, 41, 0.4);
        border: 1px solid rgba(108, 99, 255, 0.08);
        border-radius: 16px;
        padding: 8px;
        backdrop-filter: blur(10px);
    }

    /* ---------- Dataframes ---------- */
    .stDataFrame {
        border-radius: 12px;
        overflow: hidden;
    }

    /* ---------- Section headers ---------- */
    .section-header {
        background: linear-gradient(90deg, rgba(108, 99, 255, 0.12) 0%, transparent 100%);
        border-left: 3px solid #6C63FF;
        padding: 12px 20px;
        border-radius: 0 12px 12px 0;
        margin-bottom: 20px;
    }

    .section-header h3 {
        margin: 0;
        color: #FAFAFA;
        font-weight: 600;
        font-size: 1.1rem;
    }

    .section-header p {
        margin: 4px 0 0 0;
        color: #8B95A5;
        font-size: 0.85rem;
    }

    /* ---------- KPI row ---------- */
    .kpi-container {
        display: flex;
        gap: 16px;
        margin-bottom: 24px;
    }

    /* ---------- Alert cards ---------- */
    .alert-card {
        background: linear-gradient(135deg, rgba(255, 107, 107, 0.08) 0%, rgba(255, 107, 107, 0.02) 100%);
        border: 1px solid rgba(255, 107, 107, 0.2);
        border-radius: 12px;
        padding: 16px 20px;
        margin-bottom: 12px;
    }

    .alert-card-warning {
        background: linear-gradient(135deg, rgba(255, 193, 7, 0.08) 0%, rgba(255, 193, 7, 0.02) 100%);
        border-color: rgba(255, 193, 7, 0.2);
    }

    .alert-card-success {
        background: linear-gradient(135deg, rgba(72, 199, 142, 0.08) 0%, rgba(72, 199, 142, 0.02) 100%);
        border-color: rgba(72, 199, 142, 0.2);
    }

    /* ---------- Logo / title area ---------- */
    .app-header {
        text-align: center;
        padding: 12px 0 24px 0;
    }

    .app-header h1 {
        background: linear-gradient(135deg, #6C63FF, #48C78E);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 1.6rem;
        font-weight: 700;
        margin-bottom: 4px;
    }

    .app-header p {
        color: #8B95A5;
        font-size: 0.85rem;
    }

    /* ---------- Hide Streamlit branding ---------- */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}

    /* ---------- Smooth scrolling ---------- */
    html {
        scroll-behavior: smooth;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Snowflake connection (cached)
# =============================================================================
@st.cache_resource(show_spinner=False)
def get_snowflake_connection():
    """Create a Snowflake connection using environment variables.

    Required environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    - SNOWFLAKE_WAREHOUSE (optional, defaults to ML_ENERGY_WH)
    - SNOWFLAKE_DATABASE (optional, defaults to ML_ENERGY_DB)
    - SNOWFLAKE_ROLE (optional, defaults to ACCOUNTADMIN)
    """
    try:
        # Get required credentials from environment
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
            st.info("💡 Please set these variables in your .env file or environment")
            return None

        conn = snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "ML_ENERGY_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "ML_ENERGY_DB"),
            role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        )
        return conn
    except Exception as e:
        st.error(f"Snowflake connection failed: {e}")
        return None


@st.cache_data(ttl=300, show_spinner=False)
def run_query(query: str) -> pd.DataFrame:
    """Execute a Snowflake query and return a DataFrame."""
    conn = get_snowflake_connection()
    if conn is None:
        return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        data = cur.fetchall()
        cur.close()
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()


# =============================================================================
# Plotly theme
# =============================================================================
COLORS = {
    "primary": "#6C63FF",
    "secondary": "#48C78E",
    "accent": "#FF6B6B",
    "warning": "#FFC107",
    "bg": "#0E1117",
    "card": "#1A1D29",
    "text": "#FAFAFA",
    "muted": "#8B95A5",
    "grid": "rgba(108, 99, 255, 0.06)",
    "gradient_1": "#6C63FF",
    "gradient_2": "#48C78E",
    "gradient_3": "#FF6B6B",
    "gradient_4": "#FFC107",
    "gradient_5": "#36B5FF",
}

PLOTLY_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(family="Inter, sans-serif", color=COLORS["text"], size=12),
    margin=dict(l=20, r=20, t=50, b=20),
    xaxis=dict(
        gridcolor=COLORS["grid"],
        zerolinecolor=COLORS["grid"],
        showgrid=True,
    ),
    yaxis=dict(
        gridcolor=COLORS["grid"],
        zerolinecolor=COLORS["grid"],
        showgrid=True,
    ),
    legend=dict(
        bgcolor="rgba(0,0,0,0)",
        bordercolor="rgba(108, 99, 255, 0.1)",
        borderwidth=1,
        font=dict(size=11),
    ),
    hoverlabel=dict(
        bgcolor=COLORS["card"],
        bordercolor=COLORS["primary"],
        font=dict(color=COLORS["text"], size=12),
    ),
)


def apply_layout(fig, title="", height=420):
    """Apply consistent Plotly styling."""
    fig.update_layout(
        **PLOTLY_LAYOUT,
        title=dict(
            text=title,
            font=dict(size=15, weight=600, color=COLORS["text"]),
            x=0.02,
        ),
        height=height,
    )
    return fig


# =============================================================================
# Data loading queries
# =============================================================================
def load_user_list():
    return run_query("""
        SELECT DISTINCT USER_ID
        FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_PREDICTIONS
        ORDER BY USER_ID
        LIMIT 200
    """)


def load_predictions(user_id: str):
    return run_query(f"""
        SELECT TIMESTAMP, KWH_PREDICTED, MODEL_VERSION, PREDICTION_DATE
        FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_PREDICTIONS
        WHERE USER_ID = '{user_id}'
        ORDER BY TIMESTAMP
    """)


def load_actuals(user_id: str):
    return run_query(f"""
        SELECT TIMESTAMP, KWH_ACTUAL
        FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_ACTUALS
        WHERE USER_ID = '{user_id}'
        ORDER BY TIMESTAMP
    """)


def load_household(user_id: str):
    return run_query(f"""
        SELECT ACORN_GROUP, ACORN_CATEGORY, TARIFF_TYPE
        FROM ML_ENERGY_DB.ANALYTICS.HOUSEHOLDS
        WHERE USER_ID = '{user_id}'
    """)


def load_model_metrics():
    return run_query("""
        SELECT MODEL_VERSION, WALK_FORWARD_ROUND, TRAIN_END_DATE,
               TEST_MONTH, MAPE, RMSE, MAE, R2_SCORE, CREATED_AT
        FROM ML_ENERGY_DB.ANALYTICS.MODEL_METRICS
        ORDER BY CREATED_AT DESC, WALK_FORWARD_ROUND
    """)


def load_global_stats():
    return run_query("""
        SELECT
            COUNT(DISTINCT p.USER_ID) AS total_users,
            COUNT(*) AS total_predictions,
            ROUND(AVG(p.KWH_PREDICTED), 4) AS avg_predicted_kwh,
            MAX(p.PREDICTION_DATE) AS last_prediction_date
        FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_PREDICTIONS p
    """)


def load_actuals_count():
    return run_query("""
        SELECT COUNT(*) AS cnt FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_ACTUALS
    """)


# =============================================================================
# Sidebar
# =============================================================================
with st.sidebar:
    st.markdown("""
    <div class="app-header">
        <h1>Energy Insights</h1>
        <p>ML-powered consumption analytics</p>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("---")

    page = st.radio(
        "Navigation",
        ["Overview", "My Consumption", "Predicted vs Actual", "Budget", "Model Performance"],
        index=0,
        label_visibility="collapsed",
    )

    st.markdown("---")

    # User selector (shared across pages)
    with st.spinner("Loading users..."):
        users_df = load_user_list()

    if users_df.empty:
        st.warning("No data available yet. Pipeline may still be running.")
        selected_user = None
    else:
        user_list = users_df["USER_ID"].tolist()
        selected_user = st.selectbox(
            "Select household",
            user_list,
            index=0,
            help="Choose a household to view its energy data",
        )

    st.markdown("---")
    st.markdown(
        '<p style="color:#8B95A5; font-size:0.75rem; text-align:center;">'
        "Snowflake + Databricks + XGBoost<br/>v1.0"
        "</p>",
        unsafe_allow_html=True,
    )


# =============================================================================
# Helper: Section header
# =============================================================================
def section_header(title, subtitle=""):
    html = f'<div class="section-header"><h3>{title}</h3>'
    if subtitle:
        html += f"<p>{subtitle}</p>"
    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


# =============================================================================
# PAGE 1: Overview
# =============================================================================
if page == "Overview":
    st.markdown("## Platform Overview")

    with st.spinner("Loading global statistics..."):
        stats = load_global_stats()
        actuals_count = load_actuals_count()

    if not stats.empty and stats.iloc[0]["TOTAL_USERS"] > 0:
        row = stats.iloc[0]
        act_cnt = actuals_count.iloc[0]["CNT"] if not actuals_count.empty else 0

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Households", f"{int(row['TOTAL_USERS']):,}")
        with c2:
            st.metric("Predictions", f"{int(row['TOTAL_PREDICTIONS']):,}")
        with c3:
            st.metric("Actual Records", f"{int(act_cnt):,}")
        with c4:
            st.metric("Avg Predicted kWh", f"{row['AVG_PREDICTED_KWH']:.3f}")

        st.markdown("")

        # Distribution of predicted consumption
        section_header("Consumption Distribution", "Predicted kWh across all households")

        dist_df = run_query("""
            SELECT USER_ID,
                   ROUND(AVG(KWH_PREDICTED), 4) AS avg_kwh,
                   ROUND(SUM(KWH_PREDICTED), 2) AS total_kwh,
                   COUNT(*) AS n_records
            FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_PREDICTIONS
            GROUP BY USER_ID
            ORDER BY avg_kwh DESC
        """)

        if not dist_df.empty:
            col1, col2 = st.columns(2)

            with col1:
                fig_hist = px.histogram(
                    dist_df, x="AVG_KWH", nbins=40,
                    color_discrete_sequence=[COLORS["primary"]],
                    labels={"AVG_KWH": "Average kWh per half-hour"},
                )
                fig_hist.update_traces(
                    marker=dict(
                        line=dict(width=0.5, color=COLORS["card"]),
                    ),
                    opacity=0.85,
                )
                apply_layout(fig_hist, "Distribution of Average Consumption")
                st.plotly_chart(fig_hist, use_container_width=True)

            with col2:
                top_n = dist_df.head(15)
                fig_bar = px.bar(
                    top_n, x="TOTAL_KWH", y="USER_ID",
                    orientation="h",
                    color="TOTAL_KWH",
                    color_continuous_scale=["#6C63FF", "#48C78E"],
                    labels={"TOTAL_KWH": "Total kWh", "USER_ID": "Household"},
                )
                fig_bar.update_layout(coloraxis_showscale=False)
                apply_layout(fig_bar, "Top 15 Households by Total Consumption")
                st.plotly_chart(fig_bar, use_container_width=True)

        # Temporal heatmap (all users, hourly)
        section_header("Temporal Patterns", "When does consumption peak across all households?")

        heatmap_df = run_query("""
            SELECT
                DAYOFWEEK(TIMESTAMP) AS dow,
                HOUR(TIMESTAMP) AS hour_of_day,
                ROUND(AVG(KWH_PREDICTED), 4) AS avg_kwh
            FROM ML_ENERGY_DB.PREDICTIONS.ENERGY_PREDICTIONS
            GROUP BY dow, hour_of_day
            ORDER BY dow, hour_of_day
        """)

        if not heatmap_df.empty:
            pivot = heatmap_df.pivot(
                index="DOW", columns="HOUR_OF_DAY", values="AVG_KWH"
            ).fillna(0)
            day_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            # Snowflake DAYOFWEEK: 0=Mon ... 6=Sun
            pivot.index = [day_labels[i] if i < len(day_labels) else str(i) for i in pivot.index]

            fig_heat = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=[f"{h:02d}:00" for h in pivot.columns],
                y=pivot.index,
                colorscale=[
                    [0.0, "#0E1117"],
                    [0.25, "#2D2B6B"],
                    [0.5, "#6C63FF"],
                    [0.75, "#48C78E"],
                    [1.0, "#FFC107"],
                ],
                hovertemplate="<b>%{y} %{x}</b><br>Avg: %{z:.4f} kWh<extra></extra>",
                colorbar=dict(
                    title=dict(text="kWh", font=dict(size=11)),
                    thickness=12,
                    len=0.6,
                ),
            ))
            apply_layout(fig_heat, "Average Predicted Consumption Heatmap", height=340)
            st.plotly_chart(fig_heat, use_container_width=True)

    else:
        st.info(
            "No data loaded yet. The ML pipeline may still be running. "
            "Data will appear once predictions are ingested into Snowflake via Snowpipe."
        )


# =============================================================================
# PAGE 2: My Consumption
# =============================================================================
elif page == "My Consumption":
    if selected_user is None:
        st.warning("No users available. Pipeline may still be running.")
        st.stop()

    st.markdown(f"## Household `{selected_user}`")

    # Load data
    with st.spinner("Loading household data..."):
        pred_df = load_predictions(selected_user)
        actual_df = load_actuals(selected_user)
        household_df = load_household(selected_user)

    # Household profile card
    if not household_df.empty:
        h = household_df.iloc[0]
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("ACORN Group", h.get("ACORN_GROUP", "N/A"))
        with c2:
            st.metric("ACORN Category", h.get("ACORN_CATEGORY", "N/A"))
        with c3:
            st.metric("Tariff", h.get("TARIFF_TYPE", "N/A"))

    st.markdown("")

    if pred_df.empty:
        st.info("No predictions available for this household yet.")
        st.stop()

    pred_df["TIMESTAMP"] = pd.to_datetime(pred_df["TIMESTAMP"])

    # KPIs
    section_header("Consumption Summary", "Predicted energy usage for this household")

    total_kwh = pred_df["KWH_PREDICTED"].sum()
    avg_kwh = pred_df["KWH_PREDICTED"].mean()
    peak_kwh = pred_df["KWH_PREDICTED"].max()
    n_points = len(pred_df)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Predicted", f"{total_kwh:.2f} kWh")
    with c2:
        st.metric("Avg / Half-hour", f"{avg_kwh:.4f} kWh")
    with c3:
        st.metric("Peak", f"{peak_kwh:.4f} kWh")
    with c4:
        st.metric("Data Points", f"{n_points:,}")

    # Daily consumption curve
    section_header("Daily Consumption Profile", "Half-hourly predicted consumption")

    pred_df["DATE"] = pred_df["TIMESTAMP"].dt.date
    pred_df["HOUR"] = pred_df["TIMESTAMP"].dt.hour + pred_df["TIMESTAMP"].dt.minute / 60

    # Average daily profile
    daily_profile = pred_df.groupby("HOUR")["KWH_PREDICTED"].mean().reset_index()

    fig_profile = go.Figure()
    fig_profile.add_trace(go.Scatter(
        x=daily_profile["HOUR"],
        y=daily_profile["KWH_PREDICTED"],
        mode="lines",
        fill="tozeroy",
        line=dict(color=COLORS["primary"], width=2.5, shape="spline"),
        fillcolor="rgba(108, 99, 255, 0.12)",
        name="Avg predicted",
        hovertemplate="<b>%{x:.1f}h</b><br>%{y:.4f} kWh<extra></extra>",
    ))

    # Add peak annotation
    peak_hour = daily_profile.loc[daily_profile["KWH_PREDICTED"].idxmax()]
    fig_profile.add_annotation(
        x=peak_hour["HOUR"], y=peak_hour["KWH_PREDICTED"],
        text=f"Peak: {peak_hour['KWH_PREDICTED']:.4f} kWh",
        showarrow=True,
        arrowhead=2,
        arrowcolor=COLORS["accent"],
        font=dict(color=COLORS["accent"], size=11, weight=600),
        bgcolor=COLORS["card"],
        bordercolor=COLORS["accent"],
        borderwidth=1,
        borderpad=6,
    )

    apply_layout(fig_profile, "Average Daily Load Profile")
    fig_profile.update_xaxes(
        title_text="Hour of Day",
        tickvals=list(range(0, 25, 3)),
        ticktext=[f"{h:02d}:00" for h in range(0, 25, 3)],
    )
    fig_profile.update_yaxes(title_text="kWh")
    st.plotly_chart(fig_profile, use_container_width=True)

    # Time series (full)
    section_header("Consumption Timeline", "All predicted readings over time")

    fig_ts = go.Figure()
    fig_ts.add_trace(go.Scatter(
        x=pred_df["TIMESTAMP"],
        y=pred_df["KWH_PREDICTED"],
        mode="lines",
        line=dict(color=COLORS["primary"], width=1),
        opacity=0.7,
        name="Predicted kWh",
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>%{y:.4f} kWh<extra></extra>",
    ))

    # Add rolling average
    if len(pred_df) > 48:
        pred_df_sorted = pred_df.sort_values("TIMESTAMP")
        pred_df_sorted["ROLLING_AVG"] = pred_df_sorted["KWH_PREDICTED"].rolling(48, min_periods=1).mean()
        fig_ts.add_trace(go.Scatter(
            x=pred_df_sorted["TIMESTAMP"],
            y=pred_df_sorted["ROLLING_AVG"],
            mode="lines",
            line=dict(color=COLORS["secondary"], width=2.5),
            name="24h Rolling Avg",
            hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Rolling avg: %{y:.4f} kWh<extra></extra>",
        ))

    apply_layout(fig_ts, "Predicted Consumption Over Time")
    fig_ts.update_xaxes(title_text="Date")
    fig_ts.update_yaxes(title_text="kWh")
    st.plotly_chart(fig_ts, use_container_width=True)

    # Day of week box plot
    col1, col2 = st.columns(2)

    with col1:
        section_header("Weekly Pattern", "Consumption by day of week")
        pred_df["DOW"] = pred_df["TIMESTAMP"].dt.day_name()
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        pred_df["DOW"] = pd.Categorical(pred_df["DOW"], categories=dow_order, ordered=True)

        fig_box = px.box(
            pred_df.sort_values("DOW"), x="DOW", y="KWH_PREDICTED",
            color_discrete_sequence=[COLORS["primary"]],
            labels={"DOW": "", "KWH_PREDICTED": "kWh"},
        )
        fig_box.update_traces(
            marker=dict(color=COLORS["primary"], opacity=0.5),
            line=dict(color=COLORS["primary"]),
            fillcolor="rgba(108, 99, 255, 0.15)",
        )
        apply_layout(fig_box, "Consumption by Day of Week", height=380)
        st.plotly_chart(fig_box, use_container_width=True)

    with col2:
        section_header("Hourly Peaks", "Average consumption per hour")
        hourly = pred_df.groupby(pred_df["TIMESTAMP"].dt.hour)["KWH_PREDICTED"].mean().reset_index()
        hourly.columns = ["HOUR", "AVG_KWH"]

        fig_polar = go.Figure()
        fig_polar.add_trace(go.Scatterpolar(
            r=hourly["AVG_KWH"],
            theta=[f"{h:02d}h" for h in hourly["HOUR"]],
            fill="toself",
            fillcolor="rgba(108, 99, 255, 0.15)",
            line=dict(color=COLORS["primary"], width=2),
            name="Avg kWh",
        ))
        fig_polar.update_layout(
            polar=dict(
                bgcolor="rgba(0,0,0,0)",
                radialaxis=dict(
                    gridcolor=COLORS["grid"],
                    showticklabels=True,
                    tickfont=dict(size=9, color=COLORS["muted"]),
                ),
                angularaxis=dict(
                    gridcolor=COLORS["grid"],
                    tickfont=dict(size=10, color=COLORS["muted"]),
                ),
            ),
        )
        apply_layout(fig_polar, "24h Consumption Radar", height=380)
        st.plotly_chart(fig_polar, use_container_width=True)


# =============================================================================
# PAGE 3: Predicted vs Actual
# =============================================================================
elif page == "Predicted vs Actual":
    if selected_user is None:
        st.warning("No users available.")
        st.stop()

    st.markdown(f"## Predicted vs Actual  |  `{selected_user}`")

    with st.spinner("Loading comparison data..."):
        pred_df = load_predictions(selected_user)
        actual_df = load_actuals(selected_user)

    if pred_df.empty:
        st.info("No predictions for this household yet.")
        st.stop()

    pred_df["TIMESTAMP"] = pd.to_datetime(pred_df["TIMESTAMP"])

    if actual_df.empty:
        st.warning(
            "No actual consumption data loaded yet. "
            "The Snowpipe for actuals may still be processing."
        )
        # Show predictions only
        section_header("Predictions Available", "Actual data will appear once Snowpipe completes ingestion")

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=pred_df["TIMESTAMP"], y=pred_df["KWH_PREDICTED"],
            mode="lines", line=dict(color=COLORS["primary"], width=1.5),
            name="Predicted",
        ))
        apply_layout(fig, "Predictions (waiting for actuals...)")
        st.plotly_chart(fig, use_container_width=True)
        st.stop()

    # Merge predictions and actuals on TIMESTAMP
    actual_df["TIMESTAMP"] = pd.to_datetime(actual_df["TIMESTAMP"])

    merged = pd.merge(
        pred_df[["TIMESTAMP", "KWH_PREDICTED"]],
        actual_df[["TIMESTAMP", "KWH_ACTUAL"]],
        on="TIMESTAMP",
        how="inner",
    )

    if merged.empty:
        st.warning("No overlapping timestamps between predictions and actuals.")
        st.stop()

    merged["ERROR"] = merged["KWH_PREDICTED"] - merged["KWH_ACTUAL"]
    merged["ABS_ERROR"] = merged["ERROR"].abs()
    merged["APE"] = (merged["ABS_ERROR"] / merged["KWH_ACTUAL"].replace(0, np.nan) * 100)

    # KPIs
    wmape = merged["ABS_ERROR"].sum() / merged["KWH_ACTUAL"].abs().sum() * 100
    rmse = np.sqrt((merged["ERROR"] ** 2).mean())
    mae = merged["ABS_ERROR"].mean()
    r2_corr = merged[["KWH_PREDICTED", "KWH_ACTUAL"]].corr().iloc[0, 1] ** 2

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("WMAPE", f"{wmape:.1f}%")
    with c2:
        st.metric("RMSE", f"{rmse:.4f} kWh")
    with c3:
        st.metric("MAE", f"{mae:.4f} kWh")
    with c4:
        st.metric("R-squared", f"{r2_corr:.3f}")

    st.markdown("")

    # Main comparison chart
    section_header("Overlay Comparison", "Predicted vs actual consumption over time")

    fig_comp = go.Figure()
    fig_comp.add_trace(go.Scatter(
        x=merged["TIMESTAMP"], y=merged["KWH_ACTUAL"],
        mode="lines",
        line=dict(color=COLORS["secondary"], width=1.5),
        name="Actual",
        opacity=0.85,
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Actual: %{y:.4f} kWh<extra></extra>",
    ))
    fig_comp.add_trace(go.Scatter(
        x=merged["TIMESTAMP"], y=merged["KWH_PREDICTED"],
        mode="lines",
        line=dict(color=COLORS["primary"], width=1.5),
        name="Predicted",
        opacity=0.85,
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Predicted: %{y:.4f} kWh<extra></extra>",
    ))

    # Error band
    fig_comp.add_trace(go.Scatter(
        x=merged["TIMESTAMP"],
        y=merged["ERROR"],
        mode="lines",
        fill="tozeroy",
        line=dict(color=COLORS["accent"], width=0.5),
        fillcolor="rgba(255, 107, 107, 0.1)",
        name="Error",
        yaxis="y2",
        hovertemplate="%{x|%Y-%m-%d %H:%M}<br>Error: %{y:.4f} kWh<extra></extra>",
    ))

    fig_comp.update_layout(
        yaxis2=dict(
            overlaying="y",
            side="right",
            title="Error (kWh)",
            gridcolor="rgba(0,0,0,0)",
            titlefont=dict(color=COLORS["accent"]),
            tickfont=dict(color=COLORS["accent"]),
        ),
    )
    apply_layout(fig_comp, "Predicted vs Actual with Error Band", height=480)
    fig_comp.update_xaxes(title_text="Date")
    fig_comp.update_yaxes(title_text="kWh")
    st.plotly_chart(fig_comp, use_container_width=True)

    # Scatter plot + Error distribution
    col1, col2 = st.columns(2)

    with col1:
        section_header("Scatter: Predicted vs Actual")
        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scattergl(
            x=merged["KWH_ACTUAL"],
            y=merged["KWH_PREDICTED"],
            mode="markers",
            marker=dict(
                color=COLORS["primary"],
                size=3,
                opacity=0.4,
            ),
            name="Data points",
            hovertemplate="Actual: %{x:.4f}<br>Predicted: %{y:.4f}<extra></extra>",
        ))
        # Perfect prediction line
        max_val = max(merged["KWH_ACTUAL"].max(), merged["KWH_PREDICTED"].max())
        fig_scatter.add_trace(go.Scatter(
            x=[0, max_val], y=[0, max_val],
            mode="lines",
            line=dict(color=COLORS["accent"], width=1.5, dash="dash"),
            name="Perfect prediction",
            showlegend=True,
        ))
        apply_layout(fig_scatter, "Prediction Accuracy Scatter", height=400)
        fig_scatter.update_xaxes(title_text="Actual kWh")
        fig_scatter.update_yaxes(title_text="Predicted kWh")
        st.plotly_chart(fig_scatter, use_container_width=True)

    with col2:
        section_header("Error Distribution")
        fig_err = px.histogram(
            merged, x="ERROR", nbins=60,
            color_discrete_sequence=[COLORS["accent"]],
            labels={"ERROR": "Prediction Error (kWh)"},
        )
        fig_err.update_traces(opacity=0.8, marker_line=dict(width=0.3, color=COLORS["card"]))
        fig_err.add_vline(
            x=0, line_dash="dash", line_color=COLORS["secondary"], line_width=2,
            annotation_text="Zero error",
            annotation_font=dict(color=COLORS["secondary"]),
        )
        apply_layout(fig_err, "Error Distribution", height=400)
        st.plotly_chart(fig_err, use_container_width=True)

    # Daily aggregated comparison
    section_header("Daily Aggregated Comparison", "Total kWh per day: predicted vs actual")

    merged["DATE"] = merged["TIMESTAMP"].dt.date
    daily = merged.groupby("DATE").agg(
        pred_kwh=("KWH_PREDICTED", "sum"),
        actual_kwh=("KWH_ACTUAL", "sum"),
    ).reset_index()
    daily["error_pct"] = (daily["pred_kwh"] - daily["actual_kwh"]) / daily["actual_kwh"].replace(0, np.nan) * 100

    fig_daily = make_subplots(specs=[[{"secondary_y": True}]])
    fig_daily.add_trace(go.Bar(
        x=daily["DATE"], y=daily["actual_kwh"],
        name="Actual",
        marker_color=COLORS["secondary"],
        opacity=0.6,
    ), secondary_y=False)
    fig_daily.add_trace(go.Bar(
        x=daily["DATE"], y=daily["pred_kwh"],
        name="Predicted",
        marker_color=COLORS["primary"],
        opacity=0.6,
    ), secondary_y=False)
    fig_daily.add_trace(go.Scatter(
        x=daily["DATE"], y=daily["error_pct"],
        name="Error %",
        line=dict(color=COLORS["accent"], width=2),
        mode="lines+markers",
        marker=dict(size=4),
    ), secondary_y=True)

    fig_daily.update_layout(barmode="group")
    fig_daily.update_yaxes(title_text="kWh", secondary_y=False)
    fig_daily.update_yaxes(title_text="Error %", secondary_y=True)
    apply_layout(fig_daily, "Daily Predicted vs Actual", height=420)
    st.plotly_chart(fig_daily, use_container_width=True)


# =============================================================================
# PAGE 4: Budget
# =============================================================================
elif page == "Budget":
    if selected_user is None:
        st.warning("No users available.")
        st.stop()

    st.markdown(f"## Budget Estimation  |  `{selected_user}`")

    with st.spinner("Loading data..."):
        pred_df = load_predictions(selected_user)
        household_df = load_household(selected_user)

    if pred_df.empty:
        st.info("No predictions available for this household yet.")
        st.stop()

    pred_df["TIMESTAMP"] = pd.to_datetime(pred_df["TIMESTAMP"])

    # Tariff configuration
    section_header("Tariff Configuration", "Adjust rates to estimate your energy bill")

    tariff_type = "Standard"
    if not household_df.empty:
        tariff_type = household_df.iloc[0].get("TARIFF_TYPE", "Standard")

    col1, col2, col3 = st.columns(3)
    with col1:
        rate_standard = st.number_input(
            "Standard rate (p/kWh)", value=24.5, step=0.5,
            help="UK average standard electricity rate",
        )
    with col2:
        rate_peak = st.number_input(
            "Peak rate (p/kWh)", value=34.0, step=0.5,
            help="Peak hours: 16:00-19:00",
        )
    with col3:
        rate_offpeak = st.number_input(
            "Off-peak rate (p/kWh)", value=12.0, step=0.5,
            help="Off-peak hours: 00:00-07:00",
        )

    standing_charge = st.number_input(
        "Standing charge (p/day)", value=61.64, step=1.0,
        help="Daily standing charge in pence",
    )

    st.markdown("")

    # Calculate costs
    pred_df["HOUR"] = pred_df["TIMESTAMP"].dt.hour
    pred_df["DATE"] = pred_df["TIMESTAMP"].dt.date

    def get_rate(hour):
        if 0 <= hour < 7:
            return rate_offpeak
        elif 16 <= hour < 19:
            return rate_peak
        return rate_standard

    pred_df["RATE"] = pred_df["HOUR"].apply(get_rate)
    pred_df["COST_PENCE"] = pred_df["KWH_PREDICTED"] * pred_df["RATE"]

    # KPIs
    total_cost_pence = pred_df["COST_PENCE"].sum()
    n_days = pred_df["DATE"].nunique()
    standing_total = standing_charge * n_days
    grand_total = (total_cost_pence + standing_total) / 100  # in GBP

    daily_avg_cost = grand_total / max(n_days, 1)
    monthly_estimate = daily_avg_cost * 30

    peak_cost = pred_df[pred_df["HOUR"].between(16, 18)]["COST_PENCE"].sum() / 100
    offpeak_cost = pred_df[pred_df["HOUR"].between(0, 6)]["COST_PENCE"].sum() / 100

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Total Estimated", f"\u00a3{grand_total:.2f}")
    with c2:
        st.metric("Monthly Estimate", f"\u00a3{monthly_estimate:.2f}")
    with c3:
        st.metric("Peak Period Cost", f"\u00a3{peak_cost:.2f}")
    with c4:
        st.metric("Off-Peak Cost", f"\u00a3{offpeak_cost:.2f}")

    st.markdown("")

    # Daily cost breakdown
    section_header("Daily Cost Breakdown", "Energy cost per day")

    daily_cost = pred_df.groupby("DATE").agg(
        energy_pence=("COST_PENCE", "sum"),
        kwh=("KWH_PREDICTED", "sum"),
    ).reset_index()
    daily_cost["standing_pence"] = standing_charge
    daily_cost["total_gbp"] = (daily_cost["energy_pence"] + daily_cost["standing_pence"]) / 100

    fig_cost = go.Figure()
    fig_cost.add_trace(go.Bar(
        x=daily_cost["DATE"],
        y=daily_cost["energy_pence"] / 100,
        name="Energy",
        marker_color=COLORS["primary"],
        hovertemplate="%{x}<br>Energy: \u00a3%{y:.2f}<extra></extra>",
    ))
    fig_cost.add_trace(go.Bar(
        x=daily_cost["DATE"],
        y=daily_cost["standing_pence"] / 100,
        name="Standing charge",
        marker_color=COLORS["muted"],
        hovertemplate="%{x}<br>Standing: \u00a3%{y:.2f}<extra></extra>",
    ))

    fig_cost.update_layout(barmode="stack")
    apply_layout(fig_cost, "Daily Cost Breakdown (GBP)")
    fig_cost.update_yaxes(title_text="Cost (GBP)")
    st.plotly_chart(fig_cost, use_container_width=True)

    # Cost by time period (pie/sunburst)
    col1, col2 = st.columns(2)

    with col1:
        section_header("Cost by Period")

        def classify_period(h):
            if 0 <= h < 7:
                return "Off-Peak (00-07)"
            elif 16 <= h < 19:
                return "Peak (16-19)"
            return "Standard"

        pred_df["PERIOD"] = pred_df["HOUR"].apply(classify_period)
        period_cost = pred_df.groupby("PERIOD")["COST_PENCE"].sum().reset_index()
        period_cost["COST_GBP"] = period_cost["COST_PENCE"] / 100

        fig_pie = go.Figure(data=[go.Pie(
            labels=period_cost["PERIOD"],
            values=period_cost["COST_GBP"],
            hole=0.55,
            marker=dict(
                colors=[COLORS["primary"], COLORS["accent"], COLORS["secondary"]],
                line=dict(color=COLORS["card"], width=2),
            ),
            textinfo="percent+label",
            textfont=dict(size=11),
            hovertemplate="%{label}<br>\u00a3%{value:.2f}<br>%{percent}<extra></extra>",
        )])
        apply_layout(fig_pie, "Cost Distribution by Tariff Period", height=380)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        section_header("Savings Tips")

        peak_pct = pred_df[pred_df["PERIOD"].str.contains("Peak")]["KWH_PREDICTED"].sum() / pred_df["KWH_PREDICTED"].sum() * 100

        if peak_pct > 25:
            st.markdown(f"""
            <div class="alert-card">
                <b>High peak usage detected</b><br/>
                <span style="color:#FF6B6B">{peak_pct:.0f}%</span> of your consumption falls in peak hours (16:00-19:00).
                Consider shifting dishwasher, laundry, and EV charging to off-peak hours.
            </div>
            """, unsafe_allow_html=True)

        potential_savings = (rate_peak - rate_offpeak) * pred_df[pred_df["PERIOD"].str.contains("Peak")]["KWH_PREDICTED"].sum() / 100
        if potential_savings > 0:
            st.markdown(f"""
            <div class="alert-card-success" style="background: linear-gradient(135deg, rgba(72, 199, 142, 0.08) 0%, rgba(72, 199, 142, 0.02) 100%);
                border: 1px solid rgba(72, 199, 142, 0.2); border-radius: 12px; padding: 16px 20px; margin-bottom: 12px;">
                <b>Potential savings</b><br/>
                Shifting all peak consumption to off-peak could save up to
                <span style="color:#48C78E; font-weight:700">\u00a3{potential_savings:.2f}</span>
                over this period.
            </div>
            """, unsafe_allow_html=True)

        # ACORN-based recommendation
        if not household_df.empty:
            acorn = household_df.iloc[0].get("ACORN_GROUP", "")
            if acorn in ["A", "B", "C"]:
                st.markdown("""
                <div class="alert-card-warning" style="background: linear-gradient(135deg, rgba(255, 193, 7, 0.08) 0%, rgba(255, 193, 7, 0.02) 100%);
                    border: 1px solid rgba(255, 193, 7, 0.2); border-radius: 12px; padding: 16px 20px; margin-bottom: 12px;">
                    <b>Affluent household profile</b><br/>
                    Your ACORN group indicates higher-than-average energy usage.
                    Smart thermostat and zone heating could significantly reduce consumption.
                </div>
                """, unsafe_allow_html=True)
            elif acorn in ["D", "E"]:
                st.markdown("""
                <div class="alert-card-warning" style="background: linear-gradient(135deg, rgba(255, 193, 7, 0.08) 0%, rgba(255, 193, 7, 0.02) 100%);
                    border: 1px solid rgba(255, 193, 7, 0.2); border-radius: 12px; padding: 16px 20px; margin-bottom: 12px;">
                    <b>Cost-conscious household</b><br/>
                    Focus on insulation and draught-proofing. Government grants may be
                    available for your ACORN category.
                </div>
                """, unsafe_allow_html=True)


# =============================================================================
# PAGE 5: Model Performance
# =============================================================================
elif page == "Model Performance":
    st.markdown("## Model Performance")

    with st.spinner("Loading model metrics..."):
        metrics_df = load_model_metrics()

    if metrics_df.empty:
        st.info("No model metrics available. The training pipeline must complete first.")
        st.stop()

    # Latest model
    latest_version = metrics_df["MODEL_VERSION"].iloc[0]
    latest_metrics = metrics_df[metrics_df["MODEL_VERSION"] == latest_version]

    section_header("Latest Model", f"Version: {latest_version}")

    avg_mape = latest_metrics["MAPE"].mean()
    avg_rmse = latest_metrics["RMSE"].mean()
    avg_mae = latest_metrics["MAE"].mean()
    avg_r2 = latest_metrics["R2_SCORE"].mean()
    n_rounds = len(latest_metrics)

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric("MAPE", f"{avg_mape:.1f}%")
    with c2:
        st.metric("RMSE", f"{avg_rmse:.4f}")
    with c3:
        st.metric("MAE", f"{avg_mae:.4f}")
    with c4:
        st.metric("R-squared", f"{avg_r2:.3f}")
    with c5:
        st.metric("Walk-Forward Rounds", n_rounds)

    st.markdown("")

    # Walk-forward validation chart
    section_header("Walk-Forward Validation", "Performance across temporal validation folds")

    col1, col2 = st.columns(2)

    with col1:
        fig_wf = go.Figure()
        fig_wf.add_trace(go.Bar(
            x=latest_metrics["TEST_MONTH"],
            y=latest_metrics["MAPE"],
            name="MAPE %",
            marker_color=COLORS["primary"],
            marker=dict(
                line=dict(width=0),
            ),
            hovertemplate="<b>%{x}</b><br>MAPE: %{y:.2f}%<extra></extra>",
        ))
        fig_wf.add_trace(go.Scatter(
            x=latest_metrics["TEST_MONTH"],
            y=latest_metrics["R2_SCORE"] * 100,
            name="R2 x 100",
            line=dict(color=COLORS["secondary"], width=2.5),
            mode="lines+markers",
            marker=dict(size=8, symbol="diamond"),
            yaxis="y2",
            hovertemplate="<b>%{x}</b><br>R2: %{y:.1f}%<extra></extra>",
        ))
        fig_wf.update_layout(
            yaxis2=dict(
                overlaying="y", side="right",
                title="R2 x 100",
                gridcolor="rgba(0,0,0,0)",
                titlefont=dict(color=COLORS["secondary"]),
                tickfont=dict(color=COLORS["secondary"]),
            ),
        )
        apply_layout(fig_wf, "MAPE & R2 per Validation Fold", height=400)
        fig_wf.update_yaxes(title_text="MAPE %")
        st.plotly_chart(fig_wf, use_container_width=True)

    with col2:
        fig_metrics = go.Figure()
        for metric_name, color in [("RMSE", COLORS["accent"]), ("MAE", COLORS["warning"])]:
            fig_metrics.add_trace(go.Scatter(
                x=latest_metrics["TEST_MONTH"],
                y=latest_metrics[metric_name],
                mode="lines+markers",
                name=metric_name,
                line=dict(color=color, width=2.5),
                marker=dict(size=8),
                hovertemplate=f"<b>%{{x}}</b><br>{metric_name}: %{{y:.4f}}<extra></extra>",
            ))
        apply_layout(fig_metrics, "RMSE & MAE per Fold", height=400)
        fig_metrics.update_yaxes(title_text="kWh")
        st.plotly_chart(fig_metrics, use_container_width=True)

    # Radar chart of model performance
    section_header("Performance Radar", "Multi-dimensional model quality assessment")

    # Normalize metrics for radar (higher = better, 0-1 scale)
    r2_norm = avg_r2  # already 0-1
    mape_norm = max(0, 1 - avg_mape / 100)  # lower MAPE = better
    rmse_norm = max(0, 1 - avg_rmse)  # rough normalization
    mae_norm = max(0, 1 - avg_mae)
    consistency = 1 - (latest_metrics["MAPE"].std() / max(latest_metrics["MAPE"].mean(), 0.01))
    consistency = max(0, min(1, consistency))

    categories = ["R-squared", "MAPE Score", "RMSE Score", "MAE Score", "Consistency"]
    values = [r2_norm, mape_norm, rmse_norm, mae_norm, consistency]

    fig_radar = go.Figure()
    fig_radar.add_trace(go.Scatterpolar(
        r=values + [values[0]],
        theta=categories + [categories[0]],
        fill="toself",
        fillcolor="rgba(108, 99, 255, 0.15)",
        line=dict(color=COLORS["primary"], width=2.5),
        marker=dict(size=8, color=COLORS["primary"]),
        name="Current Model",
    ))
    fig_radar.update_layout(
        polar=dict(
            bgcolor="rgba(0,0,0,0)",
            radialaxis=dict(
                visible=True, range=[0, 1],
                gridcolor=COLORS["grid"],
                tickfont=dict(size=9, color=COLORS["muted"]),
            ),
            angularaxis=dict(
                gridcolor=COLORS["grid"],
                tickfont=dict(size=11, color=COLORS["text"]),
            ),
        ),
    )
    apply_layout(fig_radar, "Model Quality Radar", height=440)
    st.plotly_chart(fig_radar, use_container_width=True)

    # Raw metrics table
    section_header("Detailed Metrics", "All walk-forward validation results")

    display_df = latest_metrics[["TEST_MONTH", "TRAIN_END_DATE", "MAPE", "RMSE", "MAE", "R2_SCORE"]].copy()
    display_df.columns = ["Test Month", "Train End Date", "MAPE (%)", "RMSE (kWh)", "MAE (kWh)", "R-squared"]

    st.dataframe(
        display_df.style.format({
            "MAPE (%)": "{:.2f}",
            "RMSE (kWh)": "{:.4f}",
            "MAE (kWh)": "{:.4f}",
            "R-squared": "{:.4f}",
        }).background_gradient(subset=["R-squared"], cmap="Greens", vmin=0.5, vmax=1.0),
        use_container_width=True,
        hide_index=True,
    )
