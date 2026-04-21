import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import io
import time
from datetime import datetime

st.set_page_config(
    page_title="Food Delivery Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Syne:wght@700;800&family=DM+Sans:wght@400;500;600&display=swap');

/* ── BASE ── */
html, body, .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
section.main, .main, .main > div, .block-container {
    background-color: #ffffff !important;
    font-family: 'DM Sans', sans-serif !important;
}

/* ── ALL TEXT DARK ── */
html, body, p, span, label, li, a, td, th, h1, h2, h3, h4, h5, h6 {
    color: #0a0a0a !important;
    font-family: 'DM Sans', sans-serif !important;
}
p, span, label, li { font-size: 17px !important; }
.stMarkdown p       { font-size: 17px !important; }

/* ── PAGE TITLE ── */
h1 {
    font-family: 'Syne', sans-serif !important;
    font-size: 3.2rem !important;
    font-weight: 800 !important;
    color: #0a0a0a !important;
    letter-spacing: -0.04em !important;
    line-height: 1 !important;
}
h2 {
    font-family: 'Syne', sans-serif !important;
    font-size: 1.8rem !important;
    font-weight: 700 !important;
    color: #0a0a0a !important;
}
h3 {
    font-size: 1.3rem !important;
    font-weight: 700 !important;
    color: #0a0a0a !important;
}

/* ── SIDEBAR ── */
[data-testid="stSidebar"] {
    background-color: #ffffff !important;
}
[data-testid="stSidebar"] .stButton > button {
    background-color: #1e3a5f !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 6px !important;
    padding: 0.6rem 1rem !important;
    font-size: 15px !important;
    font-weight: 600 !important;
    width: 100% !important;
}

/* ── METRIC CARDS ── */
[data-testid="metric-container"] {
    background-color: #f9fafb !important;
    border: 2px solid #e5e7eb !important;
    border-radius: 12px !important;
    padding: 1.4rem 1.6rem !important;
}
[data-testid="metric-container"] label,
[data-testid="metric-container"] [data-testid="stMetricLabel"] p {
    font-size: 13px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    color: #6b7280 !important;
}
[data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 3rem !important;
    font-weight: 800 !important;
    color: #0a0a0a !important;
    line-height: 1.1 !important;
}

/* ── TABS ── */
.stTabs [data-baseweb="tab-list"] {
    background-color: #ffffff !important;
    border-bottom: 2px solid #e5e7eb !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background-color: #ffffff !important;
    font-family: 'Syne', sans-serif !important;
    font-size: 20px !important;
    font-weight: 700 !important;
    padding: 1rem 2.2rem !important;
    color: #9ca3af !important;
    border: none !important;
}
.stTabs [aria-selected="true"] {
    background-color: #ffffff !important;
    color: #0a0a0a !important;
    border-bottom: 3px solid #2e6fad !important;
}
[data-testid="stTabsContent"] {
    background-color: #ffffff !important;
}

/* ── SECTION LABELS ── */
.section-label {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 11px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.15em !important;
    color: #9ca3af !important;
    padding: 2rem 0 0.7rem 0 !important;
    border-bottom: 1.5px solid #f3f4f6 !important;
    margin-bottom: 1.4rem !important;
}

/* ── ALERTS ── */
.stAlert, [data-testid="stAlert"] {
    border-radius: 8px !important;
}
.stAlert p, [data-testid="stAlert"] p { font-size: 16px !important; }

/* ── EXPANDER — hide stray key label ── */
[data-testid="stExpander"] {
    background-color: #ffffff !important;
    border: 1.5px solid #e5e7eb !important;
    border-radius: 8px !important;
    margin-top: 0.5rem !important;
}
[data-testid="stExpander"] summary {
    background-color: #ffffff !important;
    color: #0a0a0a !important;
    font-size: 17px !important;
    font-weight: 600 !important;
    padding: 0.9rem 1rem !important;
}
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span {
    font-size: 17px !important;
    font-weight: 600 !important;
    color: #0a0a0a !important;
}
[data-testid="stExpanderDetails"] { background-color: #ffffff !important; }

/* Hide ALL stray "key" labels that Streamlit renders on widgets */
.stElementContainer > label[for],
.stElementContainer > label:first-child,
[data-testid="stExpander"] > label,
[data-testid="stSidebar"] .stElementContainer > label:first-child,
[data-testid="stSidebar"] label[data-testid="stWidgetLabel"],
label[class*="css"]:not([data-testid="stMarkdownContainer"]) {
    display: none !important;
    width: 0 !important;
    height: 0 !important;
    overflow: hidden !important;
    position: absolute !important;
    opacity: 0 !important;
    pointer-events: none !important;
}

/* ── DATAFRAME ── */
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] > div { background-color: #ffffff !important; }

/* ── SIDEBAR EXPANDERS ── */
[data-testid="stSidebar"] [data-testid="stExpander"] {
    background-color: #f9fafb !important;
    border: 1.5px solid #e5e7eb !important;
    border-radius: 8px !important;
    margin-bottom: 0.5rem !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary {
    background-color: #f9fafb !important;
    color: #0a0a0a !important;
    font-size: 16px !important;
    font-weight: 700 !important;
    padding: 0.7rem 1rem !important;
    font-family: 'Syne', sans-serif !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
[data-testid="stSidebar"] [data-testid="stExpander"] summary span {
    font-size: 16px !important;
    font-weight: 700 !important;
    color: #0a0a0a !important;
}
[data-testid="stSidebar"] [data-testid="stExpanderDetails"] {
    background-color: #ffffff !important;
    padding: 0.5rem 0.5rem !important;
}

/* ── SEARCH INPUT ── */
[data-testid="stSidebar"] input[type="text"],
[data-testid="stSidebar"] .stTextInput input {
    background-color: #ffffff !important;
    border: 1.5px solid #d1d5db !important;
    border-radius: 6px !important;
    color: #0a0a0a !important;
    font-size: 15px !important;
    padding: 0.5rem 0.75rem !important;
}
[data-testid="stSidebar"] .stTextInput label {
    font-size: 15px !important;
    font-weight: 600 !important;
    color: #374151 !important;
}

/* ── HIDE CHROME ── */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
/* NOTE: do NOT hide header — it contains the sidebar toggle button */
</style>
""", unsafe_allow_html=True)

# ── Cohesive colour palette — navy / slate / amber theme ─────────────────────
# Primary: deep navy  Secondary: slate-blue  Accent: amber  Danger: rose
ZONE_COLORS = {
    "Z1_Center": "#1e3a5f",   # deep navy
    "Z2_North":  "#2e6fad",   # mid blue
    "Z3_South":  "#4ea8de",   # sky blue
    "Z4_East":   "#f0a500",   # amber
    "Z5_West":   "#e05c5c",   # muted rose
}
STATUS_COLORS = {
    "IDLE":                   "#2e6fad",   # mid blue
    "EN_ROUTE_TO_RESTAURANT": "#f0a500",   # amber
    "WAITING":                "#4ea8de",   # sky blue
    "EN_ROUTE_TO_CUSTOMER":   "#1e3a5f",   # deep navy
    "OFFLINE":                "#a3b3c5",   # light slate
}
EVENT_COLORS = {
    "CREATED":      "#1e3a5f",   # deep navy
    "ACCEPTED":     "#2e6fad",   # mid blue
    "PREP_STARTED": "#f0a500",   # amber
    "READY":        "#4ea8de",   # sky blue
    "PICKED_UP":    "#6ab4e8",   # lighter blue
    "DELIVERED":    "#2e6fad",   # mid blue
    "CANCELLED":    "#e05c5c",   # muted rose
}
# Sequential palette for bar charts (single-colour series)
SEQ_PALETTE = ["#1e3a5f","#2e6fad","#4ea8de","#6ab4e8","#a3c9e8"]
# Danger colour
DANGER = "#e05c5c"
# Heatmap scales
HEATMAP_ORDERS  = "Blues"
HEATMAP_COURIERS = [[0,"#f0f7ff"],[0.5,"#2e6fad"],[1,"#1e3a5f"]]

def chart_layout(height=420):
    return dict(
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        height=height,
        font=dict(family="DM Sans", size=15, color="#111827"),
        title_font=dict(family="Syne", size=18, color="#0a0a0a"),
        xaxis=dict(
            showgrid=False,
            linecolor="#e5e7eb",
            tickfont=dict(size=14, color="#374151"),
            title_font=dict(size=15, color="#374151"),
            automargin=True,
        ),
        yaxis=dict(
            gridcolor="#f3f4f6",
            linecolor="#e5e7eb",
            tickfont=dict(size=14, color="#374151"),
            title_font=dict(size=15, color="#374151"),
            automargin=True,
        ),
        legend=dict(
            font=dict(size=14, color="#111827"),
            bgcolor="#ffffff",
            bordercolor="#e5e7eb",
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=20, r=20, t=72, b=20),
        hoverlabel=dict(
            bgcolor="#ffffff",
            font_size=14,
            font_family="DM Sans",
            font_color="#0a0a0a",
        ),
    )

def pie_layout(height=420):
    return dict(
        paper_bgcolor="#ffffff",
        height=height,
        font=dict(family="DM Sans", size=15, color="#111827"),
        title_font=dict(family="Syne", size=18, color="#0a0a0a"),
        legend=dict(font=dict(size=14, color="#111827"), bgcolor="#ffffff"),
        margin=dict(l=20, r=20, t=72, b=20),
        hoverlabel=dict(bgcolor="#ffffff", font_size=14, font_family="DM Sans", font_color="#0a0a0a"),
    )

# ── Data loader ───────────────────────────────────────────────────────────────
ACCOUNT_NAME   = "iesstsabbadbaa"
ACCOUNT_KEY    = "GfD8mpJmqw6gTqzyRpmV5tbHZ7RP1xkiO9X9hgmaMTdnHL1PL62AVmlejOmhHPFkBr2Pfl9DvmUC+AStYJXlzA=="
CONTAINER_NAME = "group5"

@st.cache_data(ttl=15)
def load_blob(prefix):
    try:
        svc  = BlobServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net",
            credential=ACCOUNT_KEY,
        )
        cont  = svc.get_container_client(CONTAINER_NAME)
        blobs = [b for b in cont.list_blobs(name_starts_with=prefix)
                 if b.name.endswith(".parquet")]
        if not blobs:
            return pd.DataFrame()
        frames = []
        for b in blobs:
            raw = cont.get_blob_client(b.name).download_blob().readall()
            frames.append(pq.read_table(io.BytesIO(raw)).to_pandas())
        df = pd.concat(frames, ignore_index=True)
        for col in ["event_time", "ingestion_time"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Could not load data: {e}")
        return pd.DataFrame()

o_raw = load_blob("orders/output/")
c_raw = load_blob("couriers/output/")

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## Dashboard")
    st.markdown("Group 5 · Milestone 2")
    st.markdown("---")

    st.markdown("### Search")
    order_search   = st.text_input("Search by Order ID", placeholder="e.g. O00000001", key="order_search")
    courier_search = st.text_input("Search by Courier ID", placeholder="e.g. C00141", key="courier_search")

    st.markdown("---")
    st.markdown("### Zone")
    all_zones = ["Z1_Center", "Z2_North", "Z3_South", "Z4_East", "Z5_West"]
    sel_zones = st.multiselect("Select zones", options=all_zones, default=all_zones)

    st.markdown("---")
    st.markdown("### Orders")
    o_evt_opts        = sorted(o_raw["event_type"].unique().tolist()) if not o_raw.empty else []
    sel_order_events  = st.multiselect("Event types", options=o_evt_opts, default=o_evt_opts, key="ord_evt")
    promo_filter      = st.selectbox("Promo applied", ["All orders", "Promo only", "No promo"], key="ord_promo")
    order_chart_style = st.selectbox("Volume chart",  ["Bar", "Line", "Area"], key="ord_chart")
    rev_metric        = st.selectbox("Revenue metric", ["Total revenue (EUR)", "Avg order value (EUR)", "Delivered orders"], key="rev_metric")
    cancel_group      = st.selectbox("Cancellations by", ["Reason", "Zone"], key="cancel_view")

    st.markdown("---")
    st.markdown("### Couriers")
    c_status_opts       = sorted(c_raw["status"].unique().tolist()) if not c_raw.empty else []
    sel_statuses        = st.multiselect("Status", options=c_status_opts, default=c_status_opts, key="cur_status")
    c_evt_opts          = sorted(c_raw["event_type"].unique().tolist()) if not c_raw.empty else []
    sel_c_events        = st.multiselect("Event types", options=c_evt_opts, default=c_evt_opts, key="cur_evt")
    courier_chart_style = st.selectbox("Status chart", ["Grouped bar", "Stacked bar", "Line"], key="cur_chart")
    map_status_filter   = st.multiselect("Map status", options=c_status_opts, default=c_status_opts, key="map_status")

    st.markdown("---")
    st.markdown("### Refresh")
    refresh_secs = st.slider("Interval (s)", 10, 120, 30, 10)
    auto_refresh = st.toggle("Auto-refresh", value=True)
    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")
    st.caption("iesstsabbadbaa-grp-01-05")

# ── Apply filters ─────────────────────────────────────────────────────────────
o = o_raw.copy() if not o_raw.empty else pd.DataFrame()
c = c_raw.copy() if not c_raw.empty else pd.DataFrame()

if not o.empty:
    if "zone_id" in o.columns:       o = o[o["zone_id"].isin(sel_zones)]
    if sel_order_events:             o = o[o["event_type"].isin(sel_order_events)]
    if promo_filter == "Promo only": o = o[o["promo_applied"] == True]
    elif promo_filter == "No promo": o = o[o["promo_applied"] == False]

if not c.empty:
    if "zone_id" in c.columns: c = c[c["zone_id"].isin(sel_zones)]
    if sel_statuses:           c = c[c["status"].isin(sel_statuses)]
    if sel_c_events:           c = c[c["event_type"].isin(sel_c_events)]

# ── Apply search filters ──────────────────────────────────────────────────────
order_search   = order_search.strip()   if "order_search"   in dir() else ""
courier_search = courier_search.strip() if "courier_search" in dir() else ""

# Will be used in search results section below

# ── PAGE HEADER ───────────────────────────────────────────────────────────────
st.markdown("# Food Delivery Analytics")
st.markdown("Real-time pipeline · Azure Event Hubs → Spark → Blob Storage (Parquet)")
st.markdown("---")

h1, h2, h3 = st.columns(3)
h1.success(f"Orders: **{len(o):,}** events" if not o.empty else "Orders: no data yet")
h2.success(f"Couriers: **{len(c):,}** events" if not c.empty else "Couriers: no data yet")
h3.info(f"Refreshing every {refresh_secs}s")

if o.empty and c.empty:
    st.warning("No data found. Make sure Section 7 of your Spark notebook is running.")
    st.stop()

st.markdown("<br>", unsafe_allow_html=True)

# ── Search results ───────────────────────────────────────────────────────────
if order_search:
    st.markdown('<div class="section-label">Search Results — Order ID</div>', unsafe_allow_html=True)
    if not o_raw.empty and "order_id" in o_raw.columns:
        results = o_raw[o_raw["order_id"].str.contains(order_search, case=False, na=False)]
        if not results.empty:
            st.success(f"Found **{len(results):,}** events matching order ID **{order_search}**")
            st.dataframe(
                results.sort_values("event_time", ascending=True)
                .rename(columns={"order_id":"Order ID","event_type":"Event","event_time":"Time",
                                  "zone_id":"Zone","courier_id":"Courier","total_amount_eur":"Amount (EUR)",
                                  "promo_applied":"Promo","cancel_reason":"Cancel Reason"}),
                use_container_width=True, hide_index=True)
        else:
            st.warning(f"No events found for order ID: **{order_search}**")
    st.markdown("<br>", unsafe_allow_html=True)

if courier_search:
    st.markdown('<div class="section-label">Search Results — Courier ID</div>', unsafe_allow_html=True)
    if not c_raw.empty and "courier_id" in c_raw.columns:
        results_c = c_raw[c_raw["courier_id"].str.contains(courier_search, case=False, na=False)]
        if not results_c.empty:
            st.success(f"Found **{len(results_c):,}** events for courier **{courier_search}**")
            st.dataframe(
                results_c.sort_values("event_time", ascending=True)
                .rename(columns={"courier_id":"Courier ID","event_type":"Event","event_time":"Time",
                                  "zone_id":"Zone","status":"Status","battery_pct":"Battery %",
                                  "current_order_id":"Current Order","lat":"Lat","lon":"Lon"}),
                use_container_width=True, hide_index=True)
        else:
            st.warning(f"No events found for courier ID: **{courier_search}**")
    st.markdown("<br>", unsafe_allow_html=True)

tab1, tab2 = st.tabs(["  Order Performance  ", "  Courier Operations  "])

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — ORDER PERFORMANCE
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    if o.empty:
        st.info("No order data matches your current filters.")
        st.stop()

    delivered = o[o.event_type == "DELIVERED"]
    cancelled = o[o.event_type == "CANCELLED"]
    created   = o[o.event_type == "CREATED"]

    total_rev = delivered["total_amount_eur"].sum()  if not delivered.empty else 0
    avg_val   = delivered["total_amount_eur"].mean() if not delivered.empty else 0
    crate     = round(len(cancelled) / max(len(created), 1) * 100, 1)
    promo_pct = round(o["promo_applied"].mean() * 100, 1) if "promo_applied" in o.columns else 0

    # ── KPIs ──────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Key Performance Indicators</div>', unsafe_allow_html=True)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Events",       f"{len(o):,}")
    k2.metric("Delivered Orders",   f"{len(delivered):,}")
    k3.metric("Total Revenue",      f"€{total_rev:,.0f}")
    k4.metric("Avg Order Value",    f"€{avg_val:.2f}")
    k5.metric("Cancellation Rate",  f"{crate}%")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Volume over time ──────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Order Volume Over Time</div>', unsafe_allow_html=True)
    if "event_time" in o.columns:
        dft = o.copy()
        dft["hour"] = dft["event_time"].dt.floor("h")
        oph = dft.groupby(["hour", "event_type"]).size().reset_index(name="Events")
        if order_chart_style == "Bar":
            fv = px.bar(oph, x="hour", y="Events", color="event_type",
                        title="Order Events Per Hour by Type",
                        color_discrete_map=EVENT_COLORS,
                        labels={"hour": "Hour", "event_type": "Event Type"})
        elif order_chart_style == "Area":
            fv = px.area(oph, x="hour", y="Events", color="event_type",
                         title="Order Event Volume Per Hour by Type",
                         color_discrete_map=EVENT_COLORS,
                         labels={"hour": "Hour", "event_type": "Event Type"})
        else:
            fv = px.line(oph, x="hour", y="Events", color="event_type",
                         title="Order Event Trend Per Hour by Type",
                         color_discrete_map=EVENT_COLORS,
                         labels={"hour": "Hour", "event_type": "Event Type"})
        fv.update_layout(**chart_layout(420))
        st.plotly_chart(fv, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Heatmap: orders by hour of day × day of week ──────────────────────────
    st.markdown('<div class="section-label">Order Density Heatmap — Hour of Day vs Day of Week</div>', unsafe_allow_html=True)
    if "event_time" in o.columns and not o.empty:
        hm = o.copy()
        hm = hm[hm["event_type"] == "CREATED"]
        if not hm.empty:
            hm["hour_of_day"] = hm["event_time"].dt.hour
            hm["day_of_week"] = hm["event_time"].dt.day_name()
            day_order = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
            pivot = (hm.groupby(["day_of_week","hour_of_day"])
                       .size()
                       .reset_index(name="Orders")
                       .pivot(index="day_of_week", columns="hour_of_day", values="Orders")
                       .reindex(day_order)
                       .fillna(0))
            fig_hm = go.Figure(data=go.Heatmap(
                z=pivot.values,
                x=[f"{h:02d}:00" for h in pivot.columns],
                y=pivot.index.tolist(),
                colorscale=[[0,"#f0f7ff"],[0.25,"#9ecae1"],[0.5,"#4ea8de"],[0.75,"#2e6fad"],[1,"#1e3a5f"]],
                hoverongaps=False,
                hovertemplate="<b>%{y} %{x}</b><br>Orders: %{z}<extra></extra>",
                colorbar=dict(
                    title=dict(text="Orders", font=dict(size=14, family="DM Sans")),
                    tickfont=dict(size=13, family="DM Sans"),
                ),
            ))
            fig_hm.update_layout(
                title="New Orders Created by Hour of Day and Day of Week",
                plot_bgcolor="#ffffff",
                paper_bgcolor="#ffffff",
                height=380,
                font=dict(family="DM Sans", size=14, color="#111827"),
                title_font=dict(family="Syne", size=18, color="#0a0a0a"),
                xaxis=dict(title="Hour of Day", tickfont=dict(size=13), title_font=dict(size=14), tickangle=-45),
                yaxis=dict(title="Day of Week",  tickfont=dict(size=13), title_font=dict(size=14)),
                margin=dict(l=20, r=20, t=72, b=20),
                hoverlabel=dict(bgcolor="#ffffff", font_size=14, font_family="DM Sans", font_color="#0a0a0a"),
            )
            st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Revenue & Cancellations ───────────────────────────────────────────────
    st.markdown('<div class="section-label">Revenue & Cancellations</div>', unsafe_allow_html=True)
    ca, cb = st.columns(2)

    with ca:
        if not delivered.empty:
            rbz = (delivered.groupby("zone_id")
                   .agg(delivered_orders=("order_id","count"),
                        total_revenue_eur=("total_amount_eur","sum"),
                        avg_order_value_eur=("total_amount_eur","mean"))
                   .reset_index()
                   .sort_values("total_revenue_eur", ascending=False))
            rbz["total_revenue_eur"]   = rbz["total_revenue_eur"].round(2)
            rbz["avg_order_value_eur"] = rbz["avg_order_value_eur"].round(2)
            metric_map = {
                "Total revenue (EUR)":   ("total_revenue_eur",   "Total Delivered Revenue by Zone (EUR)"),
                "Avg order value (EUR)": ("avg_order_value_eur", "Average Order Value by Zone (EUR)"),
                "Delivered orders":      ("delivered_orders",    "Delivered Orders by Zone"),
            }
            ycol, ctitle = metric_map[rev_metric]
            fr = px.bar(rbz, x="zone_id", y=ycol, color="zone_id",
                        title=ctitle, color_discrete_map=ZONE_COLORS,
                        labels={"zone_id": "Zone", ycol: rev_metric})
            fr.update_layout(**chart_layout(420), showlegend=False)
            st.plotly_chart(fr, use_container_width=True)
        else:
            st.info("No delivered orders match current filters.")

    with cb:
        if not cancelled.empty and "cancel_reason" in cancelled.columns:
            if cancel_group == "Reason":
                cr = cancelled["cancel_reason"].value_counts().reset_index()
                cr.columns = ["Category", "Cancellations"]
                ctc = "Cancellations by Reason"
            else:
                cr = cancelled["zone_id"].value_counts().reset_index()
                cr.columns = ["Category", "Cancellations"]
                ctc = "Cancellations by Zone"
            fc = px.bar(cr, x="Category", y="Cancellations", color="Category",
                        title=ctc,
                        color_discrete_sequence=SEQ_PALETTE)
            fc.update_layout(**chart_layout(420), showlegend=False)
            st.plotly_chart(fc, use_container_width=True)
        else:
            st.info("No cancellations match current filters.")

    if not delivered.empty:
        st.markdown("---")
        st.markdown("**Revenue Breakdown by Zone**")
        st.dataframe(
            rbz.rename(columns={"zone_id": "Zone", "delivered_orders": "Delivered Orders",
                                "total_revenue_eur": "Total Revenue (EUR)",
                                "avg_order_value_eur": "Avg Order Value (EUR)"}),
            use_container_width=True, hide_index=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Promo impact ──────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Promotional Campaign Impact</div>', unsafe_allow_html=True)
    pp1, pp2 = st.columns(2)

    with pp1:
        if "promo_applied" in o.columns and not delivered.empty:
            pm = (delivered.groupby("promo_applied")["total_amount_eur"]
                  .agg(["mean","count"]).reset_index())
            pm["promo_applied"] = pm["promo_applied"].map({True:"With Promo", False:"No Promo"})
            pm.columns = ["Group","Avg Order Value","Order Count"]
            fp = px.bar(pm, x="Group", y="Avg Order Value", color="Group",
                        title="Average Order Value: Promotional vs Non-Promotional Orders",
                        color_discrete_sequence=["#1e3a5f","#a3b3c5"],
                        labels={"Group":"","Avg Order Value":"Avg Order Value (EUR)"})
            fp.update_layout(**chart_layout(380), showlegend=False)
            st.plotly_chart(fp, use_container_width=True)

    with pp2:
        if "promo_applied" in o.columns:
            pc = o["promo_applied"].value_counts().reset_index()
            pc.columns = ["Applied","Count"]
            pc["Applied"] = pc["Applied"].map({True:"Promo Applied", False:"No Promo"})
            fp2 = px.pie(pc, names="Applied", values="Count",
                         title="Share of Orders Where a Promotion Was Applied",
                         color_discrete_sequence=["#2e6fad","#e0ecf8"])
            fp2.update_traces(textinfo="none")
            fp2.update_layout(**pie_layout(380))
            st.plotly_chart(fp2, use_container_width=True)

    st.markdown("---")
    st.markdown("**Raw Order Event Data (first 500 rows)**")
    st.dataframe(
        o.sort_values("event_time", ascending=False).head(500),
        use_container_width=True,
        hide_index=True,
    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — COURIER OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    if c.empty:
        st.info("No courier data matches your current filters.")
        st.stop()

    anom            = c[(c.event_type == "OFFLINE") & c.current_order_id.notna()]
    avg_batt        = c["battery_pct"].mean() if "battery_pct" in c.columns else 0
    unique_couriers = c["courier_id"].nunique() if "courier_id" in c.columns else 0
    offline_pct     = round(len(c[c["status"]=="OFFLINE"]) / max(len(c),1) * 100, 1)

    # ── KPIs ──────────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Key Performance Indicators</div>', unsafe_allow_html=True)
    ck1, ck2, ck3, ck4, ck5 = st.columns(5)
    ck1.metric("Total Events",      f"{len(c):,}")
    ck2.metric("Unique Couriers",   f"{unique_couriers:,}")
    ck3.metric("Avg Battery",       f"{avg_batt:.0f}%")
    ck4.metric("Dropout Anomalies", f"{len(anom):,}")
    ck5.metric("Offline Rate",      f"{offline_pct}%")

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Status distribution ───────────────────────────────────────────────────
    st.markdown('<div class="section-label">Courier Status Distribution</div>', unsafe_allow_html=True)
    cs1, cs2 = st.columns(2)

    with cs1:
        ca_data = c.groupby(["zone_id","status"]).size().reset_index(name="Count")
        if courier_chart_style == "Stacked bar":
            fcs = px.bar(ca_data, x="zone_id", y="Count", color="status", barmode="stack",
                         title="Courier Status per Zone (Stacked)",
                         color_discrete_map=STATUS_COLORS,
                         labels={"zone_id":"Zone","status":"Status","Count":"Events"})
        elif courier_chart_style == "Line":
            fcs = px.line(ca_data, x="zone_id", y="Count", color="status",
                          title="Courier Status per Zone",
                          color_discrete_map=STATUS_COLORS,
                          labels={"zone_id":"Zone","status":"Status","Count":"Events"})
        else:
            fcs = px.bar(ca_data, x="zone_id", y="Count", color="status", barmode="group",
                         title="Courier Status per Zone (Grouped)",
                         color_discrete_map=STATUS_COLORS,
                         labels={"zone_id":"Zone","status":"Status","Count":"Events"})
        fcs.update_layout(**chart_layout(420))
        st.plotly_chart(fcs, use_container_width=True)

    with cs2:
        st2 = c["status"].value_counts().reset_index()
        st2.columns = ["Status","Count"]
        fp_pie = px.pie(st2, names="Status", values="Count",
                        title="Overall Courier Status Breakdown",
                        color="Status", color_discrete_map=STATUS_COLORS)
        fp_pie.update_traces(textinfo="none")
        fp_pie.update_layout(**pie_layout(420))
        st.plotly_chart(fp_pie, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Heatmap: courier activity by hour × zone ──────────────────────────────
    st.markdown('<div class="section-label">Courier Activity Heatmap — Hour of Day vs Zone</div>', unsafe_allow_html=True)
    if "event_time" in c.columns and not c.empty:
        chm = c.copy()
        chm["hour_of_day"] = chm["event_time"].dt.hour
        cpivot = (chm.groupby(["zone_id","hour_of_day"])
                     .size()
                     .reset_index(name="Events")
                     .pivot(index="zone_id", columns="hour_of_day", values="Events")
                     .fillna(0))
        fig_chm = go.Figure(data=go.Heatmap(
            z=cpivot.values,
            x=[f"{h:02d}:00" for h in cpivot.columns],
            y=cpivot.index.tolist(),
            colorscale=[[0,"#fffbf0"],[0.25,"#fde68a"],[0.5,"#f0a500"],[0.75,"#c47f00"],[1,"#1e3a5f"]],
            hoverongaps=False,
            hovertemplate="<b>%{y} — %{x}</b><br>Events: %{z}<extra></extra>",
            colorbar=dict(
                title=dict(text="Events", font=dict(size=14, family="DM Sans")),
                tickfont=dict(size=13, family="DM Sans"),
            ),
        ))
        fig_chm.update_layout(
            title="Courier Events by Hour of Day and Zone",
            plot_bgcolor="#ffffff",
            paper_bgcolor="#ffffff",
            height=340,
            font=dict(family="DM Sans", size=14, color="#111827"),
            title_font=dict(family="Syne", size=18, color="#0a0a0a"),
            xaxis=dict(title="Hour of Day", tickfont=dict(size=13), title_font=dict(size=14), tickangle=-45),
            yaxis=dict(title="Zone",        tickfont=dict(size=13), title_font=dict(size=14)),
            margin=dict(l=20, r=20, t=72, b=20),
            hoverlabel=dict(bgcolor="#ffffff", font_size=14, font_family="DM Sans", font_color="#0a0a0a"),
        )
        st.plotly_chart(fig_chm, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Activity over time & battery ──────────────────────────────────────────
    st.markdown('<div class="section-label">Activity Over Time & Battery Health</div>', unsafe_allow_html=True)
    t1, t2 = st.columns(2)

    with t1:
        if "event_time" in c.columns:
            dfc = c.copy()
            dfc["hour"] = dfc["event_time"].dt.floor("h")
            cph = dfc.groupby(["hour","event_type"]).size().reset_index(name="Events")
            fct = px.line(cph, x="hour", y="Events", color="event_type",
                          title="Courier Events Per Hour by Type",
                          labels={"hour":"Hour","event_type":"Event Type"})
            fct.update_layout(**chart_layout(380))
            st.plotly_chart(fct, use_container_width=True)

    with t2:
        if "battery_pct" in c.columns:
            batt = c.groupby("zone_id")["battery_pct"].mean().reset_index()
            batt.columns = ["Zone","Avg Battery %"]
            fb = px.bar(batt, x="Zone", y="Avg Battery %", color="Zone",
                        title="Average Courier Battery Level by Zone",
                        color_discrete_map=ZONE_COLORS,
                        labels={"Zone":"Zone","Avg Battery %":"Battery (%)"})
            fb.update_layout(**chart_layout(380), showlegend=False)
            st.plotly_chart(fb, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Live map ──────────────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Live Courier Positions — Madrid</div>', unsafe_allow_html=True)
    latest = c.sort_values("event_time", ascending=False).drop_duplicates("courier_id")
    if "lat" in latest.columns and "lon" in latest.columns:
        map_data = latest[latest["status"].isin(map_status_filter)] if map_status_filter else latest
        fm = px.scatter_mapbox(
            map_data, lat="lat", lon="lon", color="status",
            color_discrete_map=STATUS_COLORS,
            hover_data=["courier_id","zone_id","battery_pct"],
            zoom=11, center={"lat":40.4168,"lon":-3.7038},
            mapbox_style="open-street-map", height=520,
            title="Most Recent GPS Position per Courier",
        )
        fm.update_layout(
            margin={"r":0,"t":52,"l":0,"b":0},
            paper_bgcolor="#ffffff",
            font=dict(family="DM Sans", size=15),
            title_font=dict(family="Syne", size=18, color="#0a0a0a"),
            legend=dict(font=dict(size=14, color="#0a0a0a"), bgcolor="#ffffff"),
        )
        st.plotly_chart(fm, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Anomaly detection ─────────────────────────────────────────────────────
    st.markdown('<div class="section-label">Anomaly Detection — Couriers Going Offline During Active Delivery</div>', unsafe_allow_html=True)

    st.metric("Dropout Anomalies Detected", len(anom))

    if not anom.empty:
        ac1, ac2 = st.columns(2)
        with ac1:
            bz = anom["zone_id"].value_counts().reset_index()
            bz.columns = ["Zone","Anomalies"]
            fa = px.bar(bz, x="Zone", y="Anomalies", color="Zone",
                        title="Courier Dropout Anomalies by Zone",
                        color_discrete_map=ZONE_COLORS)
            fa.update_layout(**chart_layout(400), showlegend=False)
            st.plotly_chart(fa, use_container_width=True)
        with ac2:
            mean_b = anom["battery_pct"].mean()
            fig_b = px.histogram(anom, x="battery_pct", nbins=20,
                                 title="Battery Level at the Point of Dropout",
                                 labels={"battery_pct":"Battery Level (%)"},
                                 color_discrete_sequence=["#e05c5c"])
            fig_b.add_vline(x=mean_b, line_dash="dash", line_color="#f0a500",
                            annotation_text=f"  Mean: {mean_b:.0f}%",
                            annotation_font_size=14,
                            annotation_font_color="#0a0a0a")
            fig_b.update_layout(**chart_layout(400))
            st.plotly_chart(fig_b, use_container_width=True)

        st.markdown("**Couriers that went offline while carrying an active order**")
        cols = [x for x in ["courier_id","current_order_id","zone_id","battery_pct","event_time"]
                if x in anom.columns]
        st.dataframe(
            anom[cols].sort_values("event_time", ascending=False).head(20)
            .rename(columns={"courier_id":"Courier ID","current_order_id":"Active Order",
                              "zone_id":"Zone","battery_pct":"Battery %","event_time":"Timestamp"}),
            use_container_width=True, hide_index=True)
    else:
        st.success("No anomalies detected with your current filter settings.")

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown("---")
    st.markdown("**Raw Courier Event Data (first 500 rows)**")
    st.dataframe(
        c.sort_values("event_time", ascending=False).head(500),
        use_container_width=True,
        hide_index=True,
    )

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh_secs)
    st.cache_data.clear()
    st.rerun()