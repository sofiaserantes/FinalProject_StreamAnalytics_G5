import streamlit as st
import pandas as pd
import plotly.express as px
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
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* ═══════════════════════════════════════════════
   FORCE WHITE BACKGROUND — SPECIFIC SELECTORS ONLY
   ═══════════════════════════════════════════════ */
html,
body,
.stApp,
[data-testid="stAppViewContainer"],
[data-testid="stAppViewBlockContainer"],
section.main,
.main,
.main > div,
.block-container {
    background-color: #ffffff !important;
    font-family: 'Inter', sans-serif !important;
}

/* ═══════════════════════════════════════════════
   ALL TEXT: DARK ON WHITE
   ═══════════════════════════════════════════════ */
html, body, p, span, label, li, a, td, th,
h1, h2, h3, h4, h5, h6 {
    color: #111827 !important;
    font-family: 'Inter', sans-serif !important;
}

/* ═══════════════════════════════════════════════
   FONT SIZES — LARGE AND READABLE
   ═══════════════════════════════════════════════ */
body, p, span, label, li { font-size: 19px !important; }
.stMarkdown p { font-size: 19px !important; }
h1 { font-size: 2.6rem !important; font-weight: 800 !important; color: #111827 !important; }
h2 { font-size: 2rem   !important; font-weight: 700 !important; color: #111827 !important; }
h3 { font-size: 1.6rem !important; font-weight: 700 !important; color: #111827 !important; }
h4 { font-size: 1.3rem !important; font-weight: 700 !important; color: #111827 !important; }

/* ═══════════════════════════════════════════════
   SIDEBAR — WHITE, DARK TEXT
   ═══════════════════════════════════════════════ */
[data-testid="stSidebar"],
[data-testid="stSidebar"] > div,
[data-testid="stSidebar"] section,
[data-testid="stSidebar"] .block-container,
[data-testid="stSidebar"] [data-testid="stVerticalBlock"],
[data-testid="stSidebar"] .element-container {
    background-color: #ffffff !important;
    border-right: 2px solid #e5e7eb !important;
}
[data-testid="stSidebar"] * {
    color: #111827 !important;
    font-size: 17px !important;
}
[data-testid="stSidebar"] h2,
[data-testid="stSidebar"] h3 {
    font-size: 20px !important;
    font-weight: 700 !important;
    color: #111827 !important;
    margin-top: 1.2rem !important;
    margin-bottom: 0.4rem !important;
}

/* Sidebar widget containers — white */
[data-testid="stSidebar"] [data-baseweb="select"] > div,
[data-testid="stSidebar"] [data-baseweb="popover"],
[data-testid="stSidebar"] [data-baseweb="menu"],
[data-testid="stSidebar"] ul[role="listbox"],
[data-testid="stSidebar"] li[role="option"],
[data-testid="stSidebar"] .stMultiSelect > div > div,
[data-testid="stSidebar"] .stSelectbox > div > div {
    background-color: #ffffff !important;
    color: #111827 !important;
}
[data-testid="stSidebar"] [data-baseweb="tag"] {
    background-color: #dbeafe !important;
    color: #1e40af !important;
    font-size: 14px !important;
}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    font-size: 14px !important;
    color: #6b7280 !important;
}

/* Sidebar button */
[data-testid="stSidebar"] .stButton > button {
    background-color: #1d4ed8 !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 0.6rem 1rem !important;
    font-size: 16px !important;
    font-weight: 600 !important;
    width: 100% !important;
}

/* ═══════════════════════════════════════════════
   METRIC CARDS — WHITE BG, DARK TEXT
   ═══════════════════════════════════════════════ */
[data-testid="metric-container"] {
    background-color: #ffffff !important;
    border: 2px solid #e5e7eb !important;
    border-radius: 12px !important;
    padding: 1.2rem 1.4rem !important;
}
[data-testid="metric-container"] label,
[data-testid="metric-container"] [data-testid="stMetricLabel"] p {
    font-size: 16px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.07em !important;
    color: #6b7280 !important;
}
[data-testid="stMetricValue"] {
    font-size: 2.8rem !important;
    font-weight: 800 !important;
    color: #111827 !important;
    line-height: 1.1 !important;
}

/* ═══════════════════════════════════════════════
   TABS — WHITE BG, DARK SELECTED TEXT
   ═══════════════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    background-color: #ffffff !important;
    border-bottom: 2px solid #e5e7eb !important;
    gap: 0 !important;
}
.stTabs [data-baseweb="tab"] {
    background-color: #ffffff !important;
    font-size: 21px !important;
    font-weight: 600 !important;
    padding: 0.9rem 2rem !important;
    color: #9ca3af !important;
    border: none !important;
}
.stTabs [aria-selected="true"] {
    background-color: #ffffff !important;
    color: #111827 !important;
    border-bottom: 3px solid #1d4ed8 !important;
}
[data-testid="stTabsContent"] {
    background-color: #ffffff !important;
}

/* ═══════════════════════════════════════════════
   ALERTS / BANNERS — WHITE BG
   ═══════════════════════════════════════════════ */
.stAlert,
[data-testid="stAlert"],
div[data-baseweb="notification"] {
    background-color: #f0f9ff !important;
    border: 1.5px solid #bae6fd !important;
    border-radius: 8px !important;
}
.stAlert p, .stAlert div,
[data-testid="stAlert"] p {
    color: #0c4a6e !important;
    font-size: 16px !important;
}
.stSuccess, [data-testid="stAlert"][kind="success"] {
    background-color: #f0fdf4 !important;
    border-color: #bbf7d0 !important;
}
.stSuccess p { color: #14532d !important; }
.stWarning, [data-testid="stAlert"][kind="warning"] {
    background-color: #fffbeb !important;
    border-color: #fde68a !important;
}
.stWarning p { color: #78350f !important; }

/* ═══════════════════════════════════════════════
   EXPANDER — WHITE, NO KEY OVERLAP
   ═══════════════════════════════════════════════ */
[data-testid="stExpander"] {
    background-color: #ffffff !important;
    border: 1.5px solid #e5e7eb !important;
    border-radius: 8px !important;
    margin-top: 0.5rem !important;
}
[data-testid="stExpander"] summary {
    background-color: #ffffff !important;
    color: #111827 !important;
    font-size: 18px !important;
    font-weight: 600 !important;
    padding: 0.8rem 1rem !important;
}
[data-testid="stExpander"] summary p,
[data-testid="stExpander"] summary span {
    font-size: 18px !important;
    font-weight: 600 !important;
    color: #111827 !important;
}
[data-testid="stExpanderDetails"] {
    background-color: #ffffff !important;
}
/* Hide the stray key label Streamlit renders next to expanders */
.stElementContainer:has(> [data-testid="stExpander"]) > label,
.stElementContainer > label[for] {
    display: none !important;
    font-size: 0 !important;
    height: 0 !important;
    overflow: hidden !important;
}

/* ═══════════════════════════════════════════════
   DATAFRAME — WHITE
   ═══════════════════════════════════════════════ */
[data-testid="stDataFrame"],
[data-testid="stDataFrame"] > div,
.dvn-scroller {
    background-color: #ffffff !important;
}

/* ═══════════════════════════════════════════════
   SECTION LABEL
   ═══════════════════════════════════════════════ */
.section-label {
    font-size: 13px !important;
    font-weight: 800 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    color: #6b7280 !important;
    padding: 2rem 0 0.6rem 0 !important;
    border-bottom: 2px solid #e5e7eb !important;
    margin-bottom: 1.2rem !important;
    background-color: #ffffff !important;
}

/* ═══════════════════════════════════════════════
   HIDE STREAMLIT CHROME
   ═══════════════════════════════════════════════ */
#MainMenu { visibility: hidden; }
footer    { visibility: hidden; }
header    { visibility: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Colours ───────────────────────────────────────────────────────────────────
ZONE_COLORS = {
    "Z1_Center": "#1d4ed8",
    "Z2_North":  "#7c3aed",
    "Z3_South":  "#059669",
    "Z4_East":   "#d97706",
    "Z5_West":   "#dc2626",
}
STATUS_COLORS = {
    "IDLE":                   "#16a34a",
    "EN_ROUTE_TO_RESTAURANT": "#d97706",
    "WAITING":                "#2563eb",
    "EN_ROUTE_TO_CUSTOMER":   "#7c3aed",
    "OFFLINE":                "#6b7280",
}
EVENT_COLORS = {
    "CREATED":      "#2563eb",
    "ACCEPTED":     "#7c3aed",
    "PREP_STARTED": "#d97706",
    "READY":        "#16a34a",
    "PICKED_UP":    "#0891b2",
    "DELIVERED":    "#059669",
    "CANCELLED":    "#dc2626",
}

# ── Chart layout — white, large fonts, NO bar text labels (prevents overlap) ──
def chart_layout(height=400):
    return dict(
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        height=height,
        font=dict(family="Inter", size=16, color="#111827"),
        title_font=dict(family="Inter", size=19, color="#111827"),
        xaxis=dict(
            showgrid=False,
            linecolor="#e5e7eb",
            tickfont=dict(size=15, color="#374151"),
            title_font=dict(size=16, color="#374151"),
            automargin=True,
            tickangle=0,
        ),
        yaxis=dict(
            gridcolor="#f3f4f6",
            linecolor="#e5e7eb",
            tickfont=dict(size=15, color="#374151"),
            title_font=dict(size=16, color="#374151"),
            automargin=True,
        ),
        legend=dict(
            font=dict(size=15, color="#111827"),
            bgcolor="#ffffff",
            bordercolor="#e5e7eb",
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=24, r=24, t=80, b=24),
        hoverlabel=dict(
            bgcolor="#ffffff",
            font_size=15,
            font_family="Inter",
            font_color="#111827",
        ),
    )

def pie_layout(height=400):
    return dict(
        paper_bgcolor="#ffffff",
        height=height,
        font=dict(family="Inter", size=16, color="#111827"),
        title_font=dict(family="Inter", size=19, color="#111827"),
        legend=dict(
            font=dict(size=15, color="#111827"),
            bgcolor="#ffffff",
        ),
        margin=dict(l=24, r=24, t=80, b=24),
        hoverlabel=dict(
            bgcolor="#ffffff",
            font_size=15,
            font_family="Inter",
            font_color="#111827",
        ),
    )

# ── Data loader ───────────────────────────────────────────────────────────────
ACCOUNT_NAME   = "iesstsabbadbaa"
ACCOUNT_KEY    = "TZoXWmij7SjGyogaWJYIi179/BrsUcyWcPE5XleNTXI4Wqwak9JrkOSpNXzM88w39j5sjxupYtX6+AStInJJfQ=="
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

# ── Load raw data ─────────────────────────────────────────────────────────────
o_raw = load_blob("orders/output/")
c_raw = load_blob("couriers/output/")

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR — ALL FILTERS
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## Analytics Dashboard")
    st.markdown("Group 5 · Milestone 2")
    st.markdown("---")

    st.markdown("### Zone Filter")
    all_zones  = ["Z1_Center", "Z2_North", "Z3_South", "Z4_East", "Z5_West"]
    sel_zones  = st.multiselect("Select zones", options=all_zones, default=all_zones)

    st.markdown("---")
    st.markdown("### Order Filters")

    o_evt_opts = sorted(o_raw["event_type"].unique().tolist()) if not o_raw.empty else []
    sel_order_events = st.multiselect(
        "Order event types", options=o_evt_opts, default=o_evt_opts, key="ord_evt"
    )
    promo_filter = st.selectbox(
        "Promo applied", ["All orders", "Promo only", "No promo"], key="ord_promo"
    )
    order_chart_style = st.selectbox(
        "Volume chart style", ["Bar", "Line", "Area"], key="ord_chart"
    )
    rev_metric = st.selectbox(
        "Revenue chart metric",
        ["Total revenue (EUR)", "Avg order value (EUR)", "Delivered orders"],
        key="rev_metric",
    )
    cancel_group = st.selectbox(
        "Group cancellations by", ["Reason", "Zone"], key="cancel_view"
    )

    st.markdown("---")
    st.markdown("### Courier Filters")

    c_status_opts = sorted(c_raw["status"].unique().tolist()) if not c_raw.empty else []
    sel_statuses  = st.multiselect(
        "Courier status", options=c_status_opts, default=c_status_opts, key="cur_status"
    )
    c_evt_opts   = sorted(c_raw["event_type"].unique().tolist()) if not c_raw.empty else []
    sel_c_events = st.multiselect(
        "Courier event types", options=c_evt_opts, default=c_evt_opts, key="cur_evt"
    )
    courier_chart_style = st.selectbox(
        "Status chart style", ["Grouped bar", "Stacked bar", "Line"], key="cur_chart"
    )
    map_status_filter = st.multiselect(
        "Map: couriers by status",
        options=c_status_opts, default=c_status_opts, key="map_status"
    )

    st.markdown("---")
    st.markdown("### Refresh")
    refresh_secs = st.slider("Auto-refresh interval (s)", 10, 120, 30, 10)
    auto_refresh = st.toggle("Auto-refresh on", value=True)
    if st.button("Refresh now"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.caption(f"Last loaded: {datetime.now().strftime('%H:%M:%S')}")
    st.caption("iesstsabbadbaa-grp-01-05")

# ── Apply filters ─────────────────────────────────────────────────────────────
o = o_raw.copy() if not o_raw.empty else pd.DataFrame()
c = c_raw.copy() if not c_raw.empty else pd.DataFrame()

if not o.empty and "zone_id" in o.columns:
    o = o[o["zone_id"].isin(sel_zones)]
if not o.empty and sel_order_events:
    o = o[o["event_type"].isin(sel_order_events)]
if not o.empty:
    if promo_filter == "Promo only":
        o = o[o["promo_applied"] == True]
    elif promo_filter == "No promo":
        o = o[o["promo_applied"] == False]

if not c.empty and "zone_id" in c.columns:
    c = c[c["zone_id"].isin(sel_zones)]
if not c.empty and sel_statuses:
    c = c[c["status"].isin(sel_statuses)]
if not c.empty and sel_c_events:
    c = c[c["event_type"].isin(sel_c_events)]

# ── Page header ───────────────────────────────────────────────────────────────
st.markdown("# Food Delivery Analytics")
st.markdown(
    "Real-time pipeline · Azure Event Hubs → Spark Structured Streaming → Azure Blob Storage (Parquet)"
)
st.markdown("---")

# Status row
s1, s2, s3 = st.columns(3)
s1.success(f"Orders: **{len(o):,}** events loaded" if not o.empty else "Orders: no data yet")
s2.success(f"Couriers: **{len(c):,}** events loaded" if not c.empty else "Couriers: no data yet")
s3.info(f"Auto-refresh every {refresh_secs} seconds")

if o.empty and c.empty:
    st.warning("No data found in Blob Storage. Make sure Section 7 of your Spark notebook has completed at least one batch.")
    st.stop()

st.markdown("<br>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# TABS
# ══════════════════════════════════════════════════════════════════════════════
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

    # KPIs
    st.markdown('<div class="section-label">Key Metrics</div>', unsafe_allow_html=True)
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Events",      f"{len(o):,}")
    m2.metric("Delivered Orders",  f"{len(delivered):,}")
    m3.metric("Total Revenue",     f"€{total_rev:,.0f}")
    m4.metric("Avg Order Value",   f"€{avg_val:.2f}")
    m5.metric("Cancellation Rate", f"{crate}%")

    st.markdown("<br>", unsafe_allow_html=True)

    # Volume over time
    st.markdown('<div class="section-label">Order Volume Over Time</div>', unsafe_allow_html=True)
    if "event_time" in o.columns and not o.empty:
        dft = o.copy()
        dft["hour"] = dft["event_time"].dt.floor("h")
        oph = dft.groupby(["hour", "event_type"]).size().reset_index(name="Events")

        if order_chart_style == "Bar":
            fig_vol = px.bar(
                oph, x="hour", y="Events", color="event_type",
                title="Number of Order Events Per Hour by Type",
                color_discrete_map=EVENT_COLORS,
                labels={"hour": "Hour", "event_type": "Event Type"},
            )
        elif order_chart_style == "Area":
            fig_vol = px.area(
                oph, x="hour", y="Events", color="event_type",
                title="Order Event Volume Per Hour by Type",
                color_discrete_map=EVENT_COLORS,
                labels={"hour": "Hour", "event_type": "Event Type"},
            )
        else:
            fig_vol = px.line(
                oph, x="hour", y="Events", color="event_type",
                title="Order Event Trend Per Hour by Type",
                color_discrete_map=EVENT_COLORS,
                labels={"hour": "Hour", "event_type": "Event Type"},
            )
        fig_vol.update_layout(**chart_layout(420))
        st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Revenue & Cancellations
    st.markdown('<div class="section-label">Revenue & Cancellations</div>', unsafe_allow_html=True)
    col_a, col_b = st.columns(2)

    with col_a:
        if not delivered.empty:
            rbz = (
                delivered.groupby("zone_id")
                .agg(
                    delivered_orders=("order_id", "count"),
                    total_revenue_eur=("total_amount_eur", "sum"),
                    avg_order_value_eur=("total_amount_eur", "mean"),
                )
                .reset_index()
                .sort_values("total_revenue_eur", ascending=False)
            )
            rbz["total_revenue_eur"]   = rbz["total_revenue_eur"].round(2)
            rbz["avg_order_value_eur"] = rbz["avg_order_value_eur"].round(2)

            metric_map = {
                "Total revenue (EUR)":   ("total_revenue_eur",   "Total Revenue by Zone (EUR)"),
                "Avg order value (EUR)": ("avg_order_value_eur", "Average Order Value by Zone (EUR)"),
                "Delivered orders":      ("delivered_orders",    "Delivered Orders by Zone"),
            }
            ycol, chart_title = metric_map[rev_metric]
            fr = px.bar(
                rbz, x="zone_id", y=ycol, color="zone_id",
                title=chart_title,
                color_discrete_map=ZONE_COLORS,
                labels={"zone_id": "Zone", ycol: rev_metric},
            )
            # NO text on bars — prevents overlap
            fr.update_layout(**chart_layout(420), showlegend=False)
            st.plotly_chart(fr, use_container_width=True)
        else:
            st.info("No delivered orders match your current filters.")

    with col_b:
        if not cancelled.empty and "cancel_reason" in cancelled.columns:
            if cancel_group == "Reason":
                cr = cancelled["cancel_reason"].value_counts().reset_index()
                cr.columns = ["Category", "Cancellations"]
                title_c = "Cancellations by Reason"
            else:
                cr = cancelled["zone_id"].value_counts().reset_index()
                cr.columns = ["Category", "Cancellations"]
                title_c = "Cancellations by Zone"

            fc = px.bar(
                cr, x="Category", y="Cancellations", color="Category",
                title=title_c,
                color_discrete_sequence=["#dc2626","#ef4444","#f87171","#fca5a5","#fecaca"],
                labels={"Category": cancel_group},
            )
            # NO text on bars — prevents overlap
            fc.update_layout(**chart_layout(420), showlegend=False)
            st.plotly_chart(fc, use_container_width=True)
        else:
            st.info("No cancellations match your current filters.")

    if not delivered.empty:
        with st.expander("View full revenue breakdown table"):
            st.dataframe(
                rbz.rename(columns={
                    "zone_id":             "Zone",
                    "delivered_orders":    "Delivered Orders",
                    "total_revenue_eur":   "Total Revenue (EUR)",
                    "avg_order_value_eur": "Avg Order Value (EUR)",
                }),
                use_container_width=True,
                hide_index=True,
            )

    st.markdown("<br>", unsafe_allow_html=True)

    # Promo impact
    st.markdown('<div class="section-label">Promotional Campaign Impact</div>', unsafe_allow_html=True)
    p1, p2 = st.columns(2)

    with p1:
        if "promo_applied" in o.columns and not delivered.empty:
            pm = (
                delivered.groupby("promo_applied")["total_amount_eur"]
                .agg(["mean", "count"])
                .reset_index()
            )
            pm["promo_applied"] = pm["promo_applied"].map(
                {True: "With Promo", False: "No Promo"}
            )
            pm.columns = ["Group", "Avg Order Value", "Order Count"]
            fp = px.bar(
                pm, x="Group", y="Avg Order Value", color="Group",
                title="Average Order Value: Promo vs No Promo",
                color_discrete_sequence=["#1d4ed8", "#9ca3af"],
                labels={"Group": "", "Avg Order Value": "Avg Order Value (EUR)"},
            )
            # NO text on bars
            fp.update_layout(**chart_layout(380), showlegend=False)
            st.plotly_chart(fp, use_container_width=True)

    with p2:
        if "promo_applied" in o.columns:
            pc = o["promo_applied"].value_counts().reset_index()
            pc.columns = ["Applied", "Count"]
            pc["Applied"] = pc["Applied"].map({True: "Promo Applied", False: "No Promo"})
            fp2 = px.pie(
                pc, names="Applied", values="Count",
                title="Share of Orders with a Promotion Applied",
                color_discrete_sequence=["#1d4ed8", "#e5e7eb"],
            )
            fp2.update_traces(textinfo="none")
            fp2.update_layout(**pie_layout(380))
            st.plotly_chart(fp2, use_container_width=True)

    with st.expander("View raw order event data (first 500 rows)"):
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

    # KPIs
    st.markdown('<div class="section-label">Key Metrics</div>', unsafe_allow_html=True)
    ck1, ck2, ck3, ck4 = st.columns(4)
    ck1.metric("Total Events",      f"{len(c):,}")
    ck2.metric("Unique Couriers",   f"{unique_couriers:,}")
    ck3.metric("Avg Battery Level", f"{avg_batt:.0f}%")
    ck4.metric("Dropout Anomalies", f"{len(anom):,}")

    st.markdown("<br>", unsafe_allow_html=True)

    # Status distribution
    st.markdown('<div class="section-label">Courier Status Distribution</div>', unsafe_allow_html=True)
    s1, s2 = st.columns(2)

    with s1:
        ca = c.groupby(["zone_id", "status"]).size().reset_index(name="Count")
        if courier_chart_style == "Stacked bar":
            fcs = px.bar(
                ca, x="zone_id", y="Count", color="status", barmode="stack",
                title="Courier Status per Zone (Stacked)",
                color_discrete_map=STATUS_COLORS,
                labels={"zone_id": "Zone", "status": "Status", "Count": "Events"},
            )
        elif courier_chart_style == "Line":
            fcs = px.line(
                ca, x="zone_id", y="Count", color="status",
                title="Courier Status per Zone (Line)",
                color_discrete_map=STATUS_COLORS,
                labels={"zone_id": "Zone", "status": "Status", "Count": "Events"},
            )
        else:
            fcs = px.bar(
                ca, x="zone_id", y="Count", color="status", barmode="group",
                title="Courier Status per Zone (Grouped)",
                color_discrete_map=STATUS_COLORS,
                labels={"zone_id": "Zone", "status": "Status", "Count": "Events"},
            )
        fcs.update_layout(**chart_layout(420))
        st.plotly_chart(fcs, use_container_width=True)

    with s2:
        st2 = c["status"].value_counts().reset_index()
        st2.columns = ["Status", "Count"]
        fp_pie = px.pie(
            st2, names="Status", values="Count",
            title="Overall Courier Status Breakdown",
            color="Status",
            color_discrete_map=STATUS_COLORS,
        )
        fp_pie.update_traces(textinfo="none")
        fp_pie.update_layout(**pie_layout(420))
        st.plotly_chart(fp_pie, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Activity over time & battery
    st.markdown('<div class="section-label">Activity Over Time & Battery Health</div>', unsafe_allow_html=True)
    t1, t2 = st.columns(2)

    with t1:
        if "event_time" in c.columns and not c.empty:
            dfc = c.copy()
            dfc["hour"] = dfc["event_time"].dt.floor("h")
            cph = dfc.groupby(["hour", "event_type"]).size().reset_index(name="Events")
            fct = px.line(
                cph, x="hour", y="Events", color="event_type",
                title="Courier Events Per Hour by Type",
                labels={"hour": "Hour", "event_type": "Event Type"},
            )
            fct.update_layout(**chart_layout(380))
            st.plotly_chart(fct, use_container_width=True)

    with t2:
        if "battery_pct" in c.columns and "zone_id" in c.columns:
            batt = c.groupby("zone_id")["battery_pct"].mean().reset_index()
            batt.columns = ["Zone", "Avg Battery %"]
            fb = px.bar(
                batt, x="Zone", y="Avg Battery %", color="Zone",
                title="Average Battery Level by Zone",
                color_discrete_map=ZONE_COLORS,
                labels={"Zone": "Zone", "Avg Battery %": "Battery (%)"},
            )
            # NO text on bars — prevents overlap
            fb.update_layout(**chart_layout(380), showlegend=False)
            st.plotly_chart(fb, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Live map
    st.markdown('<div class="section-label">Live Courier Positions — Madrid</div>', unsafe_allow_html=True)
    latest = c.sort_values("event_time", ascending=False).drop_duplicates("courier_id")
    if "lat" in latest.columns and "lon" in latest.columns:
        map_data = (
            latest[latest["status"].isin(map_status_filter)]
            if map_status_filter else latest
        )
        fm = px.scatter_mapbox(
            map_data, lat="lat", lon="lon", color="status",
            color_discrete_map=STATUS_COLORS,
            hover_data=["courier_id", "zone_id", "battery_pct"],
            zoom=11,
            center={"lat": 40.4168, "lon": -3.7038},
            mapbox_style="open-street-map",
            height=540,
            title="Most Recent GPS Position per Courier",
        )
        fm.update_layout(
            margin={"r": 0, "t": 52, "l": 0, "b": 0},
            paper_bgcolor="#ffffff",
            font=dict(family="Inter", size=15),
            title_font=dict(size=17, color="#111827"),
            legend=dict(font=dict(size=14, color="#111827"), bgcolor="#ffffff"),
        )
        st.plotly_chart(fm, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Anomaly detection
    st.markdown('<div class="section-label">Anomaly Detection — Couriers Going Offline During Active Delivery</div>', unsafe_allow_html=True)

    # Metric full width
    st.metric("Dropout Anomalies Detected", len(anom))

    if not anom.empty:
        # Two charts side by side
        ch1, ch2 = st.columns(2)
        with ch1:
            bz = anom["zone_id"].value_counts().reset_index()
            bz.columns = ["Zone", "Anomalies"]
            fa = px.bar(
                bz, x="Zone", y="Anomalies", color="Zone",
                title="Courier Dropout Anomalies by Zone",
                color_discrete_map=ZONE_COLORS,
            )
            fa.update_layout(**chart_layout(400), showlegend=False)
            st.plotly_chart(fa, use_container_width=True)
        with ch2:
            mean_b = anom["battery_pct"].mean()
            fig_b = px.histogram(
                anom, x="battery_pct", nbins=20,
                title="Battery Level Distribution at Dropout",
                labels={"battery_pct": "Battery Level (%)"},
                color_discrete_sequence=["#dc2626"],
            )
            fig_b.add_vline(
                x=mean_b,
                line_dash="dash",
                line_color="#d97706",
                annotation_text=f"  Mean: {mean_b:.0f}%",
                annotation_font_size=15,
                annotation_font_color="#111827",
            )
            fig_b.update_layout(**chart_layout(400))
            st.plotly_chart(fig_b, use_container_width=True)

        # Table full width below charts
        st.markdown("**Couriers that went offline during an active delivery**")
        cols = [x for x in
                ["courier_id","current_order_id","zone_id","battery_pct","event_time"]
                if x in anom.columns]
        st.dataframe(
            anom[cols]
            .sort_values("event_time", ascending=False)
            .head(20)
            .rename(columns={
                "courier_id":       "Courier ID",
                "current_order_id": "Active Order",
                "zone_id":          "Zone",
                "battery_pct":      "Battery %",
                "event_time":       "Timestamp",
            }),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No anomalies detected with your current filter settings.")

    st.markdown("<br>", unsafe_allow_html=True)
    with st.expander("View raw courier event data (first 500 rows)"):
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
