import streamlit as st
import pandas as pd
import plotly.express as px
from azure.storage.blob import BlobServiceClient
import pyarrow.parquet as pq
import io
import time
from datetime import datetime

st.set_page_config(page_title="Food Delivery · Live Analytics", page_icon="🛵", layout="wide")

ACCOUNT_NAME   = "iesstsabbadbaa"
ACCOUNT_KEY    = "TZoXWmij7SjGyogaWJYIi179/BrsUcyWcPE5XleNTXI4Wqwak9JrkOSpNXzM88w39j5sjxupYtX6+AStInJJfQ=="
CONTAINER_NAME = "group5"

ZONE_COLORS   = {"Z1_Center":"#FF6B35","Z2_North":"#4ECDC4","Z3_South":"#45B7D1","Z4_East":"#96CEB4","Z5_West":"#FFEAA7"}
STATUS_COLORS = {"IDLE":"#2ECC71","EN_ROUTE_TO_RESTAURANT":"#F39C12","WAITING":"#3498DB","EN_ROUTE_TO_CUSTOMER":"#9B59B6","OFFLINE":"#95A5A6"}

@st.cache_data(ttl=15)
def load_blob(prefix):
    try:
        svc  = BlobServiceClient(account_url=f"https://{ACCOUNT_NAME}.blob.core.windows.net", credential=ACCOUNT_KEY)
        cont = svc.get_container_client(CONTAINER_NAME)
        blobs = [b for b in cont.list_blobs(name_starts_with=prefix) if b.name.endswith(".parquet")]
        if not blobs: return pd.DataFrame()
        frames = []
        for b in blobs:
            data = cont.get_blob_client(b.name).download_blob().readall()
            frames.append(pq.read_table(io.BytesIO(data)).to_pandas())
        df = pd.concat(frames, ignore_index=True)
        for col in ["event_time","ingestion_time"]:
            if col in df.columns: df[col] = pd.to_datetime(df[col], errors="coerce")
        return df
    except Exception as e:
        st.error(f"Blob error: {e}")
        return pd.DataFrame()

st.title("🛵 Food Delivery · Real-Time Analytics")
st.caption("Azure Event Hubs → Spark Structured Streaming → Azure Blob Storage (Parquet)")

with st.sidebar:
    st.markdown("**Group 5 · Milestone 2 · BBADBA A**")
    st.markdown("---")
    st.markdown("**Namespace**")
    st.code("iesstsabbadbaa-grp-01-05")
    st.markdown("**Topics**")
    st.code("group_5_orders\ngroup_5_couriers")
    st.markdown("**Blob container**")
    st.code("group5")
    st.markdown("---")
    refresh = st.slider("Refresh every (s)", 10, 120, 30, 10)
    auto_refresh = st.toggle("Auto-refresh", value=True)
    if st.button("🔄 Refresh now"):
        st.cache_data.clear()
        st.rerun()
    st.markdown("---")
    st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')}")

o = load_blob("orders/output/")
c = load_blob("couriers/output/")

col1, col2, col3 = st.columns(3)
col1.success(f"✅ Orders: {len(o):,} events" if not o.empty else "⚠️ Orders: no data yet")
col2.success(f"✅ Couriers: {len(c):,} events" if not c.empty else "⚠️ Couriers: no data yet")
col3.info(f"🕐 Auto-refreshing every {refresh}s")

if o.empty and c.empty:
    st.warning("No data in Blob Storage yet. Make sure Section 7 of your Spark notebook is running and wait ~20 seconds.")
    st.stop()

st.markdown("---")

# ── KPIs ──────────────────────────────────────────────────────────────────────
st.subheader("📊 Key Metrics")
if not o.empty:
    delivered = o[o.event_type == "DELIVERED"]
    cancelled = o[o.event_type == "CANCELLED"]
    created   = o[o.event_type == "CREATED"]
    rev   = delivered["total_amount_eur"].sum() if not delivered.empty else 0
    avg   = delivered["total_amount_eur"].mean() if not delivered.empty else 0
    crate = round(len(cancelled) / max(len(created), 1) * 100, 1)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total events",      f"{len(o):,}")
    k2.metric("Delivered orders",  f"{len(delivered):,}")
    k3.metric("Total revenue",     f"€{rev:,.0f}")
    k4.metric("Avg order value",   f"€{avg:.2f}")
    k5.metric("Cancellation rate", f"{crate}%")
else:
    delivered = cancelled = pd.DataFrame()

st.markdown("---")

# ── UC1 & UC2 ─────────────────────────────────────────────────────────────────
st.subheader("📦 Use Case 1 & 2 — Order Events & Cancellation Monitor")
if not o.empty:
    c1, c2 = st.columns(2)
    with c1:
        ec = o["event_type"].value_counts().reset_index()
        ec.columns = ["event_type", "count"]
        fig = px.bar(ec, x="event_type", y="count", color="event_type",
                     title="Order events by type",
                     color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(showlegend=False, height=350)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        if not cancelled.empty and "cancel_reason" in cancelled.columns:
            cr = cancelled["cancel_reason"].value_counts().reset_index()
            cr.columns = ["reason", "count"]
            fig2 = px.pie(cr, names="reason", values="count",
                          title="Cancellation reasons",
                          color_discrete_sequence=["#FF6B35", "#E74C3C", "#C0392B"])
            fig2.update_layout(height=350)
            st.plotly_chart(fig2, use_container_width=True)
        else:
            st.info("No cancellations recorded yet.")
    if not cancelled.empty:
        st.markdown("**Cancellations by zone and reason**")
        ct = (cancelled.groupby(["zone_id", "cancel_reason"])
              .size().reset_index(name="cancellations")
              .sort_values("cancellations", ascending=False))
        st.dataframe(ct, use_container_width=True, hide_index=True)
else:
    st.info("Waiting for orders data...")

st.markdown("---")

# ── UC3 ───────────────────────────────────────────────────────────────────────
st.subheader("💰 Use Case 3 — Revenue by Zone")
if not o.empty and not delivered.empty:
    rbz = (delivered.groupby("zone_id")
           .agg(delivered_orders=("order_id","count"),
                total_revenue_eur=("total_amount_eur","sum"),
                avg_order_value_eur=("total_amount_eur","mean"))
           .reset_index().sort_values("total_revenue_eur", ascending=False))
    rbz["total_revenue_eur"]   = rbz["total_revenue_eur"].round(2)
    rbz["avg_order_value_eur"] = rbz["avg_order_value_eur"].round(2)
    c1, c2 = st.columns(2)
    with c1:
        f = px.bar(rbz, x="zone_id", y="total_revenue_eur", color="zone_id",
                   text="total_revenue_eur", title="Total revenue by zone (€)",
                   color_discrete_map=ZONE_COLORS)
        f.update_traces(texttemplate="€%{text:,.0f}", textposition="outside")
        f.update_layout(showlegend=False, height=380)
        st.plotly_chart(f, use_container_width=True)
    with c2:
        f2 = px.bar(rbz, x="zone_id", y="avg_order_value_eur", color="zone_id",
                    text="avg_order_value_eur", title="Avg order value by zone (€)",
                    color_discrete_map=ZONE_COLORS)
        f2.update_traces(texttemplate="€%{text:.2f}", textposition="outside")
        f2.update_layout(showlegend=False, height=380)
        st.plotly_chart(f2, use_container_width=True)
    st.markdown("**Revenue summary table**")
    st.dataframe(rbz.rename(columns={
        "zone_id":"Zone","delivered_orders":"Delivered orders",
        "total_revenue_eur":"Total revenue (€)","avg_order_value_eur":"Avg order value (€)"}),
        use_container_width=True, hide_index=True)
else:
    st.info("Waiting for delivered orders data...")

st.markdown("---")

# ── UC4 ───────────────────────────────────────────────────────────────────────
st.subheader("🏍️ Use Case 4 — Courier Availability by Zone")
if not c.empty:
    ca = c.groupby(["zone_id","status"]).size().reset_index(name="courier_count")
    c1, c2 = st.columns(2)
    with c1:
        st.plotly_chart(px.bar(ca, x="zone_id", y="courier_count", color="status",
            barmode="stack", title="Courier status by zone",
            color_discrete_map=STATUS_COLORS).update_layout(height=380),
            use_container_width=True)
    with c2:
        st2 = c["status"].value_counts().reset_index()
        st2.columns = ["status","count"]
        st.plotly_chart(px.pie(st2, names="status", values="count",
            title="Overall courier status", color="status",
            color_discrete_map=STATUS_COLORS).update_layout(height=380),
            use_container_width=True)
    st.markdown("**Live courier positions — latest ping per courier (Madrid)**")
    latest = c.sort_values("event_time", ascending=False).drop_duplicates("courier_id")
    if "lat" in latest.columns and "lon" in latest.columns:
        fm = px.scatter_mapbox(latest, lat="lat", lon="lon", color="status",
            color_discrete_map=STATUS_COLORS,
            hover_data=["courier_id","zone_id","battery_pct"],
            zoom=11, center={"lat":40.4168,"lon":-3.7038},
            mapbox_style="open-street-map", height=460)
        fm.update_layout(margin={"r":0,"t":10,"l":0,"b":0})
        st.plotly_chart(fm, use_container_width=True)
else:
    st.info("Waiting for couriers data...")

st.markdown("---")

# ── UC5 ───────────────────────────────────────────────────────────────────────
st.subheader("🚨 Use Case 5 — Couriers Offline Mid-Delivery (Anomaly Detection)")
if not c.empty:
    anom = c[(c.event_type == "OFFLINE") & c.current_order_id.notna()].copy()
    c1, c2 = st.columns([1, 2])
    with c1:
        st.metric("⚠️ Anomalies detected", len(anom))
        if not anom.empty:
            bz = anom["zone_id"].value_counts().reset_index()
            bz.columns = ["zone_id","count"]
            st.plotly_chart(px.bar(bz, x="zone_id", y="count", color="zone_id",
                title="Anomalies by zone", color_discrete_map=ZONE_COLORS
                ).update_layout(showlegend=False, height=280),
                use_container_width=True)
    with c2:
        if not anom.empty:
            st.markdown("**Couriers that went offline while carrying an active order**")
            cols = [x for x in ["courier_id","current_order_id","zone_id","battery_pct","event_time"]
                    if x in anom.columns]
            st.dataframe(anom[cols].sort_values("event_time", ascending=False).head(20)
                .rename(columns={"courier_id":"Courier","current_order_id":"Order being carried",
                                  "zone_id":"Zone","battery_pct":"Battery %","event_time":"Time"}),
                use_container_width=True, hide_index=True)
            fig_b = px.histogram(anom, x="battery_pct", nbins=20,
                title="Battery % when couriers dropped offline",
                color_discrete_sequence=["#E74C3C"])
            fig_b.add_vline(x=anom["battery_pct"].mean(), line_dash="dash", line_color="orange",
                annotation_text=f"Mean: {anom['battery_pct'].mean():.0f}%")
            fig_b.update_layout(height=280)
            st.plotly_chart(fig_b, use_container_width=True)
        else:
            st.success("No anomalies detected — all couriers completing deliveries normally.")
else:
    st.info("Waiting for couriers data...")

st.markdown("---")

# ── Volume over time ──────────────────────────────────────────────────────────
st.subheader("📈 Event Volume Over Time")
c1, c2 = st.columns(2)
if not o.empty and "event_time" in o.columns:
    with c1:
        dft = o.copy()
        dft["hour"] = dft["event_time"].dt.floor("h")
        oph = dft.groupby(["hour","event_type"]).size().reset_index(name="count")
        st.plotly_chart(px.line(oph, x="hour", y="count", color="event_type",
            title="Order events per hour").update_layout(height=320),
            use_container_width=True)
if not c.empty and "event_time" in c.columns:
    with c2:
        dfc = c.copy()
        dfc["hour"] = dfc["event_time"].dt.floor("h")
        cph = dfc.groupby(["hour","event_type"]).size().reset_index(name="count")
        st.plotly_chart(px.line(cph, x="hour", y="count", color="event_type",
            title="Courier events per hour").update_layout(height=320),
            use_container_width=True)

# ── Auto-refresh ──────────────────────────────────────────────────────────────
if auto_refresh:
    time.sleep(refresh)
    st.cache_data.clear()
    st.rerun()
