# Real-Time Food Delivery Analytics Pipeline
## Stream Analytics — Milestone 2: Real-Time Data Processing & Storage
---

## Table of Contents

[Project Overview](#project-overview)
[Team Structure](#team-structure)
[Architecture](#architecture)
[Technology Stack](#technology-stack)
[Feed A: Order Lifecycle Events](#feed-a-order-lifecycle-events)
[Feed B: Courier Status & Location Events](#feed-b-courier-status--location-events)
[Analytics Use Cases](#analytics-use-cases)
[Real-Time Dashboard](#real-time-dashboard)
[Azure Configuration](#azure-configuration)
[Repository Structure](#repository-structure)
[How to Run](#how-to-run)

---

## Project Overview

Building on the data feed design from Milestone 1, this milestone implements the full end-to-end real-time streaming analytics pipeline for a food delivery platform. The pipeline ingests AVRO-encoded events from two streaming feeds, processes them with Apache Spark Structured Streaming, persists the results to Azure Blob Storage in Parquet format, and visualises live insights in a Streamlit dashboard.

| Milestone | Scope | Technology |
|---|---|---|
| M1 | Data feed design & generation | Python, AVRO, JSON |
| **M2 (this repo)** | **Stream analytics implementation** | **Azure Event Hubs, Spark Structured Streaming, Azure Blob Storage, Streamlit** |

The pipeline processes two feeds simultaneously:

| Topic | Schema | Event Hub |
|---|---|---|
| `group_5_orders` | `OrderLifecycleEvent` | iesstsabbadbaa-grp-01-05 |
| `group_5_couriers` | `CourierStatusEvent` | iesstsabbadbaa-grp-01-05 |

---

## Team Structure

Group 5 — Members:

| Name | GitHub |
|---|---|
| Bernarda Andrade | @22andradeb |
| Javier Comin | — |
| Nour Farhat | @nour-farhat |
| Sofía Serantes | @sofiaserantes |
| Tessa Correig | @tessacorreigmartra |
| Rakan Hourani | @rakanhourani |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Google Colab                            │
│                                                             │
│  ┌─────────────────┐      ┌─────────────────┐              │
│  │ AVRO Producer   │      │ AVRO Producer   │              │
│  │ (Orders)        │      │ (Couriers)      │              │
│  │ confluent-kafka │      │ confluent-kafka  │              │
│  └────────┬────────┘      └────────┬────────┘              │
│           │ AVRO / SASL_SSL        │ AVRO / SASL_SSL       │
└───────────┼────────────────────────┼───────────────────────┘
            │                        │
            ▼                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Azure Event Hubs                               │
│         iesstsabbadbaa-grp-01-05                            │
│                                                             │
│   ┌──────────────────┐    ┌──────────────────┐             │
│   │  group_5_orders  │    │ group_5_couriers  │             │
│   │  4 partitions    │    │  4 partitions     │             │
│   └────────┬─────────┘    └────────┬──────────┘            │
└────────────┼──────────────────────┼─────────────────────────┘
             │                      │
             ▼                      ▼
┌─────────────────────────────────────────────────────────────┐
│         Spark Structured Streaming (Google Colab)           │
│                    Spark 4.1.x / Java 21                    │
│                                                             │
│  readStream (Kafka) → from_avro() → flatten DataFrame       │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐   │
│  │              Analytical Queries                      │   │
│  │  UC1 — All orders monitor      (outputMode: update)  │   │
│  │  UC2 — Cancellation monitor    (outputMode: update)  │   │
│  │  UC3 — Revenue by zone         (outputMode: complete)│   │
│  │  UC4 — Courier availability    (outputMode: complete)│   │
│  │  UC5 — Offline mid-delivery    (outputMode: update)  │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  writeStream → Parquet (trigger: 20 seconds)                │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│              Azure Blob Storage                             │
│                  iesstsabbadbaa                             │
│                  container: group5                          │
│                                                             │
│   group5/orders/output/      ← Parquet files (orders)      │
│   group5/orders/checkpoint/  ← Spark offsets               │
│   group5/couriers/output/    ← Parquet files (couriers)    │
│   group5/couriers/checkpoint/← Spark offsets               │
└───────────────────────┬─────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────┐
│         Streamlit Dashboard (localhost / Streamlit Cloud)   │
│                                                             │
│  streamanalyticsgroup5.streamlit.app                        │
│  Reads Parquet directly from Blob · refreshes every 15s    │
└─────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology | Version |
|---|---|---|
| Message broker | Azure Event Hubs (Kafka-compatible API) | Standard tier |
| Stream producer | confluent-kafka + fastavro | Python 3.12 |
| Serialisation | Apache AVRO (schemaless writer) | fastavro 1.x |
| Stream processing | Apache Spark Structured Streaming | Spark 4.1.x |
| Runtime | Google Colab (Java 21, local[*]) | — |
| Kafka connector | spark-sql-kafka-0-10 | 4.1.1 |
| AVRO deserialiser | spark-avro | 4.1.1 |
| Cloud storage | Azure Blob Storage (wasbs://) | — |
| Blob connector | hadoop-azure + azure-storage | 3.3.1 / 8.6.6 |
| Data at rest format | Apache Parquet | — |
| Batch query engine | DuckDB | latest |
| Dashboard | Streamlit + Plotly | 1.56.0 |

---

## Feed A: Order Lifecycle Events

Identical schema to Milestone 1. In Milestone 2 events are generated continuously by a live AVRO producer and streamed to `group_5_orders` on Azure Event Hubs.

**Producer:** `avro_producer_orders.py` — runs continuously via `nohup`, generating one full order lifecycle per iteration (CREATED → … → DELIVERED or CANCELLED), serialising each event with `fastavro.schemaless_writer`, and producing to Event Hub over SASL_SSL.

**Spark consumer:** reads via `readStream` with `startingOffsets: earliest`, deserialises with `from_avro()`, flattens to a DataFrame with `event_time` and `ingestion_time` cast to `TimestampType`.

---

## Feed B: Courier Status & Location Events

Identical schema to Milestone 1. Streamed continuously to `group_5_couriers`.

**Producer:** `avro_producer_couriers.py` — maintains per-courier state (online/offline, current zone, current order) and emits realistic state transitions including the edge case of going OFFLINE mid-delivery (`COURIER_OFFLINE_MID_DELIVERY_PROB = 0.02`).

---

## Analytics Use Cases

All five use cases are implemented as Spark Structured Streaming queries. Each writes first to an in-memory table (for live inspection) and then to Parquet files in Azure Blob Storage.

### Use Case 1 — Raw order event monitor
- **Feed:** Orders
- **Operation:** Passthrough — all event types streamed to memory
- **Output mode:** `update`
- **Purpose:** Live visibility into every event arriving from Event Hub; used to verify producer health and schema correctness

### Use Case 2 — Cancellation monitor
- **Feed:** Orders
- **Operation:** `filter(event_type == "CANCELLED")`
- **Output mode:** `update`
- **Business value:** Real-time detection of cancellation spikes during surge periods; grouped by `zone_id` and `cancel_reason` to identify whether cancellations are driven by restaurant capacity, courier unavailability, or customer behaviour
- **Edge cases handled:** Surge periods add +5% cancellation probability; promo periods subtract −2%

### Use Case 3 — Revenue by zone
- **Feed:** Orders (DELIVERED events only)
- **Operation:** `groupBy("zone_id").agg(count, sum, avg)` on `total_amount_eur`
- **Output mode:** `complete`
- **Business value:** Real-time revenue dashboard per delivery zone; identifies Z1_Center as the highest-value zone (40% demand weight) and enables dynamic pricing decisions

### Use Case 4 — Courier availability by zone
- **Feed:** Couriers
- **Operation:** `groupBy("zone_id", "status").count()`
- **Output mode:** `complete`
- **Business value:** Live heatmap of courier supply per zone; surfaces imbalances between zones with high order demand (Z1_Center) and available couriers (IDLE status); inputs to dispatch optimisation

### Use Case 5 — Couriers going offline mid-delivery (anomaly detection)
- **Feed:** Couriers
- **Operation:** `filter(event_type == "OFFLINE" AND current_order_id IS NOT NULL)`
- **Output mode:** `update`
- **Business value:** Detects couriers that drop off while carrying an active order — a critical operational alert requiring immediate manual order reassignment; battery_pct is surfaced as the primary driver (67% of anomalies below 20%)

---

## Real-Time Dashboard

The Streamlit dashboard reads Parquet files directly from Azure Blob Storage and refreshes every 15 seconds.

**Live URL:** [https://streamanalyticsgroup5.streamlit.app](https://streamanalyticsgroup5.streamlit.app)

**Local URL:** `http://localhost:8501`

### Dashboard sections

| Section | Use case | Charts |
|---|---|---|
| KPI row | All | Total events, delivered orders, total revenue, avg order value, cancellation rate |
| UC1 & UC2 | Orders | Event type bar chart + cancellation reasons pie + cancellations by zone table |
| UC3 | Orders | Total revenue by zone + avg order value by zone + summary table |
| UC4 | Couriers | Stacked status bar by zone + status distribution pie + live courier map (Madrid) |
| UC5 | Couriers | Anomaly count + anomalies by zone + courier table + battery % histogram |
| Volume | Both | Order events per hour + courier events per hour (lunch/dinner peaks visible) |

### Running the dashboard locally

```bash
cd laptop/
pip install -r requirements.txt
streamlit run dashboard.py
```

---

## Azure Configuration

| Resource | Value |
|---|---|
| Event Hub namespace | `iesstsabbadbaa-grp-01-05` |
| Orders topic | `group_5_orders` |
| Couriers topic | `group_5_couriers` |
| Partitions per topic | 4 (aligned with `spark.sql.shuffle.partitions = 4`) |
| Storage account | `iesstsabbadbaa` |
| Blob container | `group5` |
| Orders output path | `wasbs://group5@iesstsabbadbaa.blob.core.windows.net/orders/output/` |
| Couriers output path | `wasbs://group5@iesstsabbadbaa.blob.core.windows.net/couriers/output/` |

**Partition count rationale:** 4 partitions were chosen to align with Spark's `shuffle.partitions = 4` setting, maximising parallelism within the constraints of Colab's CPU allocation. Zone-based partitioning was considered but rejected — Event Hub partitions data by message key, not by field value, so zone-based partitioning requires application-level routing rather than broker-level configuration.

---

## Repository Structure

```
Milestone2_StreamAnalytics_G5/
│
├── colab/
│   └── Milestone2_Group5.ipynb          ← Main Spark notebook (run on Google Colab)
│
├── laptop/
│   ├── dashboard.py                     ← Streamlit dashboard (run locally)
│   └── requirements.txt                 ← Dashboard dependencies
│
├── schemas/
│   ├── order_lifecycle_events.avsc      ← AVRO schema — Feed A (unchanged from M1)
│   └── courier_status_events.avsc       ← AVRO schema — Feed B (unchanged from M1)
│
└── README.md                            ← This file
```

---

## How to Run

### Prerequisites
- Google Colab account
- Python 3.x on your laptop
- Azure credentials (already embedded in the notebook)

### Step 1 — Run the Spark notebook on Google Colab

1. Open `colab/Milestone2_Group5.ipynb` in Google Colab
2. Run all cells **top to bottom** in order:

| Section | What it does | Keep running? |
|---|---|---|
| Section 1 | Load credentials and config | Run once |
| Section 2 | Define AVRO schemas | Run once |
| Section 3 | Write producer scripts + launch via `nohup` | **Yes — producers must stay active** |
| Section 4 | Install Java 21 + Spark 4.1.x | Run once (~5 min) |
| Section 5 | Create Spark session + `readStream` for both feeds | Run once |
| Section 6 | Five analytical use cases → in-memory tables | Run once |
| **Section 7** | **`writeStream` → Parquet to Azure Blob** | **Yes — must stay active** |
| Section 8 | Read Parquet back from Blob (verification) | Run once |
| Section 9 | DuckDB batch queries on Parquet | Run once |
| Section 10 | Stop all queries (run only when finished) | — |

> Sections 3 and 7 must remain active throughout the session. These are the producer processes and the Blob writers respectively.

### Step 2 — Run the dashboard on your laptop

```bash
cd laptop/
pip install -r requirements.txt
streamlit run dashboard.py
```

Opens at `http://localhost:8501`. The dashboard reads from Azure Blob Storage independently — it does not need to be on the same machine as Colab.

### Step 3 — Verify data is flowing

Check that Parquet files are landing in Blob Storage:

```python
from azure.storage.blob import BlobServiceClient
svc = BlobServiceClient(
    account_url="https://iesstsabbadbaa.blob.core.windows.net",
    credential="<account_key>"
)
blobs = list(svc.get_container_client("group5").list_blobs())
print(f"Files in Blob: {len(blobs)}")
for b in blobs[:10]:
    print(" ", b.name)
```

New Parquet files appear every 20 seconds once Section 7 is running.
