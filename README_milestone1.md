# Real-Time Food Delivery Analytics Pipeline
## Stream Analytics — Milestone 1: Data Feed Design & Generation
---

## Table of Contents

- [Project Overview](#project-overview)
- [Team Structure](#team-structure)
- [Feed A: Order Lifecycle Events](#feed-a-order-lifecycle-events)
- [Feed B: Courier Status & Location Events](#feed-b-courier-status--location-events)
- [Design Note](#design-note)
- [Event-Time Processing & Late Data Handling](#event-time-processing--late-data-handling)
- [Planned Analytics](#planned-analytics)
- [Assumptions](#assumptions)
- [Repository Structure](#repository-structure)

---

## Project Overview

On demand food delivery platforms (e.g., Uber Eats, Glovo, Deliveroo) operate as real time marketplaces: customers place orders, restaurants accept and prepare meals, couriers pick up and deliver, and the platform continuously optimises dispatch, ETAs, cancellations, promotions, and fraud prevention. These systems produce high volume streaming data that must be turned into business value in real time.

This repository implements **Milestone 1** of a two milestone real time analytics pipeline:

| Milestone | Scope | Technology |
|---|---|---|
| **M1 (this repo)** | Data feed design & generation | Python, AVRO, JSON |
| M2 | Stream Analytics Implementation | Azure Event Hubs |


The goal of the first milestone was to design and generate two synthetic streaming feeds that together capture the operational dynamics of the  delivery service platforms, with schemas and fields to support event time.

---
## Team Structure

**Group 5 — Members:**

| Name | GitHub |
|---|---|
| Bernarda Andrade | [@22andradeb](https://github.com/22andradeb) |
| Javier Comin | — |
| Nour Farhat | [@nour-farhat](https://github.com/nour-farhat) |
| Sofía Serantes | [@sofiaserantes](https://github.com/sofiaserantes) |
| Tessa Correig | [@tessacorreigmartra](https://github.com/tessacorreigmartra) |
| Rakan Hourani | [@rakanhourani](https://github.com/rakanhourani) |

---

## Feed A: Order Lifecycle Events

### What it captures

Every state transition an order goes through: from the moment a customer places it to final delivery or cancellation.

### Why this feed is essential

Order lifecycle events are the central operational record of any delivery platform. Without them it is impossible to know whether an order is pending, being prepared, in transit, or completed. Specifically, this feed:

- Enables real time customer facing status updates and ETA recalculation
- Powers SLA monitoring (e.g., orders pending acceptance for more than 3 minutes)
- Supports cancellation pattern detection and fraud prevention
- Provides the financial record that triggers payment on `DELIVERED`
- Produces the join keys (`order_id`, `courier_id`) needed to correlate with Feed B

### State machine

```
CREATED → ACCEPTED → PREP_STARTED → READY → PICKED_UP → DELIVERED
    │           │            │
    └───────────┴────────────┴──────────────────────────► CANCELLED
                                                       (any stage)
```

`PREP_STARTED` may be skipped (missing step edge case, 3% of orders). `PICKED_UP` may also be skipped (order jumps directly to `DELIVERED`). `CANCELLED` can follow `CREATED` or `ACCEPTED`.

### Schema — Feed A (`OrderLifecycleEvent`)

AVRO namespace: `food.delivery` | Schema version: `1.0`

| Field | AVRO Type | Nullable | Description |
|---|---|---|---|
| `schema_version` | `string` | No | Always `"1.0"` — supports schema evolution |
| `event_id` | `string` | No | Unique event identifier (`ORD_<12-digit>`); deduplication key |
| `order_id` | `string` | No | Unique order identifier (`O<8-digit>`); join key with Feed B |
| `event_type` | `enum` | No | One of: `CREATED`, `ACCEPTED`, `PREP_STARTED`, `READY`, `PICKED_UP`, `DELIVERED`, `CANCELLED` |
| `event_time` | `string` | No | ISO 8601 UTC timestamp of when the state transition occurred — **watermark column** |
| `ingestion_time` | `string` | No | ISO 8601 UTC timestamp of pipeline arrival; intentionally offset for late-event simulation |
| `customer_id` | `string` | No | Customer identifier (`U<5-digit>`) |
| `restaurant_id` | `string` | No | Restaurant identifier (`R<4-digit>`); join key with restaurant reference table |
| `courier_id` | `["null", "string"]` | Yes | Courier identifier (`C<5-digit>`); `null` on `CREATED`, populated from `ACCEPTED` onward |
| `zone_id` | `string` | No | Delivery zone (`Z1_Center` … `Z5_West`); partition key for zone-level aggregations |
| `estimated_prep_min` | `["null", "int"]` | Yes | Estimated prep time in minutes; set on `CREATED` and `ACCEPTED` only, `null` otherwise |
| `estimated_delivery_min` | `["null", "int"]` | Yes | Estimated delivery time in minutes; set on `CREATED` and `ACCEPTED` only, `null` otherwise |
| `total_amount_eur` | `float` | No | Order value in EUR (range €8–€55) |
| `promo_applied` | `boolean` | No | Whether a promotional discount was active at order time |
| `cancel_reason` | `["null", "string"]` | Yes | One of `CUSTOMER_CHANGED_MIND`, `RESTAURANT_TOO_BUSY`, `COURIER_UNAVAILABLE`; `null` unless `event_type = CANCELLED` |

---

## Feed B: Courier Status & Location Events

### What it captures

The continuous status and location updates of couriers — emitted on every state change (`ONLINE`, `OFFLINE`, `ASSIGNED`, `UNASSIGNED`) and as periodic location pings (`LOCATION` events).

### Why this feed is essential

Courier telemetry is the operational backbone of dispatch optimisation. Without a live view of courier positions and availability, the platform cannot route orders efficiently. Specifically, this feed:

- Powers real time courier assignment by proximity and availability
- Enables route anomaly detection (e.g., courier not moving during `EN_ROUTE`)
- Supports zone level supply/demand balancing (available couriers vs. open orders)
- Provides the join keys (`courier_id`, `current_order_id`) needed to correlate with Feed A
- Captures `battery_pct`, which can feed predictive offline alerts

### State machine

```
ONLINE(IDLE) ──► ASSIGNED ──► EN_ROUTE_TO_RESTAURANT ──► WAITING ──► EN_ROUTE_TO_CUSTOMER ──► IDLE
      ▲                                                                                          │
      └──────────────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
                                     OFFLINE  (can occur at any point — edge case)
```

`event_type` records transitions (`ONLINE`, `OFFLINE`, `ASSIGNED`, `UNASSIGNED`, `LOCATION`). `status` records current operational state (`IDLE`, `EN_ROUTE_TO_RESTAURANT`, `WAITING`, `EN_ROUTE_TO_CUSTOMER`, `OFFLINE`).

### Schema — Feed B (`CourierStatusEvent`)

AVRO namespace: `food.delivery` | Schema version: `1.0`

| Field | AVRO Type | Nullable | Description |
|---|---|---|---|
| `schema_version` | `string` | No | Always `"1.0"` — supports schema evolution |
| `event_id` | `string` | No | Unique event identifier (`CUR_<12-digit>`); deduplication key |
| `courier_id` | `string` | No | Courier identifier (`C<5-digit>`); join key with Feed A |
| `event_type` | `enum` | No | One of: `ONLINE`, `OFFLINE`, `LOCATION`, `ASSIGNED`, `UNASSIGNED` |
| `event_time` | `string` | No | ISO 8601 UTC timestamp of the event — **watermark column** |
| `ingestion_time` | `string` | No | ISO 8601 UTC timestamp of pipeline arrival; intentionally offset for late-event simulation |
| `zone_id` | `string` | No | Current zone of the courier; partition key for zone-level aggregations |
| `lat` | `float` | No | GPS latitude (synthetic, jittered ±800 m around zone centroid) |
| `lon` | `float` | No | GPS longitude (synthetic, jittered ±800 m around zone centroid) |
| `status` | `enum` | No | One of: `IDLE`, `EN_ROUTE_TO_RESTAURANT`, `WAITING`, `EN_ROUTE_TO_CUSTOMER`, `OFFLINE` |
| `current_order_id` | `["null", "string"]` | Yes | Order currently assigned to this courier; `null` when IDLE or OFFLINE |
| `battery_pct` | `int` | No | Device battery percentage (1–100) |

---

## Design Note

### Why these two feeds and not others

A food delivery platform generates many signal types (restaurant prep updates, customer app events, payment events, etc.). We selected these two feeds because they together capture the **complete operational loop** of a delivery: an order is created and progresses through preparation (Feed A), while a courier is dispatched, navigates to the restaurant and then the customer, and becomes available again (Feed B). Every core analytical question — ETA accuracy, cancellation drivers, courier efficiency, zone demand imbalance — can be answered from these two feeds alone or by joining them against small reference tables.

### Why two separate feeds rather than one

Feed A and Feed B have fundamentally different cardinality, frequency, and semantic ownership:

- Feed A is sparse (~4–7 events per order lifetime) and carries business-critical financial and operational state.
- Feed B is dense (location pings plus state-change events per courier per shift) and is primarily a telemetry signal.

Merging them would force a wide schema with many nullable fields and conflate two different watermark cadences. Keeping them separate enables independent scaling, cleaner windowed aggregations, and explicit stream-stream join patterns where each side has its own watermark.

### Join key design

`order_id` appears in both feeds — as `order_id` in Feed A and `current_order_id` in Feed B. `courier_id` also appears in both. These two keys support stream-stream joins and stream-table joins in Milestone 3 without requiring any schema changes.

### AVRO and schema versioning

Both schemas include a `schema_version` string field (default `"1.0"`). All optional fields use AVRO's `["null", type]` union with a `null` default, which allows new optional fields to be added in future milestones without breaking existing consumers. The schemas are saved as standalone `.avsc` files in `schemas/` for use with a Schema Registry in Milestone 2.

---

## Event-Time Processing & Late Data Handling

### Dual timestamp design

Every event in both feeds carries two timestamps:

- `event_time`: when the state transition actually occurred. This is the field used for all windowed aggregations and watermarking in Spark.
- `ingestion_time`: when the event arrives at the pipeline. In the generator this is `event_time` plus a small random delay (1–45 seconds), but for 5% of events (`LATE_EVENT_PROB`) the delay is 2–25 minutes, simulating real-world network lag, mobile connectivity gaps, or broker backpressure.

This dual-timestamp pattern is standard for event-time processing: windows are defined on `event_time`, and the watermark advances based on the maximum observed `event_time` minus a configurable late-arrival tolerance.

### How each schema field supports event-time processing

| Field | Role in event-time processing |
|---|---|
| `event_time` | Primary watermark column; determines window membership for all aggregations |
| `ingestion_time` | Measures pipeline lag; `ingestion_time − event_time` quantifies per-event lateness |
| `event_id` | Deduplication key — enables idempotent re-processing after late delivery or replay |
| `order_id` / `current_order_id` | Stream-stream join key; requires time-bounded join windows on both sides |
| `courier_id` | Secondary join key; used in stateful tracking of courier lifecycle |
| `zone_id` | Partition key; enables per-zone tumbling window aggregations with lower state overhead |
| `status` (Feed B) | Enables stateful detection of missing transitions and courier timeout alerts |

### Late event calibration

The generator injects late events with delays up to 25 minutes. In Milestone 3 we plan to configure a **10-minute watermark** on both feeds. This will correctly include the majority of late arrivals (those within 2–10 minutes of their window boundary) while bounding state size. Events delayed beyond the watermark will be intentionally dropped, and the `event_id` deduplication layer handles duplicates before any windowing occurs.

---

## Planned Analytics

The two feeds are jointly designed to enable the following Spark Structured Streaming queries in Milestone 3:

### Feed A — Order analytics

| Query | Window type | Key fields |
|---|---|---|
| Order volume per zone per 5-minute window | Tumbling | `zone_id`, `event_time`, `event_type=CREATED` |
| Cancellation rate by zone and hour | Tumbling | `zone_id`, `event_time`, `event_type=CANCELLED` |
| Average restaurant prep time (PREP_STARTED → READY) | Per-order stateful | `order_id`, `event_time`, `event_type` |
| SLA breach: acceptance lag > 3 minutes | Stateful timeout | `order_id`, `event_time`, `event_type` |
| Demand surge detection (order rate spike) | Sliding | `zone_id`, `event_time` |
| Promo impact on cancellation rate | Tumbling | `promo_applied`, `event_type`, `event_time` |

### Feed B — Courier analytics

| Query | Window type | Key fields |
|---|---|---|
| Available (IDLE) couriers per zone per 5-minute window | Tumbling | `zone_id`, `event_time`, `status=IDLE` |
| Average courier wait time at restaurant (WAITING duration) | Per-assignment stateful | `courier_id`, `current_order_id`, `event_time` |
| Courier utilisation rate (time ASSIGNED vs. time ONLINE) | Session | `courier_id`, `status`, `event_time` |
| Courier offline mid-delivery detection | Stateful | `courier_id`, `current_order_id`, `event_type=OFFLINE` |

### Cross-feed stream-stream join

The richest planned query joins both feeds on `order_id` / `current_order_id` within a bounded time window to compute **pickup gap**: time between `READY` (Feed A) and the courier's next location update (Feed B) in the same zone. This requires a stream-stream join with watermarks on both sides — a direct application of the dual-timestamp design above, and a key demonstration of event-time correctness in Milestone 3.

---

## Assumptions

### Platform and domain

- A single city is simulated, modelled loosely on Madrid (zone centroids use approximate real coordinates). Five geographic zones are defined with a realistic urban density gradient: `Z1_Center` receives 40% of demand, the four peripheral zones share the remaining 60%.
- Restaurants are assumed to operate continuously throughout the simulation window; no opening or closing hours are modelled in M1.
- One courier is assigned per order; shared or pooled deliveries are out of scope.
- Payment, customer rating, and restaurant-side events are out of scope for M1, but `order_id`, `customer_id`, and `restaurant_id` are preserved as join keys for future extension.

### Data generation

- All timestamps are ISO 8601 UTC strings. No timezone conversion is applied.
- `event_time` reflects the logical time the state transition occurred. `ingestion_time` is derived from `event_time` with a small random delay, or a larger delay for the 5% of late event injections.
- Order amounts are uniformly sampled between €8 and €55.
- Courier GPS coordinates are jittered within ±800 m of each zone centroid using a simple degree offset approximation (not geodesic-accurate, sufficient for synthetic data).
- The simulation period is **1 February 2026 to 1 March 2026**. A fixed random seed (`random.seed(7)`) ensures full reproducibility.
- 4,000 total events are targeted per feed. Because each non-cancelled order produces approximately 6 events, the generator creates roughly 667 orders for Feed A and trims or extends to hit the 4,000 target exactly.

### Temporal distributions

- **Peak hours**: lunch (12:00–14:00) events are weighted 2.3× and dinner (19:00–22:00) events 2.8× relative to off-peak hours, using rejection sampling.
- **Weekend uplift**: events on Saturday and Sunday are weighted 1.25× relative to weekdays.
- **Surge periods**: during peak hours, 8% of events trigger a demand surge, raising cancellation probability by +5%.
- **Promo periods**: 10% of orders have a promotion applied (`promo_applied = true`), lowering cancellation probability by −2%.

### Schema

- `courier_id` in Feed A is `null` on the `CREATED` event and populated from `ACCEPTED` onward.
- `estimated_prep_min` and `estimated_delivery_min` are only meaningful on `CREATED` and `ACCEPTED`; they are `null` on all other event types.
- `cancel_reason` is only populated when `event_type = CANCELLED`.
- `current_order_id` in Feed B is `null` when a courier is IDLE or OFFLINE.
- AVRO schemas are at version `1.0`. Future optional fields will be added using `["null", type]` unions with `null` defaults to preserve backward compatibility.

---

## Repository Structure

```
Milestone1_StreamAnalytics_G5/
│
├── data/
│   ├── order_lifecycle_events.json         # Feed A — 4,000 events, JSON
│   ├── order_lifecycle_events.avro         # Feed A — 4,000 events, AVRO binary
│   ├── courier_status_events.json          # Feed B — 4,000 events, JSON
│   ├── courier_status_events.avro          # Feed B — 4,000 events, AVRO binary
│   ├── sample_order_lifecycle_events.json  # First 10 events of Feed A
│   ├── sample_order_lifecycle_events.avro  # First 10 events of Feed A
│   ├── sample_courier_status_events.json   # First 10 events of Feed B
│   └── sample_courier_status_events.avro   # First 10 events of Feed B
│
├── schemas/
│   ├── order_lifecycle_events.avsc         # Standalone AVRO schema — Feed A
│   └── courier_status_events.avsc          # Standalone AVRO schema — Feed B
│
├── Milestone1.ipynb                        # Generator notebook
├── Generator_README.md                     # Generator setup and configuration guide
└── README.md                               # This file
```

---

*Real-Time Food Delivery Analytics Pipeline — Stream Analytics Course, Group 5*
