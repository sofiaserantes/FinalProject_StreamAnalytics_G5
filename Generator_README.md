# Feed Generation
This document explains how to install, configure, and run the synthetic data generator for the two streaming feeds used in this project.

Milestone1_ipynb produces two synthetic streaming data feeds that simulate the operational dynamics of a real-time food delivery platform

## Feed A
OrderLifecycle

Output: order_lifecycle_events.json/.avro

Events: 4,000

## Feed B
CourierStatus

Output: courier_status_events.json/.avro

Events: 4,000

## Sample Files

It also writes smaller sample files (first 10 events of each feed) for quick inspection:

sample_order_lifecycle_events.json / .avro

sample_courier_status_events.json / .avro

## Requirements

Python 3.10 or higher

fastavro for AVRO serialisation
## Install the dependency:

pip install fastavro
## How to run it

1. Open `Milestone1.ipynb` 
2. Run all cells: **Kernel → Restart & Run All**

All output files will be written to the current working directory. The script prints a confirmation line for each file created:

✅ order_lifecycle_events.json written

✅ order_lifecycle_events.avro written

✅ courier_status_events.json written

✅ courier_status_events.avro written

✅ sample_order_lifecycle_events.json written

✅ sample_order_lifecycle_events.avro written

✅ sample_courier_status_events.json written

✅ sample_courier_status_events.avro written

📂 All missing deliverables generated.

## Configuration
There are no CLI arguments. All parameters are defined as constants at the top of milestone1_generator.py and can be edited directly before running

### Volume

NUM_ORDER_EVENTS   = 4000   # total events in the order lifecycle stream

NUM_COURIER_EVENTS = 4000   # total events in the courier status stream

NUM_RESTAURANTS    = 120

NUM_COURIERS       = 300

NUM_CUSTOMERS      = 800

### Time Window

START_DATE = datetime.datetime(2026, 2, 1, 0, 0, 0)

END_DATE   = datetime.datetime(2026, 3, 1, 0, 0, 0)

### Geographic zones

ZONES        = ["Z1_Center", "Z2_North", "Z3_South", "Z4_East", "Z5_West"]

ZONE_WEIGHTS = [0.40, 0.18, 0.15, 0.14, 0.13]  # higher = more demand

### Business Parameters

BASE_CANCEL_PROB   = 0.06    # baseline cancellation probability

SURGE_PROB         = 0.08    # chance of a demand surge period

PROMO_PROB         = 0.10    # chance of a promotional period

WEEKEND_MULTIPLIER = 1.25    # extra demand on Sat/Sun

### Edge case injection rates
These control how frequently intentional data quality issues are injected into the stream 

DUPLICATE_EVENT_PROB              = 0.02  # 2%  — exact duplicate events

LATE_EVENT_PROB                   = 0.05  # 5%  — ingestion_time delayed vs event_time

MISSING_STEP_PROB                 = 0.03  # 3%  — orders with a missing lifecycle step

IMPOSSIBLE_DURATION_PROB          = 0.01  # 1%  — negative or extreme durations

COURIER_OFFLINE_MID_DELIVERY_PROB = 0.02  # 2%  — courier goes offline mid-delivery

 ## Output file reference
| File     | Format   | Contents |
|----------|----------|----------|
|order_lifecycle_events.json |JSON array |All 4,000 
|order eventsorder_lifecycle_events.avro|AVRO binary |Same, in binary AVRO format
|courier_status_events.json |JSON array |All 4,000 courier events |
|courier_status_events.avro |AVRO binary |Same, in binary AVRO format
|sample_order_lifecycle_events.jsonJ |SON array |First 10 order events
|sample_order_lifecycle_events.avro |AVRO binary |First 10 order events
|sample_courier_status_events.json |JSON array |First 10 courier events
|sample_courier_status_events.avro |AVRO binary |First 10 courier events
|order_lifecycle_events.avsc |JSON (AVRO schema) |Standalone schema for Feed A
|courier_status_events.avsc |JSON (AVRO schema) |Standalone schema for Feed B









