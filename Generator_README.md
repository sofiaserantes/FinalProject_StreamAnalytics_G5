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

- Python 3.10 or higher
- `fastavro` for AVRO serialisation

Install the dependency:
```python
pip install fastavro
```

3. The space before `## How to verify the output` and `## Output file reference` will cause them not to render as headings — remove the leading space.

**One thing to add** — a brief note on what edge cases are and why they exist, just before the edge case config block:
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
There are no CLI arguments. All parameters are defined as constants at the top of Milestone1.pynb and can be edited directly before running

### Volume
```python
NUM_ORDER_EVENTS   = 4000   # total events in the order lifecycle stream

NUM_COURIER_EVENTS = 4000   # total events in the courier status stream

NUM_RESTAURANTS    = 120

NUM_COURIERS       = 300

NUM_CUSTOMERS      = 800
```
### Time Window
```python
START_DATE = datetime.datetime(2026, 2, 1, 0, 0, 0)

END_DATE   = datetime.datetime(2026, 3, 1, 0, 0, 0)
```
### Geographic zones
```python
ZONES        = ["Z1_Center", "Z2_North", "Z3_South", "Z4_East", "Z5_West"]

ZONE_WEIGHTS = [0.40, 0.18, 0.15, 0.14, 0.13]  # higher = more demand
```
### Business Parameters
```python
BASE_CANCEL_PROB   = 0.06    # baseline cancellation probability

SURGE_PROB         = 0.08    # chance of a demand surge period

PROMO_PROB         = 0.10    # chance of a promotional period

WEEKEND_MULTIPLIER = 1.25    # extra demand on Sat/Sun
```
### Edge case injection rates
These control how frequently intentional data quality issues are injected into the stream 
```python
DUPLICATE_EVENT_PROB              = 0.02  # 2%  — exact duplicate events

LATE_EVENT_PROB                   = 0.05  # 5%  — ingestion_time delayed vs event_time

MISSING_STEP_PROB                 = 0.03  # 3%  — orders with a missing lifecycle step

IMPOSSIBLE_DURATION_PROB          = 0.01  # 1%  — negative or extreme durations

COURIER_OFFLINE_MID_DELIVERY_PROB = 0.02  # 2%  — courier goes offline mid-delivery
```
 ## How to verify the output

 **Inspect a sample event (JSON):**
```bash
python -c "
import json
events = json.load(open('sample_order_lifecycle_events.json'))
print(json.dumps(events[0], indent=2))
"
```

**Count events by type:**
```bash
python -c "
import json
from collections import Counter
events = json.load(open('order_lifecycle_events.json'))
print(Counter(e['event_type'] for e in events))
"
```

**Read the AVRO file:**
```python
from fastavro import reader
with open('sample_order_lifecycle_events.avro', 'rb') as f:
    for record in reader(f):
        print(record)
        break
```

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









