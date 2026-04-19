# Real-Time Food Delivery Analytics Pipeline
## Stream Analytics: Milestone 2: Real-Time Data Processing & Storage
---

# Real Time Food Delivery Analytics Pipeline

## Stream Analytics Milestone 2

This project implements a real time analytics pipeline for a food delivery platform. It takes two live event streams, processes them with Spark Structured Streaming, stores the results in Azure Blob Storage as Parquet files, and shows the output in a Streamlit dashboard.

The goal of Milestone 2 is to show the full pipeline working end to end, from event generation to live analytics and storage.

**Live Demo:**https://streamanalyticsmiletone2group5.streamlit.app/

## What the project does

The project works with two event feeds.

1. `group_5_orders`

This feed contains order lifecycle events such as `CREATED`, `ASSIGNED`, `PICKED_UP`, `DELIVERED`, and `CANCELLED`.

2. `group_5_couriers`

This feed contains courier status and location events such as `IDLE`, `BUSY`, and `OFFLINE`.

Both feeds are sent to Azure Event Hubs in AVRO format. Spark reads them as streaming data, transforms them into structured DataFrames, applies the use cases, and writes the results to Azure Blob Storage. The dashboard then reads the stored Parquet files and displays the most important metrics in a simple visual way.

## Main tools used

1. Python

2. Apache Spark Structured Streaming

3. Azure Event Hubs

4. Azure Blob Storage

5. AVRO

6. Streamlit

7. DuckDB for small batch style checks on the stored Parquet files

## Team

Group 5

1. Bernarda Andrade  
2. Javier Comin  
3. Nour Farhat  
4. Sofía Serantes  
5. Tessa Correig  
6. Rakan Hourani  

## Pipeline explanation

The pipeline follows this order.

1. Producer scripts generate live order and courier events.

2. The events are sent to Azure Event Hubs.

3. Spark Structured Streaming reads both streams from Event Hubs.

4. Spark deserializes the AVRO messages and flattens them into usable columns.

5. Spark runs five analytics use cases in real time.

6. Spark writes the streaming results to Azure Blob Storage in Parquet format.

7. The dashboard reads those Parquet files and refreshes to show live metrics.

## Implemented use cases

### 1. Raw order event monitor

This use case shows all incoming order events in real time. It is useful for checking that the producer is running correctly and that events are arriving with the expected schema.

### 2. Cancellation monitor

This use case filters cancelled orders so the team can see cancellation activity as it happens. It helps identify possible operational issues such as courier shortage, restaurant delays, or customer side cancellations.

### 3. Revenue by zone

This use case focuses on delivered orders and calculates metrics such as total revenue, average order value, and order count by zone. It helps show which delivery zones are performing best.

### 4. Courier availability by zone

This use case counts couriers by `zone_id` and `status`. It gives a live view of supply across zones and helps detect areas where there may not be enough available couriers.

### 5. Couriers going offline during delivery

This is the anomaly detection use case. It filters courier events where the courier becomes `OFFLINE` while still having an active `current_order_id`. This matters because those orders may need to be reassigned manually.

## Important note about how to run the project

This project should be run from one computer.

The safest and clearest setup is the following.

1. Open the notebook on Google Colab from your computer.

2. Run the full streaming pipeline there.

3. From that same computer, open the Streamlit dashboard if you want to test it locally.

Running everything from one computer avoids confusion with paths, credentials, browser sessions, and active streaming processes. Even though the data is stored in Azure, the practical workflow for this milestone is based on one person running the notebook session and the dashboard from the same machine.

In other words, one computer should act as the main control point for the whole demo.

## How to run the notebook

Open `Milestone2_Group5.ipynb` in Google Colab and run the sections in order.

### Section 1

Loads the configuration and credentials.

Run this once at the beginning.

### Section 2

Defines the AVRO schemas for orders and couriers.

Run this once.

### Section 3

Creates the two producer scripts and launches them in the background using `nohup`.

This section must remain effectively active because it starts the live event generation.

### Section 4

Installs Java and Spark.

Run this once.

### Section 5

Creates the Spark session and reads both event streams from Azure Event Hubs.

Run this once.

### Section 6

Builds the five analytical streaming queries.

Run this once so the real time logic is available.

### Section 7

Writes both streams to Azure Blob Storage in Parquet format.

This is one of the most important sections. It must stay running during the demo because this is what continuously saves the streaming output.

### Section 8

Reads the Parquet files back from Blob Storage to verify that the storage output is working correctly.

Run this after Section 7 is active.

### Section 9

Uses DuckDB to run simple checks and analytical queries on the stored Parquet files.

This is useful for validation and for showing that the output can also be queried in a batch style.

### Section 10

This section is only for stopping the pipeline when you are completely done.

This part is very important.

The notebook is written with:

```python
if False:
```

That means nothing inside Section 10 will run by default. This is intentional and it is the safe option.

If you leave it as `False`, the queries and producers keep running.

If you change it to `True`, the section will stop the active Spark streaming queries, kill both producers, and stop Spark.

So the correct interpretation is:

1. `False` means do not stop anything yet.

2. `True` means stop everything now.

Use `True` only at the very end of the demo or when you are sure you want to shut the whole pipeline down.

## Dashboard

The dashboard reads the Parquet files from Azure Blob Storage and displays the live results.

It includes:

1. KPI metrics

2. Order event summaries

3. Cancellation analysis

4. Revenue by zone

5. Courier availability by zone

6. Courier anomaly detection

7. Event volume monitoring

If you are using the hosted dashboard, you can open the deployed Streamlit app in the browser.

If you want to run it locally, use the same computer that is running the notebook workflow.

Run:

```bash
pip install -r requirements.txt
streamlit run dashboard.py
```

## Why Section 3 and Section 7 matter most

If someone wants the shortest explanation of the notebook, this is the key idea.

Section 3 starts the live data producers.

Section 7 writes the streaming results to Blob Storage.

If those two sections are not active, the pipeline is not really working as a live end to end system.

That is why they should be treated as the core running parts of the project.

## Azure setup used in the project

The notebook uses:

1. Event Hub namespace: `iesstsabbadbaa-grp-01-05`

2. Orders topic: `group_5_orders`

3. Couriers topic: `group_5_couriers`

4. Storage account: `iesstsabbadbaa`

5. Blob container: `group5`

6. Orders output path: `wasbs://group5@iesstsabbadbaa.blob.core.windows.net/orders/output/`

7. Couriers output path: `wasbs://group5@iesstsabbadbaa.blob.core.windows.net/couriers/output/`

## Simple repository guide

The most important file is:

1. `Milestone2_Group5.ipynb`

This notebook contains the full pipeline from setup to shutdown.

Depending on your project folder, you may also have local dashboard files such as:

1. `dashboard.py`

2. `requirements.txt`

These are used only for the dashboard side.

## Final practical advice

Run the notebook from top to bottom in order.

Do not jump directly to later sections.

Keep Section 3 and Section 7 active while demonstrating the project.

Use one computer for the full workflow so everything stays clear and consistent.

Leave Section 10 as `False` while the system is running.

Change Section 10 to `True` only when you want to stop everything.

This project is easiest to explain as one complete live pipeline, not as separate disconnected parts.


