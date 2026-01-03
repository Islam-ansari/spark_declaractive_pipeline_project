# Spark Declarative Pipeline (SDP) using Databricks DLT  

## Overview

This repository showcases an end-to-end **Spark Declarative Pipeline (SDP)** implemented using **Databricks Delta Live Tables (DLT)**.  
The project follows the **Medallion Architecture (Landing → Bronze → Silver → Gold)** and leverages **Unity Catalog** for centralized governance, security, and data organization.

The pipeline ingests structured and semi-structured data using **streaming ingestion**, applies **data quality (DQ) checks**, and produces a **gold-layer analytical summary** optimized for downstream analytics.

The entire solution is built **exclusively on Databricks**, without relying on external orchestration or processing tools.

<img width="1505" height="597" alt="image" src="https://github.com/user-attachments/assets/bdc2b9ce-82f8-4715-9733-f84baeb55914" />


## Architecture Overview
<img width="831" height="396" alt="image" src="https://github.com/user-attachments/assets/8319a700-0914-401a-b17d-4e5af52b4618" />



## Technology Stack

- Apache Spark
- Spark Declarative Pipelines (SDP/DLT)
- Delta Lake
- Unity Catalog
- Structured Streaming
- PySpark & SQL


## Unity Catalog Setup

### Catalog
- **Catalog Name:** `circuitbox`

### Schemas
- `landing`
- `bronze`
- `silver`
- `gold`

### Volumes
- **Schema:** `landing`
- **Volume Name:** `operational_data`


| Dataset    | Format | Description |
|------------|--------|-------------|
| Addresses  | CSV    | Customer address information |
| Customers  | CSV    | Customer master data |
| Orders     | JSON   | Customer order transactions |

## Pipeline Layers

### 1. Landing Layer

- Raw data stored in **Unity Catalog Volumes - `operational_data`**


### 2. Bronze Layer (Streaming Tables)

**Purpose:**  
Ingest raw data incrementally using **streaming ingestion** with minimal transformations.

**Streaming Tables Created:**
- `bronze.addresses`
- `bronze.customers`
- `bronze.orders`

**Key Characteristics:**
- Auto Loader (`cloudFiles`) based ingestion
- Schema inference
- Append-only streaming pattern
- Added additional metadata columns like _input_file_path_ and _created_timestamp_


### 3. Silver Layer (Streaming Tables with Data Quality)

**Purpose:**  
Clean, standardize, and validate data while enforcing **data quality constraints**.

**Streaming Tables Created:**
- `silver.addresses_clean` - applied DQ checks and casting columns to correct data-types
- `silver.customers_clean` - same as above
- `silver.orders_clean` - same as above
- `silver.addresses` - created SCD type 2 table using `AUTO_CDC`
- `silver.customers` - created SCD type 1 table using `AUTO_CDC`
- `silver.orders` - created materialized view

Refer to diagram above to view these tables in UI.

**Data Quality Enforcement:**
- Not-null constraints
- Primary key validation
- Business rule checks using **DLT Expectations**
- Invalid records handled using expectation policies (drop or fail)

### 4. Gold Layer (Analytics)

**Purpose:**  
Provide business-ready, aggregated datasets for reporting and analytics.

**Materialized View Created:**
- `gold_customer_order_summary`

**Description:**
- Joins customer, address, and order datasets
- Produces customer-level order summaries
- Optimized for BI and analytical workloads

## Key Features

- Fully declarative pipeline using DLT
- End-to-end streaming architecture
- Data Quality checks using DLT Expectations
- Unity Catalog-based governance
- Production-style medallion architecture
- Scalable and fault-tolerant design

---

## Author

**Islam Ansari**  
Data Engineer  

- LinkedIn: *(https://www.linkedin.com/in/islam-ansari-133261196/)*


This project has been build via the Udemy course - Databricks Certified Data Engineer Associate (by Ramesh Retnaswamy)



