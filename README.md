# 🛒 FreshMart Retail Analytics — End-to-End ETL Pipeline

A production-grade batch ETL pipeline for **FreshMart**, a multi-city grocery delivery platform, built with **PySpark**, **Delta Lake**, and **Databricks**, following the **Medallion Architecture** (Bronze → Silver → Gold).

---

## 🎯 Project Overview

FreshMart operates across **Delhi**, **Mumbai**, and **Bengaluru**, generating daily transactional data from orders, order line-items, deliveries, and customer registrations. This project implements a **batch ETL pipeline** that:

1. **Ingests** raw CSV/JSON data into a Bronze layer (schema-enforced, append-only).
2. **Transforms** and cleans the data in a Silver layer (deduplication, type casting, PII masking, derived metrics).
3. **Aggregates** business-ready analytical tables in a Gold layer (revenue analysis, return rates, delivery performance, customer lifetime value).

The pipeline is designed to be **idempotent** : Silver tables use Delta Lake **Merge (Upsert)** to safely handle re-runs without data duplication.

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Data Sources                                 │
│  orders/*.csv   order_items/*.csv   delivery/*.csv   customers.json │
└──────────────┬──────────────────────────────────────────────────────┘
               │
               ▼
┌──────────────────────────────┐
│       🥉 BRONZE LAYER        │
│  Raw ingestion (append-only) │
│  + _ingested_date            │
│  + _source_file              │
│  Schema enforcement          │
│  Partitioned by date         │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       🥈 SILVER LAYER        │
│  Data type casting           │
│  Null / empty handling       │
│  Deduplication (primary key) │
│  PII masking (SHA-256)       │
│  Derived columns:            │
│    • order_total             │
│    • total_discount          │
│    • delivery_duration_mins  │
│    • is_incomplete           │
│  Delta Merge (Upsert)        │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│       🥇 GOLD LAYER          │
│  daily_revenue_by_city       │
│  product_return_summary      │
│  delivery_zone_performance   │
│  customer_summary            │
│  Full refresh (overwrite)    │
└──────────────────────────────┘
```

---

## 🛠 Technologies Used

| Technology      | Purpose                                      |
|-----------------|----------------------------------------------|
| **Python 3.10+**| Core programming language                    |
| **PySpark**     | Distributed data processing engine           |
| **Delta Lake**  | ACID-compliant storage layer (Merge/Upsert)  |
| **Spark SQL**   | Declarative aggregations (Gold layer)        |
| **Databricks**  | Cloud execution environment (notebooks)      |
| **Pytest**      | Unit testing framework                       |

---

## 📁 Folder Structure

```
FreshMart-ETL/
├── data/
│   └── raw/                        # Sample datasets (CSV/JSON)
│       ├── orders/                  # 7 daily order files
│       ├── order_items/             # 7 daily line-item files
│       ├── delivery/                # 7 daily delivery log files
│       └── customers/               # Customer master (JSON)
│
├── notebooks/                       # Databricks-compatible notebooks
│   ├── nb_bronze_ingest.py          # Bronze: raw → Delta with metadata
│   ├── nb_silver_transform.py       # Silver: clean, cast, dedupe, mask
│   ├── nb_gold_aggregate.py         # Gold: business aggregations
│   └── nb_orchestrator.py           # Orchestrates Bronze → Silver → Gold
│
├── src/                             # Reusable Python modules
│   ├── __init__.py
│   ├── config.py                    # Paths, env detection, schemas
│   ├── transformations.py           # All DataFrame transformations
│   └── utils.py                     # SparkSession & Delta write helpers
│
├── sql/                             # Standalone Spark SQL queries
│   ├── daily_revenue_by_city.sql
│   ├── product_return_summary.sql
│   ├── delivery_zone_performance.sql
│   └── customer_summary.sql
│
├── tests/                           # Unit tests
│   ├── __init__.py
│   └── test_transformations.py
│
├── main.py                          # Local pipeline entrypoint
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## 📊 Data Sources

| Dataset       | Format | Records/Day | Key Columns                                                |
|---------------|--------|-------------|------------------------------------------------------------|
| **Orders**    | CSV    | ~55         | `order_id`, `customer_id`, `order_date`, `city`, `status`  |
| **Order Items** | CSV  | ~140        | `item_id`, `order_id`, `product_name`, `qty`, `unit_price` |
| **Delivery**  | CSV    | ~42         | `delivery_id`, `order_id`, `pickup_time`, `delivery_time`  |
| **Customers** | JSON   | 79 total    | `customer_id`, `name`, `email`, `phone`, `registered_on`   |

Data spans **7 days** (2024-04-11 to 2024-04-17) across **3 cities**: Delhi, Mumbai, Bengaluru.

---


## 🚀 How to Run

### Prerequisites

- Python 3.10+
- Java 8 or 11+ (required by PySpark)

### Local Execution

```bash
# 1. Clone the repository
git clone https://github.com/<your-username>/FreshMart-ETL.git
cd FreshMart-ETL

# 2. Create virtual environment (recommended)
python -m venv .venv
source .venv/bin/activate        # Linux/macOS
.venv\Scripts\activate           # Windows

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the full pipeline
python main.py
```

The pipeline will create Delta tables under `data/bronze/`, `data/silver/`, and `data/gold/`.

### Databricks Execution

1. Import the `notebooks/` directory into your Databricks workspace.
2. Upload raw data to DBFS at `/FileStore/freshmart/raw/`.
3. Upload the `src/` directory as a Databricks Repo or attach it as a library.
4. Run `nb_orchestrator` — it chains Bronze → Silver → Gold via `%run`.
5. Optionally schedule the orchestrator as a **Databricks Job** for daily execution.

---

## 🧪 Running Tests

```bash
# Install test dependencies
pip install -r requirements.txt

# Run unit tests
pytest tests/ -v
```


---

## 📝 License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
