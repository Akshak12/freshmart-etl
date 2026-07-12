"""
FreshMart ETL Pipeline — Local Entrypoint
==========================================
Runs the complete Medallion pipeline (Bronze → Silver → Gold) locally
using PySpark with Delta Lake. This script mirrors the Databricks notebook
orchestration flow but is designed for standalone execution.

Usage:
    python main.py
"""

import os
import sys
import time

from pyspark.sql.functions import current_date, input_file_name, sum as spark_sum

from src.config import (
    RAW_PATH, BRONZE_PATH, SILVER_PATH, GOLD_PATH,
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA,
    CUSTOMERS_SCHEMA, DELIVERY_LOGS_SCHEMA,
)
from src.transformations import (
    transform_customers_silver,
    transform_order_items_silver,
    transform_orders_silver,
    transform_delivery_logs_silver,
    aggregate_daily_revenue_by_city,
    aggregate_product_return_summary,
    aggregate_delivery_zone_performance,
    aggregate_customer_summary,
)
from src.utils import get_spark_session, write_delta_table


def _banner(msg: str) -> None:
    """Print a formatted stage banner."""
    border = "=" * 60
    print(f"\n{border}\n  {msg}\n{border}")



# Bronze Layer

def run_bronze(spark):
    """Ingest raw CSV/JSON files into Bronze Delta tables."""
    _banner("BRONZE LAYER — Raw Ingestion")

    datasets = [
        ("orders",    "csv",  ORDERS_SCHEMA,        "raw_orders"),
        ("order_items", "csv", ORDER_ITEMS_SCHEMA,   "raw_order_items"),
        ("delivery",  "csv",  DELIVERY_LOGS_SCHEMA,  "raw_delivery_logs"),
        ("customers", "json", CUSTOMERS_SCHEMA,      "raw_customers"),
    ]

    for folder, fmt, schema, table_name in datasets:
        raw_path = os.path.join(RAW_PATH, folder)
        bronze_path = os.path.join(BRONZE_PATH, table_name)

        reader = spark.read.format(fmt).schema(schema)
        if fmt == "csv":
            reader = reader.option("header", "true")
        elif fmt == "json":
            reader = reader.option("multiline", "true")

        df = reader.load(raw_path) \
            .withColumn("_ingested_date", current_date()) \
            .withColumn("_source_file", input_file_name())

        write_delta_table(spark, df, bronze_path, partition_by="_ingested_date")
        print(f"  ✓ {table_name:25s}  →  {df.count():>5,} rows")



# Silver Layer

def run_silver(spark):
    """Clean, cast, deduplicate, and enrich Bronze data into Silver."""
    _banner("SILVER LAYER — Transformations")

    #  Customers 
    customers_bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_PATH, "raw_customers")
    )
    df_customers_silver = transform_customers_silver(customers_bronze)
    write_delta_table(
        spark, df_customers_silver,
        os.path.join(SILVER_PATH, "customers"), merge_key="customer_id",
    )
    print(f"  ✓ silver_customers          →  {df_customers_silver.count():>5,} rows")

    #  Order Items 
    items_bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_PATH, "raw_order_items")
    )
    df_items_silver = transform_order_items_silver(items_bronze)
    write_delta_table(
        spark, df_items_silver,
        os.path.join(SILVER_PATH, "order_items"), merge_key="item_id",
    )
    print(f"  ✓ silver_order_items        →  {df_items_silver.count():>5,} rows")

    #  Orders (needs item aggregates) 
    orders_bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_PATH, "raw_orders")
    )
    df_items_agg = df_items_silver.groupBy("order_id").agg(
        spark_sum("net_price").alias("order_total"),
        spark_sum("discount_amount").alias("total_discount"),
    )
    df_orders_silver = transform_orders_silver(orders_bronze, df_items_agg)
    write_delta_table(
        spark, df_orders_silver,
        os.path.join(SILVER_PATH, "orders"), merge_key="order_id",
    )
    print(f"  ✓ silver_orders             →  {df_orders_silver.count():>5,} rows")

    #  Delivery Logs 
    delivery_bronze = spark.read.format("delta").load(
        os.path.join(BRONZE_PATH, "raw_delivery_logs")
    )
    df_delivery_silver = transform_delivery_logs_silver(delivery_bronze)
    write_delta_table(
        spark, df_delivery_silver,
        os.path.join(SILVER_PATH, "delivery_logs"), merge_key="delivery_id",
    )
    print(f"  ✓ silver_delivery_logs      →  {df_delivery_silver.count():>5,} rows")



# Gold Layer

def run_gold(spark):
    """Build business-ready aggregate tables from Silver."""
    _banner("GOLD LAYER — Aggregations")

    df_orders = spark.read.format("delta").load(os.path.join(SILVER_PATH, "orders"))
    df_items = spark.read.format("delta").load(os.path.join(SILVER_PATH, "order_items"))
    df_customers = spark.read.format("delta").load(os.path.join(SILVER_PATH, "customers"))
    df_delivery = spark.read.format("delta").load(os.path.join(SILVER_PATH, "delivery_logs"))

    gold_tables = [
        ("daily_revenue_by_city",    aggregate_daily_revenue_by_city(df_orders)),
        ("product_return_summary",   aggregate_product_return_summary(df_orders, df_items)),
        ("delivery_zone_performance", aggregate_delivery_zone_performance(df_delivery)),
        ("customer_summary",         aggregate_customer_summary(df_orders, df_customers)),
    ]

    for table_name, df_agg in gold_tables:
        gold_path = os.path.join(GOLD_PATH, table_name)
        df_agg.write.format("delta").mode("overwrite").save(gold_path)
        print(f"  ✓ gold_{table_name:30s} →  {df_agg.count():>5,} rows")



# Main


def main():
    """Execute the full Bronze → Silver → Gold pipeline."""
    start = time.time()
    _banner("FreshMart Retail Analytics — ETL Pipeline")

    spark = get_spark_session("FreshMart-ETL-Local")
    spark.sparkContext.setLogLevel("WARN")

    run_bronze(spark)
    run_silver(spark)
    run_gold(spark)

    elapsed = time.time() - start
    _banner(f"Pipeline finished in {elapsed:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
