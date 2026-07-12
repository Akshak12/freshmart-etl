# Databricks notebook source
# DBTITLE 1,Imports and Setup
import os
import sys

# Allow importing from src directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import current_date, input_file_name
from src.config import (
    RAW_PATH, BRONZE_PATH,
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA,
    CUSTOMERS_SCHEMA, DELIVERY_LOGS_SCHEMA
)
from src.utils import get_spark_session, write_delta_table

# COMMAND ----------
# DBTITLE 1,Initialize Spark Session
spark = get_spark_session("FreshMart-Bronze-Ingestion")

# COMMAND ----------
# DBTITLE 1,Ingest Orders
print("Ingesting Orders...")
df_orders = spark.read.csv(
    os.path.join(RAW_PATH, "orders"),
    header=True, schema=ORDERS_SCHEMA
)
df_orders = df_orders.withColumn("_ingested_date", current_date()) \
                     .withColumn("_source_file", input_file_name())

write_delta_table(df_orders, os.path.join(BRONZE_PATH, "raw_orders"),
                  mode="append", partition_by="_ingested_date")
print(f"  Orders ingested: {df_orders.count()} rows")

# COMMAND ----------
# DBTITLE 1,Ingest Order Items
print("Ingesting Order Items...")
df_order_items = spark.read.csv(
    os.path.join(RAW_PATH, "order_items"),
    header=True, schema=ORDER_ITEMS_SCHEMA
)
df_order_items = df_order_items.withColumn("_ingested_date", current_date()) \
                               .withColumn("_source_file", input_file_name())

write_delta_table(df_order_items, os.path.join(BRONZE_PATH, "raw_order_items"),
                  mode="append", partition_by="_ingested_date")
print(f"  Order Items ingested: {df_order_items.count()} rows")

# COMMAND ----------
# DBTITLE 1,Ingest Customers
print("Ingesting Customers...")
df_customers = spark.read.json(
    os.path.join(RAW_PATH, "customers"),
    multiLine=True, schema=CUSTOMERS_SCHEMA
)
df_customers = df_customers.withColumn("_ingested_date", current_date()) \
                           .withColumn("_source_file", input_file_name())

write_delta_table(df_customers, os.path.join(BRONZE_PATH, "raw_customers"),
                  mode="append", partition_by="_ingested_date")
print(f"  Customers ingested: {df_customers.count()} rows")

# COMMAND ----------
# DBTITLE 1,Ingest Delivery Logs
print("Ingesting Delivery Logs...")
df_delivery = spark.read.csv(
    os.path.join(RAW_PATH, "delivery"),
    header=True, schema=DELIVERY_LOGS_SCHEMA
)
df_delivery = df_delivery.withColumn("_ingested_date", current_date()) \
                         .withColumn("_source_file", input_file_name())

write_delta_table(df_delivery, os.path.join(BRONZE_PATH, "raw_delivery_logs"),
                  mode="append", partition_by="_ingested_date")
print(f"  Delivery Logs ingested: {df_delivery.count()} rows")
