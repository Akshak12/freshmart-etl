# Databricks notebook source
# DBTITLE 1,Imports and Setup
import os
import sys

# Allow importing from src directory when running in Databricks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from pyspark.sql.functions import sum
from src.config import BRONZE_PATH, SILVER_PATH
from src.transformations import (
    transform_customers_silver,
    transform_order_items_silver,
    transform_orders_silver,
    transform_delivery_logs_silver
)
from src.utils import get_spark_session, write_delta_table

# COMMAND ----------
# DBTITLE 1,Initialize Spark Session
spark = get_spark_session("FreshMart-Silver-Transformations")

# COMMAND ----------
# DBTITLE 1,Transform Customers
print("Transforming Customers to Silver...")
customers_bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "raw_customers"))
df_customers_silver = transform_customers_silver(customers_bronze)

customers_silver_path = os.path.join(SILVER_PATH, "customers")
write_delta_table(spark, df_customers_silver, customers_silver_path, merge_key="customer_id")
print("Customers Silver upsert complete.")

# COMMAND ----------
# DBTITLE 1,Transform Order Items
print("Transforming Order Items to Silver...")
order_items_bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "raw_order_items"))
df_order_items_silver = transform_order_items_silver(order_items_bronze)

order_items_silver_path = os.path.join(SILVER_PATH, "order_items")
write_delta_table(spark, df_order_items_silver, order_items_silver_path, merge_key="item_id")
print("Order Items Silver upsert complete.")

# COMMAND ----------
# DBTITLE 1,Transform Orders
print("Transforming Orders to Silver...")
orders_bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "raw_orders"))

# Aggregate order items to get total price & discount per order
df_items_agg = df_order_items_silver.groupBy("order_id").agg(
    sum("net_price").alias("order_total"),
    sum("discount_amount").alias("total_discount")
)

df_orders_silver = transform_orders_silver(orders_bronze, df_items_agg)

orders_silver_path = os.path.join(SILVER_PATH, "orders")
write_delta_table(spark, df_orders_silver, orders_silver_path, merge_key="order_id")
print("Orders Silver upsert complete.")

# COMMAND ----------
# DBTITLE 1,Transform Delivery Logs
print("Transforming Delivery Logs to Silver...")
delivery_bronze = spark.read.format("delta").load(os.path.join(BRONZE_PATH, "raw_delivery_logs"))
df_delivery_silver = transform_delivery_logs_silver(delivery_bronze)

delivery_silver_path = os.path.join(SILVER_PATH, "delivery_logs")
write_delta_table(spark, df_delivery_silver, delivery_silver_path, merge_key="delivery_id")
print("Delivery Logs Silver upsert complete.")
