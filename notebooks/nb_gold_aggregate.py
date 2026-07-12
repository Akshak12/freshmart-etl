# Databricks notebook source
# DBTITLE 1,Imports and Setup
import os
import sys

# Allow importing from src directory when running in Databricks
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.config import SILVER_PATH, GOLD_PATH
from src.transformations import (
    aggregate_daily_revenue_by_city,
    aggregate_product_return_summary,
    aggregate_delivery_zone_performance,
    aggregate_customer_summary
)
from src.utils import get_spark_session

# COMMAND ----------
# DBTITLE 1,Initialize Spark Session
spark = get_spark_session("FreshMart-Gold-Aggregations")

# COMMAND ----------
# DBTITLE 1,Read Silver Tables
print("Reading Silver Delta tables...")
df_orders = spark.read.format("delta").load(os.path.join(SILVER_PATH, "orders"))
df_items = spark.read.format("delta").load(os.path.join(SILVER_PATH, "order_items"))
df_customers = spark.read.format("delta").load(os.path.join(SILVER_PATH, "customers"))
df_delivery = spark.read.format("delta").load(os.path.join(SILVER_PATH, "delivery_logs"))

# COMMAND ----------
# DBTITLE 1,Aggregate Daily Revenue by City
print("Aggregating Daily Revenue by City...")
df_revenue = aggregate_daily_revenue_by_city(df_orders)
revenue_gold_path = os.path.join(GOLD_PATH, "daily_revenue_by_city")
df_revenue.write.format("delta").mode("overwrite").save(revenue_gold_path)
print(f"Daily Revenue written to {revenue_gold_path}")

# COMMAND ----------
# DBTITLE 1,Aggregate Product Return Summary
print("Aggregating Product Return Summary...")
df_returns = aggregate_product_return_summary(df_orders, df_items)
returns_gold_path = os.path.join(GOLD_PATH, "product_return_summary")
df_returns.write.format("delta").mode("overwrite").save(returns_gold_path)
print(f"Product Return Summary written to {returns_gold_path}")

# COMMAND ----------
# DBTITLE 1,Aggregate Delivery Zone Performance
print("Aggregating Delivery Zone Performance...")
df_delivery_perf = aggregate_delivery_zone_performance(df_delivery)
delivery_gold_path = os.path.join(GOLD_PATH, "delivery_zone_performance")
df_delivery_perf.write.format("delta").mode("overwrite").save(delivery_gold_path)
print(f"Delivery Zone Performance written to {delivery_gold_path}")

# COMMAND ----------
# DBTITLE 1,Aggregate Customer Summary
print("Aggregating Customer Summary...")
df_cust_summary = aggregate_customer_summary(df_orders, df_customers)
customer_gold_path = os.path.join(GOLD_PATH, "customer_summary")
df_cust_summary.write.format("delta").mode("overwrite").save(customer_gold_path)
print(f"Customer Summary written to {customer_gold_path}")
