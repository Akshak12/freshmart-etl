import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Check if running in Databricks
try:
    from pyspark.dbutils import DBUtils
    is_databricks = True
except ImportError:
    is_databricks = False

# Paths configuration
if is_databricks:
    # Databricks DBFS paths
    RAW_PATH = "/dbfs/FileStore/freshmart/raw"
    BRONZE_PATH = "/dbfs/FileStore/freshmart/bronze"
    SILVER_PATH = "/dbfs/FileStore/freshmart/silver"
    GOLD_PATH = "/dbfs/FileStore/freshmart/gold"
else:
    # Local paths (relative to workspace root)
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_PATH = os.path.join(BASE_DIR, "data", "raw")
    BRONZE_PATH = os.path.join(BASE_DIR, "data", "bronze")
    SILVER_PATH = os.path.join(BASE_DIR, "data", "silver")
    GOLD_PATH = os.path.join(BASE_DIR, "data", "gold")

# Raw Datasets Schemas (used during ingestion into Bronze)
ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("city", StringType(), True),
    StructField("payment_mode", StringType(), True),
    StructField("status", StringType(), True)
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("item_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("discount_pct", DoubleType(), True)
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("registered_on", StringType(), True),
    StructField("loyalty_points", IntegerType(), True)
])

DELIVERY_LOGS_SCHEMA = StructType([
    StructField("delivery_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("rider_id", StringType(), True),
    StructField("pickup_time", StringType(), True),
    StructField("delivery_time", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("status", StringType(), True)
])
