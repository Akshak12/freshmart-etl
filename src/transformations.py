from pyspark.sql.functions import (
    col, sha2, to_date, to_timestamp, coalesce, round,
    when, unix_timestamp, sum, count, avg, max, lit
)

def transform_customers_silver(df_customers):
    """
    Cleans and conforms customer data:
    - Deduplicates by customer_id
    - Casts registered_on to DateType
    - Casts loyalty_points to IntegerType
    - Masks PII (email, phone) using SHA-256
    """
    return df_customers.dropDuplicates(["customer_id"]) \
        .withColumn("registered_on", to_date(col("registered_on"), "yyyy-MM-dd")) \
        .withColumn("loyalty_points", col("loyalty_points").cast("integer")) \
        .withColumn("email", sha2(col("email"), 256)) \
        .withColumn("phone", sha2(col("phone"), 256))

def transform_order_items_silver(df_order_items):
    """
    Cleans and conforms order line items:
    - Deduplicates by item_id
    - Casts qty to IntegerType
    - Casts pricing columns to Decimals
    - Computes net_price: qty * unit_price * (1 - discount_pct/100)
    - Computes discount_amount: qty * unit_price * (discount_pct/100)
    """
    df_clean = df_order_items.dropDuplicates(["item_id"]) \
        .withColumn("qty", col("qty").cast("integer")) \
        .withColumn("unit_price", col("unit_price").cast("decimal(10,2)")) \
        .withColumn("discount_pct", col("discount_pct").cast("decimal(5,2)"))
    
    return df_clean \
        .withColumn(
            "net_price", 
            round(col("qty") * col("unit_price") * (lit(1.0) - col("discount_pct") / lit(100.0)), 2).cast("decimal(10,2)")
        ) \
        .withColumn(
            "discount_amount", 
            round(col("qty") * col("unit_price") * (col("discount_pct") / lit(100.0)), 2).cast("decimal(10,2)")
        )

def transform_orders_silver(df_orders, df_items_agg):
    """
    Cleans and conforms customer orders:
    - Deduplicates by order_id
    - Casts order_date to TimestampType
    - Handles empty/null customer_id (keeps as NULL, avoids breaking join)
    - Joins aggregated order items to populate order_total and total_discount
    """
    df_clean = df_orders.dropDuplicates(["order_id"]) \
        .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("customer_id", when(col("customer_id") == "", None).otherwise(col("customer_id")))
        
    return df_clean.join(df_items_agg, on="order_id", how="left") \
        .withColumn("order_total", coalesce(col("order_total"), lit(0.0)).cast("decimal(10,2)")) \
        .withColumn("total_discount", coalesce(col("total_discount"), lit(0.0)).cast("decimal(10,2)"))

def transform_delivery_logs_silver(df_delivery_logs):
    """
    Cleans and conforms delivery logs:
    - Deduplicates by delivery_id
    - Parses pickup and delivery timestamps robustly (with or without seconds)
    - Computes delivery_duration_mins: difference in minutes
    - Flags is_incomplete if status is failed or timestamps are missing
    """
    df_parsed = df_delivery_logs.dropDuplicates(["delivery_id"]) \
        .withColumn("pickup_time", coalesce(
            to_timestamp(col("pickup_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("pickup_time"), "yyyy-MM-dd HH:mm")
        )) \
        .withColumn("delivery_time", coalesce(
            to_timestamp(col("delivery_time"), "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("delivery_time"), "yyyy-MM-dd HH:mm")
        )) \
        .withColumn("distance_km", col("distance_km").cast("decimal(10,2)"))
        
    return df_parsed \
        .withColumn(
            "delivery_duration_mins",
            when(col("delivery_time").isNotNull() & col("pickup_time").isNotNull(),
                 round((unix_timestamp(col("delivery_time")) - unix_timestamp(col("pickup_time"))) / lit(60), 2))
            .otherwise(None)
        ) \
        .withColumn(
            "is_incomplete",
            when((col("status") != "success") | col("delivery_time").isNull() | col("pickup_time").isNull(), True)
            .otherwise(False)
        )

# --- Gold Layer Aggregations ---

def aggregate_daily_revenue_by_city(df_orders_silver):
    """
    Daily revenue rollup per city for delivered orders:
    - total_revenue: sum of order totals
    - total_orders: total unique orders
    - avg_basket_size: total_revenue / total_orders
    """
    return df_orders_silver.filter(col("status") == "delivered") \
        .groupBy(to_date(col("order_date")).alias("order_date"), col("city")) \
        .agg(
            round(sum("order_total"), 2).alias("total_revenue"),
            count("order_id").alias("total_orders"),
            round(sum("order_total") / count("order_id"), 2).alias("avg_basket_size")
        ) \
        .orderBy("order_date", "city")

def aggregate_product_return_summary(df_orders_silver, df_order_items_silver):
    """
    Product return rate summary by product category:
    - return_count: total quantity returned (based on order status='returned')
    - total_sold_count: total quantity ordered
    - return_rate: return_count / total_sold_count
    """
    df_joined = df_order_items_silver.join(df_orders_silver, on="order_id", how="inner")
    return df_joined.groupBy("product_id", "product_name", "category") \
        .agg(
            sum(when(col("status") == "returned", col("qty")).otherwise(0)).alias("return_count"),
            sum("qty").alias("total_sold_count"),
            round(
                sum(when(col("status") == "returned", col("qty")).otherwise(0)) / sum("qty"), 4
            ).alias("return_rate")
        ) \
        .orderBy(col("return_count").desc())

def aggregate_delivery_zone_performance(df_delivery_logs_silver):
    """
    Delivery zone efficiency metrics:
    - avg_delivery_time_mins: average time for success deliveries
    - failure_rate: failed deliveries / total delivery attempts
    - total_attempts: count of delivery attempts
    """
    return df_delivery_logs_silver.groupBy("zone") \
        .agg(
            round(avg(when(col("status") == "success", col("delivery_duration_mins"))), 2).alias("avg_delivery_time_mins"),
            round(
                sum(when(col("status") == "failed", 1).otherwise(0)) / count("delivery_id"), 4
            ).alias("failure_rate"),
            count("delivery_id").alias("total_attempts")
        ) \
        .orderBy("zone")

def aggregate_customer_summary(df_orders_silver, df_customers_silver):
    """
    Customer lifetime stats:
    - total_spend: sum of order_totals for delivered orders
    - order_count: number of delivered orders
    - last_order_date: date of the last delivered order
    """
    df_orders_agg = df_orders_silver.filter(col("status") == "delivered") \
        .groupBy("customer_id") \
        .agg(
            round(sum("order_total"), 2).alias("total_spend"),
            count("order_id").alias("order_count"),
            max("order_date").alias("last_order_date")
        )
        
    return df_customers_silver.join(df_orders_agg, on="customer_id", how="left") \
        .select(
            col("customer_id"),
            col("name"),
            col("email"),
            col("phone"),
            col("city"),
            col("registered_on"),
            col("loyalty_points"),
            coalesce(col("total_spend"), lit(0.0)).alias("total_spend"),
            coalesce(col("order_count"), lit(0)).alias("order_count"),
            col("last_order_date")
        ) \
        .orderBy(col("total_spend").desc())
