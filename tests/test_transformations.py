"""
Unit tests for FreshMart ETL transformation functions.
======================================================
Uses a shared SparkSession (session-scoped fixture) to avoid
repeated JVM startup overhead.

Run with:
    pytest tests/test_transformations.py -v
"""

import pytest
from datetime import date, datetime
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
)

from src.transformations import (
    transform_customers_silver,
    transform_order_items_silver,
    transform_orders_silver,
    transform_delivery_logs_silver,
    aggregate_daily_revenue_by_city,
    aggregate_delivery_zone_performance,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for all tests."""
    session = (
        SparkSession.builder
        .master("local[*]")
        .appName("FreshMart-Tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


# ---------------------------------------------------------------------------
# Customer transformation tests
# ---------------------------------------------------------------------------

class TestTransformCustomersSilver:
    """Tests for transform_customers_silver."""

    def test_deduplication(self, spark):
        """Duplicate customer_id rows should be collapsed to one."""
        data = [
            ("CUST-001", "Alice", "a@b.com", "+91-123", "Delhi", "2023-01-01", 100),
            ("CUST-001", "Alice", "a@b.com", "+91-123", "Delhi", "2023-01-01", 100),
            ("CUST-002", "Bob",   "b@c.com", "+91-456", "Mumbai", "2023-06-15", 200),
        ]
        schema = StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("city", StringType()),
            StructField("registered_on", StringType()),
            StructField("loyalty_points", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        result = transform_customers_silver(df)
        assert result.count() == 2

    def test_pii_masking(self, spark):
        """Email and phone should be SHA-256 hashed (64-char hex strings)."""
        data = [("CUST-001", "Alice", "alice@example.com", "+91-9876543210",
                 "Delhi", "2023-01-01", 100)]
        schema = StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("city", StringType()),
            StructField("registered_on", StringType()),
            StructField("loyalty_points", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        result = transform_customers_silver(df).collect()[0]

        # SHA-256 hex digest is 64 characters
        assert len(result["email"]) == 64
        assert len(result["phone"]) == 64
        # Original value must NOT appear
        assert result["email"] != "alice@example.com"

    def test_date_casting(self, spark):
        """registered_on should be cast to DateType."""
        data = [("CUST-001", "Alice", "a@b.com", "+91-123",
                 "Delhi", "2023-05-20", 50)]
        schema = StructType([
            StructField("customer_id", StringType()),
            StructField("name", StringType()),
            StructField("email", StringType()),
            StructField("phone", StringType()),
            StructField("city", StringType()),
            StructField("registered_on", StringType()),
            StructField("loyalty_points", IntegerType()),
        ])
        df = spark.createDataFrame(data, schema)
        result = transform_customers_silver(df).collect()[0]
        assert result["registered_on"] == date(2023, 5, 20)


# ---------------------------------------------------------------------------
# Order Items transformation tests
# ---------------------------------------------------------------------------

class TestTransformOrderItemsSilver:
    """Tests for transform_order_items_silver."""

    def test_derived_columns(self, spark):
        """net_price and discount_amount should be calculated correctly."""
        data = [("ITM-001", "ORD-001", "PRD-001", "Product A", "Cat", 2, 100.0, 10.0)]
        schema = StructType([
            StructField("item_id", StringType()),
            StructField("order_id", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("qty", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("discount_pct", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema)
        row = transform_order_items_silver(df).collect()[0]

        # qty(2) * unit_price(100) * (1 - 10/100) = 180.00
        assert row["net_price"] == Decimal("180.00")
        # qty(2) * unit_price(100) * (10/100) = 20.00
        assert row["discount_amount"] == Decimal("20.00")

    def test_deduplication(self, spark):
        """Duplicate item_id rows should be collapsed."""
        data = [
            ("ITM-001", "ORD-001", "PRD-001", "A", "Cat", 1, 50.0, 0.0),
            ("ITM-001", "ORD-001", "PRD-001", "A", "Cat", 1, 50.0, 0.0),
        ]
        schema = StructType([
            StructField("item_id", StringType()),
            StructField("order_id", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("qty", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("discount_pct", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema)
        assert transform_order_items_silver(df).count() == 1

    def test_zero_discount(self, spark):
        """When discount_pct is 0, net_price = qty * unit_price."""
        data = [("ITM-010", "ORD-010", "PRD-010", "X", "Cat", 3, 40.0, 0.0)]
        schema = StructType([
            StructField("item_id", StringType()),
            StructField("order_id", StringType()),
            StructField("product_id", StringType()),
            StructField("product_name", StringType()),
            StructField("category", StringType()),
            StructField("qty", IntegerType()),
            StructField("unit_price", DoubleType()),
            StructField("discount_pct", DoubleType()),
        ])
        df = spark.createDataFrame(data, schema)
        row = transform_order_items_silver(df).collect()[0]
        assert row["net_price"] == Decimal("120.00")
        assert row["discount_amount"] == Decimal("0.00")


# ---------------------------------------------------------------------------
# Delivery Logs transformation tests
# ---------------------------------------------------------------------------

class TestTransformDeliveryLogsSilver:
    """Tests for transform_delivery_logs_silver."""

    def test_duration_calculation(self, spark):
        """delivery_duration_mins should be the diff between delivery and pickup."""
        data = [("DEL-001", "ORD-001", "RDR-001",
                 "2024-04-11 10:00:00", "2024-04-11 11:03:00",
                 "ZoneA", 5.0, "success")]
        schema = StructType([
            StructField("delivery_id", StringType()),
            StructField("order_id", StringType()),
            StructField("rider_id", StringType()),
            StructField("pickup_time", StringType()),
            StructField("delivery_time", StringType()),
            StructField("zone", StringType()),
            StructField("distance_km", DoubleType()),
            StructField("status", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        row = transform_delivery_logs_silver(df).collect()[0]
        assert row["delivery_duration_mins"] == 63.0
        assert row["is_incomplete"] is False

    def test_incomplete_flag_on_failed(self, spark):
        """Failed deliveries should be flagged as incomplete."""
        data = [("DEL-002", "ORD-002", "RDR-002",
                 "2024-04-11 10:00:00", None,
                 "ZoneB", 3.0, "failed")]
        schema = StructType([
            StructField("delivery_id", StringType()),
            StructField("order_id", StringType()),
            StructField("rider_id", StringType()),
            StructField("pickup_time", StringType()),
            StructField("delivery_time", StringType()),
            StructField("zone", StringType()),
            StructField("distance_km", DoubleType()),
            StructField("status", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        row = transform_delivery_logs_silver(df).collect()[0]
        assert row["is_incomplete"] is True
        assert row["delivery_duration_mins"] is None

    def test_null_delivery_time_flags_incomplete(self, spark):
        """Even with status='success', a null delivery_time → incomplete."""
        data = [("DEL-003", "ORD-003", "RDR-003",
                 "2024-04-11 10:00:00", None,
                 "ZoneC", 2.0, "success")]
        schema = StructType([
            StructField("delivery_id", StringType()),
            StructField("order_id", StringType()),
            StructField("rider_id", StringType()),
            StructField("pickup_time", StringType()),
            StructField("delivery_time", StringType()),
            StructField("zone", StringType()),
            StructField("distance_km", DoubleType()),
            StructField("status", StringType()),
        ])
        df = spark.createDataFrame(data, schema)
        row = transform_delivery_logs_silver(df).collect()[0]
        assert row["is_incomplete"] is True


# ---------------------------------------------------------------------------
# Orders transformation tests
# ---------------------------------------------------------------------------

class TestTransformOrdersSilver:
    """Tests for transform_orders_silver."""

    def test_null_customer_handling(self, spark):
        """Empty string customer_id should become NULL."""
        orders_data = [("ORD-001", "", "2024-04-11 10:00:00", "Delhi", "UPI", "delivered")]
        orders_schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_date", StringType()),
            StructField("city", StringType()),
            StructField("payment_mode", StringType()),
            StructField("status", StringType()),
        ])
        items_agg_data = [("ORD-001", 500.0, 50.0)]
        items_agg_schema = StructType([
            StructField("order_id", StringType()),
            StructField("order_total", DoubleType()),
            StructField("total_discount", DoubleType()),
        ])
        df_orders = spark.createDataFrame(orders_data, orders_schema)
        df_items_agg = spark.createDataFrame(items_agg_data, items_agg_schema)
        row = transform_orders_silver(df_orders, df_items_agg).collect()[0]
        assert row["customer_id"] is None
        assert row["order_total"] is not None

    def test_order_total_from_join(self, spark):
        """order_total should come from the items aggregate join."""
        orders_data = [("ORD-010", "CUST-001", "2024-04-12 09:30:00",
                        "Mumbai", "Card", "delivered")]
        orders_schema = StructType([
            StructField("order_id", StringType()),
            StructField("customer_id", StringType()),
            StructField("order_date", StringType()),
            StructField("city", StringType()),
            StructField("payment_mode", StringType()),
            StructField("status", StringType()),
        ])
        items_agg_data = [("ORD-010", 1234.50, 123.45)]
        items_agg_schema = StructType([
            StructField("order_id", StringType()),
            StructField("order_total", DoubleType()),
            StructField("total_discount", DoubleType()),
        ])
        df_orders = spark.createDataFrame(orders_data, orders_schema)
        df_items_agg = spark.createDataFrame(items_agg_data, items_agg_schema)
        row = transform_orders_silver(df_orders, df_items_agg).collect()[0]
        assert float(row["order_total"]) == pytest.approx(1234.50, abs=0.01)
