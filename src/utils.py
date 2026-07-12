"""
Utility functions for SparkSession creation and Delta table writes.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name="FreshMart-ETL"):
    """
    Creates and returns a SparkSession with Delta Lake support enabled.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    return spark


def write_delta_table(df, path, mode="overwrite", partition_by=None):
    """
    Writes a PySpark DataFrame as a Delta table.

    Args:
        df: PySpark DataFrame to write.
        path: File system path where the Delta table will be stored.
        mode: Write mode — 'overwrite' (default) or 'append'.
        partition_by: Optional column name to partition the data by.
    """
    writer = df.write.format("delta").mode(mode)

    if partition_by:
        writer = writer.partitionBy(partition_by)

    writer.save(path)
