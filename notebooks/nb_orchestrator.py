# Databricks notebook source
# DBTITLE 1,FreshMart ETL Orchestrator Notebook
# MAGIC %md
# MAGIC # FreshMart ETL Orchestrator Notebook
# MAGIC This notebook runs the Bronze, Silver, and Gold layers of the FreshMart batch ETL pipeline in sequence.
# MAGIC It can be scheduled to run daily via Databricks Jobs.

# COMMAND ----------
# DBTITLE 1,Step 1: Bronze Ingestion
# MAGIC %run ./nb_bronze_ingest

# COMMAND ----------
# DBTITLE 1,Step 2: Silver Cleaning & Transformation
# MAGIC %run ./nb_silver_transform

# COMMAND ----------
# DBTITLE 1,Step 3: Gold Aggregations
# MAGIC %run ./nb_gold_aggregate

# COMMAND ----------
# DBTITLE 1,Pipeline Execution Completed
# MAGIC %md
# MAGIC ### Pipeline execution completed successfully!
