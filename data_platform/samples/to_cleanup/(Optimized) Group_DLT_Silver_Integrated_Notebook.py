# Databricks notebook source
# MAGIC %md
# MAGIC ### Global Imports

# COMMAND ----------

import dlt

from pyspark.sql import functions as F
# from pyspark.sql.functions import to_timestamp, date_format
import os
import sys
import re
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Include folder path to utils.py and dlt_utils.py files

# COMMAND ----------

current_dir = "/Volumes/edl_dev_ctlg/rawfiles/raw/dlt_pilot"
sys.path.append(current_dir)


# COMMAND ----------

# Import the required modules from PySpark SQL
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, expr, when, upper, create_map
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, DateType, BooleanType, MapType

from utils import get_source_metadata, get_run_as_username, get_cdc_attributes
from udf_utils import substitute_string_udf
# generate_bronzetables
from dlt_utils import generate_scd_tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters required for this DLT

# COMMAND ----------

"""
     
    1. Fetch the run_date based on the dimension_table and status = 'IN_PROGRESS' from the workflow_runtime_audit table.
    2. Only 1 DLT pipeline per dimension_table can be run at a time, so you should always see 1 workflow IN_PROGRESS for any given source and run_date
    # E.g. DF_GROUP_FACETS, DF_GROUP_VHP
"""

# source = spark.conf.get("source")
# target = spark.conf.get("target")
dimension_table = spark.conf.get("dimension_table")
catalog = spark.conf.get("catalog")
schema_name = spark.conf.get("schema_name")
business_start_date = spark.conf.get("business_start_date")
# input_table = spark.conf.get("input_table")
run_as_username = get_run_as_username(dbutils)
load_type = "incremental"
table_suffix = spark.conf.get("table_suffix")

print (f"DLT parameters: dimension_table-{dimension_table}, catalog-{catalog}, schema-{schema_name}, business_start_date-{business_start_date}, run_as_username-{run_as_username}")



# COMMAND ----------

# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

if business_start_date == 'N/A':
  load_type = "one_time"
metadata_sources_df = get_source_metadata(spark, catalog, schema_name, dimension_table, load_type)
display(metadata_sources_df)

# COMMAND ----------


#  Repalces {} with actula business date
substitute_string_udf = substitute_string_udf(business_start_date)
metadata_sources_modified_df = metadata_sources_df.withColumn("source_details",substitute_string_udf(F.col("source_details")))
display (metadata_sources_modified_df)

# COMMAND ----------

input_table_to_col_mapping = {
  "t_dlt_bronze_cmc_grgr_group": ["GRGR_CK", "GRGR_ID", "GRGR_NAME", "GRGR_MCTR_TYPE", "GRGR_ITS_CODE", "GRGR_ORIG_EFF_DT", "GRGR_CONV_DT", "loadtimestamp"],
  "t_dlt_bronze_cmc_grgr_group_history": ["GRGR_CK","GRGR_ID","GRGR_STS","GRGR_ADDR1","GRGR_ADDR2","GRGR_ADDR3","GRGR_CITY","GRGR_STATE","GRGR_COUNTY","GRGR_ZIP","GRGR_CTRY_CD","GRGR_EMAIL","GRGR_PHONE","GRGR_PHONE_EXT","GRGR_FAX","GRGR_FAX_EXT","GRGR_TERM_DT","GRGR_MCTR_TRSN","GRGR_RNST_DT","GRGR_RENEW_MMDD","GRGR_ERIS_MMDD","CICI_ID","GRGR_EIN", "loadtimestamp"],
  "t_dlt_bronze_cmc_grgc_group_count": ["GRGR_CK","GRGC_MCTR_CTYP","GRGC_EFF_DT","GRGC_TERM_DT","GRGC_COUNT", "loadtimestamp"]
}


def get_columns_for_table(table_name):
  return input_table_to_col_mapping.get(table_name)


# COMMAND ----------

# Load metadata rules from metadata_dlt_validation_rules
def get_data_validation_rules(catalog_name, schema_name, input_table, target_table, action):
    """
    loads data quality rules from a table
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
    """
    rules_list = (
        spark.read.format("delta")
        .table(catalog_name + "." + schema_name + "." + "t_dlt_metadata_validation_rules")
        .filter(
            (col("source_table") == input_table)
            & (col("target_table") == target_table)
            & (col("is_active") == "Y")
            & (col("action") == action)
        )
        .select(col("action"), col("constraint"))
    ).collect()
    return {row["constraint"]: row["constraint"] for row in rules_list}

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Logic starts here

# COMMAND ----------

def generate_df_tables(
    spark,
    source_table,
    target_table,
    loadtimestamp   
):
  
  print(f"===1...Reading from source_table::{source_table}, target_table::{target_table}") 
    
  @dlt.table(
    name = f"{target_table}",
  )
  def create_final_table():
      return spark.readStream.table(source_table)

# COMMAND ----------


def generate_bronzetables(
    spark,
    volume_path,
    bronze_table,
    columns, 
    loadtimestamp   
):

    # create streaming table          
    dlt.create_streaming_table(bronze_table)

    # add a flow
    @dlt.append_flow(
      name = f"flow_{bronze_table}",
      target=bronze_table
    )
    def create_final_table():
        df = (spark.readStream.format('cloudFiles')
              .option('cloudFiles.format', 'parquet')
              .option('rescuedDataColumn', 'rescue')
              .load(volume_path)
              .select(*columns) 
            )

        for col_name, col_type in special_columns.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))

        df = df.withColumn("processing_time", F.current_timestamp())
        return df
      

# COMMAND ----------

from pyspark.sql.functions import expr, lit, create_map, when, col
from pyspark.sql.types import MapType, StringType
import dlt

def generate_dq_table(spark, input_table, source_table, target_table, dq_type):
    """
    Generalized DQ table generator supporting FAIL, WARN, and QUARANTINE.

    Parameters:
    - spark: SparkSession
    - input_table (str): Base table name for rules lookup
    - source_table (str): Table to read from
    - target_table (str): Target DLT table to create
    - dq_type (str): One of 'FAIL', 'WARN', or 'QUARANTINE'
    """
    dq_type = dq_type.upper()
    print(f"=== generate_dq_table [{dq_type}] :: input_table::{input_table}, source_table::{source_table}, target_table::{target_table}")
    
    # Get validation rules from your metadata source
    rules = get_data_validation_rules(catalog, schema_name, input_table, target_table, dq_type)
    rules = {key: f"not({value})" for key, value in rules.items()}
    expr_rules = f"({' AND '.join(rules.values())})" if rules else "()"

    if dq_type == "FAIL":
        @dlt.table(
            comment="Handle failure condition",
            name=target_table
        )
        @dlt.expect_all_or_fail(rules)
        def dlt_dq_fail():
            return spark.readStream.table(source_table)
    else:
        @dlt.table(
            comment=f"Process data for {dq_type.lower()} rules",
            name=target_table,
            partition_cols=["is_valid"]
        )
        @dlt.expect_all(rules)
        def dlt_dq_non_fail():
            df = dlt.readStream(source_table)

            # Add is_valid column
            df = df.withColumn("is_valid", lit(True) if expr_rules == "()" else expr(expr_rules))

            # Add action_details column
            empty_map = create_map(lit(""), lit("")).cast(MapType(StringType(), BooleanType()))
            if not rules:
                df = df.withColumn("action_details", empty_map)
            else:
                df = df.withColumn(
                    "action_details",
                    when(
                        col("is_valid") == False,
                        create_map([
                            v for k, rule in rules.items()
                            for v in (lit(k), expr(rule))
                        ])
                    ).otherwise(None)
                )
            return df


# COMMAND ----------

def apply_all_dq_rules(spark, input_table, initial_source_table, table_suffix):
    """
    Applies all DQ rules in order: fail → quarantine → warn.
    Returns the last stage's target table as output.
    """
    dq_stage_functions = {
        "fail": generate_failure_tables,
        "quarantine": generate_quarantine_tables,
        "warn": generate_warn_tables,
    }

    current_source = initial_source_table

    for stage in ["fail", "quarantine", "warn"]:
        dq_target = f"{input_table}_dq_{stage}"
        if table_suffix:
            dq_target = f"{dq_target}_{table_suffix}"

        dq_stage_functions[stage](spark, input_table, current_source, dq_target)
        current_source = dq_target  # Pass output to next stage

    return current_source  # Final DQ output table


# COMMAND ----------

def generate_dq_audit_tables(spark, input_table, table_prefix):
  
  target_table = input_table
  if table_suffix is not None:
    target_table = f"{target_table}_{table_suffix}"

  target_table = f"{target_table}_dq_audit"
  dlt.create_streaming_table(target_table)

  @dlt.append_flow(
    comment="Move quarantine records to a audit table",
    name = f"flow_{target_table}_1",
    target=target_table
  )
  def dlt_dq_quarantine_audit():
      source_table = f"{input_table}_dq_quarantine"
      if table_suffix is not None:
        source_table = f"{source_table}_{table_suffix}"

      df = (
          dlt.readStream(f"{source_table}")
          .filter("is_valid=false")
          .withColumn("validation_table", lit(f"{source_table}"))
          .withColumn(
              "reason",
              F.map_filter("action_details", lambda k, v: v != F.lit("true")),
          )
      )
      return df

  # Append warn records to a audit table
  @dlt.append_flow(
      comment="Move quarantine records to a audit table",
      name = f"flow_{target_table}_2",
      target=f"t_dlt_audit_dq_{target_table}"
  )
  def dlt_dq_warn_audit():
      source_table = f"{input_table_name}_dq_warn"
      if table_suffix is not None:
        source_table = f"{source_table}_{table_suffix}"

      df = (
          dlt.readStream(f"{source_table}")
          .filter("is_valid=false")
          .withColumn("validation_table", lit(f"{target_table}_dq_warn"))
          .withColumn(
              "reason",
              F.map_filter("action_details", lambda k, v: v != F.lit("true")),
          )
      )
      return df 

# COMMAND ----------

# List of optional CDC metadata fields to cast if present
special_columns = {
    "__$start_lsn": "binary",
    "__$operation": "string",
    "__$update_mask": "string",
}

for row in metadata_sources_modified_df.collect():
    try:
        source = row.source
        input_tables = [item.strip() for item in row.input_table.split(",")]
        target_tables = [item.strip() for item in row.target_table.split(",")]
        volume_path = row.source_details
        is_active = row.is_active

        print(
            f"Processing source: {source}, volume_path: {volume_path}, "
            f"input_tables: {input_tables}, target_tables: {target_tables}, is_active: {is_active}"
        )

        if is_active == 'N':
            continue

        for index, input_table in enumerate(input_tables):
            # === Step 1: Generate Bronze Table ===
            bronze_table = f"{input_table}_{table_suffix}" if table_suffix else input_table
            columns = get_columns_for_table(input_table)

            _ = generate_bronzetables(spark, volume_path, bronze_table, columns, special_columns)
            source_table = bronze_table

            # === Step 2: Generate CDC Table (if applicable) ===
            cdc_row = get_cdc_attributes(spark, catalog, schema_name, source, input_table)
            print(f"cdc_row: {cdc_row}")

            if cdc_row:
                cdc_target = f"{input_table}_cdc_type_{cdc_row.scd_type}"
                if table_suffix:
                    cdc_target = f"{cdc_target}_{table_suffix}"

                _ = generate_scd_tables(
                    source_table=source_table,
                    target_table=cdc_target,
                    key_attr=cdc_row.key_attr,
                    timestamp_col="loadtimestamp",
                    scd_type=cdc_row.scd_type,
                    compute_cdc=cdc_row.compute_cdc,
                )
                source_table = cdc_target

            # === Step 3-5: Apply all DQ rules using helper ===
            # Step 1: FAIL
            fail_target_table = f"{input_table_name}_dq_fail_{table_suffix}"
            generate_dq_table(spark, input_table_name, bronze_table_name, fail_target_table, "FAIL")

            # Step 2: QUARANTINE
            quarantine_source_table = fail_target_table
            quarantine_target_table = f"{input_table_name}_dq_quarantine_{table_suffix}"
            generate_dq_table(spark, input_table_name, quarantine_source_table, quarantine_target_table, "QUARANTINE")

            # Step 3: WARN
            warn_source_table = quarantine_target_table
            warn_target_table = f"{input_table_name}_dq_warn_{table_suffix}"
            generate_dq_table(spark, input_table_name, warn_source_table, warn_target_table, "WARN")

            # === Step 6: Final DF Table ===
            final_target = f"{target_tables[index]}_{table_suffix}" if table_suffix else target_tables[index]
            generate_df_tables(spark, source_table, final_target, "loadtimestamp")

            # === Step 7: Audit Tables ===
            generate_dq_audit_tables(spark, input_table, table_suffix)

    except Exception as e:
        print(f"Error processing source '{source}' table '{input_table}': {e}")


# COMMAND ----------

# df2 = get_cdc_attributes(catalog, schema_name, input_table)
# display(df2)