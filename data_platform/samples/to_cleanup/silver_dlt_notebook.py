# Databricks notebook source
# DBTITLE 1,Required for regular expression modules
# MAGIC %pip install regex
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #DLT Pipeline 
# MAGIC ![DLT Pipeline](/Volumes/infinity-db-catalog/pre_mdm_test_schema/design_images/DLT_pipeline.png)
# MAGIC
# MAGIC ## Prerequisites
# MAGIC 1. All metadata tables are loaded using SQL Scripts. 
# MAGIC 2. A workflow step initialize_run to load all the metadata parameters
# MAGIC
# MAGIC ## Processing Logic ##
# MAGIC Read all the sources to process for a target dimensions
# MAGIC API: process_sources
# MAGIC
# MAGIC For each source, run the below __DLT pipeline steps__
# MAGIC
# MAGIC __Step 1:__ Read the raw source data from Volumes and create 
# MAGIC [target_table]_l1_raw_data
# MAGIC
# MAGIC __Step 2:__ Delta layer: [target_table]_l2_delta
# MAGIC     Use DLT apply_changes to do automatic delta detection. 
# MAGIC     For understanding apply_changes, [docs](https://docs.databricks.com/en/delta-live-tables/cdc.html)
# MAGIC     This step will apply the changes to the delta table based on some CDC column values
# MAGIC     Support for both type 1 and type 2
# MAGIC
# MAGIC __Step 3:__ 5 data quality use cases(DQ Step 1-5): dq_(warn,quarantine,fail,drop,keep)_ [target_table] and audit_dq_ [target_table]  
# MAGIC     1. Load metadata rules from metadata_dlt_validation_rules (E.g for source_table = 'CMC_GRGR_GROUP' and target_table = 'DF_Group' and action = 'WARN') using the API get_data_quality_rules and applies the appropriate rules.   
# MAGIC     Note: The dictionary returned has to be designed for this usecase, return type should be as per DLT expectations framework.       
# MAGIC     2. Pass the dictionary to DLT expectations framework  
# MAGIC     3. Generates 2 tables based on the above rules dq_(warn,quarantine,fail,drop,keep)_ [target_table] and audit_dq_ [target_table]  
# MAGIC
# MAGIC __Step 4:__ Create the final DF_ [target_table] with the following transformations
# MAGIC 1. Apply source to target transformations - Map source columns to target columns using SQL
# MAGIC 2. Apply any filters if applicable. For example, if the source has a column 'A' and the target has a column 'B', then you can apply a filter to only include records where 'A' is not null.
# MAGIC 3. All the functions related to this step will be defined in the silver_[dim table].py or gold_ [dim table].py file and will be dynamically loaded . E.g. silver_D_Group.py or gold_D_Group.py. 
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Imports

# COMMAND ----------

import dlt

from pyspark.sql import functions as F
from pyspark.sql.functions import *
import re
from datetime import datetime
import sys

# load dependent libraries here
from utils import get_column_mappings, get_data_quality_rules


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Parameters required for this DLT
# MAGIC **source** (All source tables by Dimensions. For e.g. all Group raw data sources, retrieved from metadata)

# COMMAND ----------

# DBTITLE 1,DLT parameters
"""
     
    1. Fetch the run_date based on the dimension_table and status = 'IN_PROGRESS' from the workflow_runtime_audit table.
    2. Only 1 DLT pipeline per dimension_table can be run at a time, so you should always see 1 workflow IN_PROGRESS for any given source and run_date
    # E.g. DF_GROUP_FACETS, DF_GROUP_VHP
"""
source = spark.conf.get("source")

query  = f"""
SELECT catalog_name, metadata_schema_name, run_date FROM  `catalog`.`schema`.`workflow_runtime_audit` where source = '{source}' and status = 'IN_PROGRESS';
"""
run_date_df = spark.sql (query)
row_dict = run_date_df.first()
run_date_as_str = row_dict["run_date"]

run_date =  datetime.now().strftime("%Y-%m-%d")
if run_date_as_str not in "N/A":    
    run_date = datetime.strptime(run_date_as_str, "%Y-%m-%d").date()
print(run_date)

catalog_name = row_dict["catalog_name"]
metadata_schema_name = row_dict["metadata_schema_name"]

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ### Read parameters from metadata_source_info table
# MAGIC
# MAGIC Read all the parameters required by this DLT pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DLT Logic

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### API - generate_silver_tables
# MAGIC
# MAGIC - This is the main DLT pipeline that generates the silver layer tables.  
# MAGIC - This method is common for all the sources and is metadata driven. This should work seamlessly for all the source (VEEVA, CUSTOMERMDM, CVENT etc.)
# MAGIC

# COMMAND ----------

def generate_silver_tables(
    catalog_name, metadata_schema_name, p_source_table, p_target_table
):
    print(
        f"#########START: Target Table::{p_target_table}:: Keys::{p_keys} ###########"
    )

    # DLT LOGIC STARTES HERE
    ######## DLT STEP: LOAD RAW DATA FROM VOLUMES ###############
    @dlt.table(
        name=f"structured.{p_target_table}_l1_raw_data",
        comment="Bronze table, reading from Volumes",
        # table_properties={"delta.autoMerge.enabled": "true"}
        # schema="structured",
    )
    def dlt_l1_tables():
        try:
            # Logic to read the data from the source
            df = None
            return df

        except Exception as e:
            # Capture and log any errors
            print(f"Error in table_func: {str(e)}")
            raise  # Re-raise the exception to propagate the error

    ######## DLT STEP: HANDLE CDC ###############
    # Fetch CDC attributes
    cdc_dict = get_cdc_attributes(
        catalog_name, metadata_schema_name, p_source_table, p_target_table
    )
    target_table_keys = cdc_dict["key_attr"]
    keys = target_table_keys.split(",")
    cdc_columns = cdc_dict["cdc_columns"]
    compute_cdc = cdc_dict["compute_cdc"]
    # run_date
    sequence_by = cdc_dict["sequence_by"]
    stored_as_scd_type = cdc_dict["stored_as_scd_type"]

    # Step 2: L2 layer - Delta table creation
    delta_table = f"curated.{p_target_table}_l2_delta"
    dlt.create_streaming_table(delta_table)

    # Apply the changes to the target table using SCD Type 1
    dlt.apply_changes(
        target=delta_table,
        source=f"{p_target_table}_l1",
        keys=keys,
        sequence_by=sequence_by,
        stored_as_scd_type=stored_as_scd_type,  # SCD Type 2: Add new rows and keep historical records
    )

    ######## DLT STEP: HANDLE FAILURE RULES ###############
    # Step 3: L3 layer - Handle data quality for action=FAIL. This API defined in utils.py
    # The pipeline will fail in case of expetation failure
    failure_rules = get_data_quality_rules(
        catalog_name, schema_name, p_source, p_target_table, "FAIL"
    )

    @dlt.table(
        comment="Handle failure condition",
        name=f"{p_target_table}_l3_dq_fail",
        schema="curated",
    )
    @dlt.expect_all_or_fail(failure_rules)
    def dlt_l3_dq_fail():
        df = dlt.read(f"{p_target_table}_l2_delta")

        return df

    ######## DLT STEP: HANDLE QUARANTINE RULES ###############
    quarantine_rules = get_data_quality_rules(
        catalog_name, schema_name, p_source, p_target_table, "QUARANTINE"
    )

    quarantine_expr_rules = "NOT({0})".format(" AND ".join(quarantine_rules.values()))

    @dlt.table(
        comment="Preprocess data for quarantine rules",
        name=f"{p_target_table}_l3_dq_quarantine_pp",
        schema="curated",
        temporary=True,
        partition_cols=["is_valid"],
    )
    @dlt.expect_all(quarantine_rules)
    def dlt_l3_dq_quarantine_pp():
        df = (
            dlt.read(f"{p_target_table}_l3_dq_fail")
            .withColumn("is_valid", expr(quarantine_expr_rules))
            .withColumn(
                "quarantine_details",
                when(
                    F.col("is_valid")
                    == False,  # Only add quarantine details when is_valid is false
                    create_map(
                        [
                            v
                            for k, rule in quarantine_rules.items()
                            for v in (lit(k), expr(rule))
                        ]
                    ),
                ).otherwise(None),
            )
        )

        return df

    @dlt.table(
        comment="Handle data for quarantine rules",
        name=f"{p_target_table}_l3_dq_quarantine",
    )
    def dlt_l3_dq_quarantine():
        df = dlt.read(f"{p_target_table}_l3_dq_quarantine_pp").filter("is_valid=true")
        return df

    @dlt.table(
        comment="Move quarantined records to a audit table",
        name=f"audit_dq_{p_target_table}",
        schema="curated",
    )
    def dlt_l3_dq_quarantine_audit():
        df = (
            dlt.read(f"{p_target_table}_l3_dq_quarantine_pp")
            .filter("is_valid=false")
            .withColumn("validation_table", f"{p_target_table}_l3_dq_quarantine_pp")
            .withColumn(
                "reason",
                F.map_filter("quarantine_details", lambda k, v: v != F.lit("true")),
            )
        )

        return df

    ######## DLT STEP: HANDLE IN_PLACE TRANSFORMATION ###############
    # Handle source to target column mapping and any filter conditions to be applied to the target table
    @dlt.table(
        "In-Place trnasformation of DF_tables for the target table",
        name=f"{p_target_table}_l4_dt",
        schema="curated",
    )
    def dlt_l4_dt_targets():
        df = dlt.read(f"{p_target_table}_l3_dq_quarantine")
        # Wrtite logic to run UDF
        quarantine_rules = get_data_trandformation_rules(
            catalog_name, schema_name, p_source, p_target_table, "TRANSFORM"
        )
        return df

    ######## DLT STEP: HANDLE DF_TARGET_TABLES ###############
    # Handle source to target column mapping and any filter conditions to be applied to the target table
    @dlt.table(
        comment="Final DF_tables for the target table",
        name=f"{p_target_table}",
        schema="curated"
    )
    def dlt_l5_df_targets():
        df = dlt.read(f"{p_target_table}_l4_dt")

        # Load column mappings and filter condition dynamically
        column_mappings, filter_condition = load_table_config(
            dimension_table, p_target_table, "silver"
        )
        # Dynamically rename columns
        renamed_df = df.selectExpr(
            *[
                f"`{old}` as `{new}`"
                for old, new in column_mappings.items()
                if old in df.columns
            ]
        )

        # Apply dynamic filter if it exists
        if filter_condition:
            renamed_df = renamed_df.filter(filter_condition)

        return renamed_df

    return

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Layer Tables
# MAGIC
# MAGIC This API runs the Data Cleansing and Referential Integrity transformation steps using the DLT expectations framework
# MAGIC
# MAGIC [DLT expectations framework](https://docs.databricks.com/en/delta-live-tables/expectations.html)

# COMMAND ----------

# DBTITLE 1,Main DLT logic
def process_sources(source):
    print(f"START: Processing source ::{source}")
    df = get_sources_to_process(catalog_name, metadata_schema_name, source)
    rows = df.collect()

    # Main Logic starts here
    for row in rows:
        # read all the variables required for the pipleline
        source_table = row["source_table"]
        target_table = row["target_table"]
        dimension_table = row["dimension_table"]
        source_details = row["source_details"]
        load_type = row["load_type"]

        # Step1: Generate Silver Tables
        generate_silver_tables(
            catalog_name, metadata_schema_name, source_table, target_table
        )

    print(f"END: Processing source ::{source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entry Point

# COMMAND ----------

#  We only run one source
process_sources (source)