# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver DLT Pipeline Task 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Global Imports

# COMMAND ----------

import dlt

from pyspark.sql import functions as F
from pyspark.sql.window import Window
# Import the required modules from PySpark SQL
from pyspark.sql import SparkSession, Row
# from pyspark.sql.functions import col, lit, expr, when, upper, create_map, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, TimestampType, DateType, BooleanType, MapType


import os
import sys
import re
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ### Include folder path to include framwork python files

# COMMAND ----------

current_dir = "/Volumes/edl_dev_ctlg/rawfiles/raw/dlt_pilot"
sys.path.append(current_dir)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Framework specific imports

# COMMAND ----------

from utils import get_run_as_username
# , get_cdc_attributes
# get_source_metadata
from udf_utils import substitute_string_udf
# generate_bronzetables
# from dlt_utils import generate_scd_tables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Auto merges schema mismatch issues. 

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parameters required for this DLT

# COMMAND ----------

"""
     
    1. Fetch the run_date based on the dimension_table and status = 'IN_PROGRESS' from the workflow_runtime_audit table.
    2. Only 1 DLT pipeline per dimension_table can be run at a time, so you should always see 1 workflow IN_PROGRESS for any given source and run_date
    # E.g. DF_GROUP_FACETS, DF_GROUP_VHP
"""
catalog = spark.conf.get("catalog")
schema = spark.conf.get("schema")

# This parameter is used only for testing purpose, for multiple people working on their own DLT pipleines. This is not used in the production.
# Note: When using Databricks Asset Bundle, this can be handled automatically as a configuration
table_suffix = spark.conf.get("table_suffix")

# This is the time when the DLT pipeline is run. 
processing_time = int(datetime.now().timestamp())

# business_date = spark.conf.get("business_date")
# dimension_table = spark.conf.get("dimension_table")
# run_as_username = get_run_as_username(dbutils)

print(f"Parameters passed from the DLT pipeline: catalog:{catalog}, schema:{schema}, processing_time::{processing_time}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### API: get_runtime_parameters
# MAGIC
# MAGIC Note: Move to util.py

# COMMAND ----------

def get_runtime_parameters(catalog, schema, table):
    """
    This function retrieves the latest run_date for each dimension_table from the specified metadata table.

    Parameters:
    catalog (str): The catalog name.
    schema (str): The schema name.
    table (str): The table name.

    Returns:
    list: A list of Row objects containing the latest run_date for each dimension_table.
    """
    # Load the metadata table
    metadata_df = spark.table(f"{catalog}.{schema}.{table}")
    
    # Define window spec to get the latest run_date per dimension_table
    window_spec = Window.partitionBy("dimension_table").orderBy(F.col("run_date").desc())

    # Add row number and filter only the latest per group
    latest_metadata_df = (
        metadata_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    # Show result (optional)
    latest_metadata_df.show(truncate=False)

    return latest_metadata_df.collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: get_source_metadata 
# MAGIC Note: Move to util.py 

# COMMAND ----------

def get_source_metadata(
    spark, catalog, schema, dimension_table_name, target_table, business_start_date
):
    """
    This function retrieves metadata information from t_dlt_metadata_source_info for a given dimension table and target table from the metadata source info table.
    It supports both incremental and one-time load types based on the business start date.

    Parameters:
    spark (SparkSession): The Spark session object.
    catalog (str): The catalog name.
    schema (str): The schema name.
    dimension_table_name (str): The name of the dimension table. E.g. D_GROUP
    target_table (str): The name of the target table. E.g. DF_GROUP
    business_start_date (str): The business start date. If set to "N/A", a one-time load is performed. 

    Returns:
    Row: A Row object containing the metadata information for the specified dimension table and target table.
    """
    load_type = "incremental"
    if business_start_date == "N/A":
        load_type = "one_time"

    query = f"""
        SELECT * FROM {catalog}.{schema}.t_dlt_metadata_source_info WHERE lower(dimension_table) = lower('{dimension_table_name}') and lower(target_table) = lower("{target_table}") and is_active = 'Y' and load_type = '{load_type}';
    """
    print(f"get_source_metadata::query: {query}")
    metadata_source_df = spark.sql(query)

    result = None
    if business_start_date != "N/A":
        fn_substitute_string_udf = substitute_string_udf(business_start_date)
        # Substitutes the business start date in the source_details (stores volumes path) column when a business date is passed
        metadata_sources_modified_df = metadata_source_df.withColumn(
            "source_details", fn_substitute_string_udf(F.col("source_details"))
        )

        metadata_sources_modified_df.show(truncate=False)
        result = metadata_sources_modified_df.first()
        return result
    else:
        metadata_source_df.show(truncate=False)
        result = metadata_source_df.first()
        return result

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: get_cdc_attributes
# MAGIC
# MAGIC Note: Move to dlt_util.py

# COMMAND ----------

def get_cdc_attributes(spark, catalog_name, schema_name, source, input_table):
    """
    This function retrieves Change Data Capture (CDC) metadata attributes from t_dlt_metadata_cdc for a given source and input table from the metadata table.

    Parameters:
    spark (SparkSession): The Spark session object.
    catalog_name (str): The catalog name.
    schema_name (str): The schema name.
    source (str): The source name.E.g. DF_GROUP_FACETS DF_VHP_FACETS
    input_table (str): The input table name.E.g. t_dlt_bronze_cmc_grgr_group, t_dlt_bronze_cmc_grgc_group_count

    Returns:
    Row: A Row object containing the CDC attributes (key_attr, compute_cdc, scd_type, exclude_columns) for the specified source and input table.
    key_attr = Primary key to uniquely identify the row. E.g. GRGR_CK
    compute_cdc = 'Y' or 'N'. If 'Y', then CDC is computed using apply_changes_from_snapshot. If 'N' then the CDC is computed using apply_changes
    scd_type = '1' or '2'. If '1', then SCD Type 1 is used. If '2' then SCD Type 2 is used. 
    exclude_columns = List of columns to exclude from CDC. E.g. ['GRGR_CK', 'GRGR_ID']
    """
    # Add exclude columns from CDC list
    query = f"""
        SELECT key_attr, compute_cdc, scd_type, exclude_columns FROM {catalog_name}.{schema_name}.t_dlt_metadata_cdc 
        WHERE source = '{source}' AND input_table = '{input_table}';
    """
    print(query)
    cdc_df = spark.sql(query)
    row = cdc_df.first()

    return row

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variable: bronze_table_to_column_mapping
# MAGIC This is used in creating the bronze tables.
# MAGIC
# MAGIC Note: Move to dlt_utils_df_groups.py

# COMMAND ----------

bronze_table_to_column_mapping = {
    # This dictionary maps source tables to their required and additional columns for different schemas and tables within those schemas. It also has a GLOBALS schema for all tables, these are columns that will be added to all tables.
    # The structure is as follows:
    # {
    #     "source": {
    #         "schema": {
    #             "table": {
    #                 "columns_to_fetch": {
    #                     "column_name": "data_type"
    #                 }
    #             }
    #         }
    #     }
    # }
    "GLOBALS": {
        "all": {
            "all": {
                "global_columns": {
                    "loadtimestamp": "long",
                    "processing_time": "long",
                    "business_date": "date",
                }
            }
        }
    },
    "DF_GROUP_FACETS": {
        "cdc": {
            "t_dlt_bronze_cmc_grgr_group": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_NAME",
                    "GRGR_MCTR_TYPE",
                    "GRGR_ITS_CODE",
                    "GRGR_ORIG_EFF_DT",
                    "GRGR_CONV_DT",
                ],
                "additional_columns": [
                    "__$operation",
                ],
            },
            "t_dlt_bronze_cmc_grgr_group_history": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_STS",
                    "GRGR_ADDR1",
                    "GRGR_ADDR2",
                    "GRGR_ADDR3",
                    "GRGR_CITY",
                    "GRGR_STATE",
                    "GRGR_COUNTY",
                    "GRGR_ZIP",
                    "GRGR_CTRY_CD",
                    "GRGR_EMAIL",
                    "GRGR_PHONE",
                    "GRGR_PHONE_EXT",
                    "GRGR_FAX",
                    "GRGR_FAX_EXT",
                    "GRGR_TERM_DT",
                    "GRGR_MCTR_TRSN",
                    "GRGR_RNST_DT",
                    "GRGR_RENEW_MMDD",
                    "GRGR_ERIS_MMDD",
                    "CICI_ID",
                    "GRGR_EIN",
                ],
                "additional_columns": [
                    "__$operation"
                ],
            },
        },
        "dbo": {
            "t_dlt_bronze_cmc_grgc_group_count": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGC_MCTR_CTYP",
                    "GRGC_EFF_DT",
                    "GRGC_TERM_DT",
                    "GRGC_COUNT",
                ],
                "additional_columns": [
                ],
            },
            "t_dlt_bronze_cmc_grgr_group": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_NAME",
                    "GRGR_MCTR_TYPE",
                    "GRGR_ITS_CODE",
                    "GRGR_ORIG_EFF_DT",
                    "GRGR_CONV_DT",
                ],
                "additional_columns": [
                    "__$operation",
                ],
            },
        },
        "audit": {
            "t_dlt_bronze_cmc_grgr_group_history": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_STS",
                    "GRGR_ADDR1",
                    "GRGR_ADDR2",
                    "GRGR_ADDR3",
                    "GRGR_CITY",
                    "GRGR_STATE",
                    "GRGR_COUNTY",
                    "GRGR_ZIP",
                    "GRGR_CTRY_CD",
                    "GRGR_EMAIL",
                    "GRGR_PHONE",
                    "GRGR_PHONE_EXT",
                    "GRGR_FAX",
                    "GRGR_FAX_EXT",
                    "GRGR_TERM_DT",
                    "GRGR_MCTR_TRSN",
                    "GRGR_RNST_DT",
                    "GRGR_RENEW_MMDD",
                    "GRGR_ERIS_MMDD",
                    "CICI_ID",
                    "GRGR_EIN",
                    "HIST_CREATE_DTM"                 
                ],
                "additional_columns": [  
                    "__$operation"                               
                ],
            }
        },
    },
}

def get_bronze_table_columns(source, source_schema, table, columns_to_fetch):
    """
    Retrieves columns based on the provided conditions from the bronze_table_to_column_mapping dictionary.

    Parameters:
    source (str): The source name (e.g., 'DF_GROUP_FACETS').
    source_schema (str): The schema name (e.g., 'cdc').
    table (str): The table name (e.g., 't_dlt_bronze_cmc_grgr_group').
    columns_to_fetch (str): The type of columns to fetch (e.g., 'required_columns').

    Returns:
    list: A list of columns based on the provided conditions.

    Raises:
    KeyError: If no mapping is found for the provided conditions.
    """
    try:
        return bronze_table_to_column_mapping[source][source_schema][table][
            columns_to_fetch
        ]
    except KeyError:
        raise KeyError(
            f"No mapping found for {source=} {source_schema=} {table=} {columns_to_fetch=}"
        )


# Example usage
# try:
#     req_cols = get_bronze_table_columns("GLOBALS", "all", "all", "global_columns")
#     print("Required Columns:", req_cols)
# except KeyError as e:
#     print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: transform_df_tables
# MAGIC Note: Move to dlt_utils_d_groups.py

# COMMAND ----------

def transform_df_tables(source_df, target_entity):
    """
    Transforms the source DataFrame based on the target entity schema. This will create the final DF_Groups tables.
    This API also computes the Type 2 data (Row_Effective_Date and Row_Effective_End_Date ) based on transformation logic.

    Parameters:
    source_df (DataFrame): The source DataFrame to be transformed.
    target_entity (str): The target entity name. Possible values are "DF_Group_Count", "DF_Group_History", "DF_Group".

    Returns:
    DataFrame: The transformed DataFrame with columns renamed and additional columns added as per the target schema.
    """

    if target_entity == "DF_Group_Count":
        # Select and rename the required columns as per target schema
        df_rename = (
            source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumnRenamed("GRGC_MCTR_CTYP", "Count_Type")
            .withColumnRenamed("GRGC_EFF_DT", "Row_Effective_Date")
            .withColumnRenamed("GRGC_TERM_DT", "Row_Term_Date")
            .withColumnRenamed("GRGC_COUNT", "Employee_Count")
            .withColumn("Row_Change_Datetime", F.lit(business_date))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.lit("N")
                # F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
        )
    elif target_entity == "DF_Group_History":
        # Modify the business_date column with HIST_CREATE_DTM if present
        if "HIST_CREATE_DTM" in source_df.columns:
            source_df = source_df.withColumn(
                "business_date", F.date_format(F.col("HIST_CREATE_DTM"), "yyyy-MM-dd")
            ).drop("HIST_CREATE_DTM")
        else:
            source_df = source_df.withColumn(
                "business_date", F.date_format(F.col("business_date"), "yyyy-MM-dd")
            )

        # Rename the columns as per target schema
        window_spec = Window.partitionBy("Group_Source_Key").orderBy("business_date")
        df_rename = (
            source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumnRenamed("GRGR_ID", "Group_ID")
            .withColumnRenamed("GRGR_STS", "Status_Code")
            .withColumnRenamed("GRGR_ADDR1", "Address_Line_1")
            .withColumnRenamed("GRGR_ADDR2", "Address_Line_2")
            .withColumnRenamed("GRGR_ADDR3", "Address_Line_3")
            .withColumnRenamed("GRGR_CITY", "Address_City")
            .withColumnRenamed("GRGR_STATE", "Address_State")
            .withColumnRenamed("GRGR_COUNTY", "Address_County")
            .withColumnRenamed("GRGR_ZIP", "Zip_Code")
            .withColumnRenamed("GRGR_CTRY_CD", "Country")
            .withColumnRenamed("GRGR_EMAIL", "Email")
            .withColumnRenamed("GRGR_PHONE", "Phone_Nbr")
            .withColumnRenamed("GRGR_PHONE_EXT", "Phone_Ext")
            .withColumnRenamed("GRGR_FAX", "Fax_Nbr")
            .withColumnRenamed("GRGR_FAX_EXT", "Fax_Ext")
            .withColumnRenamed("GRGR_TERM_DT", "Termination_Date")
            .withColumnRenamed("GRGR_MCTR_TRSN", "Term_Reason_Code")
            .withColumnRenamed("GRGR_RNST_DT", "Reinstatement_Date")
            .withColumnRenamed("GRGR_RENEW_MMDD", "Renewal_MMDD")
            .withColumnRenamed("GRGR_ERIS_MMDD", "ERISA_Plan_Begin_MMDD")
            .withColumnRenamed("CICI_ID", "Client_Code")
            .withColumnRenamed("GRGR_EIN", "EIN")
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
            .withColumn("Row_Effective_Date", F.col("business_date"))
            .withColumn(
                "Row_Effective_End_Date",
                F.coalesce(
                    F.lead("business_date").over(window_spec),
                    F.lit("9999-12-31").cast("date"),
                ),
            )
        )
    elif target_entity == "DF_Group":
        # Rename the columns as per target schema
        df_rename = (
            source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumnRenamed("GRGR_ID", "Group_ID")
            .withColumnRenamed("GRGR_NAME", "Group_Name")
            .withColumnRenamed("GRGR_MCTR_TYPE", "Group_Type_Code")
            .withColumnRenamed("GRGR_ITS_CODE", "ITS_Code")
            .withColumnRenamed("GRGR_ORIG_EFF_DT", "Original_Effective_Date")
            .withColumnRenamed("GRGR_CONV_DT", "Group_Facets_Conversion_Date")
            .withColumn("Row_Change_Datetime", F.col("business_date"))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
        )

    return df_rename

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: get_data_transformation_rules
# MAGIC Note: Move to dlt_utils.py

# COMMAND ----------

def get_data_transformation_rules(catalog, schema, input_table, target_table):
    """
    Loads data transformation rules from a t_dlt_transform_rules table.
    These rules are applied dynamically using the expectations framework using UDF's

    Parameters:
    catalog (str): The catalog name.
    schema (str): The schema name.
    input_table (str): The input table name.
    target_table (str): The target table name.

    Returns:
    list: A list of rows containing the source column name, type, and transformation function name for active transformation rules.

    Example:
    >>> rules = get_data_transformation_rules("my_catalog", "my_schema", "input_table", "target_table")
    >>> for rule in rules:
    >>>     print(rule["source_column_name"], rule["type"], rule["transform_fn_name"])
    """
    transformation_rules = (
        spark.read.format("delta")
        .table(f"{catalog}.{schema}.t_dlt_transform_rules")
        .filter(
            (F.col("source_table") == input_table)
            & (F.col("target_table") == target_table)
            & (F.col("is_active") == "Y")
        )
        .select(F.col("source_column_name"), F.col("type"), F.col("transform_fn_name"))
    ).collect()
    return transformation_rules

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation UDF's
# MAGIC These are used in transformation step   
# MAGIC _Note: Move to dlt_udfs.py_
# MAGIC
# MAGIC

# COMMAND ----------

default_values = ['Facets']

# to trim the string fields
@F.udf(returnType=StringType())
def trim_udf(val):
    """
    UDF to trim leading and trailing whitespace from a string.

    Parameters:
    val (str): The input string to be trimmed.

    Returns:
    str: The trimmed string, or None if the input is None.
    """
    return str(val).strip() if val is not None else val

# to convert the string fields to title case
@F.udf(returnType=StringType())
def title_case_udf(val):
    """
    UDF to convert a string to title case.

    Parameters:
    val (str): The input string to be converted to title case.

    Returns:
    str: The string in title case, or None if the input is None.
    """
    return val.title() if val else val

# Map the udf_name string to actual Python UDFs
udf_mapping = {
    "trim_udf": trim_udf,
    "title_case_udf": title_case_udf
}

# to pass the default values to the columns
def add_default_columns_udf(df, columns, values):
    """
    Adds default values to specified columns in a DataFrame.

    Parameters:
    df (DataFrame): The input DataFrame.
    columns (list): List of column names to add default values to.
    values (list or dict): List or dictionary of default values.

    Returns:
    DataFrame: The DataFrame with default values added to specified columns.
    """
    if isinstance(values, dict):
        for col in columns:
            df = df.withColumn(col, F.lit(values.get(col)))
    else:  # assume it's a list
        for col, val in zip(columns, values):
            df = df.withColumn(col, F.lit(val))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: get_data_validation_rules
# MAGIC _Note: Move to dlt_utils.py_

# COMMAND ----------

def get_data_validation_rules(catalog, schema, input_table, target_table, action):
    """
    Loads data quality rules from a t_dlt_metadata_validation_rules table.

    Parameters:
    catalog (str): The catalog name.
    schema (str): The schema name.
    input_table (str): The input table name.
    target_table (str): The target table name.
    action (str): The action to match.

    Returns:
    dict: A dictionary of rules that matched the action.

    Example:
    >>> rules = get_data_validation_rules("my_catalog", "my_schema", "input_table", "target_table", "warn")
    >>> for constraint, rule in rules.items():
    >>>     print(constraint, rule)
    """
    rules_list = (
        spark.read.format("delta")
        .table(catalog + "." + schema + "." + "t_dlt_metadata_validation_rules")
        .filter(
            (F.col("source_table") == input_table)
            & (F.col("target_table") == target_table)
            & (F.col("is_active") == "Y")
            & (F.col("action") == action)
        )
        .select(F.col("action"), F.col("constraint"))
    ).collect()
    return {row["constraint"]: row["constraint"] for row in rules_list}

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: generate_bronzetables
# MAGIC _Note: Move to dlt_utils.py_

# COMMAND ----------

def generate_bronzetables(
    spark,
    volume_path,
    bronze_table,
    source,
    source_schema,
    input_table,
    business_date,
    processing_time,
    business_date_format="yyyyMMdd",
):
    """
    Generates bronze tables for the given source data.

    Parameters:
    - spark: SparkSession
    - volume_path (str): Path to the data volume.
    - bronze_table (str): Name of the bronze table to be created.
    - source (str): Source system name. (E.g. DF_GROUP_FACETS)
    - source_schema (str): Schema of the source data. (audit, dbo, cdc)
    - input_table (str): Name of the input table. (E.g. t_dlt_bronze_cmc_grgr_group)
    - business_date (str): Business date for the data.
    - processing_time (str): Processing time for the data.
    - business_date_format (str): Format of the business date. Default is "yyyyMMdd".

    Returns:
    - None: This function does not return any value. It creates a streaming table and appends data to it.
    """
    print(
        f"generate_bronzetables:: source ::{source}, source_schema::{source_schema}, input_table::{input_table}, business_date:: {business_date}, processing_time::{processing_time}"
    )
    global_cols = get_bronze_table_columns("GLOBALS", "all", "all", "global_columns")
    required_cols = get_bronze_table_columns(
        source, source_schema, input_table, "required_columns"
    )
    additional_cols = get_bronze_table_columns(
        source, source_schema, input_table, "additional_columns"
    )

    print(f"generate_bronzetables:: global_cols ::{global_cols}")

    # create streaming table
    dlt.create_streaming_table(bronze_table)

    # add a flow
    @dlt.append_flow(name=f"flow_{bronze_table}", target=bronze_table)
    def create_bronze_table():
        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("rescuedDataColumn", "rescue")
            .load(volume_path)
        ).select(*required_cols)

        for col_name, col_type in global_cols.items():
            if col_name == "business_date":
                df = df.withColumn(
                    col_name,
                    F.to_date(F.lit(business_date), business_date_format).cast(
                        col_type
                    ),
                )
            if col_name == "processing_time":
                df = df.withColumn(col_name, F.lit(processing_time).cast(col_type))

        for col_name in additional_cols:
            if col_name == "__$operation":
                if col_name in df.columns:
                    df = df.withColumn(col_name, F.col(col_name).cast("int"))
                else:
                    df = df.withColumn(col_name, F.lit(0).cast("int"))

        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### generate_scd_tables
# MAGIC _Note: Move to dlt_utils.py_

# COMMAND ----------

def generate_scd_tables(
    source_table,
    target_table,
    keys,
    seq_col="processing_time",
    scd_type=1,
    compute_cdc="N",
    exclude_columns_from_cdc=[],
):
    """
    Generates Slowly Changing Dimension (SCD) tables using Delta Live Tables.

    Parameters:
    - source_table (str): The source table name.
    - target_table (str): The target table name.
    - keys (str): Comma-separated list of primary key columns.
    - seq_col (str): Column used for sequencing changes. Default is "processing_time".
    - scd_type (int): Type of SCD (1 or 2). Default is 1.
    - compute_cdc (str): Flag to compute CDC from snapshot ('Y') or not ('N'). Default is 'N'.
    - exclude_columns_from_cdc (list): List of columns to exclude from CDC. Default is an empty list.

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """
    key_list = [item.strip() for item in keys.split(",")]
    dlt.create_streaming_table(target_table)

    dlt.apply_changes(
        target=target_table,
        source=source_table,
        keys=key_list,
        sequence_by=F.col(seq_col),
        stored_as_scd_type=scd_type,
        # except_column_list=exclude_columns_from_cdc,
    )
    # if compute_cdc == "N":
    #     dlt.apply_changes(
    #         target=target_table,
    #         source=source_table,
    #         keys=key_list,
    #         sequence_by=F.col(seq_col),
    #         stored_as_scd_type=scd_type,
    #         # except_column_list=exclude_columns_from_cdc,
    #     )
    # else:
    #     dlt.apply_changes_from_snapshot(
    #         target=target_table,
    #         source=source_table,
    #         keys=key_list,
    #         stored_as_scd_type=scd_type,
    #     )

# COMMAND ----------

# MAGIC %md
# MAGIC ### generate_dq_table
# MAGIC _Note: Move to dlt_utils.py_

# COMMAND ----------

def generate_dq_table(
    spark,
    input_table,
    source_table,
    target_table,
    dq_type,
    target_entity,
    create_streaming_table = True
):
    """
    Generalized DQ table generator supporting FAIL, WARN, and QUARANTINE.

    Parameters:
    - spark: SparkSession
    - input_table (str): Base table name for rules lookup
    - source_table (str): Table to read from
    - target_table (str): Target DLT table to create
    - dq_type (str): One of 'FAIL', 'WARN', or 'QUARANTINE'
    - target_entity (str): Entity for which DQ rules are applied
    - create_streaming_table (bool): Flag to create streaming table. Default is True.
    """
    dq_type = dq_type.upper()
    print(
        f"generate_dq_table [{dq_type}] :: input_table::{input_table}, source_table::{source_table}, target_table::{target_table}"
    )

    # Get validation rules from your metadata source
    rules = get_data_validation_rules(
        catalog, schema, input_table, target_table, dq_type
    )
    rules = {key: f"not({value})" for key, value in rules.items()}
    expr_rules = f"({' AND '.join(rules.values())})" if rules else "()"

    if dq_type == "FAIL":

        @dlt.table(comment="Handle failure condition", name=target_table)
        @dlt.expect_all_or_fail(rules)
        def dlt_dq_fail():
            # Filter the data to only do validations on data that has changed since the last run
            df = None
            if create_streaming_table:
                df = dlt.readStream(source_table).filter(
                    F.col("processing_time") >= processing_time
                )
            else:
                df = dlt.read(source_table).filter(
                    F.col("processing_time") >= processing_time
                )

            return df

    else:

        @dlt.table(
            comment=f"Process data for {dq_type.lower()} rules",
            name=target_table,
            partition_cols=["is_valid"],
        )
        @dlt.expect_all(rules)
        def dlt_dq_non_fail():
            if create_streaming_table:
                df = dlt.readStream(source_table)
            else:
                df = dlt.read(source_table)

            # Add is_valid column
            df = df.withColumn(
                "is_valid", F.lit(True) if expr_rules == "()" else F.expr(expr_rules)
            )

            # Add action_details column
            empty_map = F.create_map(F.lit(""), F.lit("")).cast(
                MapType(StringType(), BooleanType())
            )
            if not rules:
                df = df.withColumn("action_details", empty_map)
            else:
                df = df.withColumn(
                    "action_details",
                    F.when(
                        F.col("is_valid") == False,
                        F.create_map(
                            [
                                v
                                for k, rule in rules.items()
                                for v in (F.lit(k), F.expr(rule))
                            ]
                        ),
                    ).otherwise(None),
                )            

            if dq_type == "QUARANTINE":
                df = df.filter("is_valid=true")
            return df
        
        # Handle audit records
        generate_dq_audit_tables(spark, input_table, target_table, dq_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: generate_tranformation_rules_df
# MAGIC _Note: Move to dq_utils.py_

# COMMAND ----------

def generate_tranformation_rules_df(
    spark, input_table, source_table, target_table, create_streaming_table=True
):
    """
    Generates a DataFrame by applying transformation rules from metadata.

    Parameters:
    - spark: SparkSession
    - input_table (str): Table name for transformation rules lookup.
    - source_table (str): Table to read from.
    - target_table (str): Target DLT table to create.
    - create_streaming_table (bool): Flag to create streaming table. Default is True.

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """
    df_transformation_rules = get_data_transformation_rules(
        catalog, schema, input_table, target_table
    )

    @dlt.table(
        comment="apply transformation rules for required columns",
        name=f"{target_table}",
    )
    def apply_transformation_rules():
        if create_streaming_table:
            df = dlt.readStream(f"{source_table}")
        else:
            df = dlt.read(f"{source_table}")
        df_dt=df
        # Apply each UDF based on metadata
        if df_transformation_rules:
            # apply the transformation rules other than default
            for row in df_transformation_rules:
                if row["type"] != "default":
                    col_name = row["source_column_name"]
                    transformation_type = row["type"]
                    udf_name = row["transform_fn_name"]
                    if udf_name:
                        udf_function = udf_mapping[udf_name]
                        df_dt = df_dt.withColumn(col_name, udf_function(col(col_name)))
            # apply the default transformation rules
            default_columns = []
            for row in df_transformation_rules:
                if row["type"] == "default":
                    col_name = row["source_column_name"]
                    default_columns.append(col_name)
                    df_dt = add_default_columns_udf(
                        df_dt, default_columns, default_values
                    )
        else:
            df_dt = df
        return df_dt

# COMMAND ----------

# MAGIC %md
# MAGIC ### API: generate_df_tables
# MAGIC _Note: Move to dlt_utils.py_

# COMMAND ----------

def generate_df_tables(
    spark, target_entity, source_table, target_table, business_date
):
    """
    Generates the final DataFrame tables (DF_GROUP, DF_GROUP_COUNT, DF_GROUP_HISTORY) by reading from the source table and applying necessary transformations.

    Parameters:
    - spark: SparkSession
    - target_entity (str): Entity for which the final table is created
    - source_table (str): Source table to read from
    - target_table (str): Target table to create    
    - business_date (str): Business date for the data

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """
    print(
        f"generate_df_tables::Reading from source_table::{source_table}, target_table::{target_table}, target_entity::{target_entity}, business_date::{business_date}"
    )

    @dlt.table(
        name=f"{target_table}",
    )
    def create_final_table():
        df = dlt.read(source_table)
        # unchanged_data_df = dlt.read(unchanged_data_view)

        # df = unchanged_data_df.unionByName(df, allowMissingColumns=True)
        df_transformed = transform_df_tables(df, target_entity)        
        return df_transformed

# COMMAND ----------

# MAGIC %md
# MAGIC ### API : generate_dq_audit_tables
# MAGIC _Note: Move to dq_utils.py_

# COMMAND ----------

def generate_dq_audit_tables(spark, input_table, target_table, dq_type):
    """
    Generates Data Quality (DQ) audit tables by appending invalid records to a Delta sink.
    Multiple DLT pipelines can write to Delta Sink Tables. 

    Parameters:
    - spark: SparkSession
    - input_table (str): Source table name for reading data.
    - target_table (str): Target table name for appending invalid records.
    - dq_type (str): Type of data quality check (e.g., "FAIL", "QUARANTINE", "WARN").

    Returns:
    - None: This function does not return any value. It creates or updates the audit table.
    """
    flow_name = f"{target_table}_{dq_type.lower()}_flow"
    delta_sink_target = f"{catalog}.{schema}.t_dlt_audit"

    print(
        f"generate_dq_audit_tables:: flow_name:{flow_name}, delta_sink_target::{delta_sink_target}"
    )

    dlt.create_sink(
        name="audit_delta_sink",
        format="delta",
        options={"tableName": delta_sink_target},
    )

    @dlt.append_flow(name=flow_name, target="audit_delta_sink")
    def delta_sink_flow():
        df = (
            spark.readStream.table(target_table)
            .filter("is_valid=false")
            .withColumn("validation_table", F.lit(f"{target_table}"))
            .withColumn(
                "reason",
                F.map_filter("action_details", lambda k, v: v != F.lit("true")),
            )
        )

        return df

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Logic starts here
# MAGIC This API  is responsible for creating the DLT pipelines for particular dimension table and target table

# COMMAND ----------

# def run_dlt(dimension_table, target_table, business_date):
#     """
#     Runs the Delta Live Tables (DLT) pipeline for the specified dimension table and target table.

#     Parameters:
#     - dimension_table (str): The dimension table to process. E.g: D_GROUP
#     - target_table (str): The target table to create or update. DF_GROUP, DF_GROUP_COUNT, DF_GROUP_HISTORY
#     - business_date (str): The business date for the data.

#     Returns:
#     - None: This function does not return any value. It processes the DLT pipeline.
#     """
#     print(
#         f"run_dlt:: dimension_table:{dimension_table}, target_table:{target_table}, business_date:{business_date}"
#     )
    
#     # fetch the metadata from the metadata table

#     metadata_source = get_source_metadata(
#         spark, catalog, schema, dimension_table, target_table, business_date
#     )

#     print(f"run_dlt::metadata_source::{metadata_source}")
#     row = metadata_source
#     source = row.source
#     source_schema = row.source_schema
#     input_table_name_list = [
#         item.strip() for item in row.impacted_input_tables.split(",")
#     ]
#     target_table_list = [item.strip() for item in row.impacted_target_tables.split(",")]
#     volume_path = row.source_details
#     is_active = row.is_active

#     print(
#         f"source:{source}, volume_path::{volume_path}, input_table_name_list::{input_table_name_list}, target_table_list:{target_table_list}, is_active:{is_active}"
#     )

#     for index, input_table_name in enumerate(input_table_name_list):
#         print(
#             f"index:{index}, input_table_name:{input_table_name}, target_table_list:{target_table_list[index]}"
#         )
#         # actual DF_GROUP, DF_GROUP_COUNT, DF_GROUP_HISTORY
#         target_entity = target_table_list[index]
#         bronze_table_name = input_table_name
#         if table_suffix is not None:
#             bronze_table_name = f"{bronze_table_name}_{table_suffix}"

#         # Step 1: Handle Bronze
#         # columns_to_fetch = get_columns_for_table(input_table_name)

#         df = generate_bronzetables(
#             spark,
#             volume_path,
#             bronze_table_name,
#             source,
#             source_schema,
#             input_table_name,
#             business_date,
#             processing_time,
#         )

#         # Step 2: Handle CDC
#         cdc_row = get_cdc_attributes(spark, catalog, schema, source, input_table_name)
#         print(f"cdc_row::{cdc_row}")
#         cdc_target_table = None
#         if cdc_row is not None:
#             key_attr = cdc_row.key_attr
#             scd_type = cdc_row.scd_type
#             compute_cdc = cdc_row.compute_cdc
#             cdc_source_table = f"{bronze_table_name}"
#             exclude_columns_from_cdc = cdc_row.exclude_columns

#             cdc_target_table = f"{input_table_name}_cdc_type_{scd_type}"
#             if table_suffix is not None:
#                 cdc_target_table = f"{cdc_target_table}_{table_suffix}"
#             df = generate_scd_tables(
#                 cdc_source_table,
#                 cdc_target_table,
#                 key_attr,
#                 "processing_time",
#                 scd_type,
#                 compute_cdc,
#                 exclude_columns_from_cdc,
#             )

#         # Step 3: Handle Failure
#         source_table_name = cdc_target_table
#         target_table_name = None
#         if cdc_row is None:
#             source_table_name = bronze_table_name

#         # Step 1: FAIL
#         # From this step, make sure only records that have changed are sent downwards for validation and transformation
#         fail_target_table = f"{input_table_name}_dq_fail_ok_{table_suffix}"
#         generate_dq_table(
#             spark,
#             input_table_name,
#             source_table_name,
#             fail_target_table,
#             "FAIL",
#             target_entity,
#         )

#         # Step 2: QUARANTINE
#         quarantine_source_table = fail_target_table
#         quarantine_target_table = f"{input_table_name}_dq_quarantine_ok_{table_suffix}"
#         generate_dq_table(
#             spark,
#             input_table_name,
#             quarantine_source_table,
#             quarantine_target_table,
#             "QUARANTINE",
#             target_entity,
#         )

#         # Step 3: WARN
#         warn_source_table = quarantine_target_table
#         warn_target_table = f"{input_table_name}_dq_warn_ok_{table_suffix}"
#         df = generate_dq_table(
#             spark,
#             input_table_name,
#             warn_source_table,
#             warn_target_table,
#             "WARN",
#             target_entity,
#         )

#         # Apply simple transformation rules.
#         transform_source_table = warn_target_table
#         transform_target_table = f"{input_table_name}_dt_{table_suffix}"
#         generate_tranformation_rules_df(
#             spark, input_table_name, transform_source_table, transform_target_table
#         )

#         # Step 4: Final DLT table generation
#         final_source_table = transform_target_table
#         final_target_table = f"{target_table_list[index]}_{table_suffix}"
#         generate_df_tables(
#             spark,
#             target_entity,
#             source_table=final_source_table,
#             target_table=final_target_table,
#             business_date=business_date,
#         )

# COMMAND ----------

def run_dlt(dimension_table, target_table, business_date, scenario):
    """
    Runs the Delta Live Tables (DLT) pipeline for the specified dimension table and target table,
    including Change Data Capture (CDC) processing at the end.

    Parameters:
    - dimension_table (str): The dimension table to process. E.g: D_GROUP
    - target_table (str): The target table to create or update. DF_GROUP, DF_GROUP_COUNT, DF_GROUP_HISTORY
    - business_date (str): The business date for the data.

    Returns:
    - None: This function does not return any value. It processes the DLT pipeline.
    """
    print(
        f"run_dlt:: dimension_table:{dimension_table}, target_table:{target_table}, business_date:{business_date}"
    )
    
    # fetch the metadata from the metadata table

    metadata_source = get_source_metadata(
        spark, catalog, schema, dimension_table, target_table, business_date
    )

    print(f"run_dlt::metadata_source::{metadata_source}")
    row = metadata_source
    source = row.source
    source_schema = row.source_schema
    input_table_name_list = [
        item.strip() for item in row.impacted_input_tables.split(",")
    ]
    target_table_list = [item.strip() for item in row.impacted_target_tables.split(",")]
    volume_path = row.source_details
    is_active = row.is_active

    print(
        f"source:{source}, volume_path::{volume_path}, input_table_name_list::{input_table_name_list}, target_table_list:{target_table_list}, is_active:{is_active}"
    )

    for index, input_table_name in enumerate(input_table_name_list):
        print(
            f"index:{index}, input_table_name:{input_table_name}, target_table_list:{target_table_list[index]}"
        )
        # actual DF_GROUP, DF_GROUP_COUNT, DF_GROUP_HISTORY
        target_entity = target_table_list[index]
        bronze_table_name = input_table_name
        if table_suffix is not None:
            bronze_table_name = f"{bronze_table_name}_{table_suffix}"

        # Step 1: Handle Bronze
        # columns_to_fetch = get_columns_for_table(input_table_name)

        df = generate_bronzetables(
            spark,
            volume_path,
            bronze_table_name,
            source,
            source_schema,
            input_table_name,
            business_date,
            processing_time,
        )

        # Step 3: Handle Failure
        source_table_name = bronze_table_name        
        # Step 1: FAIL
        # From this step, make sure only records that have changed are sent downwards for validation and transformation
        fail_target_table = f"{input_table_name}_dq_fail_ok_{table_suffix}"
        generate_dq_table(
            spark,
            input_table_name,
            source_table_name,
            fail_target_table,
            "FAIL",
            target_entity,
        )

        # Step 2: QUARANTINE
        quarantine_source_table = fail_target_table
        quarantine_target_table = f"{input_table_name}_dq_quarantine_ok_{table_suffix}"
        generate_dq_table(
            spark,
            input_table_name,
            quarantine_source_table,
            quarantine_target_table,
            "QUARANTINE",
            target_entity,
        )

        # Step 3: WARN
        warn_source_table = quarantine_target_table
        warn_target_table = f"{input_table_name}_dq_warn_ok_{table_suffix}"
        df = generate_dq_table(
            spark,
            input_table_name,
            warn_source_table,
            warn_target_table,
            "WARN",
            target_entity,
        )

        # Apply simple transformation rules.
        transform_source_table = warn_target_table
        transform_target_table = f"{input_table_name}_dt_{table_suffix}"
        generate_tranformation_rules_df(
            spark, input_table_name, transform_source_table, transform_target_table
        )

        # # Step 5: Handle CDC
        cdc_row = get_cdc_attributes(spark, catalog, schema, source, input_table_name) 
        print(f"cdc_row::{cdc_row}")       
        # For History Load , no CDC 
        if cdc_row is not None:
        #     final_source_table = transform_source_table
        # else:        
          cdc_source_table = transform_target_table
          cdc_target_table = None          
          key_attr = cdc_row.key_attr
          scd_type = cdc_row.scd_type
          compute_cdc = cdc_row.compute_cdc
          # cdc_source_table = f"{bronze_table_name}"
          exclude_columns_from_cdc = cdc_row.exclude_columns

          cdc_target_table = f"{input_table_name}_cdc"
          if table_suffix is not None:
              cdc_target_table = f"{cdc_target_table}_{table_suffix}"
          df = generate_scd_tables(
              cdc_source_table,
              cdc_target_table,
              key_attr,
              "processing_time",
              scd_type,
              compute_cdc,
              exclude_columns_from_cdc,
          )

        # Step 4: Final DLT table generation
        final_source_table = transform_target_table if cdc_row is None else cdc_target_table

        final_target_table = f"{target_table_list[index]}_{table_suffix}"
        generate_df_tables(
            spark,
            target_entity,
            source_table=final_source_table,
            target_table=final_target_table,
            business_date=business_date,
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution starts here
# MAGIC This section initiates the execution of the defined DLT flow.   
# MAGIC - Reads the data from dim_group_runtime_metadata, which is set from the ControlM job (run_entity_job for POC)
# MAGIC - Runs the DLT pipeline
# MAGIC

# COMMAND ----------

latest_runtime_parameters = get_runtime_parameters(catalog, schema, "dim_group_runtime_metadata")
print(f"\n🔍 latest_runtime_parameters: {latest_runtime_parameters}")

# TODO: Setup some defaults for testing the validation step
if latest_runtime_parameters == []:
    # get defaults
    dimension_table = "D_GROUP"
    # spark.conf.get("default_dimension_table") 
    target_name = "", 
    business_date = "", 
    scenario = ""
    run_dlt(dimension_table, target_name, business_date, scenario)
else:
    # Iterate over the result
    for row in latest_runtime_parameters:
        dimension_table = row["dimension_table"]
        sources_metadata = row["sources_metadata"]  # this is a dict

        print(f"\n🔍 Processing Dimension Table: {dimension_table}")

        for target_name, metadata in sources_metadata.items():
            scenario = metadata["scenario"]
            business_date = metadata["business_date"]

            # Custom logic here
            print(f"➡️ Silver Target Tables: {target_name}, Business Date: {business_date}")
            # run_dlt(dimension_table, target_name, business_date)
            if scenario == "normal_run":
                run_dlt(dimension_table, target_name, business_date, scenario)

     