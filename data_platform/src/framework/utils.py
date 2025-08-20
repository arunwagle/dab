
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from framework.udf_utils import *

def get_runtime_parameters(p_spark, p_catalog, p_schema, p_dlt_runtime_config_table, p_dimension_table):
    """
    This function retrieves the latest run_date for each dimension_table from the specified metadata table.

    Parameters:
    p_spark (SparkSession): The Spark session object.
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_dlt_runtime_config_table (str): The runtime config table name.
    p_dimension_table (str): The name of the dimension table.

    Returns:
    list: A list of Row objects containing the latest run_date for each dimension_table.
    """
    # Load the metadata table
    print(f"get_runtime_parameters:: Load metadata for table: {p_catalog}.{p_schema}.{p_dlt_runtime_config_table}")
    metadata_df = p_spark.table(f"{p_catalog}.{p_schema}.{p_dlt_runtime_config_table}").filter(
        F.col("dimension_table") == p_dimension_table
    )

    # Define window spec to get the latest run_date per dimension_table
    window_spec = Window.partitionBy("dimension_table").orderBy(
        F.col("run_date").desc()
    )
    # Add row number and filter only the latest per group
    latest_metadata_df = (
        metadata_df.withColumn("row_num", F.row_number().over(window_spec))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    
    row = latest_metadata_df.first()
    print(f"\n get_runtime_parameters:: latest_runtime_parameters: {row}")
    return row


def get_source_metadata(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_source_metadata_table,
    p_target_table,
    p_business_date,
    p_scenario,
    p_source_system,
    p_test_mode
):
    """
    This function retrieves metadata information from source metadata table for a given dimension table (E.g. D_GROUP) and target table (E.g. DF_GROUP) from the metadata source info table.
    It supports both incremental and one-time load types based on the business date.
    This function will replace the source_details column with the business date for incrementa run use cases.

    Parameters:
    p_spark (SparkSession): The Spark session object.
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_dimension_table (str): The name of the dimension table. E.g. D_GROUP
    p_target_table (str): The name of the target table. E.g. DF_GROUP
    p_business_date (str): The business date on which the job is run. If set to "N/A", a one-time load is performed.
    p_load_type (str): The load type, either "incremental" or "one_time".
    p_source_system (str): The source system name. (E.g. VHP, Facets)
    p_test_mode (str): The test mode.


    Returns:
    Row: A Row object containing the metadata information for the specified dimension table and target table.
    """

    # query = f"""
    #     SELECT * FROM {p_catalog}.{p_schema}.{p_source_metadata_table} WHERE lower(dimension_table) = lower('{p_dimension_table}') and lower(target_table) = lower("{p_target_table}") and is_active = 'Y' and load_type = '{p_load_type}'
    # """
    query = f"""
        SELECT distinct * FROM {p_catalog}.{p_schema}.{p_source_metadata_table} 
        WHERE lower(dimension_table) = lower('{p_dimension_table}') 
        and lower(target_table) = lower("{p_target_table}") 
        and lower(source_system) = lower('{p_source_system}')
        and lower(scenario) like '%{p_scenario}%'
        and is_active = 'Y'
    """
    # fetch specific source data
    # if p_source_system != "":
    #     query += f" and lower(source_system) = lower('{p_source_system}')"

    if p_test_mode != "": 
        query += f" and lower(test_mode) = lower('{p_test_mode}')"

    print(f"get_source_metadata:: \nquery: {query}")
    metadata_source_df = p_spark.sql(query)

    result = None
    # Substitutes the business start date in the source_details (stores volumes path) column when a business date is passed. This is used for incremental runs which are run per business date
    if p_scenario == 'incremental_run':
        fn_substitute_string_udf = substitute_string_udf(p_business_date)
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
    
def get_target_tables_for_run(
    p_spark,
    p_catalog,
    p_schema,
    p_source_metadata_table,
    p_dimension_table,
    p_sources_metadata,
    p_scenario
):
    """
    This function retrieves target tables for a given dimension table from the specified metadata table.

    Parameters:
    p_spark (SparkSession): The Spark session object.
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_dimension_table (str): The name of the dimension table.

    Returns:
    list: list of unique target tables for a dimension
    """
    query = None
    target_table_list = []
    source_system_list = []
    for key, metadata in p_sources_metadata.items():
        target_table = metadata["target_table"]
        business_date = metadata["business_date"]
        source_system = metadata["source_system"]
        # Set to default value for historical load
        if business_date is None or business_date == "" or business_date == "N/A":
            load_type = "one_time"

        target_table_list.append(target_table)
        source_system_list.append(source_system)

    # print(f"get_target_tables_for_run:: target_table_list: {target_table_list}")
    # print(f"get_target_tables_for_run:: source_system_list: {source_system_list}")

    target_table_in_clause = (
        "(" + ", ".join("'" + v.lower() + "'" for v in target_table_list) + ")"
    )
    source_system_in_clause = (
        "("
        + ", ".join("'" + v.lower() + "'" for v in list(set(source_system_list)))
        + ")"
    )

    query = f"""
        WITH exploded_tables AS (
            SELECT source_system, explode(split(derived_target_tables, ',')) AS target_table
            FROM {p_catalog}.{p_schema}.{p_source_metadata_table}
            WHERE lower(dimension_table) = lower('{p_dimension_table}') 
                AND is_active = 'Y'
                AND lower(target_table) IN {target_table_in_clause}
                AND lower(source_system) IN  {source_system_in_clause}
                and lower(scenario) like '%{p_scenario}%'
            )
            SELECT source_system, trim(target_table) AS target_table
            FROM exploded_tables;
    """

    print(f"get_target_tables_for_run:: \nquery: {query}")
    metadata_source_df = p_spark.sql(query)

    rows = metadata_source_df.groupBy("target_table").agg(F.collect_set("source_system").alias("source_system")).collect()   
    unique_target_tables = {row["target_table"].strip(): row["source_system"] for row in rows}

    print(
        f"get_target_tables_for_run:: \n unique_target_tables: {unique_target_tables}"
    )

    return unique_target_tables


def get_scd_attributes(p_spark, p_catalog, p_schema, p_dlt_metadata_cdc_config, p_target_table):
    """
    This function retrieves Change Data Capture (CDC) metadata attributes from dlt_metadata_cdc_config for a given source and input table from the metadata table.

    Parameters:
    p_spark (SparkSession): The Spark session object.
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_source (str): The source name.E.g. DF_GROUP_FACETS DF_VHP_FACETS
    p_target_table (str): The input table name.E.g. DF_Group, DF_Group_History, DF_Group_Count

    Returns:
    Row: A Row object containing the CDC attributes (key_attr, compute_cdc, scd_type, exclude_columns) for the specified source and input table.
    key_attr = Primary key to uniquely identify the row. E.g. GRGR_CK
    compute_cdc = 'Y' or 'N'. If 'Y', then CDC is computed using apply_changes_from_snapshot. If 'N' then the CDC is computed using apply_changes
    scd_type = '1' or '2'. If '1', then SCD Type 1 is used. If '2' then SCD Type 2 is used.
    exclude_columns = List of columns to exclude from CDC. E.g. ['GRGR_CK', 'GRGR_ID']
    """
    # Add exclude columns from CDC list
    query = f"""
        SELECT * FROM {p_catalog}.{p_schema}.{p_dlt_metadata_cdc_config} 
        WHERE lower(target_table) = lower('{p_target_table}');
    """
    print(f"get_scd_attributes::{query}")
    cdc_df = p_spark.sql(query)
    row = cdc_df.first()

    return row

def get_data_validation_rules(
    p_spark, p_catalog, p_schema, p_dimension_table, p_source_system, p_target_table, p_action, p_dlt_metadata_dq_config
):
    """
    Loads data quality rules from a dlt_metadata_dq_config table.

    Parameters:
    p_spark (SparkSession): Sparh Session
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_input_table (str): The input table name.
    p_target_table (str): The target table name.
    p_action (str): The action to match.
    p_dlt_metadata_dq_config (str): Data Quality metadata config table

    Returns:
    dict: A dictionary of rules that matched the action.

    Example:
    >>> rules = get_data_validation_rules("my_catalog", "my_schema", "input_table", "target_table", "warn")
    >>> for constraint, rule in rules.items():
    >>>     print(constraint, rule)
    """
    rules_list = (
        p_spark.read.format("delta")
        .table(f"{p_catalog}.{p_schema}.{p_dlt_metadata_dq_config}")
        .filter(
            (F.lower(F.col("dimension_table")) == p_dimension_table.lower())
            &  (F.lower(F.col("source_system")) == p_source_system.lower())
            & (F.lower(F.col("target_table")) == p_target_table.lower())
            & (F.col("is_active") == "Y")
            & (F.col("action") == p_action)
        )
        .select(F.col("action"), F.col("constraint"))
    ).collect()
    return {row["constraint"]: row["constraint"] for row in rules_list}




def get_data_transformation_rules(p_spark, p_catalog, p_schema, p_dimension_table, p_source_system, p_target_table, p_dlt_metadata_transform_config):
    """
    Loads data transformation rules from a t_dlt_transform_rules table.
    These rules are applied dynamically using the expectations framework using UDF's

    Parameters:
    p_catalog (str): The catalog name.
    p_schema (str): The schema name.
    p_input_table (str): The input table name.
    p_target_table (str): The target table name.

    Returns:
    list: A list of rows containing the source column name, type, and transformation function name for active transformation rules.

    Example:
    >>> rules = get_data_transformation_rules("my_catalog", "my_schema", "input_table", "target_table")
    >>> for rule in rules:
    >>>     print(rule["source_column_name"], rule["transform_type"], rule["transform_fn_name"])
    """
    transformation_rules = (
        p_spark.read.format("delta")
        .table(f"{p_catalog}.{p_schema}.{p_dlt_metadata_transform_config}")
        .filter(
            (F.lower(F.col("dimension_table")) == p_dimension_table.lower())
            &  (F.lower(F.col("source_system")) == p_source_system.lower())
            & (F.lower(F.col("target_table")) == p_target_table.lower())
            & (F.col("is_active") == "Y")
        )
        .select(F.col("source_column_name"), F.col("transform_type"), F.col("transform_fn_name"))
    ).collect()
    return transformation_rules



def get_table_name(input_string, table_name, step_name=""):
    input_string = input_string.replace("[TABLE_NAME]", table_name)
    if "[STEP_NAME]" in input_string:
        input_string = input_string.replace("[STEP_NAME]", f"_{step_name}")

    return input_string


def get_bronze_table_columns(p_bronze_table_to_column_map, p_source, p_source_schema, p_table, p_columns_to_fetch):
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
        return p_bronze_table_to_column_map[p_source][p_source_schema][p_table][
            p_columns_to_fetch
        ]
    except KeyError:
        raise KeyError(
            f"No mapping found for {p_source} {p_source_schema} {p_table} {p_columns_to_fetch}"
        )


def compute_deletes_from_snapshots(p_spark, p_scenario, p_catalog_name, p_schema_name, p_landing_table, p_source_df, p_scd_row):
    print("Calling compute_deletes")

    compute_cdc = p_scd_row.compute_cdc
    print(f"compute_cdc: {compute_cdc}, p_scenario: {p_scenario}")
    source_table_keys = []
    if p_scenario == "historical_run" or compute_cdc == "N" :
        return p_source_df
    else:
        source_table_keys = ["GRGR_CK", "GRGC_MCTR_CTYP", "GRGC_EFF_DT"]

    # Fully qualified delta table name
    landing_table_name = f"{p_catalog_name}.{p_schema_name}.{p_landing_table}"
    print(f"landing_table_name: {landing_table_name}")
    
    # Check if the delta table exists, will be null for the first time.
    table_exists = p_spark.catalog.tableExists(landing_table_name)
    delta_record_count = 0
    if table_exists:
        # check the count
        delta_record_count = p_spark.read.table(landing_table_name).count()

        print(f"Table {landing_table_name} exist: {table_exists}.")

    result_df = None
    landing_table_df = None
    if table_exists and delta_record_count > 0:
        # Delta table dataframe
        landing_table_df = p_spark.read.table(landing_table_name)

    # Get the column order of source DataFrame. this ensures consistency when applying the delta detection logic
    column_order = p_source_df.columns

    # CODE FOR HANDLING DELETE
    # Deletions: Keys in delta table but not in source table

    delete_df = (
        landing_table_df.join(p_source_df, on=source_table_keys, how="left_anti").withColumn(
            "__$operation", F.lit("1")
        )
    ).select(*column_order)

    # delete_df.printSchema()

    # Combine all changes
    result_df = p_source_df.union(delete_df)

    # display(result_df)

    return result_df