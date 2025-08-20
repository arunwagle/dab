import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

from framework.utils import *


def generate_bronzetables(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_input_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_source,
    p_source_schema,
    p_business_date,
    p_business_date_format,
    p_processing_time,
    p_global_cols,
    p_required_cols,
    p_additional_cols
):
    """
    Generates bronze tables using Delta Live Tables.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup.
    - p_schema (str): Schema name for metadata lookup.
    - p_dimension_table (str): Dimension table name for metadata lookup.
    - p_input_table (str): Base table name for rules lookup.
    - p_dlt_source_table (str): Source DLT table name.
    - p_dlt_target_table (str): Target DLT table name.
    - p_source (str): The source system name.
    - p_source_schema (str): The source schema name.
    - p_business_date (str): The business date.
    - p_business_date_format (str): The format of the business date.
    - p_processing_time (str): The processing time.

    Returns:
    - None: This function does not return any value. It creates or updates the bronze table.
    """

    print(
        f"Start:: generate_bronzetables::\n"
        f"p_catalog::{p_catalog}\n"
        f"p_schema::{p_schema}\n"
        f"p_dimension_table::{p_dimension_table}\n"
        f"p_input_table::{p_input_table}\n"
        f"p_dlt_source_table::{p_dlt_source_table}\n"
        f"p_dlt_target_table::{p_dlt_target_table}\n"
        f"p_source::{p_source}\n"
        f"p_source_schema::{p_source_schema}\n"
        f"p_business_date::{p_business_date}\n"
        f"p_business_date_format::{p_business_date_format}\n"
        f"p_processing_time::{p_processing_time}\n"
    )


    # create streaming table
    dlt.create_streaming_table(p_dlt_target_table)

    # add a flow
    @dlt.append_flow(name=f"flow_{p_dlt_target_table}", target=p_dlt_target_table)
    def create_bronze_table():
        try:
            loadtimestamp_format="yyyyMMddHHmm"            
            df = p_spark.readStream.format("delta").option("ignoreDeletes", "true").option("skipChangeCommits", "true").table(
                f"{p_catalog}.{p_schema}.{p_dlt_source_table}"
            )

            df = df.select(*p_required_cols)
            df = df.withColumn(
                "loadtimestamp",
                F.to_timestamp(
                    F.col("loadtimestamp").cast("string"), loadtimestamp_format
                ),
            )
            df = df.withColumn(
                "business_date",
                F.when(
                    F.length(F.col("business_date").cast("string")) == 8,
                    F.to_date(F.col("business_date").cast("string"), "yyyyMMdd"),
                ).otherwise(F.to_date(F.col("business_date"), "yyyy-MM-dd")),
            )
            

            # Add global columns to all the brnze tables
            for col_name, col_type in p_global_cols.items():
                df = df.withColumn(col_name, F.lit(p_processing_time).cast(col_type))

            # Add additional columns
            for col_name, col_type in p_additional_cols.items():
                df = df.withColumn(col_name, F.col(col_name).cast(col_type))

            # replace " " with _ in column names
            df_clean = df.select(
                [F.col(c).alias(c.replace(" ", "_")) for c in df.columns]
            )
            
            return df_clean
        except pyspark.errors.exceptions.captured.AnalysisException as e:
            print(f"generate_bronzetables:: AnalysisException ::{e}")
            empty_df = p_spark.createDataFrame([], schema=None)
            return empty_df
        

def generate_scd_tables(
    # p_spark,
    # p_catalog,
    # p_schema,
    # p_dimension_table,
    # p_input_table,
    p_target_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_dlt_stream_silver_target_table,
    p_keys,
    p_seq_col,
    p_scd_type,
    # p_compute_cdc="N",
    p_exclude_column_list=[],
):
    """
    Generates Slowly Changing Dimension (SCD) tables using Delta Live Tables.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup
    - p_dimension_table (str): Dimension table name for metadata lookup
    - p_input_table (str): Base table name for rules lookup
    - p_target_table (str): Target DLT table to create
    - p_dlt_source_table (str): Source DLT table name
    - p_dlt_target_table (str): Target DLT table name
    - p_keys (str): Comma-separated list of primary key columns
    - p_seq_col (str): Column indicating order of changes
    - p_scd_type (int): SCD type (1 or 2)
    - p_compute_cdc (str): Flag to compute CDC ("Y" or "N")
    - p_exclude_columns_from_cdc (list): List of columns to exclude from CDC

    Returns:
    - None: This function does not return any value. It creates or updates the SCD table.
    """
    key_list = [item.strip() for item in p_keys.split(",")]
    # except_column_list = [item.strip() for item in p_exclude_column_list.split(",")]
    dlt_scd_target_table = f"{p_dlt_target_table}"


    print(
        f"""########Step 1: generate_scd_tables::dlt_scd_target_table::{dlt_scd_target_table}:: 
            p_dlt_source_table::{p_dlt_source_table}:: 
            key_list{key_list}::
            p_exclude_column_list::{p_exclude_column_list}"""
    )

    dlt.create_streaming_table(dlt_scd_target_table)

    dlt.create_auto_cdc_flow(
        target=dlt_scd_target_table,
        source=p_dlt_source_table,
        keys=key_list,
        # sequence_by=F.col(p_seq_col),
        sequence_by=F.struct(p_seq_col, "processing_time"),
        stored_as_scd_type=p_scd_type,
        # apply_as_deletes=True,
        # ignore_null_updates=True,
        except_column_list =p_exclude_column_list,
    )

    # input for next step
    dlt_source_table = dlt_scd_target_table
    dlt_scd_target_table = get_table_name(
        f"{p_dlt_stream_silver_target_table}",
        p_target_table.lower(),
    )

    # print(
    #     f"########Step 2: generate_df_tables::dlt_source_table::{dlt_source_table}:: dlt_scd_target_table::{dlt_scd_target_table}"
    # )


    @dlt.table(comment="Handle SCD Condition", name=dlt_scd_target_table)
    def generate_df_tables():
        df = dlt.read(dlt_source_table)
        if p_scd_type == "2":
            df = (
                df.withColumnRenamed("__START_AT", "Row_Effective_Date")
                .withColumn(
                    "Row_Effective_Date",
                    F.to_date(F.col("Row_Effective_Date.business_date"))
                )
                # .withColumn(
                #     "Row_Effective_Date", F.to_date(F.col("Row_Effective_Date"))
                # )
                # .withColumnRenamed("__END_AT", "Row_Term_Date").fillna({"Row_Term_Date": F.to_date(F.lit("9999-12-31"))})
                .withColumnRenamed("__END_AT", "Row_Term_Date")
                .withColumn(
                    "Row_Term_Date",
                    F.to_date(F.col("Row_Term_Date.business_date"))
                )
                .fillna({"Row_Term_Date": "9999-12-31"})
                .withColumn("Row_Term_Date", F.to_date(F.col("Row_Term_Date")))
                .withColumn(
                    "Row_Term_Date",
                    F.when(
                        F.col("Row_Is_Deleted_Ind") == "Y", F.col("business_date")
                    ).otherwise(F.col("Row_Term_Date")),
                )
            )
            window_spec = Window.partitionBy(*key_list,F.col("business_date")).orderBy( F.col("processing_time").desc())
            df = df.withColumn("rank", F.rank().over(window_spec))
            df = df.withColumn("Row_Is_Active_Ind",F.when(F.col("rank") == 1,"Y").otherwise("N"))
            df = df.drop("rank")
        return df
    
def generate_dq_table(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_source_system,
    p_target_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_dlt_audit_table,
    p_dq_type,
    p_dlt_metadata_dq_config
):
    """
    Generalized Data Quality (DQ) table generator supporting FAIL, WARN, and QUARANTINE.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup
    - p_dimension_table (str): Dimension table name for metadata lookup
    - p_target_table (str): Target DLT table to create
    - p_dlt_source_table (str): Source DLT table name
    - p_dlt_target_table (str): Target DLT table name
    - p_dlt_audit_table (str): Audit DLT table name
    - p_dq_type (str): One of 'FAIL', 'WARN', or 'QUARANTINE'
    """
    dq_type = p_dq_type.upper()
    print(
        f"generate_dq_table:: \n"
        f"p_catalog:{p_catalog}\n"
        f"p_schema:{p_schema}\n"
        f"p_dimension_table:{p_dimension_table}\n"
        f"p_target_table:{p_target_table}\n"
        f"p_dlt_source_table:{p_dlt_source_table}\n"
        f"p_dlt_target_table:{p_dlt_target_table}\n"
        f"p_dlt_audit_table:{p_dlt_audit_table}\n"
        f"p_dq_type:{p_dq_type}"
    )

    # Get validation rules from your metadata source
    rules = get_data_validation_rules(
        p_spark, p_catalog, p_schema, p_dimension_table, p_source_system, p_target_table, p_dq_type, p_dlt_metadata_dq_config
    )

    rules = {key: f"NOT ({value})" for key, value in rules.items()}
    # rules = {key: f" ({value})" for key, value in rules.items()}
    expr_rules = f"({' AND '.join(rules.values())})" if rules else "()"
    print(f"generate_dq_table:: \nrules::{rules} \n expr_rules::{expr_rules}")

    if p_dq_type == "FAIL":

        @dlt.table(comment="Handle failure condition", name=p_dlt_target_table)
        @dlt.expect_all_or_fail(rules)
        def dlt_dq_fail():
            # Filter the data to only do validations on data that has changed since the last run
            df = dlt.readStream(p_dlt_source_table)
            # .filter(
            #     F.col("processing_time") >= processing_time
            # )
            return df

    elif p_dq_type == "QUARANTINE":

        @dlt.table(
            comment=f"Process data for {p_dq_type.lower()} rules",
            name=f"{p_dlt_target_table}_temp",
            temporary=True,
            partition_cols=["is_valid"],
        )
        @dlt.expect_all(rules)
        def dlt_dq_quarantine_temp():
            df = dlt.readStream(p_dlt_source_table)
            # Add is_valid column
            df = df.withColumn(
                "is_valid", F.lit(True) if expr_rules == "()" else F.expr(expr_rules)
            )

            # Add action_details column
            empty_map = F.create_map(F.lit(""), F.lit("")).cast(
                T.MapType(StringType(), T.BooleanType())
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

            # if dq_type == "QUARANTINE":
            #     df = df.filter("is_valid=true")
            return df
        # Handle audit records
        generate_dq_audit_tables(
            p_spark=p_spark,
            p_catalog=p_catalog,
            p_schema=p_schema,
            p_dimension_table=p_dimension_table,
            p_target_table=p_target_table,
            p_dlt_source_table=f"{p_dlt_target_table}_temp",
            p_dlt_target_table=p_dlt_audit_table,
            p_dq_type=p_dq_type,
        )

        @dlt.table(
            comment=f"Create the quarantine dq table for {p_dq_type.lower()} ",
            name=f"{p_dlt_target_table}",
        )
        def dlt_dq_non_quarantine():
            df = dlt.readStream(f"{p_dlt_target_table}_temp").filter("is_valid=true")
            return df

    else:

        @dlt.table(
            comment=f"Process data for {p_dq_type.lower()} rules",
            name=f"{p_dlt_target_table}_temp",
            temporary=True,
            partition_cols=["is_valid"],
        )
        @dlt.expect_all(rules)
        def dlt_dq_warn_temp():
            df = dlt.readStream(p_dlt_source_table)
            # Add is_valid column
            df = df.withColumn(
                "is_valid", F.lit(True) if expr_rules == "()" else F.expr(expr_rules)
            )

            # Add action_details column
            empty_map = F.create_map(F.lit(""), F.lit("")).cast(
                T.MapType(StringType(), T.BooleanType())
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

            # if dq_type == "QUARANTINE":
            #     df = df.filter("is_valid=true")
            return df
        
        # Handle audit records
        generate_dq_audit_tables(
            p_spark=p_spark,
            p_catalog=p_catalog,
            p_schema=p_schema,
            p_dimension_table=p_dimension_table,
            p_target_table=p_target_table,
            p_dlt_source_table=f"{p_dlt_target_table}_temp",
            p_dlt_target_table=p_dlt_audit_table,
            p_dq_type=p_dq_type,
        )

        @dlt.table(
            comment=f"Create the warn dq table for {p_dq_type.lower()} ",
            name=f"{p_dlt_target_table}",
        )
        def dlt_dq_warn():
            df = dlt.readStream(
                f"{p_dlt_target_table}_temp"
            )  # .filter("is_valid=true")
            return df
        
def generate_tranformation_rules_df(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_source_system,
    p_target_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_dlt_metadata_transform_config
):
    """
    Generates a DataFrame by applying transformation rules based on metadata.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup
    - p_dimension_table (str): Dimension table name for metadata lookup
    - p_input_table (str): Base table name for rules lookup
    - p_target_table (str): Target DLT table to create
    - p_dlt_source_table (str): Source DLT table name
    - p_dlt_target_table (str): Target DLT table name

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """

    df_transformation_rules = get_data_transformation_rules(
        p_spark, p_catalog, p_schema, p_dimension_table, p_source_system, p_target_table, p_dlt_metadata_transform_config
    )

    print(
        f"###########generate_tranformation_rules_df::df_transformation_rules::{df_transformation_rules}"
    )

    @dlt.table(
        comment="apply transformation rules for required columns",
        name=f"{p_dlt_target_table}",
    )
    def apply_transformation_rules():
        df = dlt.readStream(f"{p_dlt_source_table}")
        df_dt = df
        # Apply each UDF based on metadata
        if df_transformation_rules:
            # Apply the transformation rules to the DataFrame
            for row in df_transformation_rules:
                col_name = row["source_column_name"]
                transformation_type = row["transform_type"]
                transformation_fn = row["transform_fn_name"]
                # Apply default transformation if specified
                if transformation_type == "default" and transformation_fn:
                    df_dt = df_dt.withColumn(col_name, F.lit(transformation_fn))
                # Apply custom UDF transformation if specified
                elif transformation_type == "custom" and transformation_fn:
                    udf_function = udf_mapping[transformation_fn]
                    df_dt = df_dt.withColumn(col_name, udf_function(col(col_name)))
                # Apply SQL expression transformation if specified
                elif transformation_fn:
                    df_dt = df_dt.withColumn(
                        col_name, F.expr(f"{transformation_fn}({col_name})")
                    )

        # Return the transformed DataFrame
        return df_dt
    
def generate_df_append_tables(
    p_spark,
    p_catalog,
    p_schema,    
    p_dlt_source_table,
    p_dlt_target_table
):
    """
    Generates the final DataFrame table by reading from the source and applying transformations.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup    
    - p_dlt_source_table (str): Source DLT table name
    - p_dlt_target_table (str): Target DLT table name

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """
    
    print(
        f"generate_df_append_tables::\n"
        f"p_catalog::{p_catalog}, \n"
        f"p_schema::{p_schema}, \n"        
        f"p_dlt_source_table::{p_dlt_source_table}, \n"
        f"p_dlt_target_table::{p_dlt_target_table}\n"
    )

    # create streaming table
    # dlt.create_streaming_table(p_dlt_target_table)

    # add a flow
    @dlt.append_flow(name=f"flow_{p_dlt_source_table}", target=p_dlt_target_table)
    def create_table():
        print(f"generate_df_append_tables::p_dlt_source_table::{p_dlt_source_table}")
        df = p_spark.readStream.table(p_dlt_source_table)
        return df

    
def generate_mapped_tables_by_source_system(
    p_spark,
    p_catalog,
    p_schema,
    p_source,
    p_dimension_table,
    p_input_table,
    p_target_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_business_date
):
    """
    Generates the final DataFrame table by reading from the source and applying transformations.

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup
    - p_source (str): Source name. E.g. DF_GROUP_FACETS, DF_GROUP_VHP
    - p_dimension_table (str): Dimension table name for metadata lookup
    - p_input_table (str): Base table name for rules lookup
    - p_target_table (str): Target DLT table to create
    - p_dlt_source_table (str): Source DLT table name
    - p_dlt_target_table (str): Target DLT table name
    - p_business_date (str): Business date for the data

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """
    
    print(
        f"generate_df_tables_by_source_system::\n"
        f"p_catalog::{p_catalog}, \n"
        f"p_schema::{p_schema}, \n"
        f"p_source::{p_source},\n"
        f"p_dimension_table::{p_dimension_table}, \n"
        f"p_input_table::{p_input_table}, \n"
        f"p_target_table::{p_target_table}, \n"
        f"p_dlt_source_table::{p_dlt_source_table}, \n"
        f"p_dlt_target_table::{p_dlt_target_table}\n"
        f"p_business_date::{p_business_date}"
    )

    import importlib
    module_name = f"framework.{p_dimension_table.lower()}.{p_dimension_table.lower()}_silver"
    module = importlib.import_module(module_name)

    @dlt.table(
        name=f"{p_dlt_target_table}"
    )
    def create_final_table():
        df = dlt.readStream(p_dlt_source_table)
        df_transformed = module.source_to_target_mapping(p_source, df, p_target_table)
        return df_transformed
    
def generate_df_tables(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_dlt_target_table,
    p_dlt_source_table_list,
):
    """
    Generates the final DF silver tables .

    Parameters:
    - p_spark: SparkSession
    - p_catalog (str): Catalog name for metadata lookup
    - p_schema (str): Schema name for metadata lookup
    - p_dimension_table (str): Dimension table name for metadata lookup
    - p_dlt_target_table (str): Target DLT table to create
    - p_dlt_source_table_list (list) list of source tables

    Returns:
    - None: This function does not return any value. It creates or updates the target table.
    """

    print(
        f"generate_df_tables::\n"
        f"p_catalog::{p_catalog}, \n"
        f"p_schema::{p_schema}, \n"
        f"p_dimension_table::{p_dimension_table}, \n"
        f"p_dlt_target_table::{p_dlt_target_table}, \n"
        f"p_dlt_source_table_list::{p_dlt_source_table_list}"
    )

    # @dlt.table(name=f"{p_dlt_target_table}")
    def create_final_table():
        print(f"######dlt_target_table::{p_dlt_target_table}")

        dfs = [
            dlt.readStream(source_system)                
            for source_system in p_dlt_source_table_list
        ]        
        df_union = dfs[0]
        for df in dfs[1:]:
            df_union = df_union.unionByName(df, allowMissingColumns=True)
        return df_union
    

def generate_dq_audit_tables(
    p_spark,
    p_catalog,
    p_schema,
    p_dimension_table,
    p_target_table,
    p_dlt_source_table,
    p_dlt_target_table,
    p_dq_type,
):
    """
    Generates Data Quality (DQ) audit tables by appending flows from source to target.

    Parameters:
    - p_spark: SparkSession
    - p_catalog: Catalog name
    - p_schema: Schema name
    - p_dimension_table: Dimension table name
    - p_target_table: Target table name
    - p_dlt_source_table: DLT source table name
    - p_dlt_target_table: DLT target table name
    - p_dq_type: Data Quality type (e.g., FAIL, WARN)
    - p_business_date: Business date for the data
    - p_processing_time: Processing time for the data
    Returns:
    - None: This function does not return any value. It creates or updates the audit tables.
    """

    # flow_name = f"{p_target_table}_{p_dq_type.lower()}_flow"
    flow_name = f"{p_dlt_source_table}_{p_dq_type.lower()}_flow"
    delta_sink_target = f"{p_catalog}.{p_schema}.{p_dlt_target_table}"

    print(
        f"generate_dq_audit_tables:: \n"
        f"p_catalog::{p_catalog}, \n"
        f"p_schema::{p_schema}, \n"
        f"p_dimension_table::{p_dimension_table}, \n"
        f"p_target_table::{p_target_table}, \n"
        f"p_dlt_source_table::{p_dlt_source_table}, \n"
        f"p_dlt_target_table::{p_dlt_target_table}, \n"
        f"p_dq_type::{p_dq_type}, \n"
        f"flow_name::{flow_name}"
    )

    dlt.create_sink(
        name="audit_delta_sink",
        format="delta",
        options={"tableName": delta_sink_target},
    )

    @dlt.append_flow(name=flow_name, target="audit_delta_sink")
    def delta_sink_flow():
        df = (
            p_spark.readStream.table(p_dlt_source_table)
            .filter("is_valid=false")
            .withColumn("validated_table", F.lit(f"{p_dlt_source_table}"))
            .withColumn(
                "reason",
                F.map_filter("action_details", lambda k, v: v != F.lit("true")),
            )
            .withColumn("dimension_table", F.lit(p_dimension_table))
            .withColumn("target_table", F.lit(p_target_table))
            .withColumn("dq_type", F.lit(p_dq_type))
            .withColumn("insert_date", F.current_date())
        )

        df = df.withColumn(
            "input_details", F.to_json(F.struct([col for col in df.columns]))
        )

        df = df.select(
            "dimension_table",
            "target_table",
            "validated_table",
            "source",
            "source_schema",
            "input_details",
            "reason",
            "dq_type",
            "business_date",
            "processing_time",
        )

        return df
    