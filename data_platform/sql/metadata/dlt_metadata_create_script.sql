-- ============================================================================
-- DLT METADATA TABLES
-- ============================================================================
-- This script creates  all four DLT metadata configuration tables
-- Used to configure and manage Delta Live Tables pipeline processes
-- 
-- Tables included:
-- 1. dlt_metadata_cdc_config - Change Data Capture configuration
-- 2. dlt_metadata_dq_config - Data Quality configuration  
-- 3. dlt_metadata_source_config - Source configuration
-- 4. dlt_metadata_transform_config - Transform configuration
-- ============================================================================

-- ============================================================================
-- CREATE TABLE STATEMENTS
-- ============================================================================

USE CATALOG edl_dev_ctlg;
USE SCHEMA structured;

ALTER TABLE dlt_meta_cdc_config DROP CONSTRAINT IF EXISTS fk_dlt_meta_scd_config_dimension_table;
ALTER TABLE dlt_meta_dq_config DROP CONSTRAINT IF EXISTS fk_dlt_meta_dq_config_dimension_table;
ALTER TABLE dlt_meta_transform_config DROP CONSTRAINT IF EXISTS fk_dlt_meta_transform_config_dimension_table;
ALTER TABLE dlt_meta_source_config DROP CONSTRAINT IF EXISTS pk_dlt_meta_source_config_dimension_table;

CREATE OR REPLACE TABLE  dlt_meta_source_config (
  `dimension_table` STRING COMMENT 'Final business dimension table that receives data from the target.',
  `source_system` STRING COMMENT 'System or product name where the source originates (e.g., Facets, VHP).',
  `source_schema` STRING COMMENT 'Name of the schema the source follows.',
  `source_type` STRING COMMENT 'Type of source system (e.g., Databricks Volumes, database, file, API).',
  `sourced_from` STRING COMMENT 'Ingestion mechanism (e.g., SQL SErver, Flat File).',
  `source_details` STRING COMMENT 'Connection or metadata details for the source.',
  `derived_input_tables` STRING COMMENT 'Intermediate table generated directly from the source.There can be more than one bronze tables created per derived input table (e.g. df_group facets source generates df_group and df_group_history )',
  `derived_target_tables` STRING COMMENT 'Target tables to create from source. There can be more than one output tables created per derived input table (e.g. df_group facets source generates df_group and df_group_history )',  
  `target_table` STRING COMMENT 'Target table after transformation from the input.',  
  `scenario` STRING COMMENT 'Optional label to identify different variations or environments (e.g., prod, dev, experiment).',
  `frequency` STRING COMMENT 'Expected ingestion frequency (e.g., daily, hourly, weekly).',
  `file_format` STRING COMMENT 'File or data format (e.g., parquet, csv, json).',
  `business_date_column_to_load` STRING COMMENT 'Column name that represents the business date from source data to map to landing table business date output column. This is used in historical load if we want to load and map a particular business data from audit tables to landing table business date column.',
  `test_mode` STRING COMMENT 'Mode of ingestion (e.g., prod, test).',
  `is_active` String COMMENT 'Flag indicating if the source is currently active.',
  `deactivation_date` TIMESTAMP COMMENT 'Timestamp when the source was deactivated, if applicable.',
  `create_date` TIMESTAMP DEFAULT current_timestamp()  COMMENT 'Record creation timestamp.',
  `update_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record last update timestamp.',
  CONSTRAINT pk_dlt_meta_source_config_dimension_table PRIMARY KEY (dimension_table)
)
USING DELTA
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Configuration metadata for each data source used in the pipeline.';


CREATE OR REPLACE TABLE dlt_meta_cdc_config (
  `dimension_table` STRING COMMENT 'Final business dimension table that receives data from the target.',
  `target_table` STRING COMMENT 'Target Table for which the SCD rules are configured. (e.g. df_group, df_group_count, df_group_history)',
  `key_attr` STRING COMMENT 'Primary key or business key column(s) used to compare records.',
  `sequence_col` STRING COMMENT 'Column used to identify the order of changes (e.g., business_date, processing_time).',
  `exclude_columns` ARRAY<STRING> COMMENT 'List of columns to ignore when comparing for CDC.',
  `compute_cdc` STRING default 'Y' COMMENT 'Flag or strategy for CDC computation (e.g., true/false, strategy name).',
  `scd_type` STRING COMMENT 'SCD type used for tracking history (e.g., type1, type2, snapshot).',
  `create_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp.',
  `update_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record last update timestamp.',
  CONSTRAINT fk_dlt_meta_scd_config_dimension_table FOREIGN KEY (dimension_table) REFERENCES dlt_meta_source_config (dimension_table)
)
USING DELTA
COMMENT 'Configuration table that defines how CDC (Change Data Capture) is applied for each source, dimension and target table.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE TABLE dlt_meta_dq_config (
  `dimension_table` STRING COMMENT 'Final business dimension table that receives data from the target.',
  `source_system` STRING COMMENT 'System or product name where the source originates (e.g., Facets, VHP).',
  `target_table` STRING COMMENT 'Target Table for which the validations rules are configured. (e.g. df_group, df_group_count, df_group_history)',
  `source_column_name` STRING COMMENT 'Column on which the data quality check is applied.',
  `action` STRING COMMENT 'Action to be taken when the rule fails (e.g., drop, quarantine, log, fix).',
  `constraint` STRING COMMENT 'Constraint expression or rule logic (e.g., NOT NULL, value range).',
  `is_active` String COMMENT 'Whether this rule is currently active.',
  `deactivation_date` TIMESTAMP COMMENT 'Timestamp when this rule was deactivated, if applicable.',
  `mocked_ind` BOOLEAN COMMENT 'Flag indicating if the check is mocked or bypassed (e.g., in dev/test environments).',
  `create_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record creation timestamp.',
  `update_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Record last update timestamp.',
  CONSTRAINT fk_dlt_meta_dq_config_dimension_table FOREIGN KEY (dimension_table) REFERENCES dlt_meta_source_config (dimension_table)
)
USING DELTA
COMMENT 'Configuration for data quality rules to be applied on input data before loading into dimension tables.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');

CREATE OR REPLACE TABLE dlt_meta_transform_config (
  `dimension_table` STRING COMMENT 'Final dimension table that the transformation contributes to.',
  `source_system` STRING COMMENT 'System or product name where the source originates (e.g., Facets, VHP).',
  `target_table` STRING COMMENT 'Target table where the transformed data is written.',
  `source_column_name` STRING COMMENT 'Column in the input table to be transformed.',
  `transform_type` STRING COMMENT 'Type/category of transformation (e.g., standardize, mask, format).',
  `transform_fn_name` STRING COMMENT 'Name of the transformation function to be applied.',
  `is_active` String COMMENT 'Whether this transformation is currently active.',
  `deactivation_date` TIMESTAMP COMMENT 'Timestamp when the transformation was deactivated, if applicable.',
  `create_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Timestamp when the record was created.',
  `update_date` TIMESTAMP DEFAULT current_timestamp() COMMENT 'Timestamp when the record was last updated.',
  CONSTRAINT fk_dlt_meta_transform_config_dimension_table FOREIGN KEY (dimension_table) REFERENCES dlt_meta_source_config (dimension_table)
)
USING DELTA
COMMENT 'Configuration of column-level transformations applied during data processing for each source and dimension table.'
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
