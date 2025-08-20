-- NORMALIZED METADATA SCHEMA DESIGN
-- Based on existing dlt_metadata_ tables with proper normalization
-- Created to eliminate redundancy and improve data integrity

-- ===========================================
-- MASTER REFERENCE TABLES
-- ===========================================

-- Central registry for all data sources
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_data_source (
    source_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_name STRING NOT NULL,
    source_system STRING,
    source_schema STRING,
    source_type STRING,
    source_details STRING,
    sourced_from STRING,
    file_format STRING,
    frequency STRING,
    business_date_column_name STRING,
    enable_change_data_feed BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM'
);

-- Central registry for all database tables/objects
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_data_table (
    table_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name STRING NOT NULL,
    table_type STRING NOT NULL, -- 'INPUT', 'TARGET', 'STAGING', 'DIMENSION'
    schema_name STRING,
    catalog_name STRING,
    full_table_name STRING GENERATED ALWAYS AS (
        CASE 
            WHEN catalog_name IS NOT NULL AND schema_name IS NOT NULL 
            THEN CONCAT(catalog_name, '.', schema_name, '.', table_name)
            WHEN schema_name IS NOT NULL 
            THEN CONCAT(schema_name, '.', table_name)
            ELSE table_name
        END
    ),
    table_description STRING,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM'
);

-- Configuration scenarios/environments
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_scenario (
    scenario_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario_name STRING NOT NULL UNIQUE,
    scenario_description STRING,
    test_mode BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM'
);

-- ===========================================
-- CONFIGURATION TABLES
-- ===========================================

-- Main configuration registry linking sources to targets
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_pipeline_config (
    pipeline_config_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id INT NOT NULL,
    input_table_id INT NOT NULL,
    target_table_id INT NOT NULL,
    dimension_table_id INT,
    scenario_id INT NOT NULL,
    pipeline_type STRING NOT NULL, -- 'CDC', 'DQ', 'TRANSFORM', 'SOURCE'
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM',
    
    CONSTRAINT fk_pipeline_source FOREIGN KEY (source_id) REFERENCES edl_dev_ctlg.structured.meta_data_source(source_id),
    CONSTRAINT fk_pipeline_input_table FOREIGN KEY (input_table_id) REFERENCES edl_dev_ctlg.structured.meta_data_table(table_id),
    CONSTRAINT fk_pipeline_target_table FOREIGN KEY (target_table_id) REFERENCES edl_dev_ctlg.structured.meta_data_table(table_id),
    CONSTRAINT fk_pipeline_dimension_table FOREIGN KEY (dimension_table_id) REFERENCES edl_dev_ctlg.structured.meta_data_table(table_id),
    CONSTRAINT fk_pipeline_scenario FOREIGN KEY (scenario_id) REFERENCES edl_dev_ctlg.structured.meta_scenario(scenario_id)
);

-- CDC (Change Data Capture) specific configuration
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_cdc_config (
    cdc_config_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_config_id INT NOT NULL,
    key_attributes STRING NOT NULL, -- Primary key columns
    except_column_list STRING, -- Columns to exclude from CDC
    exclude_columns ARRAY<STRING>, -- Array of excluded columns
    compute_cdc BOOLEAN DEFAULT TRUE,
    scd_type STRING DEFAULT 'SCD1', -- SCD1, SCD2, SCD3
    sequence_column STRING, -- Column for sequencing changes
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM',
    
    CONSTRAINT fk_cdc_pipeline_config FOREIGN KEY (pipeline_config_id) REFERENCES edl_dev_ctlg.structured.meta_pipeline_config(pipeline_config_id)
);

-- Data Quality configuration
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_dq_config (
    dq_config_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_config_id INT NOT NULL,
    source_column_name STRING NOT NULL,
    dq_rule_name STRING NOT NULL,
    dq_action STRING NOT NULL, -- 'QUARANTINE', 'FAIL', 'WARN', 'DROP'
    dq_constraint STRING NOT NULL, -- The actual constraint/rule
    dq_type STRING DEFAULT 'VALIDATION', -- 'VALIDATION', 'TRANSFORMATION', 'COMPLETENESS'
    severity_level STRING DEFAULT 'ERROR', -- 'ERROR', 'WARNING', 'INFO'
    is_mocked BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM',
    
    CONSTRAINT fk_dq_pipeline_config FOREIGN KEY (pipeline_config_id) REFERENCES edl_dev_ctlg.structured.meta_pipeline_config(pipeline_config_id)
);

-- Transformation configuration
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_transform_config (
    transform_config_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_config_id INT NOT NULL,
    source_column_name STRING NOT NULL,
    target_column_name STRING, -- If different from source
    transform_type STRING NOT NULL, -- 'MAPPING', 'CALCULATION', 'LOOKUP', 'AGGREGATION'
    transform_function_name STRING NOT NULL,
    transform_expression STRING, -- SQL expression or function call
    transform_order INT DEFAULT 1, -- Order of transformation execution
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    deactivated_date TIMESTAMP,
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM',
    
    CONSTRAINT fk_transform_pipeline_config FOREIGN KEY (pipeline_config_id) REFERENCES edl_dev_ctlg.structured.meta_pipeline_config(pipeline_config_id)
);

-- ===========================================
-- RUNTIME AND AUDIT TABLES
-- ===========================================

-- Runtime execution parameters
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_runtime_config (
    runtime_config_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    scenario_id INT NOT NULL,
    dimension_table_id INT,
    run_date TIMESTAMP NOT NULL,
    business_date DATE,
    source_parameters MAP<STRING, STRUCT<
        business_date: STRING,
        source_system: STRING,
        target_table: STRING,
        source_details: STRING,
        format: STRING
    >>,
    execution_status STRING DEFAULT 'PENDING', -- 'PENDING', 'RUNNING', 'SUCCESS', 'FAILED'
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    created_by STRING DEFAULT 'SYSTEM',
    updated_by STRING DEFAULT 'SYSTEM',
    
    CONSTRAINT fk_runtime_scenario FOREIGN KEY (scenario_id) REFERENCES edl_dev_ctlg.structured.meta_scenario(scenario_id),
    CONSTRAINT fk_runtime_dimension_table FOREIGN KEY (dimension_table_id) REFERENCES edl_dev_ctlg.structured.meta_data_table(table_id)
);

-- Audit trail for configuration changes
CREATE TABLE IF NOT EXISTS edl_dev_ctlg.structured.meta_config_audit (
    audit_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    table_name STRING NOT NULL,
    record_id INT NOT NULL,
    action_type STRING NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE', 'ACTIVATE', 'DEACTIVATE'
    old_values MAP<STRING, STRING>,
    new_values MAP<STRING, STRING>,
    changed_by STRING NOT NULL,
    changed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    change_reason STRING
);

-- ===========================================
-- VIEWS FOR BACKWARD COMPATIBILITY
-- ===========================================

-- View to replicate original dlt_metadata_source_config structure
CREATE OR REPLACE VIEW edl_dev_ctlg.structured.dlt_metadata_source_config_view AS
SELECT 
    ds.source_name as source,
    CONCAT_WS(',', COLLECT_LIST(DISTINCT it.table_name)) as derived_input_tables,
    ds.source_schema,
    CONCAT_WS(',', COLLECT_LIST(DISTINCT tt.table_name)) as derived_target_tables,
    dt.table_name as dimension_table,
    ds.source_details,
    ds.source_type,
    ds.frequency,
    sc.scenario_name as scenario,
    ds.source_system,
    ds.sourced_from,
    CASE WHEN ds.is_active THEN 'Y' ELSE 'N' END as is_active,
    ds.deactivated_date as deactivation_date,
    ds.created_date as create_date,
    tt.table_name as target_table,
    ds.enable_change_data_feed,
    ds.business_date_column_name,
    ds.file_format,
    CASE WHEN sc.test_mode THEN 'Y' ELSE 'N' END as test_mode
FROM edl_dev_ctlg.structured.meta_data_source ds
JOIN edl_dev_ctlg.structured.meta_pipeline_config pc ON ds.source_id = pc.source_id
JOIN edl_dev_ctlg.structured.meta_data_table it ON pc.input_table_id = it.table_id
JOIN edl_dev_ctlg.structured.meta_data_table tt ON pc.target_table_id = tt.table_id
LEFT JOIN edl_dev_ctlg.structured.meta_data_table dt ON pc.dimension_table_id = dt.table_id
JOIN edl_dev_ctlg.structured.meta_scenario sc ON pc.scenario_id = sc.scenario_id
WHERE ds.is_active = TRUE
GROUP BY ds.source_name, ds.source_schema, ds.source_details, ds.source_type, ds.frequency, 
         sc.scenario_name, ds.source_system, ds.sourced_from, ds.is_active, ds.deactivated_date, 
         ds.created_date, tt.table_name, ds.enable_change_data_feed, ds.business_date_column_name, 
         ds.file_format, sc.test_mode, dt.table_name;

-- View to replicate original dlt_metadata_cdc_config structure
CREATE OR REPLACE VIEW edl_dev_ctlg.structured.dlt_metadata_cdc_config_view AS
SELECT 
    ds.source_name as source,
    it.table_name as input_table,
    tt.table_name as target_table,
    cdc.key_attributes as key_attr,
    cdc.except_column_list,
    CASE WHEN cdc.compute_cdc THEN 'Y' ELSE 'N' END as compute_cdc,
    cdc.scd_type,
    cdc.created_date as create_date,
    cdc.updated_date as update_date,
    cdc.exclude_columns,
    cdc.sequence_column as sequence_col
FROM edl_dev_ctlg.structured.meta_cdc_config cdc
JOIN edl_dev_ctlg.structured.meta_pipeline_config pc ON cdc.pipeline_config_id = pc.pipeline_config_id
JOIN edl_dev_ctlg.structured.meta_data_source ds ON pc.source_id = ds.source_id
JOIN edl_dev_ctlg.structured.meta_data_table it ON pc.input_table_id = it.table_id
JOIN edl_dev_ctlg.structured.meta_data_table tt ON pc.target_table_id = tt.table_id
WHERE cdc.is_active = TRUE;

-- View to replicate original dlt_metadata_dq_config structure
CREATE OR REPLACE VIEW edl_dev_ctlg.structured.dlt_metadata_dq_config_view AS
SELECT 
    it.table_name as input_table,
    tt.table_name as target_table,
    dq.source_column_name,
    dq.dq_action as action,
    dq.dq_constraint as constraint,
    CASE WHEN dq.is_active THEN 'Y' ELSE 'N' END as is_active,
    dq.deactivated_date as deactivation_date,
    dq.created_date as create_date,
    CASE WHEN dq.is_mocked THEN 'Y' ELSE 'N' END as mocked_ind
FROM edl_dev_ctlg.structured.meta_dq_config dq
JOIN edl_dev_ctlg.structured.meta_pipeline_config pc ON dq.pipeline_config_id = pc.pipeline_config_id
JOIN edl_dev_ctlg.structured.meta_data_table it ON pc.input_table_id = it.table_id
JOIN edl_dev_ctlg.structured.meta_data_table tt ON pc.target_table_id = tt.table_id;

-- View to replicate original dlt_metadata_transform_config structure
CREATE OR REPLACE VIEW edl_dev_ctlg.structured.dlt_metadata_transform_config_view AS
SELECT 
    it.table_name as input_table,
    tt.table_name as target_table,
    tc.source_column_name,
    tc.transform_type as type,
    tc.transform_function_name as transform_fn_name,
    CASE WHEN tc.is_active THEN 'Y' ELSE 'N' END as is_active,
    tc.deactivated_date as deactivation_date,
    tc.created_date as create_date
FROM edl_dev_ctlg.structured.meta_transform_config tc
JOIN edl_dev_ctlg.structured.meta_pipeline_config pc ON tc.pipeline_config_id = pc.pipeline_config_id
JOIN edl_dev_ctlg.structured.meta_data_table it ON pc.input_table_id = it.table_id
JOIN edl_dev_ctlg.structured.meta_data_table tt ON pc.target_table_id = tt.table_id
WHERE tc.is_active = TRUE;

-- -- ===========================================
-- -- INDEXES FOR PERFORMANCE
-- -- ===========================================

-- -- Performance indexes
-- CREATE INDEX IF NOT EXISTS idx_meta_data_source_name ON edl_dev_ctlg.structured.meta_data_source(source_name);
-- CREATE INDEX IF NOT EXISTS idx_meta_data_source_active ON edl_dev_ctlg.structured.meta_data_source(is_active);
-- CREATE INDEX IF NOT EXISTS idx_meta_data_table_name ON edl_dev_ctlg.structured.meta_data_table(table_name);
-- CREATE INDEX IF NOT EXISTS idx_meta_data_table_type ON edl_dev_ctlg.structured.meta_data_table(table_type);
-- CREATE INDEX IF NOT EXISTS idx_meta_pipeline_config_active ON edl_dev_ctlg.structured.meta_pipeline_config(is_active);
-- CREATE INDEX IF NOT EXISTS idx_meta_pipeline_config_type ON edl_dev_ctlg.structured.meta_pipeline_config(pipeline_type);
-- CREATE INDEX IF NOT EXISTS idx_meta_cdc_config_active ON edl_dev_ctlg.structured.meta_cdc_config(is_active);
-- CREATE INDEX IF NOT EXISTS idx_meta_dq_config_active ON edl_dev_ctlg.structured.meta_dq_config(is_active);
-- CREATE INDEX IF NOT EXISTS idx_meta_transform_config_active ON edl_dev_ctlg.structured.meta_transform_config(is_active);
-- CREATE INDEX IF NOT EXISTS idx_meta_runtime_config_scenario ON edl_dev_ctlg.structured.meta_runtime_config(scenario_id);
-- CREATE INDEX IF NOT EXISTS idx_meta_runtime_config_run_date ON edl_dev_ctlg.structured.meta_runtime_config(run_date);
-- CREATE INDEX IF NOT EXISTS idx_meta_config_audit_table ON edl_dev_ctlg.structured.meta_config_audit(table_name, record_id);

-- ===========================================
-- MIGRATION HELPER PROCEDURES
-- ===========================================

-- Comment: Migration procedures would be created here to help migrate data
-- from the existing dlt_metadata_* tables to the new normalized structure
-- This would include data mapping and transformation logic 