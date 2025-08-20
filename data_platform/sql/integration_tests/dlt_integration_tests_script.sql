USE CATALOG edl_dev_ctlg;
USE SCHEMA structured;

CREATE OR REPLACE TABLE dim_integration_test_scenarios (
    dimension_table STRING,
    scenario STRING,
    scenario_description STRING,
    scenario_type STRING,
    refresh_type STRING,    
    cleanup_table_list ARRAY<STRING>,
    scenario_input_source_system STRING, -- VHP, Facets
    scenario_input_target_table STRING, -- DF tables
    scenario_input_business_date DATE,
    scenario_input_source_details STRING,
    scenario_input_format STRING,
    created_at TIMESTAMP
);


-- INSERTS FOR scenario_1: historical_run from Facets
INSERT INTO dim_integration_test_scenarios VALUES
('D_GROUP', '1', 'Load History Data For D_Group Use Case. This input is for loading DF_GROUP history data for Facets source system', 'historical_run', 'full_refresh',
ARRAY('dlt_landing_cmc_grgr_group', 'dlt_landing_cmc_grgr_group_history', 'dlt_landing_cmc_grgc_group_count', 'dlt_landing_bcbsla_ma_group', 'dlt_landing_bcbsla_ma_group_history'),
 'Facets', 'DF_GROUP', DATE('1900-01-01'), '', '', 
 current_timestamp()),
('D_GROUP', '1', 'Load History Data For D_Group_Count Facets Use Case. This input is for loading DF_GROUP_COUNT history data for Facets source system', 'historical_run', 'full_refresh',
ARRAY('dlt_landing_cmc_grgr_group', 'dlt_landing_cmc_grgr_group_history', 'dlt_landing_cmc_grgc_group_count', 'dlt_landing_bcbsla_ma_group', 'dlt_landing_bcbsla_ma_group_history'),
 'Facets', 'DF_GROUP_COUNT', DATE('1900-01-01'), '', '', 
 current_timestamp());


-- INSERT FOR scenario_2: incremental_refresh from Historical VHP Data
INSERT INTO dim_integration_test_scenarios VALUES
('D_GROUP', '2', 'Load History Data For D_Group VHP Use Case.This will load VHP DF_Group historical data in a incremental fashion in DLT', 'normal', 'incremental_refresh',
  ARRAY(),'VHP', 'DF_GROUP', DATE('1900-01-01'), '', '', current_timestamp());

-- INSERTS FOR scenario_3: incremental_run from Facets
INSERT INTO dim_integration_test_scenarios VALUES
('D_GROUP', '3', 'Load Facets DF_Group data for a business data as incremental run.',  'incremental_run', 'incremental_refresh',
  ARRAY(),'Facets', 'DF_GROUP', DATE('2025-04-08'), '', '', current_timestamp()),
('D_GROUP', '3', 'Load Facets DF_Group_Count data for a business data in incremental_refresh mode.', 'incremental_run', 'incremental_refresh',
  ARRAY(),'Facets', 'DF_GROUP_COUNT', DATE('2025-04-22'), '', '', current_timestamp());

-- INSERT FOR scenario_4: apply_correction with file source
INSERT INTO dim_integration_test_scenarios VALUES
('D_GROUP', '4', 'This scenario is for testing production support use cases. This scenario is for testing file correction scenario in incremental_refresh mode.', 'file_correction', 'incremental_refresh',
 ARRAY(),'Facets', 'DF_GROUP', DATE('2025-04-08'), 
 '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/corrected/BusinessStartDate=20250408/cdc_cmc_grgr_group/',
 'parquet',current_timestamp());

-- INSERT FOR scenario_5: apply_correction with delta correction
INSERT INTO dim_integration_test_scenarios VALUES
('D_GROUP', '5', 'This scenario is for testing production support use cases. This scenario is for testing delta table correction scenario in incremental_refresh mode.', 'delta_correction', 'incremental_refresh',
  ARRAY(),'Facets', 'DF_GROUP', DATE('2025-04-08'), '', '', current_timestamp());
