-- INSERT SCRIPTS --
INSERT INTO edl_dev_ctlg.structured.dlt_meta_source_config (`dimension_table`, `source_system`, `source_schema`, `source_type`, `sourced_from`, `source_details`, `derived_input_tables`, `derived_target_tables`, `target_table`, `scenario`, `frequency`, `file_format`, `business_date_column_to_load`, `test_mode`, `is_active`, `deactivation_date`, `create_date`, `update_date`)
VALUES
('D_Group', 'Facets', 'dbo', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/History/dbo_cmc_grgc_group_count/', 'cmc_grgc_group_count', 'DF_Group_Count', 'DF_Group_Count', 'historical_run', 'N/A', 'parquet', NULL, 'N', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'test', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/History/dbo_cmc_grgc_group_count', 'cmc_grgc_group_count', 'DF_Group_Count', 'DF_Group_Count', 'historical_run', 'N/A', 'parquet', NULL, 'Y', 'Y', NULL, current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'dbo', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/History/dbo_cmc_grgr_group/', 'cmc_grgr_group', 'DF_Group', 'DF_Group', 'historical_run', 'N/A', 'parquet', '', 'N', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'audit', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/History/audit_cmc_grgr_group/', 'cmc_grgr_group_history', 'DF_Group_History', 'DF_Group_History', 'historical_run', 'N/A', 'parquet', 'HIST_CREATE_DTM', 'N', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'test', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/History/dbo_cmc_grgr_group/', 'cmc_grgr_group,cmc_grgr_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'historical_run', 'N/A', 'parquet', 'transaction_dtm', 'Y', 'Y', NULL, current_timestamp(), current_timestamp()),
('D_Group', 'VHP', 'N/A', 'DATABRICKS_VOLUMES', 'Flat File', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/VHP/BusinessStartDate=20250305/Group/', 'bcbsla_ma_group,bcbsla_ma_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'historical_run', 'daily', 'csv', NULL, 'Y', 'Y', NULL, current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'dbo', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/BusinessStartDate={}/dbo_cmc_grgc_group_count/', 'cmc_grgc_group_count', 'DF_Group_Count', 'DF_Group_Count', 'incremental_run,apply_correction,apply_test', 'daily', 'parquet', NULL, 'N', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'test', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/BusinessStartDate={}/dbo_cmc_grgc_group_count/', 'cmc_grgc_group_count', 'DF_Group_Count', 'DF_Group_Count', 'incremental_run,apply_correction,apply_test', 'N/A', 'parquet', NULL, 'Y', 'Y', NULL, current_timestamp(), current_timestamp()),
('D_Group', 'VHP', 'N/A', 'DATABRICKS_VOLUMES', 'Flat File', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/VHP/BusinessStartDate={}/Group/', 'bcbsla_ma_group,bcbsla_ma_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'incremental_run,apply_correction,apply_test', 'daily', 'csv', NULL, 'N', 'Y', NULL, current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'cdc', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/BusinessStartDate={}/cdc_cmc_grgr_group/', 'cmc_grgr_group, cmc_grgr_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'incremental_run,apply_correction,apply_test', 'daily', 'parquet', NULL, 'N', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('D_Group', 'Facets', 'test', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/BusinessStartDate={}/cdc_cmc_grgr_group/', 'cmc_grgr_group,cmc_grgr_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'incremental_run,apply_correction,apply_test', 'N/A', 'parquet', NULL, 'Y', 'Y', NULL, current_timestamp(), current_timestamp());

INSERT INTO edl_dev_ctlg.structured.dlt_meta_cdc_config (`dimension_table`, `target_table`, `key_attr`, `sequence_col`, `exclude_columns`, `compute_cdc`, `scd_type`, `create_date`, `update_date`)
VALUES
('d_group', 'DF_Group', 'Group_Source_Key,Source_System_Code', 'business_date', NULL, 'N', '1', current_timestamp(), current_timestamp()),
('d_group', 'DF_Group_History', 'Group_Source_Key,Source_System_Code', 'business_date', NULL, 'N', '2', current_timestamp(), current_timestamp()),
('d_group', 'DF_Group_Count', 'Group_Source_Key,Count_Type,Source_System_Code,Row_Effective_Date', 'business_date', NULL, 'Y', '1', current_timestamp(), current_timestamp());

INSERT INTO edl_dev_ctlg.structured.dlt_meta_dq_config (`dimension_table`, `source_system`, `target_table`, `source_column_name`, `action`, `constraint`, `is_active`, `deactivation_date`, `mocked_ind`, `create_date`, `update_date`)
VALUES
("d_group", "VHP", "DF_Group_History", "Group_Address_Line_1", "WARN", "Group_Address_Line_1 is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "VHP", "DF_Group_History", "Group_Address_Line_2", "WARN", "Group_Address_Line_2 is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_ID", "FAIL", "GRGR_ID is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "GRGR_CK", "FAIL", "GRGR_CK is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_Count", "GRGR_CK", "FAIL", "GRGR_CK is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_Count", "GRGC_COUNT", "QUARANTINE", "GRGC_COUNT = 0", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_CK", "FAIL", "GRGR_CK is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_MCTR_TYPE", "WARN", "trim(GRGR_MCTR_TYPE)='UNK' ", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_NAME", "WARN", "trim(GRGR_NAME)='Unknown'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_ID", "QUARANTINE", "trim(GRGR_ID)='XXXXX'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group", "GRGR_NAME", "QUARANTINE", "trim(GRGR_NAME)='XXXXX'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "CICI_ID", "WARN", "trim(CICI_ID)='UNK'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "GRGR_STS", "WARN", "trim(GRGR_STS)='XX' and trim(GRGR_STS)<>' '", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "GRGR_ID", "QUARANTINE", "trim(GRGR_ID)='XXXXX'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "GRGR_CTRY_CD", "QUARANTINE", "trim(GRGR_CTRY_CD)='UK' and trim(GRGR_CTRY_CD)<>' '", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_Count", "GRGC_MCTR_CTYP", "WARN", "trim(GRGC_MCTR_CTYP)='UNK'", "Y", "1900-01-01 00:00:00", TRUE, current_timestamp(), current_timestamp()),
("d_group", "Facets", "DF_Group_History", "GRGR_ID", "FAIL", "GRGR_ID is null", "Y", "1900-01-01 00:00:00", FALSE, current_timestamp(), current_timestamp());


INSERT INTO edl_dev_ctlg.structured.dlt_meta_transform_config (`dimension_table`, `source_system`, `target_table`, `source_column_name`, `transform_type`, `transform_fn_name`, `is_active`, `deactivation_date`, `create_date`, `update_date`)
VALUES
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_State', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Country', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_City', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Zip_Code', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Line_1', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Line_2', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Line_3', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_NAME', 'title case', 'initcap', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'Source_System_Code', 'default', 'Facets', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group_History', 'Source_System_Code', 'default', 'Facets', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group_Count', 'Source_System_Code', 'default', 'Facets', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group_Count', 'GRGC_MCTR_CTYP', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_CK', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_ID', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_NAME', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group_History', 'GRGR_ID', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Renewal_Month', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Fax_Number', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Fax_Ext', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_EIN', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_NAME', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'Facets', 'DF_Group', 'GRGR_ID', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(), current_timestamp()),
('d_group', 'Facets', 'DF_Group_History', 'GRGR_ID', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group', 'Group_Original_Effective_Date', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Phone_Number', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Phone_Ext', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group_History', 'Group_Email', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp()),
('d_group', 'VHP', 'DF_Group', 'Group_Type_Code', 'trim', 'trim', 'Y', '1900-01-01 00:00:00', current_timestamp(),current_timestamp());