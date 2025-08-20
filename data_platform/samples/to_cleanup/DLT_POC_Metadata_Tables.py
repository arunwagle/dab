# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_metadata_source_info
# MAGIC (
# MAGIC source string,
# MAGIC source_table string,
# MAGIC source_schema string,
# MAGIC target_table string,
# MAGIC dimension_table string,
# MAGIC source_details string,
# MAGIC source_type string,
# MAGIC frequency string,
# MAGIC load_type string,
# MAGIC source_system string,
# MAGIC sourced_from string,
# MAGIC skip_all_processing string,
# MAGIC is_active string,
# MAGIC deactivation_date timestamp,
# MAGIC create_date timestamp
# MAGIC )

# COMMAND ----------

# %sql
# CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_metadata_input_source_info
# (
#  input_table_name string,
#  column_names string
# )



# COMMAND ----------

# MAGIC %sql
# MAGIC insert into edl_dev_ctlg.structured.t_dlt_metadata_source_info values
# MAGIC (
# MAGIC 'DF_GROUP_FACETS','t_dlt_bronze_cmc_grgc_group_count','dbo','DF_Group_Count','D_Group','/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/History/dbo_cmc_grgc_group_count/','DATABRICKS_VOLUMES','N/A','one_time','Facets', 'SQL Server', 'Y', '1900-01-01', current_timestamp()
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_metadata_source_info where input_table= 't_dlt_bronze_cmc_grgc_group_count'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update edl_dev_ctlg.structured.t_dlt_metadata_source_info set source_details = '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/BusinessStartDate={}/dbo_cmc_grgc_group_count/' where target_table = 'DF_Group_Count' and load_type = 'incremental' and source_schema = 'dbo'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC update edl_dev_ctlg.structured.t_dlt_metadata_source_info set source_table = 't_dlt_vhp_bcbsla_ma_groups'  where  source = 'DF_GROUP_VHP' and source_schema = 'N/A'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_metadata_validation_rules
# MAGIC (
# MAGIC source_table string,
# MAGIC target_table string,
# MAGIC source_column_name string,
# MAGIC action string,
# MAGIC constraint string,
# MAGIC is_active string,
# MAGIC deactivation_date timestamp,
# MAGIC create_date timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_metadata_validation_rules 

# COMMAND ----------

# MAGIC %sql
# MAGIC update edl_dev_ctlg.structured.t_dlt_metadata_validation_rules set constraint = 'Group_Address_Line_2 is null' where source_COLUMN_NAME = 'Group_Address_Line_2'

# COMMAND ----------

c

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_transform_rules
# MAGIC (
# MAGIC source_table string,
# MAGIC target_table string,
# MAGIC source_column_name string,
# MAGIC type string,
# MAGIC transform_fn_name string,
# MAGIC is_active string,
# MAGIC deactivation_date timestamp,
# MAGIC create_date timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_transform_rules

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into edl_dev_ctlg.structured.t_dlt_transform_rules values
# MAGIC (
# MAGIC 'BCBSLA_MA_Groups_*.csv','DF_Group','Group_Original_Effective_Date','trim','', 'Y', '1900-01-01', current_timestamp()
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_workflow_runtime_audit
# MAGIC (
# MAGIC source string,
# MAGIC catalog string,
# MAGIC schema_name string,
# MAGIC input_table string,
# MAGIC business_start_date date,
# MAGIC status string,
# MAGIC description string,
# MAGIC job_id string,
# MAGIC run_id string
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_workflow_runtime_audit where source = 'DF_GROUP_FACETS'

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE edl_dev_ctlg.structured.t_dlt_workflow_runtime_audit SET business_start_date = '2025-04-24' WHERE source = 'DF_GROUP_FACETS'

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from edl_dev_ctlg.structured.t_dlt_workflow_runtime_audit where source = 'DF_GROUP_FACETS'

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into edl_dev_ctlg.structured.t_dlt_audit_workflow_runtime values
# MAGIC (
# MAGIC 'DF_GROUP_FACETS',current_timestamp(),'IN_PROGRESS','', ''
# MAGIC   
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM edl_dev_ctlg.Structured.t_database_config where config_id in (3791, 3792, 3793)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dataload where config_id in (3793)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from edl_dev_ctlg.structured.t_DLT_CMC_GRGR_GROUP_cdc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.t_dlt_metadata_cdc
# MAGIC (
# MAGIC source string,
# MAGIC input_table string,
# MAGIC target_table string,
# MAGIC key_attr string,
# MAGIC cdc_columns string,
# MAGIC compute_cdc string,
# MAGIC scd_type string,
# MAGIC create_date timestamp,
# MAGIC update_date timestamp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_metadata_cdc

# COMMAND ----------

# MAGIC %sql
# MAGIC update edl_dev_ctlg.structured.t_dlt_metadata_cdc set compute_cdc = 'N' where source = 'DF_GROUP_FACETS'

# COMMAND ----------

GRGR_CK, GRGC_MCTR_CTYP, GRGC_EFF_DT

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into edl_dev_ctlg.structured.t_dlt_metadata_cdc
# MAGIC values ('DF_GROUP_FACETS', 't_dlt_bronze_cmc_grgc_group_count', 'DF_Group_Count', 'GRGR_CK, GRGC_MCTR_CTYP, GRGC_EFF_DT','', 'Y', '1' , current_timestamp(), current_timestamp() )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edl_dev_ctlg.structured.t_dlt_bronze_cmc_grgr_group 

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct loadtimestamp from edl_dev_ctlg.structured.t_dlt_bronze_cmc_grgr_group_scd1 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM edl_dev_ctlg.structured.t_dlt_bronze_cmc_grgr_group

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE edl_dev_ctlg.structured.dim_group_runtime_metadata (
# MAGIC   dimension_table STRING,
# MAGIC   run_date TIMESTAMP,
# MAGIC   scenario STRING,
# MAGIC   sources_metadata MAP<STRING, STRUCT<business_date: String>>
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE edl_dev_ctlg.structured.t_dlt_metadata_source_info 
# MAGIC RENAME COLUMN target_table TO impacted_target_tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE edl_dev_ctlg.structured.t_dlt_metadata_source_info 
# MAGIC RENAME COLUMN input_table TO impacted_input_tables;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE edl_dev_ctlg.structured.t_dlt_metadata_source_info 
# MAGIC ADD COLUMN target_table STRING; 
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Update edl_dev_ctlg.structured.t_dlt_metadata_source_info 
# MAGIC SET target_table = impacted_target_tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC ALTER TABLE edl_dev_ctlg.structured.t_dlt_metadata_source_info 
# MAGIC ADD COLUMN enable_change_data_feed boolean;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from edl_dev_ctlg.structured.t_dlt_metadata_cdc ;
# MAGIC
# MAGIC Update edl_dev_ctlg.structured.t_dlt_metadata_cdc 
# MAGIC SET exclude_columns = ARRAY() 
# MAGIC where input_table='t_dlt_bronze_cmc_grgc_group_count';
# MAGIC