# Development guide

This readme provides basic development guide lines for developing new pipelines for new entities


## Table of Contents
- [Getting Started](#getting_started)
- [Developing a new Dimension entity](#developing-a-new-dimension-entity)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Appendix](#appendix)

## Getting Started

### Prerequisites
1. **Databricks Workspace** with DLT capabilities
2. **Databricks CLI** installed and configured
3. **Python 3.8+** for local development
4. **VSCode IDE is downloaded and installed** (For developers)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://bcbsla@dev.azure.com/bcbsla/Enterprise-Information-Management/_git/DataFoundation
   cd data_platform
   ```

2. **Configure Databricks CLI**
   ```bash
   databricks configure
   ```
3. **Validate the project**
   ```bash
   databricks bundle validate

4. **Deploy the bundle**
   ```bash
   # Development deployment
   databricks bundle deploy --target dev
   
   ```

## Developing a new Dimension entity

1. **Create new dimension configuration**
2. **Define pipeline configuration**
3. **Create job configuration**
4. **Deploy bundle**


### Create new dimension configuration

We need to configure 4 metadata configuration tables for any new dimension to be created using the DLT Workflow.

1. dlt_meta_source_config - Define input sources
2. dlt_meta_cdc_config - Define SCD configurations (Type 1 or Type2)
3. dlt_meta_dq_config - Define data quality rules for failure, warning and quarantine.
4. dlt_meta_transform_config - Define data transformations using default, custom or SQL UDF mappings. 

#### Development guidelines
1. Each table has 3 important attributes viz. `dimension_table` string, `source_system` string `target_table` string,
2. To create a new configuration, the easiest approach is to make a copy of `AzureDatabricks\data_platform\sql\metadata\metadata_d_group_facets_vhp_insert_script.sql` and modify for the new dimension.
3. Example configuration
```sql
Example :  dlt_meta_source_config
INSERT INTO edl_dev_ctlg.structured.dlt_meta_source_config (`dimension_table`, `source_system`, `source_schema`, `source_type`, `sourced_from`, `source_details`, `derived_input_tables`, `derived_target_tables`, `target_table`, `scenario`, `frequency`, `file_format`, `business_date_column_to_load`, `test_mode`, `is_active`, `deactivation_date`, `create_date`, `update_date`)VALUES('D_Group', 'Facets', 'test', 'DATABRICKS_VOLUMES', 'SQL Server', '/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/History/dbo_cmc_grgr_group/', 'cmc_grgr_group,cmc_grgr_group_history', 'DF_Group, DF_Group_History', 'DF_Group', 'historical_run', 'N/A', 'parquet', 'transaction_dtm', 'Y', 'Y', NULL, 2025-08-04 17:09:49.916713, 2025-08-04 17:09:49.916713),

-- Important attributes
dimension_table = D_Group
source_system = Facets -- Facets or VHP for d_group dimension
target_table = DF_Group -- The final target table for which the source is configured
derived_target_tables = 'cmc_grgr_group,cmc_grgr_group_history' -- This is a comma seperated string that specifies what intermediate landing tables to generate for this source. In this example, source for DF_Group generates 2 intermediate tables cmc_grgr_group,cmc_grgr_group_history.
derived_target_tables = 'DF_Group, DF_Group_History' -- This is a comma seperated string that specifies what final target tables to generate for this source. In this example, source for DF_Group generates 2 target tables DF_Group, DF_Group_History.
source_details --Specifies the source data to pull from. In this example, the source data is pulled from ADLS. There are 2 variations  /Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/History/dbo_cmc_grgr_group/ and /Volumes/edl_dev_ctlg/rawfiles/raw/EDF/VHP/BusinessStartDate={}/Group/.  For database, we will have a JSON connection string
scenario = historical_run -- 4 scenarios are supported, historical_run, incremental_run, apply_correction, apply_test
format = file format --parquet, csv
business_date_column_to_load = transaction_dtm -- In this example for historical load, we are mapping the transaction_dtm to load the business date in our workflows. 

```


```sql
Example :  dlt_meta_cdc_config
INSERT INTO edl_dev_ctlg.structured.dlt_meta_cdc_config (`dimension_table`, `target_table`, `key_attr`, `sequence_col`, `exclude_columns`, `compute_cdc`, `scd_type`, `create_date`, `update_date`)VALUES('d_group', 'DF_Group', 'Group_Source_Key,Source_System_Code', 'business_date', [], 'N', '1', 2025-04-17 15:48:44.354000, 2025-08-04 17:09:57.292797),
('d_group', 'DF_Group_History', 'Group_Source_Key,Source_System_Code', 'business_date', NULL, 'N', '2', 2025-08-04 17:09:57.292797, 2025-08-04 17:09:57.292797),

-- Important attributes
dimension_table = D_Group
target_table = DF_Group -- The final target table for which the source is configured
key_attr = 'Group_Source_Key,Source_System_Code' -- Primary key for CDC computations
sequence_col = business_date -- Sequence climn to get the latest data for CDC computation
scd_type = 1 or 2 -- For Type 1 or Type2
exclude_columns = Null --Columns to exclude from the final dataframe

```

```sql
Example :  dlt_meta_transform_config
INSERT INTO edl_dev_ctlg.structured.dlt_meta_transform_config (`dimension_table`, `source_system`, `target_table`, `source_column_name`, `transform_type`, `transform_fn_name`, `is_active`, `deactivation_date`, `create_date`, `update_date`)VALUES('d_group', 'VHP', 'DF_Group_History', 'Group_Address_State', 'trim', 'trim', 'Y', 1900-01-01 00:00:00, 2025-05-28 03:11:01.050347, 2025-08-04 17:10:00.686958),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Country', 'trim', 'trim', 'Y', 1900-01-01 00:00:00, 2025-05-28 03:11:43.237144, 2025-08-04 17:10:00.686958),

-- Important attributes
dimension_table = D_Group
source_system = Facets -- Facets or VHP for d_group dimension
target_table = DF_Group -- The final target table for which the source is configured
source_column_name = 'Group_Address_Line_1' -- Column to apply validation
action = 'Group_Address_Line_1 is null' -- validations to apply
mocked_ind = What is this ?

```

```sql
Example :  dlt_meta_dq_config
INSERT INTO edl_dev_ctlg.structured.dlt_meta_dq_config (`dimension_table`, `source_system`, `target_table`, `source_column_name`, `action`, `constraint`, `is_active`, `deactivation_date`, `mocked_ind`, `create_date`, `update_date`)VALUES('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Line_1', 'WARN', 'Group_Address_Line_1 is null', 'Y', 1900-01-01 00:00:00, FALSE, 2025-03-24 21:07:48.213000, 2025-08-04 17:09:58.776861),
('d_group', 'VHP', 'DF_Group_History', 'Group_Address_Line_2', 'WARN', 'Group_Address_Line_2 is null', 'Y', 1900-01-01 00:00:00, FALSE, 2025-03-24 21:08:07.336000, 2025-08-04 17:09:58.776861),

-- Important attributes
dimension_table = D_Group
source_system = Facets -- Facets or VHP for d_group dimension
target_table = DF_Group -- The final target table for which the source is configured
source_column_name = 'Group_Address_State' -- Column to apply transformation
transform_type = 'trim' -- type of transformation to apply. custom or sql
transform_fn_name = 'trim' -- function to apply

```

### Define pipeline configuration

**Steps for building new pipelines**
1. Create a folder by dimension_name under `resources\jobs\data_engineering\config`. 
2. Make a copy of `resources\jobs\data_engineering\templates\dlt_entity_name.pipeline_serverless.yml` under the above dimension folder.Change the file name to `dlt_entity_name.pipeline_serverless.yml` to actual dimension name. e.g. dlt_d_group.pipeline_serverless.yml
3. Edit the file and replace <<ENTITY_NAME>> in the file with the actual dimension name. 
4. Modify `databricks.yml` to add the new resources. 
```yaml
# Example
include:
  - resources/jobs/data_engineering/config/d_group/*.yml
  - resources/jobs/integration_tests/config/d_group/*.yml
  - resources/jobs/setup_schemas/config/setup/*.yml
```

**Steps for modifying existing pipelines**
For making changes to existing pipleline, edit the yaml file. 

### Define job configuration

**Steps for building new job**
1. Create a folder if it does not exist by dimension_name under `resources\jobs\data_engineering\config`. 
2. Make a copy of `resources\jobs\data_engineering\templates\dlt_entity_name_serverless.job.yml` under the above dimension folder.Change the file name to `dlt_entity_name.pipeline_serverless.yml` to actual dimension name. e.g. dlt_d_group.job_serverless.yml
3. Edit the file and replace <<ENTITY_NAME>> in the file with the actual dimension name. 
4. Modify `databricks.yml` to add the new resources if it does not exist. 
```yaml
# Example
include:
  - resources/jobs/data_engineering/config/d_group/*.yml
  - resources/jobs/integration_tests/config/d_group/*.yml
  - resources/jobs/setup_schemas/config/setup/*.yml
```

**Steps for modifying existing job**
For making changes to existing job, edit the job yaml file. 

### Deploy bundle 
```bash

cd AzureDatabricks\data_platform
databricks bundle validate
databricks bundle deploy

# Destory and redeploy
databricks bundle destroy
databricks bundle deploy

# Note: On dev environment, the jobs will be created with the developers prefix. E.g.  [dev c73550] run_integration_tests
```

## Testing

### Unit Testing
Developers are responsible for writing unit test cases for the framework.

**Run unit tests**
```bash
pytest tests/
```

### Integration Testing
Developers are responsible for doing integration testing on development environment.

Follow the [integration_test_scenarios.md](/AzureDatabricks\data_platform\docs\integration_test_scenarios.md)


## Troubleshooting

### Common Issues

#### Pipeline Failures
1. **Check DLT pipeline logs**
   ```bash
   databricks pipelines get --pipeline-id <pipeline_id>
   ```

2. **Verify metadata configuration**
   ```sql
   SELECT * FROM dlt_metadata_source_config WHERE is_active = 'Y'
   ```

3. **Check source data availability**
   ```sql
   SELECT COUNT(*) FROM source_table WHERE business_date = '2024-01-01'
   ```

#### Data Quality Issues

1. **Check validation rules**
   ```sql
   SELECT * FROM dlt_metadata_dq_config WHERE is_active = 'Y'
   ```

#### Performance Issues
1. **Optimize cluster configuration**
2. **Review partition strategies**
3. **Monitor resource utilization**

### Debug Mode
```bash
# Enable debug logging
export DATABRICKS_CLI_DEBUG=true

# Run with verbose output
databricks jobs run-now --job-id <job_id> --verbose
```

## Appendix

### Run SQL Script Job

1. Modify `resources\jobs\setup_schemas\config\dlt_setup_data_platform_db.job.yml`
```yml
# Edit the variables to specify the sql and the folder under which the sql resides 
variables:
  sql_file_name:
    description: SQL file to use for the run
    default: dlt_integration_tests_script.sql

  sql_subfolder:
    description: SQL subfolder
    default: integration_tests
```

2. **Create metadata tables**
   ```bash
   # Run metadata setup job from local or UI
   databricks jobs run-now --job-id setup_data_platform_db
   ```

### Running the workflow

```bash
databricks jobs run-now --job-id run_integration_tests \
  --job-parameters '{
    "dimension_table": "D_Group",
    "scenario": "1"
  }'
```
For different scenarios, refer to [integration_test_scenarios.md](integration_test_scenarios.md)


### Monitoring

#### Job Status
```bash
# Check job run status
databricks jobs get-run --run-id <run_id>

# List recent runs
databricks jobs list-runs --job-id <job_id>
```

#### Pipeline Monitoring
```bash
# Monitor DLT pipeline
databricks pipelines get --pipeline-id <pipeline_id>

# View pipeline events
databricks pipelines list-events --pipeline-id <pipeline_id>
```

#### Example of custom UDFs

```python
# Add to udf_utils.py
@F.udf(returnType=StringType())
def custom_transformation(value):
    # Custom logic here
    return transformed_value

# Register in udf_mapping
udf_mapping["custom_transformation"] = custom_transformation
```

