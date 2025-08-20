# DLT Metadata Tables Setup Job

This directory contains the Databricks job configuration for setting up DLT metadata tables.

## Files

- `setup_dlt_metadata_tables.yml` - Databricks job configuration file
- `README.md` - This documentation file

## Job Overview

The **Setup DLT Metadata Tables** job executes the combined SQL script to create and populate all DLT metadata configuration tables in the `edl_dev_ctlg.structured` schema.

### Tables Created

1. **dlt_metadata_cdc_config** - Change Data Capture configuration (3 records)
2. **dlt_metadata_dq_config** - Data Quality configuration (17 records)
3. **dlt_metadata_source_config** - Source configuration (12 records)
4. **dlt_metadata_transform_config** - Transform configuration (28 records)

### Job Tasks

The job consists of three sequential tasks:

1. **setup_dlt_metadata_tables** - Executes the SQL script at `/Users/arun.wagle/Databricks/Clients/2025/BCBSLA/code/data_platform/sql/dlt_metadata_all_tables_combined.sql`
2. **verify_table_creation** - Verifies all tables were created and populated correctly
3. **generate_setup_report** - Creates a summary report with record counts

## Usage

### Prerequisites

1. Ensure you have a Databricks workspace with appropriate permissions
2. Verify the SQL file exists at the specified path
3. Have a SQL warehouse available for query execution

### Running the Job

#### Option 1: Using Databricks CLI

```bash
# Navigate to the job directory
cd /Users/arun.wagle/Databricks/Clients/2025/BCBSLA/code/data_platform/resources/jobs/setup_schemas

# Create the job using Databricks CLI
databricks jobs create --json-file setup_dlt_metadata_tables.yml

# Or if using newer CLI version
databricks jobs create --json @setup_dlt_metadata_tables.yml
```

#### Option 2: Using Databricks UI

1. Open Databricks workspace
2. Navigate to **Workflows** → **Jobs**
3. Click **Create Job**
4. Choose **Import from file**
5. Upload the `setup_dlt_metadata_tables.yml` file
6. Configure the `warehouse_id` parameter
7. Click **Create**

### Configuration Parameters

Before running the job, you must configure:

- **warehouse_id**: Replace `"your_warehouse_id_here"` with your actual SQL warehouse ID

### Expected Results

After successful execution, you should see:

- 4 metadata tables created in `edl_dev_ctlg.structured` schema
- Total of 60 configuration records inserted across all tables
- Verification report showing record counts for each table
- Summary report with setup timestamp

### Troubleshooting

#### Common Issues

1. **Permission Denied**: Ensure you have CREATE TABLE permissions on the `edl_dev_ctlg.structured` schema
2. **SQL File Not Found**: Verify the path to the SQL file is correct
3. **Warehouse Not Available**: Ensure the specified warehouse_id exists and is accessible

#### Logs and Monitoring

- Check the job run logs in Databricks UI for detailed error information
- Review the verification task output to ensure all tables were created correctly
- Monitor the setup report task for final summary

### Re-running the Job

The job is designed to be idempotent:
- Uses `CREATE OR REPLACE TABLE` statements
- Can be safely re-run multiple times
- Will recreate tables with fresh data each time

### Job Scheduling (Optional)

The job includes an optional schedule configuration (commented out by default):

```yaml
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: "America/Chicago"
  pause_status: "UNPAUSED"
```

Uncomment these lines if you want the job to run on a schedule.

## Support

For issues or questions about this job configuration, please refer to:
- The original SQL script documentation
- Databricks job configuration documentation
- Your organization's Databricks support resources 