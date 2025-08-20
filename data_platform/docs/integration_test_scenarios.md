# Integration Test Scenarios - DLT Pilot Project

## Table of Contents
- [Overview](#overview)
- [Execution Guide](#execution-guide)
- [Test Scenarios](#test-scenarios)


## Overview

The DLT Pilot Project integration tests validate the end-to-end functionality of the Delta Live Tables pipeline across multiple scenarios that simulate real-world data processing requirements. These tests ensure the pipeline can handle:

- **Multi-source data integration** (Facets, VHP)
- **Different processing modes** (historical, incremental, production support use cases (corrections))
- **Data quality validation** and quarantine mechanisms
- **Data Transformation** and standardization functionality
- **Slowly Changing Dimensions (SCD)** Type 1 qnd Type2 processing

### Test Strategy

The integration tests follow a **sequential approach** where each scenario can builds upon the previous one. Each scenario can be tested individually as well after initial load.

```
Scenario 1 → Scenario 2 → Scenario 3 → Scenario 4 → Scenario 5
    ↓           ↓           ↓           ↓           ↓
Initial    Multi-Source  Incremental  File-Based  Delta-Based
 Load       Integration   Processing   Correction  Correction
```

## Execution Guide

### Prerequisites

1. **Databricks Environment**
   - DLT-enabled workspace
   - Appropriate compute resources
   - Access to test data volumes

2. **Test Data**
   - Facets historical data
   - VHP historical data
   - Sample incremental data
   - Corrected data files
   - SQL scripts with corrected records

3. **Job Configuration**
   ```yaml
   # Test environment settings
   catalog: "edl_dev_ctlg"
   schema: "structured"
   test_mode: "Y"
   table_suffix: "test" # Optional
   ```

### Setup Steps

#### Prepare Test Data

Ensure test data is available in volumes folder under `/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Test` folder.  
The data is organized as below

**Test Data Structure**

```
/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Test/Facets
├── historical/
│   ├── cmc_grgr_group/
│   └── cmc_grgc_group_count/
├── incremental/
│   ├── BusinessStartDate=20250408/
│   └── BusinessStartDate=20250422/
└── corrected/
    └── BusinessStartDate=20250408/
```

#### VHP Test Data
```
/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Test/VHP/
├── historical/
│   └── vhp_group/
└── incremental/
    └── BusinessStartDate=20250408/
```

### Run Integration Test Job
1. Validate that the job `run_integration_tests` is deployed successfully as a part of bundle deployment. E.g. on dev, the job will be [dev c73550] run_integration_tests, where c73550 is the id of the developer
2. Run job from UI or from terminal 

```bash
databricks jobs run-now --job-id dev_c73550_run_integration_tests \
  --job-parameters '{
    "dimension_table": "D_Group",
    "scenario": "1"
  }'

# Note: Replace scenario with the scenario being tested. The scenarios are configured in edl_dev_ctlg.structured.dim_integration_test_scenarios
```

## Test Scenarios

### Scenario 1: Initial Historical Load (Facets)

#### Purpose
Validates the initial data load from the Facets system, testing full refresh capabilities and multi-table processing. For historical data, initialize the business_date with 19000101

#### Configuration
| scenario_type | refresh_type | cleanup_table_list | scenario_input_source_system | scenario_input_target_table | scenario_input_business_date	| scenario_input_source_details	|scenario_input_format  | 
|------------|------------|------------|------------|------------|------------|------------|------------|
| historical_run | full_refresh	| ["dlt_landing_cmc_grgr_group","dlt_landing_cmc_grgr_group_history","dlt_landing_cmc_grgc_group_count","dlt_landing_bcbsla_ma_group","dlt_landing_bcbsla_ma_group_history"] | Facets  DF_GROUP	| 1900-01-01 | | |
| historical_run | full_refresh	|["dlt_landing_cmc_grgr_group","dlt_landing_cmc_grgr_group_history","dlt_landing_cmc_grgc_group_count","dlt_landing_bcbsla_ma_group","dlt_landing_bcbsla_ma_group_history"]	|Facets	DF_GROUP_COUNT|	1900-01-01	| | |

#### Test Objectives
- ✅ **Pipeline Initialization**: Verify DLT pipeline can be started successfully
- ✅ **Table Creation**: Confirm all landing, bronze, silver, and gold tables are created
- ✅ **Data Loading**: Validate data is loaded from Facets source system
- ✅ **Multi-Table Processing**: Ensure both DF_GROUP and DF_GROUP_COUNT are processed
- ✅ **Data Quality, custom transfomation, in-place data standardization**: Verify rules are applied correctly
- ✅ **Full Refresh**: Confirm tables are completely rebuilt

#### Expected Outcomes
- Landing tables populated with raw Facets data
- Bronze tables populated with raw Facets data
- Silver tables contain cleaned and validated data
- Gold tables have business-ready aggregated data
- Data quality audit records created
- No quarantined records for valid test data

### Scenario 2: Multi-Source Integration (VHP)

#### Purpose
Tests the integration of multiple source systems and validates incremental processing with existing data. This scenario loads VHP historical data. 

#### Configuration

| scenario_type	| refresh_type	| cleanup_table_list	| scenario_input_source_system	|scenario_input_target_table |	scenario_input_business_date | scenario_input_source_details |	scenario_input_format |
|------------|------------|------------|------------|------------|------------|------------|------------|
| historical_run | incremental_refresh |	[]	| VHP |	DF_GROUP |	1900-01-01	| |	|


#### Test Objectives
- ✅ **Multi-Source Processing**: Verify pipeline can handle multiple source systems
- ✅ **Incremental Logic**: Test SCD processing with existing data. SCD type 1 and Type 2 shoul dbe based on source systems. 
- ✅ **Source System Differentiation**: Ensure data from different sources is properly identified. Validate source system is populated correctly
- ✅ **Data Merging**: Validate how data from VHP merges with existing Facets data
- ✅ **Data Quality, custom transfomation, in-place data standardization**: Verify rules are applied correctly

#### Expected Outcomes
- VHP data integrated with existing Facets data
- Source system column properly populated
- SCD Type 1 and 2 records created for historical tracking
- Data lineage maintained across source systems

#### Known Issues
- **Investigation Required**: `dt_d_group_c73550` table showing 0 records after execution
- **Action Item**: Requires investigation by development team (Mahesh)

### Scenario 3: Incremental Processing

#### Purpose
Validates daily incremental processing capabilities with current business dates. Each input data item can have different business dates. 

#### Configuration
|scenario_type	|refresh_type	|cleanup_table_list	|scenario_input_source_system	|scenario_input_target_table|	scenario_input_business_date	|scenario_input_source_details	|scenario_input_format
|------------|------------|------------|------------|------------|------------|------------|------------|
|incremental_run|	incremental_refresh	| []	|Facets|DF_GROUP|	2025-04-08| | | |		
|incremental_run|	incremental_refresh	| []	|Facets|DF_GROUP_COUNT|	2025-04-22| | |		

#### Test Objectives
- ✅ **Incremental Processing**: Test daily incremental data processing
- ✅ **Business Date Filtering**: Validate date-based data filtering
- ✅ **Change Detection**: Ensure only changed/new records are processed
- ✅ **Performance**: Verify processing efficiency with different data volumes
- ✅ **Multi-Date Processing**: Test handling of different business dates

#### Expected Outcomes
- Only new/changed records processed
- Historical data remains unchanged
- Processing time significantly reduced compared to full refresh
- Change tracking columns updated appropriately
- Audit logs show incremental processing details

### Scenario 4: File-Based Data Correction

#### Purpose
Tests data correction workflow when vendors provide corrected data files.
DLT bronze loads data incrementally and hence the data should be append only.
This scenario will basically delete the existing data in the landing table for the particular business data and load the corrected data files automatically.
This use case will expect a corrected file as input.

#### Configuration
|scenario_type|	refresh_type|	cleanup_table_list|	scenario_input_source_system|	scenario_input_target_table|	scenario_input_business_date|	scenario_input_source_details|	scenario_input_format|
|------------|------------|------------|------------|------------|------------|------------|------------|
|file_correction|	|incremental_refresh|	[]	|Facets	|DF_GROUP	|2025-04-08	|/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/corrected/BusinessStartDate=20250408/cdc_cmc_grgr_group/|	parquet|

#### Test Objectives
- ✅ **File-Based Corrections**: Test correction processing from new files
- ✅ **Delete & Insert Logic**: Verify existing records are deleted and new ones inserted
- ✅ **Audit Trail**: Ensure correction activities are properly logged
- ✅ **Data Integrity**: Validate data consistency after corrections
- ✅ **Path Resolution**: Test custom file path handling for corrections

#### Expected Outcomes
- Original records for business date 20250408 are deleted
- Corrected records are inserted
- Audit trail shows correction activity
- Data quality rules applied to corrected data
- Historical integrity maintained

### Scenario 5: Delta-Based Data Correction

#### Purpose
Tests database-driven data correction using Delta Lake capabilities. DLT bronze loads data incrementally and hence the data should be append only.
This scenario will basically delete the existing data in the landing table for the particular business data and load the corrected data files. The production support team will be responsible for creating the correction SQL scripts. The SQL scripts should contain the delete and insert scripts. 
*Note:* 
1. **This scenario will also work for reloading the source data again**
2. **Make sure the operation code is populated correctly in landing table**.

#### Configuration

|scenario_type	|refresh_type	|cleanup_table_list	|scenario_input_source_system	|scenario_input_target_table|	scenario_input_business_date	|scenario_input_source_details	|scenario_input_format|
|------------|------------|------------|------------|------------|------------|------------|------------|
|delta_correction|	incremental_refresh	|[]	|Facets	|DF_GROUP	|2025-04-08	| | | |	

```python

# Example for creating the corrected files
# Step1: Create a delta_correction_table
CREATE or REPLACE TABLE edl_dev_ctlg.structured.delta_correction_table AS
Select * from edl_dev_ctlg.structured.dlt_landing_cmc_grgr_group_c73550 where GRGR_ID = '36S99ERC' and GRGR_CK = 44854;

# Step2: Update the data with corrections. Make sure the operation code is validated correctly. For e.g. if we have to delete some existing records as a aprt of corretion, sinc eits soft delete, make sure the operation code is set to 1
Update edl_dev_ctlg.structured.delta_correction_table set GRGR_NAME='RM STRATEGIC, L.L.C Corrected records';
Select * from edl_dev_ctlg.structured.delta_correction_table;

# Step3:Physically delete the records from landing table.
Delete from edl_dev_ctlg.structured.dlt_landing_cmc_grgr_group_c73550 where GRGR_ID = '36S99ERC' and GRGR_CK = 44854;

# Step4: Insert data from the corrected table into the landing table
INSERT INTO edl_dev_ctlg.structured.dlt_landing_cmc_grgr_group_c73550
Select * from edl_dev_ctlg.structured.delta_correction_table;


```

#### Test Objectives
- ✅ **File-Based Corrections**: Test correction processing from new files
- ✅ **Delete & Insert Logic**: Verify existing records are deleted and new ones inserted
- ✅ **Audit Trail**: Ensure correction activities are properly logged
- ✅ **Data Integrity**: Validate data consistency after corrections

#### Expected Outcomes
- Original records for business date 20250408 are deleted
- Corrected records are inserted
- Audit trail shows correction activity
- Data quality rules applied to corrected data
- Historical integrity maintained



<!-- #### Test Data Availability
```python
# Check test data availability
def validate_test_data():
    paths = [
        "/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/",
        "/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/VHP/Test/",
        "/Volumes/edl_dev_ctlg/rawfiles/raw/EDF/Facets/Test/corrected/"
    ]
    
    for path in paths:
        files = dbutils.fs.ls(path)
        assert len(files) > 0, f"No test data found in {path}"
```
 -->


### Test Data Requirements

#### Minimum Data Requirements
- **Facets Historical**: 1000+ group records, 500+ count records
- **VHP Historical**: 500+ group records
- **Incremental Data**: 100+ new/changed records per business date
- **Corrected Data**: 50+ corrected records for testing

#### Data Quality Requirements
- **Valid Records**: 80% of test data should pass all DQ rules
- **Invalid Records**: 20% should have known quality issues for quarantine testing
- **Overlapping Keys**: Include overlapping primary keys between source systems
- **Historical Coverage**: Cover multiple business dates for SCD testing

---

## Summary

The integration test scenarios provide comprehensive validation of the DLT pipeline's functionality across all critical use cases. Regular execution of these tests ensures:

- **Data Pipeline Reliability**: Continuous validation of pipeline functionality
- **Multi-Source Integration**: Proper handling of data from multiple systems
- **Data Quality Assurance**: Validation of DQ rules and quarantine mechanisms
- **Change Management**: Testing of incremental processing and corrections
- **Performance Monitoring**: Tracking of processing times and resource usage

For successful test execution, ensure all prerequisites are met, follow the sequential execution order, and validate results against the success criteria provided. 