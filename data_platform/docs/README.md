# DLT Project 

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Key Components](#key-components)
- [Data Flow](#data-flow)
- [CICD Process](#cicd-process)
- [Dashboards](#dashboards)
- [Development Guide](#development-guide)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)
- [Change Requests](#change-requests)
- [Contributing](#contributing)

## Overview

The **DLT Pilot Project** is a comprehensive data engineering solution built on Databricks Delta Live Tables (DLT) framework. This project implements a metadata-driven ETL pipeline for processing healthcare insurance group data from multiple source systems (Facets, VHP) into a unified data model.  
The current project demonstrates the end to ingestion pipeline for **D_Group dimension** with 3 common data model(CDM) entities. 

### Purpose
- Implement a scalable, metadata-driven data pipeline using Delta Live Tables
- Process healthcare group data from multiple source systems
- Provide data quality validation, data standardization and transformation capabilities
- Support both incremental and full refresh processing modes
- Provide support for Type 1 and Type 2 SCD.

### Key Features
- **Metadata-Driven Architecture**: Configuration-driven pipeline behavior
- **Multi-Source Integration**: Support for Facets, VHP.
- **Data Quality Expectations Framework**: Built-in validation rules and quarantine mechanisms
- **Change Data Capture**: Automatic tracking of data changes over time
- **Slowly Changing Dimensions**: Support for SCD Type 1 and Type 2
- **Flexible Processing**: Support for both incremental and full refresh modes
- **Testing Framework**: Comprehensive integration testing capabilities

## Architecture

The project follows a medallion architecture

### D_Group Flow Diagram
![D_Group Flow Diagram](/AzureDatabricks/data_platform/docs/images/D_Group_Job.png)

### D_Group Silver DLT Pipeline
![D_Group Flow Diagram](/AzureDatabricks/data_platform/docs/images/D_Group_Silver_DLT_Pipeline.png)

### D_Group Gold DLT Pipeline
![D_Group Flow Diagram](/AzureDatabricks/data_platform/docs/images/D_Group_Gold_DLT_Pipeline.png)


## Data Flow Architecture

### System Components

1. **Source Systems**
   - **Facets**: TODO: Describe source system
   - **VHP**: TODO: Describe source system

2. **Processing Layers**
   - **Landing Zone**: Raw data ingestion with minimal transformation
   - **Bronze**: Raw data ingestion with minimal transformation
   - **Silver**: Cleaned, validated, and transformed data
   - **Gold**: Business-ready aggregated data

3. **Metadata Framework**
   - Configuration-driven pipeline behavior
   - Dynamic rule application
   - Flexible source to target mapping

## Key Components

### 1. Core Framework (`src/framework/`)

#### DLT Utils (`dlt_utils.py`)
The utilities defining all the DLT specific functions.   
Examples include.  
- **`generate_bronzetables()`**: Creates bronze layer tables from source data
- **`generate_scd_tables()`**: Implements Slowly Changing Dimensions
- **`generate_dq_table()`**: Applies data quality rules
- **`generate_tranformation_rules_df()`**: Applies business transformation rules

#### Utils (`utils.py`)
The utilities defining all helper functions.   
Examples include.  
- **`get_runtime_parameters()`**: Retrieves runtime configuration
- **`get_source_metadata()`**: Fetches source system metadata
- **`get_scd_attributes()`**: Retrieves CDC configuration
- **`get_data_validation_rules()`**: Fetches data quality rules

#### UDF Utils (`udf_utils.py`)
The utilities defining all UDF functions used in DLT processing. The UDF functions can be custom defined or Databricks SQL functions.
Examples include.    
- Custom user-defined functions for data processing
- String substitution utilities for dynamic path handling

### 2. Notebooks (`notebooks/`)

#### Data Engineering Notebooks
- **`job_setup_notebook.ipynb`**: Initial job setup and configuration
- **`job_load_landing_zone_data_notebook.ipynb`**: Data loading from source systems
- **`dlt_silver_integrated_notebook.ipynb`**: Silver layer processing pipeline
- **`dlt_gold_integrated_notebook.ipynb`**: Gold layer aggregation pipeline

#### Integration Testing
- **`job_integration_test_notebook.ipynb`**: End-to-end pipeline testing

**Notebook naming conventions to follow**
1. For a regular notebook job start the name with job_<logical name>_notebook.ipynb
2. For a DLT pipeline notebook job start the name with dlt_<logical name>_notebook.ipynb
3. Use .ipynb extensions for all the notebooks. Its easier in editors to read and modify the code. 

### 3. Configuration (`resources/jobs/`)

#### Job Configurations
- **`dlt_d_group.job.yml`**: Main DLT job configuration
- **`dlt_d_group.pipeline.yml`**: DLT pipeline definitions
- **`integration_tests.job.yml`**: Integration test job configuration
- **`dlt_setup_metadata_tables.job.yml`**: Job to setup metadata tables

#### databricks.yml configuration
   ### Environment Variables
   The `databricks.yml` defines these under variables section and in CI/CD pipelines and will change less frequently once its configured.
   ```yaml
   # Databricks Configuration
   catalog: "edl_dev_ctlg"
   bronze_schema: "structured"
   silver_schema: "curated"
   gold_schema: "lakeview"

   # Processing Configuration
   table_suffix: ""  # For development isolation, set to ${workspace.current_user.short_name}
   warehouse_id: "WAREHOUSE_ID_TO_RUN_THE_SQL_JOB"
   cluster_policy_id: "CLUSTER_POLICY_FOR_CLUSTERS"
   all_purpose_compute_cluster_id: "ALL_PURPOSE_CLUSTER_ID"
   pipeline_cluster_definition: "The cluster to use for DLT pipelines"
   ```

**Config file naming conventions to follow**
1. For a DLT project defining regular workflow jobs, use dlt_<dimension/entity name>.job.yml
2. For a DLT project defining DLT pipelines, use dlt_<dimension/entity name>.pipeline.yml
3. Each Dimension entity should be defined in its own yml files for readability and management.

### 4. Metadata Tables (`sql/`)
Scripts to manage reference data.  
- **`dlt_metadata_create_script.sql`**: Create and load metadata tables
- **`dlt_metadata_all_tables_drop.sql`**: Drop metadata tables
- **`metadata_d_group_facets_vhp_insert_script.sql`**: D-Group metadata insert scripts
- **`dlt_integration_tests_script.sql`**: Integration test data population script

#### Core Metadata Tables
- **`dlt_meta_source_config`**: Source system configuration
- **`dlt_meta_cdc_config`**: Data transformation rules
- **`dlt_meta_dq_config`**: Data quality validation rules
- **`dlt_meta_transform_config`**: Change Data Capture configuration
- **`dlt_runtime_config`**: Runtime configuration data for DLT pipelines. This table holds the source data required to run the workflow.
- **`dlt_dq_audit`**: DLT Data Quality Audit table to store data quality validation failures.

### 5. CI/CD configurations (`cicd/*.yml`)
This will define the CI/CD confogurations to manage the continuous integration and deployment process.

### 6. Unit test cases (`tests/*`)
All unit test cases for the framework project are defined in this folder.

**Unit test cases naming conventions to follow**
1. The folder structure should match the folder struture of the actual code. E.g. tests/framework/dlt_utils_tests.py  

## Data Flow

### 1. Data Ingestion
```
ADLS Source Systems → Landing Zone → Bronze Tables
```
- Raw data from Facets, VHP, and BCBSLA MA systems
- Minimal transformation, preserving original structure
- Addition of metadata columns (load timestamp, source, business date)

### 2. Data Quality Processing
```
Bronze Tables → DQ Validation → Clean/Quarantine Tables
```
- Configurable validation rules applied
- Failed records quarantined for review
- Passed records continue to transformation

### 3. Data Transformation
```
Clean Tables → Business Rules → Transformed Tables
```
- Business logic applied based on metadata configuration
- Column mapping and data type conversions
- Standardization across source systems

### 4. SCD Processing
```
Transformed Tables → CDC Logic → Silver Tables
```
- Change Data Capture implementation
- Support for SCD Type 1 and Type 2
- Historical data preservation

### 5. Business Aggregation
```
Silver Tables → Business Logic → Gold Tables
```
- Business-ready data models
- Aggregations and calculations
- Optimized for reporting and analytics


## CI/CD Process

### Databricks Asset Bundle CI/CD Process
![Databricks Asset Bundle CI/CD Process](/AzureDatabricks/data_platform/docs/images/bundles-cicd.png)

[Ci/CD Best Practices](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/ci-cd/best-practices)

A common flow for an Azure Databricks CI/CD pipeline with bundles is:

1. Store: Store your Azure Databricks code and notebooks in a version control system like Git. This allows you to track changes over time and collaborate with other team members. See CI/CD with Databricks Git folders (Repos) and bundle Git settings.  
2. Code: Develop code and unit tests in an Azure Databricks notebook in the workspace or locally using an external IDE. Azure Databricks provides a Visual Studio Code extension that makes it easy to develop and deploy changes to Azure Databricks workspaces.   
3. Build: Use Databricks Asset Bundles settings to automatically build certain artifacts during deployments. See artifacts. In addition, Pylint extended with the Databricks Labs pylint plugin help to enforce coding standards and detect bugs in your Databricks notebooks and application code.   
4. Deploy: Deploy changes to the Azure Databricks workspace using Databricks Asset Bundles in conjunction with tools like Azure DevOps, Jenkins, or GitHub Actions. See Databricks Asset Bundle deployment modes. For GitHub Actions examples, see GitHub Actions. 
   - Deployment to dev can either be controlled throught Azure Pipelines or from IDE directly. 
   - Deployment to staging, will be done by using service principal. Once the code is tested in development , checking the code into feature branch. Create a PR and specify reviewers. Once the code is reviewed and merged into the main branch, the staging pipeline will deploy the code to the staging workspace.
   - Deployment to production will  be a controlled process. Once all the changes are tested on QA and ready to move to production, a relase branch will be created. Review all user stories for and then trigger the production pipeline to deploy the changes to production. 
5. Test: Develop and run automated tests to validate your code changes using tools like pytest. To test your integrations with workspace APIs, the Databricks Labs pytest plugin allows you to create workspace objects and clean them up after tests finish.  
6. Run: Use the Databricks CLI in conjunction with Databricks Asset Bundles to automate runs in your Azure Databricks workspaces. See Run a job, pipeline, or script.  
7. Monitor: Monitor the performance of your code and workflows in Azure Databricks using tools like Azure Monitor or Datadog. This helps you identify and resolve any issues that arise in your production environment.  
8. Iterate: Make small, frequent iterations to improve and update your data engineering or data science project. Small changes are easier to roll back than large ones.

## Dashboards

TODO

## Development guide
[Development guide](/AzureDatabricks/Pilots/data_platform/docs/development_guide.md)


### Support Resources
- **Databricks Documentation**: https://docs.databricks.com/
- **Delta Live Tables Guide**: https://docs.databricks.com/delta-live-tables/
- **Project Issues**: Create issues in the project repository

## Change Requests 

### Implementation Requirements
1. Data Normalization metadata tables, currently in denormalized tables. 
2. Cleanup attributes not required from the metadata tables
3. For d_group_count snapshot, handle the computation of updates and inserts and no changes before passing it downstream. Currently we only compute deletes.
4. Solidify documentation [Team]
5. Use current architecture to load other dimensions. E.g. claims data
6. Code cleanup
7. Make all data smaller case in the table, make code changes accordingly.
8. Remove mocked_ind from dlt_meta_dq_config [Mahesh]
9. Metadata management - How to do manage ? Do we need a Databrciks apps ? [Arun/Arvind ]
10. Service principal deployment - Work with Pradeep on CI/CD setup and SQL script execution job [Arun ]
11. Serverless [Arun - Completed]
12. Dashboards - Billing Dashboard [Arun/Pradeep ], app dashboards [Phi]
13. Address normalization API [Arun]
14. Seperate DLT pipelines by sources [Arun ]
15. Handle intra and inter duplicate records [Mahesh]
16. Change table name dlt_runtime_config to dlt_runtime_parameters. Modify references in code. Also add attributes like job_id and job_run_id [Mahesh]



## Contributing

### Code Style
- Follow PEP 8 for Python code
- Use meaningful variable names
- Add docstrings for all functions
- Include unit tests for new functionality

### Pull Request Process
1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit pull request
5. Code review and approval

### Release Process
1. Update version in `setup.py`
2. Create release notes
3. Tag release
4. Deploy to production

---

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contact

For questions or support, please contact:
- **Author**: xxx@labblue.com
- **Project Repository**: [Azure Repos Repository URL](https://dev.azure.com/bcbsla/Enterprise-Information-Management/_git/DataFoundation)


TEST