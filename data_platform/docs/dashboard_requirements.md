# Dashboard Requirements - BCBSLA DLT Pilot Project

## Table of Contents
- [Overview](#overview)
- [Project Context](#project_context)
- [Primary Data Sources](#primary_data_sources)
- [Data Pipeline Operations Dashboard](#data-pipeline-operations-dashboard)
- [Data Quality & Governance Dashboard](#data-quality--governance-dashboard)
- [Executive Summary Dashboard](#executive-summary-dashboard)

## Overview

This document outlines the dashboard requirements for the BCBSLA DLT (Delta Live Tables) pilot project monitoring and analytics solution. The dashboards are designed to support operational monitoring, data governance, and executive oversight of the healthcare insurance group data processing pipeline.

## Project Context
- **Data Sources**: Facets (healthcare claims system), VHP (Vantage Health Plan Source)
- **Architecture**: Medallion architecture (Landing Zone -> Bronze → Silver → Gold layers)
- **Primary Entity**: D_Group dimension with 3 CDM entities (DF_Group, DF_Group_Count, DF_Group_History)
- **Processing Modes**: Historical load, incremental processing, data corrections

## Primary Data Sources
```sql
-- Core tables for dashboard data
1. dlt_dq_audit - Data quality metrics and violations
2. dlt_metadata_dq_config - Data quality rule definitions
3. dlt_metadata_source_config - Source system configurations
4. dlt_df_group, dlt_df_group_count, dlt_df_group_history, dlt_d_group - Business data
    -   dlt_brz_bcbsla_ma_group
    -   dlt_brz_bcbsla_ma_group_history
    -   dlt_brz_cmc_grgc_group_count
    -   dlt_brz_cmc_grgr_group
    -   dlt_brz_cmc_grgr_group_history
    -   dlt_slv_clean_bcbsla_ma_group_fail_step
    -   dlt_slv_clean_bcbsla_ma_group_history_fail_step
    -   dlt_slv_clean_cmc_grgc_group_count_fail_step
    -   dlt_slv_clean_cmc_grgr_group_fail_step
    -   dlt_slv_clean_cmc_grgr_group_history_fail_step
    -   dlt_slv_clean_bcbsla_ma_group_quarantine_step
    -   dlt_slv_clean_bcbsla_ma_group_history_quarantine_step
    -   dlt_slv_clean_cmc_grgc_group_count_quarantine_step
    -   dlt_slv_clean_cmc_grgr_group_history_quarantine_step
    -   dlt_slv_clean_cmc_grgr_group_quarantine_step
    -   dlt_slv_clean_bcbsla_ma_group_warn_step
    -   dlt_slv_clean_bcbsla_ma_group_history_warn_step
    -   dlt_slv_clean_cmc_grgc_group_count_warn_step
    -   dlt_slv_clean_cmc_grgr_group_history_warn_step
    -   dlt_slv_clean_cmc_grgr_group_warn_step
    -   dlt_slv_transform_bcbsla_ma_group
    -   dlt_slv_transform_bcbsla_ma_group_history
    -   dlt_slv_transform_cmc_grgc_group_count
    -   dlt_slv_transform_cmc_grgr_group
    -   dlt_slv_transform_cmc_grgr_group_history
    -   dlt_slv_mapped_df_group_c73550_facets
    -   dlt_slv_mapped_df_group_vhp
    -   dlt_slv_mapped_df_group_history_facets
    -   dlt_slv_mapped_df_group_history_vhp
    -   dlt_slv_mapped_df_group_count_facets (Note: No VHP source for df_group_count)
    -   dlt_slv_mapped_df_group
    -   dlt_slv_mapped_df_group_history
    -   dlt_slv_mapped_df_group_count
    -   dlt_slv_scd_df_group
    -   dlt_slv_scd_df_group_history
    -   dlt_slv_scd_df_group_count
    -   dt_d_group
    -   inc_d_group
    -   stg_d_group
    -   d_group

5. Pipeline execution logs - Job run history and performance
6. System resource metrics - Infrastructure utilization
```

## Data Pipeline Operations Dashboard

### Target Audience
- Data Engineers
- DevOps Teams
- Platform Administrators
- On-call Support Staff

### Dashboard Objectives
- Monitor real-time pipeline health and performance
- Enable rapid troubleshooting and root cause analysis
- Track operational KPIs and SLA compliance
- Provide actionable insights for pipeline optimization

### Key Metrics & Visualizations

#### 1. Pipeline Execution Overview
**Visualization Type**: Status Cards + Time Series Chart
- **Current Pipeline Status**: Running/Success/Failed/Scheduled
- **Success Rate (Last 30 days)**: Percentage with trend indicator
- **Average Processing Time**: By processing mode (incremental vs full refresh)
- **Jobs Executed Today**: Count with comparison to previous day
- **Active Pipelines**: Currently running pipeline count

**Data Sources**:
```sql
-- Pipeline execution metrics
SELECT 
    job_id,
    job_name,
    run_id,
    start_time,
    end_time,
    DATEDIFF(minute, start_time, end_time) as duration_minutes,
    result_state,
    dimension_table,
    scenario
FROM job_runs 
WHERE start_time >= DATEADD(day, -30, GETDATE())
```

#### 2. Processing Performance Metrics (For Pilot)
**Visualization Type**: Multi-line Chart + Heat Map

**Processing Time Trends**:
- Processing duration by business date

**Record Processing Volumes**:
- Records processed per layer (Bronze/Silver/Gold)
- Data volume trends by source system

**Data Sources**:
```sql
-- Processing volume metrics from audit tables
SELECT 
    processing_time,
    dimension_table,
    target_table,
    business_date,
    COUNT(*) as records_processed,
    source_system
FROM dlt_dq_audit
GROUP BY processing_time, dimension_table, target_table, business_date, source_system
```

#### 3. Error Analysis & Alerting (For Pilot)
**Visualization Type**: Error Funnel + Alert Timeline

**Error Breakdown**:
- Error categories (Data Quality, System, Configuration)
- Error frequency by source system
- Error resolution time tracking
- Top 10 most frequent errors

**Alert Management**:
- TODO: Define Escalation patterns

**Data Sources**:
```sql
-- Error analysis from job logs and DQ audit
SELECT 
    error_category,
    error_message,
    COUNT(*) as occurrence_count,
    MIN(first_occurrence) as first_seen,
    MAX(last_occurrence) as last_seen,
    source_system
FROM pipeline_errors 
WHERE error_date >= DATEADD(day, -7, GETDATE())
GROUP BY error_category, error_message, source_system
```

#### 4. Resource Utilization
**Visualization Type**: Gauge Charts + Resource Timeline

**Cost Tracking**:
- Compute costs by pipeline


## Data Quality & Governance Dashboard
### TODO: This section needs to be defined in detail
### Target Audience
- Data Stewards
- Compliance Officers
- Data Governance Committee
- Business Analysts
- Quality Assurance Teams

### Dashboard Objectives
- Monitor data quality compliance across all source systems
- Track data governance policy adherence
- Provide visibility into data lineage and transformations
- Enable proactive data quality issue identification

### Key Metrics & Visualizations

#### 1. Data Quality Scorecard
**Visualization Type**: Scorecard Matrix + Traffic Light System

**Overall Quality Score**:
- Composite DQ score (weighted by rule criticality)
- Quality score by source system (Facets vs VHP)
- Quality score by target table (DF_Group, DF_Group_Count, DF_Group_History)
- Quality trend over time (30-day rolling average)

**Rule Category Performance**:
- **Completeness Rules**: NULL checks, required field validation
- **Validity Rules**: Data type, format, range validation
- **Consistency Rules**: Cross-system data matching
- **Accuracy Rules**: Business logic validation

**Data Sources**:
```sql
-- Data quality scorecard metrics
WITH dq_summary AS (
    SELECT 
        target_table,
        business_date,
        dq_type,
        COUNT(CASE WHEN constraint_passed = true THEN 1 END) as passed_rules,
        COUNT(*) as total_rules,
        ROUND(100.0 * COUNT(CASE WHEN constraint_passed = true THEN 1 END) / COUNT(*), 2) as quality_score
    FROM dlt_dq_audit 
    WHERE business_date >= DATEADD(day, -30, GETDATE())
    GROUP BY target_table, business_date, dq_type
)
SELECT * FROM dq_summary
```

#### 2. Rule Violation Analysis
**Visualization Type**: Pareto Chart + Drill-down Tables

**Violation Breakdown**:
- Top 10 most violated rules
- Violation count by rule severity (FAIL/WARN/QUARANTINE)
- Violation trends by business date
- Source system violation comparison

**Quarantine Analysis**:
- Quarantine rate by table and rule
- Quarantined record volume trends
- Quarantine resolution time
- Business impact of quarantined data

**Data Sources**:
```sql
-- Rule violation analysis
SELECT 
    source_column_name,
    constraint as rule_definition,
    action as rule_severity,
    COUNT(*) as violation_count,
    business_date,
    target_table
FROM dlt_metadata_dq_config dq
JOIN dlt_dq_audit audit ON dq.input_table = audit.validated_table
WHERE audit.reason IS NOT NULL 
GROUP BY source_column_name, constraint, action, business_date, target_table
ORDER BY violation_count DESC
```

#### 3. Data Completeness Tracking
**Visualization Type**: Completion Matrix + Gap Analysis

**Completeness Metrics**:
- Field-level completeness percentage
- Record completeness by source system
- Business date coverage analysis
- Critical field availability tracking

**Gap Analysis**:
- Missing business dates identification
- Incomplete record patterns
- Source system data availability windows
- Processing coverage gaps

**Data Sources**:
```sql
-- Data completeness analysis
SELECT 
    target_table,
    business_date,
    source_system,
    column_name,
    COUNT(*) as total_records,
    COUNT(CASE WHEN column_value IS NOT NULL THEN 1 END) as non_null_records,
    ROUND(100.0 * COUNT(CASE WHEN column_value IS NOT NULL THEN 1 END) / COUNT(*), 2) as completeness_pct
FROM (
    -- Union all target tables with their key columns
    SELECT 'DF_Group' as target_table, business_date, Source_System_Code as source_system, 'Group_Source_Key' as column_name, Group_Source_Key as column_value FROM dlt_df_group
    UNION ALL
    SELECT 'DF_Group' as target_table, business_date, Source_System_Code, 'Group_Name', Group_Name FROM dlt_df_group
    -- Add more critical columns...
) completeness_data
GROUP BY target_table, business_date, source_system, column_name
```

#### 4. Data Transformation Validation
**Visualization Type**: Before/After Comparison + Transformation Flow

**Transformation Metrics**:
- Record count validation (Bronze → Silver → Gold)
- Data type conversion success rates
- Business rule application effectiveness
- Transformation processing time by rule

**Validation Results**:
- Source vs target record reconciliation
- Data standardization effectiveness
- Custom UDF performance metrics
- Transformation rule coverage


## Executive Summary Dashboard

### Target Audience
- C-Level Executives
- Program Directors
- Project Sponsors
- Business Stakeholders
- Steering Committee Members

### Dashboard Objectives
- Provide high-level project health visibility
- Track business value delivery
- Monitor strategic KPIs and ROI
- Enable data-driven decision making

#### Data Asset Overview
**Visualization Type**: Asset Portfolio + Growth Trends

**Data Portfolio Health**:
- **Total Data Volume**: TB processed across all layers
- **Data Growth Rate**: Month-over-month growth percentage
- **Source System Coverage**: Facets vs VHP contribution
- **Data Freshness**: Average age of processed data

**Quality & Governance**:
- **Data Quality Score**: Weighted average across all entities
- **Compliance Status**: Regulatory requirement adherence
- **Data Lineage Coverage**: Percentage of documented flows
- **Security Posture**: Access control and audit compliance

### Refresh Requirements
- **Executive KPIs**: Daily refresh (morning updates)
- **Business metrics**: Weekly refresh
- **Strategic tracking**: Monthly refresh
- **Real-time alerts**: Critical issues only

---

#### Derived Metrics Tables
```sql
-- Recommended aggregation tables for performance
CREATE TABLE dashboard_daily_summary AS (
    SELECT 
        business_date,
        target_table,
        source_system,
        total_records,
        quality_score,
        processing_duration,
        error_count
    FROM daily_pipeline_metrics
);

CREATE TABLE dashboard_dq_trends AS (
    SELECT 
        rule_name,
        violation_count,
        trend_direction,
        severity_level,
        business_date
    FROM quality_trend_analysis
);
```

### Security & Access Control

#### Role-Based Access
```yaml
Executive_Users:
  - Executive Summary Dashboard (Full Access)
  - High-level metrics only
  - No drill-down to sensitive data

Data_Engineers:
  - Operations Dashboard (Full Access)
  - Data Quality Dashboard (Full Access)
  - Administrative functions

Data_Stewards:
  - Data Quality Dashboard (Full Access)
  - Executive Summary (Read Only)
  - Rule configuration access

Business_Users:
  - Executive Summary (Limited Access)
  - Business metrics only
  - No technical details
```

#### Data Privacy
- PII/PHI data masking in dashboards
- Audit trail for dashboard access
- Secure data transmission (HTTPS/TLS)
- Regular access review and certification

---