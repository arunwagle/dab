-- =====================================================================================
-- DLT METADATA TABLES - DROP SCRIPT
-- =====================================================================================
-- 
-- Purpose: Drop all DLT (Delta Live Tables) metadata configuration tables
-- Schema:  edl_dev_ctlg.structured
-- Author:  Generated for BCBSLA DLT Pilot Project
-- Date:    Generated automatically
-- 
-- This script drops all DLT metadata configuration tables:
-- 1. dlt_metadata_cdc_config          - Change Data Capture configuration
-- 2. dlt_metadata_dq_config           - Data Quality configuration  
-- 3. dlt_metadata_source_config       - Source configuration
-- 4. dlt_metadata_transform_config    - Transform configuration
-- 
-- Usage:
-- - Run this script to clean up/remove all DLT metadata tables
-- - Safe to run multiple times (uses DROP TABLE IF EXISTS)
-- - Includes verification queries to confirm tables are dropped
-- 
-- WARNING: This will permanently delete all metadata configuration data!
-- Make sure to backup data if needed before running this script.
-- =====================================================================================

-- Set the schema context
USE CATALOG edl_dev_ctlg;
USE SCHEMA structured;

-- =====================================================================================
-- SECTION 2: DROP TABLES
-- =====================================================================================
-- Drop all DLT metadata tables in reverse dependency order (safest approach)

-- Drop Transform Configuration Table
DROP TABLE IF EXISTS dlt_meta_transform_config;

-- Drop Data Quality Configuration Table
DROP TABLE IF EXISTS dlt_metadata_dq_config;

-- Drop CDC Configuration Table
DROP TABLE IF EXISTS dlt_metadata_cdc_config;

-- Drop Source Configuration Table  
DROP TABLE IF EXISTS dlt_meta_source_config;


-- Drop Runtime Configuration Table
DROP TABLE IF EXISTS dlt_runtime_config;

-- =====================================================================================
-- END OF DROP SCRIPT
-- ===================================================================================== 