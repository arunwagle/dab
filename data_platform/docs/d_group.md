# Dimension D_Group Entity

## Table of Contents
- [Key Components](#key-components)
- [Primary Data Sources for D_Group](#primary_data_sources_d_group)

## Key Components

### 1. Core Framework (`src/framework/d_group`)
Entity specific reusable assets are defines in this folder

1. d_group_silver.py
2. d_group_gold.py


## Primary Data Sources for D_Group
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