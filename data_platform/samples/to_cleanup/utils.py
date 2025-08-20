# Databricks notebook source
import importlib


def process_sources(catalog_name, schema_name, source):
    query  = f"""
        SELECT * FROM catalog.schema.metadata_source_info where source = '{source}';
    """
    print (query)
    metadata_source_df = spark.sql (query)

    return metadata_source_df

def get_cdc_attributes(catalog_name, schema_name, source_table, target_table):
    query  = f"""
        SELECT * FROM catalog.schema.metadata_dlt_cdc where source_table = '{source_table} and target_table = '{target_table}';
    """
    print (query)
    cdc_df = spark.sql (query)

    return cdc_df


'''
    1. Get the rules from the table for all the actions
    2. Return a list of rules as a list of dict
    
    4 Types of DQ rules are applied
    1. FAIL - Based on this, the workflow will fail
    2. QUARANTINE - Based on this, the row is dropped but an audit record will be logged    
    3. WARN - Based on this, the row is valid but an audit record will be logged
    4. KEEP - Based on this, the row is valid. 

    E.g.
    return [
        {
        "name": "group_not_null",
        "constraint": "GROUP_SOURCE_KEY IS NOT NULL",
        "source_table": "CMC_GRGC_GROUP_COUNT",
        "target_table": "DF_GROUP_COUNT",
        "source_column_name": "GROUP_SOURCE_KEY",
        "action": "FAIL"
        },
        {
        "name": "emp_count_gt_zero",
        "constraint": "EMPLOYEE_COUNT > 0",
        "source_table": "CMC_GRGC_GROUP_COUNT",
        "target_table": "DF_GROUP_COUNT",
        "source_column_name": "EMPLOYEE_COUNT",
        "action": "QUARANTINE"
        }
    ]

'''
def get_rules_as_list_of_dict(catalog_name, schema_name, source_table, target_table):
    """
    Loads data quality rules from a table.
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
    """
    rules_list = {}

    query = f"SELECT source_table, target_table, source_column_name, action, rule FROM {catalog_name}.{schema_name}.metadata_dlt_validation_rules WHERE source_table = '{source_table}' and target_table = '{target_table}' and is_active = true"
    print (f"get_data_quality_rules::query::{query}")
    df = spark.sql (query)

    
    return rules_list

'''
    
    The return type will be dictionary of rule_name and constraint.
    E.g. 
    return {
        row['name']: row['constraint']
        for row in get_rules_as_list_of_dict(catalog_name, schema_name, source_table, target_table)
        if row['source_table'] == source_table and row['target_table'] == target_table and row['action'] == action 
    }

'''
def get_data_quality_rules(catalog_name, schema_name, source_table, target_table, action):
    """
    Loads data quality rules from a table.
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
    """
    
    rules = {}

    return rules


def get_data_trandformation_rules(catalog_name, schema_name, source_table, target_table, action):
    """
    Loads data transformation rules from a table.
    :param tag: tag to match
    :return: dictionary of rules that matched the tag
    """
    
    transformation_rules = {}

    return transformation_rules



'''
    Load modules dynamically based on the target dimension table
    Also load mapping based on the layer (silver or gold)

'''
def load_table_config(dimension_table_name, p_target_table, layer):
    """Dynamically import the correct module and get column mappings."""
    try:
        module = importlib.import_module(f"{layer}_{dimension_table_name}")  # Import table-specific module        
        column_mappings = module.get_column_mappings() if hasattr(module, "get_column_mappings") else {}
        filter_condition = module.get_filter_condition() if hasattr(module, "get_filter_condition") else None
        return column_mappings, filter_condition 
    except ModuleNotFoundError:
        print(f"Warning: No module found for {table_name}. Returning empty mapping.")
        return {}, None
