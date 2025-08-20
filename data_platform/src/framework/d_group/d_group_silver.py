from pyspark.sql import functions as F
from pyspark.sql import types as T



bronze_table_to_column_mapping = {
    # This dictionary maps source tables to their required and additional columns for different schemas and tables within those schemas. It also has a GLOBALS schema for all tables, these are columns that will be added to all tables.
    "GLOBALS": {
        "all": {
            "all": {
                "global_columns": {
                    "processing_time": "long",
                }
            }
        }
    },
    "Facets": {
        "test": {
            "cmc_grgr_group": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_NAME",
                    "GRGR_MCTR_TYPE",
                    "GRGR_ITS_CODE",
                    "GRGR_ORIG_EFF_DT",
                    "GRGR_CONV_DT",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
            "cmc_grgr_group_history": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_STS",
                    "GRGR_ADDR1",
                    "GRGR_ADDR2",
                    "GRGR_ADDR3",
                    "GRGR_CITY",
                    "GRGR_STATE",
                    "GRGR_COUNTY",
                    "GRGR_ZIP",
                    "GRGR_CTRY_CD",
                    "GRGR_EMAIL",
                    "GRGR_PHONE",
                    "GRGR_PHONE_EXT",
                    "GRGR_FAX",
                    "GRGR_FAX_EXT",
                    "GRGR_TERM_DT",
                    "GRGR_MCTR_TRSN",
                    "GRGR_RNST_DT",
                    "GRGR_RENEW_MMDD",
                    "GRGR_ERIS_MMDD",
                    "CICI_ID",
                    "GRGR_EIN",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
            "cmc_grgc_group_count": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGC_MCTR_CTYP",
                    "GRGC_EFF_DT",
                    "GRGC_TERM_DT",
                    "GRGC_COUNT",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
        },
        "cdc": {
            "cmc_grgr_group": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_NAME",
                    "GRGR_MCTR_TYPE",
                    "GRGR_ITS_CODE",
                    "GRGR_ORIG_EFF_DT",
                    "GRGR_CONV_DT",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
            "cmc_grgr_group_history": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_STS",
                    "GRGR_ADDR1",
                    "GRGR_ADDR2",
                    "GRGR_ADDR3",
                    "GRGR_CITY",
                    "GRGR_STATE",
                    "GRGR_COUNTY",
                    "GRGR_ZIP",
                    "GRGR_CTRY_CD",
                    "GRGR_EMAIL",
                    "GRGR_PHONE",
                    "GRGR_PHONE_EXT",
                    "GRGR_FAX",
                    "GRGR_FAX_EXT",
                    "GRGR_TERM_DT",
                    "GRGR_MCTR_TRSN",
                    "GRGR_RNST_DT",
                    "GRGR_RENEW_MMDD",
                    "GRGR_ERIS_MMDD",
                    "CICI_ID",
                    "GRGR_EIN",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
        },
        "dbo": {
            "cmc_grgc_group_count": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGC_MCTR_CTYP",
                    "GRGC_EFF_DT",
                    "GRGC_TERM_DT",
                    "GRGC_COUNT",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
            "cmc_grgr_group": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_NAME",
                    "GRGR_MCTR_TYPE",
                    "GRGR_ITS_CODE",
                    "GRGR_ORIG_EFF_DT",
                    "GRGR_CONV_DT",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            },
        },
        "audit": {
            "cmc_grgr_group_history": {
                "required_columns": [
                    "GRGR_CK",
                    "GRGR_ID",
                    "GRGR_STS",
                    "GRGR_ADDR1",
                    "GRGR_ADDR2",
                    "GRGR_ADDR3",
                    "GRGR_CITY",
                    "GRGR_STATE",
                    "GRGR_COUNTY",
                    "GRGR_ZIP",
                    "GRGR_CTRY_CD",
                    "GRGR_EMAIL",
                    "GRGR_PHONE",
                    "GRGR_PHONE_EXT",
                    "GRGR_FAX",
                    "GRGR_FAX_EXT",
                    "GRGR_TERM_DT",
                    "GRGR_MCTR_TRSN",
                    "GRGR_RNST_DT",
                    "GRGR_RENEW_MMDD",
                    "GRGR_ERIS_MMDD",
                    "CICI_ID",
                    "GRGR_EIN",
                    "HIST_CREATE_DTM",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "__$operation"
                ],
                "additional_columns": {},
            }
        },
    },
    "VHP": {
        "N/A": {
            "bcbsla_ma_group": {
                "required_columns": [
                    "Group ID",
                    "Group Name",
                    "Group Type Code",
                    "Group Original Effective Date",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "Operation Code"
                ],
                "additional_columns": {},
            },
            "bcbsla_ma_group_history": {
                "required_columns": [
                    "Group ID",
                    "Group Termination Date",
                    "Group Address Line 1",
                    "Group Address Line 2",
                    "Group Address Line 3",
                    "Group Address City",
                    "Group Address State",
                    "Group Address Zip Code",
                    "Group Address Country",
                    "Group Email",
                    "Group Phone Number",
                    "Group Phone Ext",
                    "Group Fax Number",
                    "Group Fax Ext",
                    "Group Renewal Month",
                    "Group EIN",
                    "loadtimestamp",
                    "source",
                    "source_schema",
                    "business_date",
                    "Operation Code"
                ],
                "additional_columns": {},
            },
        },
    },
}


def get_bronze_table_to_column_map ():
    return bronze_table_to_column_mapping


# Example usage
# try:
#     req_cols = get_bronze_table_columns("GLOBALS", "all", "all", "global_columns")
#     print("Required Columns:", req_cols)
# except KeyError as e:
#     print(e)

def source_to_target_mapping(p_source_system, p_source_df, p_target_table):
    """
    Maps the source DataFrame based on the target entity schema. This will create the final DF_Groups tables.

    Parameters:
    p_source (str): The source name (e.g., 'DF_GROUP_FACETS').
    p_source_df (DataFrame): The source DataFrame to be transformed.
    p_target_table (str): The target entity name. Possible values are "DF_Group_Count", "DF_Group_History", "DF_Group".

    Returns:
    DataFrame: The transformed DataFrame with columns renamed and additional columns added as per the target schema.
    """

    if (
        p_source_system.lower() == "facets".lower()
        and p_target_table.lower() == "DF_Group_Count".lower()
    ):
        # Select and rename the required columns as per target schema
        df_rename = (
            p_source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumn(
                "Group_Source_Key", F.col("Group_Source_Key").cast(T.StringType())
            )
            .withColumn("Source_System_Code", F.lit("Facets"))
            .withColumnRenamed("GRGC_MCTR_CTYP", "Count_Type")
            .withColumnRenamed("GRGC_EFF_DT", "Row_Effective_Date")
            .withColumnRenamed("GRGC_TERM_DT", "Row_Term_Date")
            .withColumnRenamed("GRGC_COUNT", "Employee_Count")
            .withColumn("Row_Change_Datetime", F.to_timestamp(F.col("business_date")))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
            # Remove it in scd
            .drop(
                "__$operation",
                "loadtimestamp",
                "source",
                "source_schema",
                # "processing_time",
            )
        )
    elif (
        p_source_system.lower() == "facets".lower()
        and p_target_table.lower() == "DF_Group_History".lower()
    ):
        # Rename the columns as per target schema
        df_rename = (
            p_source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumn(
                "Group_Source_Key", F.col("Group_Source_Key").cast(T.StringType())
            )
            .withColumn("Source_System_Code", F.lit("Facets"))
            .withColumnRenamed("GRGR_ID", "Group_Id")
            .withColumnRenamed("GRGR_STS", "Status_Code")
            .withColumnRenamed("GRGR_ADDR1", "Address_Line_1")
            .withColumnRenamed("GRGR_ADDR2", "Address_Line_2")
            .withColumnRenamed("GRGR_ADDR3", "Address_Line_3")
            .withColumnRenamed("GRGR_CITY", "Address_City")
            .withColumnRenamed("GRGR_STATE", "Address_State")
            .withColumnRenamed("GRGR_COUNTY", "Address_County")
            .withColumnRenamed("GRGR_ZIP", "Zip_Code")
            .withColumnRenamed("GRGR_CTRY_CD", "Country")
            .withColumnRenamed("GRGR_EMAIL", "Email")
            .withColumnRenamed("GRGR_PHONE", "Phone_Nbr")
            .withColumnRenamed("GRGR_PHONE_EXT", "Phone_Ext")
            .withColumnRenamed("GRGR_FAX", "Fax_Nbr")
            .withColumnRenamed("GRGR_FAX_EXT", "Fax_Ext")
            .withColumnRenamed("GRGR_TERM_DT", "Termination_Date")
            .withColumn("Termination_Date", F.to_date(F.col("Termination_Date")))
            .withColumnRenamed("GRGR_MCTR_TRSN", "Term_Reason_Code")
            .withColumnRenamed("GRGR_RNST_DT", "Reinstatement_Date")
            .withColumn("Reinstatement_Date", F.to_date(F.col("Reinstatement_Date")))
            .withColumnRenamed("GRGR_RENEW_MMDD", "Renewal_MMDD")
            .withColumn("Renewal_MMDD", F.col("Renewal_MMDD").cast(T.StringType()))
            .withColumnRenamed("GRGR_ERIS_MMDD", "ERISA_Plan_Begin_MMDD")
            .withColumn(
                "ERISA_Plan_Begin_MMDD",
                F.col("ERISA_Plan_Begin_MMDD").cast(T.StringType()),
            )
            .withColumnRenamed("CICI_ID", "Client_Code")
            .withColumnRenamed("GRGR_EIN", "EIN")
            .withColumn("Row_Change_Datetime", F.to_timestamp(F.col("business_date")))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
            # .withColumnRenamed("__START_AT", "Row_Effective_Date")
            # .withColumn("Row_Effective_Date", F.to_date(F.col("Row_Effective_Date")))
            # .withColumnRenamed("__END_AT", "Row_Term_Date")
            # .withColumn("Row_Term_Date", F.to_date(F.col("Row_Term_Date")))
            # Remove it in scd
            .drop(
                "__$operation",
                "loadtimestamp",
                "source",
                "source_schema",
                "HIST_CREATE_DTM",
                # "processing_time",
            )
        )
    elif (
        p_source_system.lower() == "facets".lower()
        and p_target_table.lower() == "DF_Group".lower()
    ):
        # Rename the columns as per target schema
        df_rename = (
            p_source_df.withColumnRenamed("GRGR_CK", "Group_Source_Key")
            .withColumn(
                "Group_Source_Key", F.col("Group_Source_Key").cast(T.StringType())
            )
            .withColumn("Source_System_Code", F.lit("Facets"))
            .withColumnRenamed("GRGR_ID", "Group_Id")
            .withColumnRenamed("GRGR_NAME", "Group_Name")
            .withColumnRenamed("GRGR_MCTR_TYPE", "Group_Type_Code")
            .withColumnRenamed("GRGR_ITS_CODE", "ITS_Code")
            .withColumnRenamed("GRGR_ORIG_EFF_DT", "Original_Effective_Date")
            .withColumn(
                "Original_Effective_Date",
                F.col("Original_Effective_Date").cast(T.DateType()),
            )
            .withColumnRenamed("GRGR_CONV_DT", "Group_Facets_Conversion_Date")
            .withColumn(
                "Group_Facets_Conversion_Date",
                F.col("Group_Facets_Conversion_Date").cast(T.DateType()),
            )
            .withColumn("Row_Change_Datetime", F.to_timestamp(F.col("business_date")))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("__$operation") == 1, "Y").otherwise("N"),
            )
            # Remove it in scd
            .drop(
                "__$operation",
                "loadtimestamp",
                "source",
                "source_schema",
                # "processing_time",
            )
        )
    elif (
        p_source_system.lower() == "vhp".lower()
        and p_target_table.lower() == "DF_Group_History".lower()
    ):
        # Rename the columns as per target schema
        df_rename = (
            p_source_df.withColumnRenamed("Group_ID", "Group_Id")
            .withColumn("Group_Id", F.col("Group_Id").cast(T.StringType()))
            .withColumn("Group_Source_Key", F.col("Group_Id"))
            .withColumn("Source_System_Code", F.lit("VHP"))
            # If Group_Termination_Date is 12/31/9999 or null or year Group_Termination_Date = 2078 then 'AC' else 'TM'
            .withColumn("Status_Code", F.lit(""))
            .withColumnRenamed("Group_Address_Line_1", "Address_Line_1")
            .withColumnRenamed("Group_Address_Line_2", "Address_Line_2")
            .withColumnRenamed("Group_Address_Line_3", "Address_Line_3")
            .withColumnRenamed("Group_Address_City", "Address_City")
            .withColumn("Address_County", F.lit(""))
            .withColumnRenamed("Group_Address_State", "Address_State")
            .withColumnRenamed("Group_Address_Zip_Code", "Zip_Code")
            .withColumnRenamed("Group_Address_Country", "Country")
            .withColumnRenamed("Group_Email", "Email")
            .withColumnRenamed("Group_Phone_Number", "Phone_Nbr")
            .withColumnRenamed("Group_Phone_Ext", "Phone_Ext")
            .withColumnRenamed("Group_Fax_Number", "Fax_Nbr")
            .withColumnRenamed("Group_Fax_Ext", "Fax_Ext")
            .withColumn("Term_Reason_Code", F.lit(""))
            .withColumnRenamed("Group_Termination_Date", "Termination_Date")
            .withColumn(
                "Termination_Date",
                F.when(
                    F.col("Termination_Date").cast("string").substr(1, 4) == "2078",
                    "9999-12-31",
                ).otherwise(
                    F.to_date(F.col("Termination_Date").cast("string"), "yyyyMMdd")
                ),
            )
            .withColumn("Termination_Date", F.to_date(F.col("Termination_Date")))
            .withColumn("Reinstatement_Date", F.to_date(F.lit("9999-12-31")))
            .withColumnRenamed("Group_Renewal_Month", "Renewal_MMDD")
            .withColumn("Renewal_MMDD", F.col("Renewal_MMDD").cast(T.StringType()))
            .withColumn("ERISA_Plan_Begin_MMDD", F.lit("0000"))
            .withColumn("Client_Code", F.lit(""))
            .withColumnRenamed("Group_EIN", "EIN")
            .withColumn("Row_Change_Datetime", F.to_timestamp(F.col("business_date")))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("Operation_Code") == "D", "Y").otherwise("N"),
            )
            # .withColumnRenamed("__START_AT", "Row_Effective_Date")
            # .withColumn("Row_Effective_Date", F.to_date(F.col("Row_Effective_Date")))
            # .withColumnRenamed("__END_AT", "Row_Term_Date")
            # .withColumn("Row_Term_Date", F.to_date(F.col("Row_Term_Date")))
            .drop(
                "Operation_Code",
                "loadtimestamp",
                "source",
                "source_schema",
                # "processing_time",
            )
        )
    elif (
        p_source_system.lower() == "vhp".lower()
        and p_target_table.lower() == "DF_Group".lower()
    ):
        # Rename the columns as per target schema
        df_rename = (
            p_source_df.withColumnRenamed("Group_ID", "Group_Id")
            .withColumn("Group_Id", F.col("Group_Id").cast(T.StringType()))
            .withColumn("Group_Source_Key", F.col("Group_Id"))
            .withColumn("Source_System_Code", F.lit("VHP"))
            .withColumnRenamed("Group_Name", "Group_Name")
            .withColumnRenamed("Group_Type_Code", "Group_Type_Code")
            .withColumn("ITS_Code", F.lit(""))
            .withColumnRenamed(
                "Group_Original_Effective_Date", "Original_Effective_Date"
            )
            .withColumn(
                "Original_Effective_Date",
                F.col("Original_Effective_Date").cast(T.DateType()),
            )
            .withColumn("Group_Facets_Conversion_Date", F.lit("9999-12-31"))
            .withColumn(
                "Group_Facets_Conversion_Date",
                F.col("Group_Facets_Conversion_Date").cast(T.DateType()),
            )
            .withColumn("Row_Change_Datetime", F.to_timestamp(F.col("business_date")))
            .withColumn(
                "Row_Is_Deleted_Ind",
                F.when(F.col("Operation_Code") == "D", "Y").otherwise("N"),
            )
            .drop(
                "Operation_Code",
                "loadtimestamp",
                "source",
                "source_schema",
                # "processing_time",
            )
        )

    return df_rename