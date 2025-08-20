# import required modules
import dlt
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, sum as spark_sum, monotonically_increasing_id, min as spark_min, max as spark_max, expr, lit, row_number, lead


def run_gold_dlt(p_spark, p_catalog, p_structured_schema, p_curated_schema, p_table_suffix):

    #static variables
    dlt_df_group="dlt_df_group"
    dlt_df_group_count="dlt_df_group_count"
    dlt_df_group_history="dlt_df_group_history"
    dt_d_group = f"dt_d_group{p_table_suffix}"
    inc_d_group = f"inc_d_group{p_table_suffix}"
    stg_d_group = f"stg_d_group{p_table_suffix}"
    d_group = f"d_group{p_table_suffix}"
    # Step 1: Define a table that holds all the distinct groups with multiple row start and row end
    @dlt.table(
        comment="create table with distinct groups along with start and end dates",
        name=f"{p_curated_schema}.{dt_d_group}",
    )
    def dt_d_group_fn():
        # Create a CTE to capture distinct Group_Source_Key from multiple tables
        cteChangedGroups = (
            p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group}")
            .select("Group_source_Key")
            .distinct()
            .union(
                p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_count}")
                .select("Group_source_Key")
                .distinct()
            )
            .union(
                p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_history}")
                .select("Group_source_Key")
                .distinct()
            )
        )

        # Select relevant columns and filter data from df_group_count_int_1
        df1 = (
            p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_count}")
            .join(cteChangedGroups, "Group_source_Key")
            .filter(col("Employee_Count") != 0)
            .select(
                col("Group_Source_Key"),
                col("Row_Effective_Date").cast("date").alias("RowDate"),
            )
        )

        # Select relevant columns and filter data from df_group_count_int_1
        df2 = (
            p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_count}")
            .join(cteChangedGroups, "Group_source_Key")
            .filter(col("Employee_Count") != 0)
            .select(
                col("Group_Source_Key"),
                col("Row_Term_Date").cast("date").alias("RowDate"),
            )
        )

        # Select relevant columns from df_group_history_int_1
        df3 = (
            p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_history}")
            .join(cteChangedGroups, "Group_source_Key")
            .select(
                col("Group_Source_Key"),
                col("Row_Effective_Date").cast("date").alias("RowDate"),
            )
        )

        # Select relevant columns from df_group_history_int_1
        df4 = (
            p_spark.table(f"{p_catalog}.{p_structured_schema}.{dlt_df_group_history}")
            .join(cteChangedGroups, "Group_source_Key")
            .select(
                col("Group_Source_Key"),
                col("Row_Term_Date").cast("date").alias("RowDate"),
            )
        )

        # Union all dataframes and drop duplicates
        vt_changed_groups = (
            df1.union(df2)
            .union(df3)
            .union(df4)
            .dropDuplicates(["Group_Source_Key", "RowDate"])
        )
        # Define a window specification for calculating end dates
        window_spec = (
            Window.partitionBy("Group_Source_Key").orderBy("RowDate").rowsBetween(1, 1)
        )

        # Select and calculate start and end dates
        df_f = vt_changed_groups.select(
            col("Group_Source_Key"),
            col("RowDate").alias("Row_Start_Date"),
            spark_max("RowDate").over(window_spec).alias("Row_End_Date"),
        ).filter(col("Row_End_Date").isNotNull())

        return df_f

    # Step 2: Define a inc table to integrate all the three silver tables and apply transformations for groups
    @dlt.table(
        comment="create a inc table to calculate transformations for groups",
        name=f"{p_catalog}.{p_curated_schema}.{inc_d_group}",
    )
    def inc_d_group_fn():
        # dt_d_group_df = dlt.read(f"{p_catalog}.{p_curated_schema}.{dt_d_group}")

        # SQL query to join and select relevant columns from multiple tables
        return p_spark.sql(
            """
            SELECT 
                Group_Source_Key, Source_System_Code, Row_Effective_Date, Row_Term_Date, Group_Id, Group_Name, Group_Type_Code, ITS_Code, 
                Original_Effective_Date, Group_Facets_Conversion_Date, Status_Code, Address_Line_1, Address_Line_2, Address_Line_3, 
                Address_City, Address_State, Address_County, Zip_Code, Country, Email, Phone_Nbr, Phone_Ext, Fax_Nbr, Fax_Ext, 
                Termination_Date, Term_Reason_Code, Reinstatement_Date, Renewal_MMDD, ERISA_Plan_Begin_MMDD, Client_Code, EIN, 
                Employee_Count, Count_as_of_Year, Count_Type
            FROM (
                SELECT DISTINCT 
                    grp.Group_Source_Key, 'Facets' as Source_System_Code, dt_d.Row_Start_Date AS Row_Effective_Date, dt_d.Row_End_Date AS Row_Term_Date, 
                    grp.Group_Id, grp.Group_Name, grp.Group_Type_Code, grp.ITS_Code, grp.Original_Effective_Date, grp.Group_Facets_Conversion_Date, 
                    grp_hist.Status_Code, grp_hist.Address_Line_1, grp_hist.Address_Line_2, grp_hist.Address_Line_3, grp_hist.Address_City, 
                    grp_hist.Address_State, grp_hist.Address_County, grp_hist.Zip_Code, grp_hist.Address_County as Country, grp_hist.Email, grp_hist.Phone_Nbr, 
                    grp_hist.Phone_Ext, grp_hist.Fax_Nbr, grp_hist.Fax_Ext, grp_hist.Termination_Date, grp_hist.Term_Reason_Code, grp_hist.Reinstatement_Date, 
                    grp_hist.Renewal_MMDD, grp_hist.ERISA_Plan_Begin_MMDD, grp_hist.Client_Code, grp_hist.EIN, cnt.Employee_Count, 
                    EXTRACT(YEAR FROM dt_d.Row_Start_Date) AS Count_as_of_Year, 
                    CASE 
                        WHEN Count_Type = 'MLRR' THEN 'Group Response'
                        WHEN Count_Type = 'MLRP' THEN 'Previous Group Response'
                        WHEN Count_Type = 'MLRD' THEN 'Default'
                        ELSE 'Not Applicable'
                    END AS Count_Type
                FROM {1}.{5} dt_d 
                JOIN {0}.{2} grp ON dt_d.Group_Source_Key = grp.Group_Source_Key
                JOIN {0}.{3} grp_hist ON dt_d.Group_Source_Key = grp_hist.Group_Source_Key
                    AND dt_d.Row_Start_Date >= cast(grp_hist.Row_Effective_Date as date)
                    AND dt_d.Row_End_Date <= cast(grp_hist.Row_Term_Date as date)
                JOIN {0}.{4} cnt ON cnt.Group_Source_Key = dt_d.Group_Source_Key
                    AND dt_d.Row_Start_Date >= cast(cnt.Row_Effective_Date as date)
                    AND dt_d.Row_End_Date <= cast(cnt.Row_Term_Date as date) 
                    and cnt.Employee_Count<>0 --we can remove this once we apply quarantine rule on source table
            ) 
            """.format(
                p_structured_schema,
                p_curated_schema,
                dlt_df_group,
                dlt_df_group_history,
                dlt_df_group_count,
                dt_d_group,
            )
        )

    # Step 3: Define a stg table to collapse date windows for the continuous date windows for the same Group_Source_Key that had the exact same values except Row_Start_Date and Row_End_Date
    @dlt.table(
        comment="create a stg table to collapse date windows",
        name=f"{p_catalog}.{p_curated_schema}.{stg_d_group}",
    )
    def stg_d_group_fn():
        inc_d_group_df = dlt.read(f"{p_catalog}.{p_curated_schema}.{inc_d_group}")

        # Define a window specification for ordering data by Row_Effective_Date
        window_spec = Window.partitionBy(
            "Group_Source_Key",
            "Source_System_Code",
            "Group_Id",
            "Group_Name",
            "Group_Type_Code",
            "ITS_Code",
            "Original_Effective_Date",
            "Group_Facets_Conversion_Date",
            "Status_Code",
            "Address_Line_1",
            "Address_Line_2",
            "Address_Line_3",
            "Address_City",
            "Address_State",
            "Address_County",
            "Zip_Code",
            "Country",
            "Email",
            "Phone_Nbr",
            "Phone_Ext",
            "Fax_Nbr",
            "Fax_Ext",
            "Termination_Date",
            "Term_Reason_Code",
            "Reinstatement_Date",
            "Renewal_MMDD",
            "ERISA_Plan_Begin_MMDD",
            "Client_Code",
            "EIN",
            "Employee_Count",
            "Count_as_of_Year",
            "Count_Type",
        ).orderBy("Row_Effective_Date")

        # Add columns for previous end date and flag new groups
        flagged_data = inc_d_group_df.withColumn(
            "prev_end", lag("Row_Term_Date").over(window_spec)
        ).withColumn(
            "is_new_group",
            expr(
                "CASE WHEN prev_end IS NULL OR Row_Effective_Date > DATE_ADD(prev_end, 1) THEN 1 ELSE 0 END"
            ),
        )

        # Group data based on new group flag and calculate effective and termination dates
        merged_data = (
            flagged_data.withColumn(
                "new_group",
                spark_sum("is_new_group").over(
                    Window.partitionBy("Group_Source_Key")
                    .orderBy("Row_Effective_Date")
                    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
                ),
            )
            .groupBy(
                "Group_Source_Key",
                "Source_System_Code",
                "Group_Id",
                "Group_Name",
                "Group_Type_Code",
                "ITS_Code",
                "Original_Effective_Date",
                "Group_Facets_Conversion_Date",
                "Status_Code",
                "Address_Line_1",
                "Address_Line_2",
                "Address_Line_3",
                "Address_City",
                "Address_State",
                "Address_County",
                "Zip_Code",
                "Country",
                "Email",
                "Phone_Nbr",
                "Phone_Ext",
                "Fax_Nbr",
                "Fax_Ext",
                "Termination_Date",
                "Term_Reason_Code",
                "Reinstatement_Date",
                "Renewal_MMDD",
                "ERISA_Plan_Begin_MMDD",
                "Client_Code",
                "EIN",
                "Employee_Count",
                "Count_as_of_Year",
                "Count_Type",
                "new_group",
            )
            .agg(
                spark_min("Row_Effective_Date").alias("Row_Effective_Date"),
                spark_max("Row_Term_Date").alias("Row_Term_Date"),
            )
        )

        # Add columns for next start date and adjusted end date
        final_data = (
            merged_data.withColumn(
                "next_start_date",
                lead("Row_Effective_Date").over(
                    Window.partitionBy("Group_Source_Key").orderBy("Row_Effective_Date")
                ),
            )
            .withColumn(
                "adjusted_end_date",
                expr(
                    "CASE WHEN next_start_date IS NOT NULL AND Row_Term_Date >= next_start_date THEN DATE_SUB(next_start_date, 1) ELSE Row_Term_Date END"
                ),
            )
            .select(
                "Group_Source_Key",
                "Source_System_Code",
                "Row_Effective_Date",
                col("adjusted_end_date").alias("Row_Term_Date"),
                "Group_Id",
                "Group_Name",
                "Group_Type_Code",
                "ITS_Code",
                "Original_Effective_Date",
                "Group_Facets_Conversion_Date",
                "Status_Code",
                "Address_Line_1",
                "Address_Line_2",
                "Address_Line_3",
                "Address_City",
                "Address_State",
                "Address_County",
                "Zip_Code",
                "Country",
                "Email",
                "Phone_Nbr",
                "Phone_Ext",
                "Fax_Nbr",
                "Fax_Ext",
                "Termination_Date",
                "Term_Reason_Code",
                "Reinstatement_Date",
                "Renewal_MMDD",
                "ERISA_Plan_Begin_MMDD",
                "Client_Code",
                "EIN",
                "Employee_Count",
                "Count_as_of_Year",
                "Count_Type",
            )
            .filter("Row_Effective_Date <= Row_Term_Date")
        )

        return final_data

    # Step 4: insert full computed data to gold D_Group table
    @dlt.table(
        comment="create a final table with full computed data",
        name=f"{p_catalog}.{p_curated_schema}.{d_group}",
    )
    def d_group():
        return dlt.read(f"{p_catalog}.{p_curated_schema}.{stg_d_group}")
