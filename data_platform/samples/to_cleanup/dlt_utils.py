# Databricks notebook source
def generate_bronzetables(table, volume_path):  
    @dlt.table(
        name=table,
        comment="BRONZE: {}".format(table)    
    )  
    def create_table(): 
        try:   
            if table == "t_dlt_bronze_cmc_grgr_group":
                return (      
                    spark.readStream.format('cloudFiles')
                    .option('cloudFiles.format', 'parquet')
                    .load(volume_path)                     
                    .withColumn("__$start_lsn", F.col("__$start_lsn").cast("binary"))   
                    .withColumn("__$operation", F.col("__$operation").cast("string"))   
                    .withColumn("__$update_mask", F.col("__$update_mask").cast("string"))                    
                    .withColumn("processing_time", F.current_timestamp())
                )
            else:
                return (      
                    spark.readStream.format('cloudFiles')
                    .option('cloudFiles.format', 'parquet')
                    .load(volume_path)              
                    .withColumn("processing_time", F.current_timestamp())
                )
        except Exception as e:
            # Capture and log any errors
            print(f"Error in create_table: {str(e)}")
            raise  # Re-raise the exception to propagate the error

# COMMAND ----------

def generate_scd_tables(table, key, seq_col="loadtimestamp", scd_type=1):
  try:
    dlt.create_streaming_live_table("{}_scd{}".format(table, scd_type))
    dlt.apply_changes(
      target = "{}_scd{}".format(table, scd_type),
      source = table, 
      keys = [key], 
      sequence_by = col(seq_col),
      stored_as_scd_type = scd_type
    )
  except Exception as e:
            # Capture and log any errors
            print(f"Error in table_func: {str(e)}")
            raise  # Re-raise the exception to propagate the error