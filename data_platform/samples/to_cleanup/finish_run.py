# Databricks notebook source
# MAGIC %md
# MAGIC # Complete the workflow run 
# MAGIC

# COMMAND ----------


source = dbutils.jobs.taskValues.get(taskKey = "initialize_run", key = "source", debugValue = "VEEVA")
run_date = dbutils.jobs.taskValues.get(taskKey = "initialize_run", key = "run_date", debugValue = "")
job_id = dbutils.jobs.taskValues.get(taskKey = "initialize_run", key = "job_id", debugValue = "")
run_id = dbutils.jobs.taskValues.get(taskKey = "initialize_run", key = "run_id", debugValue = "")


# COMMAND ----------

# DBTITLE 1,Mark the workflow completed
query = f"""
    Update {catalog}.{schema}.audit_workflow_runtime 
    SET status = 'COMPLETE'
    WHERE 
    source = '{source}' AND
    run_date = '{run_date}' AND
    job_id = '{job_id}' AND
    job_run_id = '{run_id}'
"""
spark.sql(query)