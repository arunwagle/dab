# Databricks notebook source
# MAGIC %md
# MAGIC # Complete the workflow run 
# MAGIC

# COMMAND ----------


source = dbutils.jobs.taskValues.get(taskKey = "start_workflow", key = "source", debugValue = "")
run_as_username = dbutils.jobs.taskValues.get(taskKey = "start_workflow", key = "run_as", debugValue = "")
business_start_date = dbutils.jobs.taskValues.get(taskKey = "start_workflow", key = "business_start_date", debugValue = "")
job_id = dbutils.jobs.taskValues.get(taskKey = "start_workflow", key = "job_id", debugValue = "")
run_id = dbutils.jobs.taskValues.get(taskKey = "start_workflow", key = "run_id", debugValue = "")


# COMMAND ----------

# DBTITLE 1,Mark the workflow completed
query = f"""
    Update {catalog}.{schema}.t_dlt_workflow_runtime_audit 
    SET status = 'COMPLETE'
    WHERE 
    source = '{source}' AND
    run_as = '{run_as_username}' AND
    business_start_date = to_date('{business_start_date}', 'yyyyMMdd') AND
    job_id = '{job_id}' AND
    job_run_id = '{run_id}'
"""
spark.sql(query)