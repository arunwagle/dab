# Databricks notebook source
# MAGIC %md
# MAGIC ## Control-M to pass these values to the Databricks workflow

# COMMAND ----------

current_dir = "/Volumes/edl_dev_ctlg/rawfiles/raw/dlt_pilot"
sys.path.append(current_dir)

# COMMAND ----------

from utils import get_run_as_username

# COMMAND ----------

# E.g. DF_GROUP_FACETS, DF_GROUP_VHP
source = dbutils.widgets.getArgument("source")
catalog = dbutils.widgets.getArgument("catalog")
schema_name = dbutils.widgets.getArgument("schema_name")
business_start_date = dbutils.widgets.getArgument("business_start_date")
input_table = dbutils.widgets.getArgument("input_table")
job_id = dbutils.widgets.getArgument("job_id")
run_id = dbutils.widgets.getArgument("job_run_id")

run_as_username = get_run_as_username(dbutils)

print (f"source::{source}, business_start_date::{business_start_date}, input_table::{input_table}, job_id::{job_id}, run_id::{run_id}", "run_as_username::{run_as_username}")



# COMMAND ----------

# MAGIC %md
# MAGIC ### Set these values in the task variables for accessing in other wprkflow steps

# COMMAND ----------

dbutils.jobs.taskValues.set("source", source)
dbutils.jobs.taskValues.set("catalog", catalog)
dbutils.jobs.taskValues.set("schema_name", schema_name)
dbutils.jobs.taskValues.set("business_start_date", business_start_date)
dbutils.jobs.taskValues.set("input_table", input_table)
dbutils.jobs.taskValues.set("job_id", job_id)
dbutils.jobs.taskValues.set("run_id", run_id)
dbutils.jobs.taskValues.set("run_as_username", run_as_username)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Create an entry in the  audit_workflow_runtime table.
# MAGIC
# MAGIC This table is required to make sure that at any one instance only 1 DLT pipeline is running for a particular source and run date. 

# COMMAND ----------

from pyspark.sql.functions import to_timestamp
from datetime import datetime

query = f"""
SELECT count(*)  from  {catalog}.{schema_name}.t_dlt_workflow_runtime_audit 
where source = '{source}' 
and to_date(business_start_date, 'yyyyMMdd') = to_date('{business_start_date}', 'yyyyMMdd') 
and run_as = '{run_as_username}' 
and status = 'IN_PROGRESS'
"""

print(f"select query::{query}")

record_count = spark.sql(query).first()[0]
print(f"record_count::{record_count}")

if record_count > 0:
    raise Exception(
        f"A workflow run is already in progress for input_source = {source} and run_date = {business_start_date}. We cannot have multiple runs for the source and a run_date as there we have a DLT pipeline as a part of this workflow."
    )
else:
    insert_query = f"""
        Insert into {catalog}.{schema_name}.t_dlt_workflow_runtime_audit 
        (source, run_as, catalog, schema_name, input_table, business_start_date, status, job_id, run_id) 
        values ('{source}' , '{run_as_username}', '{catalog}', '{schema_name}', '{input_table}', to_date('{business_start_date}', 'yyyyMMdd'),  'IN_PROGRESS', '{job_id}', '{run_id}')
    """
    print(f"insert_query::{insert_query}")

    spark.sql(insert_query)