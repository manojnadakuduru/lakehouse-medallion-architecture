# Databricks notebook source
"""last_load_date = '2000-01-01'"""

# COMMAND ----------

if spark.catalog.tableExists("datamodeling.bronze.bronze_table"):
    last_load_date = spark.sql("SELECT max(order_date) FROM datamodeling.bronze.bronze_table").collect()[0][0]
else:
    last_load_date = '1000-01-01'
    """Other than creating a copy of source data, Here we want the incremental load after the last load date"""
    #Once the data is added then if condition gets execute and till then the data is not added the last_load_date is set to 1000-01-01

# COMMAND ----------

last_load_date

# COMMAND ----------

spark.sql(f"""select *from datamodeling.default.source_data
where order_date > '{last_load_date}'""").createOrReplaceTempView("bronze_source")
"""here we use both python and sql because former is used to control the flow of execution and later is useful for processing. 
Spark SQL is used with Python because Python handles orchestration and dynamic logic, while SQL efficiently processes large datasets in a distributed engine.   
 f-strings allow runtime parameters to be injected into SQL, enabling reusable, configurable, and dynamic data pipelines.
 
 At the last added a temporary view on top of the query"""

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from bronze_source
# MAGIC --Initially it shows all the records after incremental loading it will only show new records

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.bronze.bronze_table
# MAGIC AS
# MAGIC SELECT * FROM bronze_source