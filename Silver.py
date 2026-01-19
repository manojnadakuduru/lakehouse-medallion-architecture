# Databricks notebook source
# MAGIC %sql
# MAGIC select * from datamodeling.bronze.bronze_table

# COMMAND ----------

"""Here applying transformations to the bronze table and creating a silver table """
spark.sql("""
SELECT * ,
      upper(customer_name) as Customer_Name_Upper,
      date(current_timestamp()) as processDate
FROM datamodeling.bronze.bronze_table""").createOrReplaceTempView("silver_source")


# COMMAND ----------

# MAGIC %sql
# MAGIC select *from silver_source

# COMMAND ----------

# MAGIC %md
# MAGIC ### **MERGE Using PySpark**

# COMMAND ----------

#Here in silver layer we need to merge the data from bronze layer to silver layer. In order to do that first we need a destination. first let's create a destination table in silver layer.
if spark.catalog.tableExists('datamodeling.silver.silver_table'):
    pass
#Here we have to add the code in order to make it work. For merge we are only using SQL

else:
    spark.sql("""
                CREATE TABLE IF NOT EXISTS datamodeling.silver.silver_table
                AS 
                SELECT * FROM silver_source""")
"""Here we are checking if the silver table exists or not and if not we are creating the silver table"""
#comments

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Merge using SQL**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS datamodeling.silver.silver_table
# MAGIC                 AS 
# MAGIC                 SELECT * FROM silver_source

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.silver.silver_table
# MAGIC USING silver_source
# MAGIC ON datamodeling.silver.silver_table.order_id = silver_source.order_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC --First time it will give same values as before but after that it will give different values as merge command is used once incremental loading is done

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.silver.silver_table
# MAGIC /*we are checking it to see if all the records are there or not. Here there are no
# MAGIC  duplicates since we are using merge command*/
# MAGIC  --Below is the OBT and now we want to break it into multiple tables