# Databricks notebook source
# MAGIC %md
# MAGIC ### SCD TYPE-1

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.default.scdtyp1_source
# MAGIC (
# MAGIC   prod_id INT,
# MAGIC   prod_name STRING,
# MAGIC   prod_cat STRING,
# MAGIC   processDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO datamodeling.default.scdtyp1_source
# MAGIC VALUES
# MAGIC (1,'prod1','cat1',CURRENT_DATE()),
# MAGIC (2,'prod2','cat2',CURRENT_DATE()),
# MAGIC (3,'prod3','cat3',CURRENT_DATE())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Here we have created a source table and now to demonstrate scd type1 we need to change the dimension. we have to apply the merge command and for that we need a target table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.gold.scdtyp1_table
# MAGIC (
# MAGIC   prod_id INT,
# MAGIC   prod_name STRING,
# MAGIC   prod_cat STRING,
# MAGIC   processDate DATE
# MAGIC )

# COMMAND ----------

spark.sql("select * from datamodeling.default.scdtyp1_source").createOrReplaceTempView("src")
#Here we are just creating a source and view on top of it

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtyp1_table AS trg
# MAGIC USING src
# MAGIC ON src.prod_id = trg.prod_id
# MAGIC WHEN MATCHED AND src.processDate >= trg.processDate THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM datamodeling.gold.scdtyp1_table

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE datamodeling.default.scdtyp1_source SET prod_cat = 'newcategory'
# MAGIC WHERE prod_id = 3
# MAGIC --here we are changing the existing dimesnion and then run the source and merge again to see the cahnged dimension

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC # **SCD TYPE 2**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Source Creation**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.default.scdtyp2_source
# MAGIC (
# MAGIC   prod_id INT,
# MAGIC   prod_name STRING,
# MAGIC   prod_cat STRING,
# MAGIC   processDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO datamodeling.default.scdtyp2_source
# MAGIC VALUES
# MAGIC (1,'prod1','cat1',CURRENT_DATE()),
# MAGIC (2,'prod2','cat2',CURRENT_DATE()),
# MAGIC (3,'prod3','cat3',CURRENT_DATE())

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Target Table**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE datamodeling.gold.scdtype2_table
# MAGIC (
# MAGIC   prod_id INT,
# MAGIC   prod_name STRING,
# MAGIC   prod_cat STRING,
# MAGIC   processDate DATE,
# MAGIC   start_date DATE,
# MAGIC   end_date DATE,
# MAGIC   is_current STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC **Adding additional 3 columns for SCD Type - 2**

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.default.scdtyp2_source

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *,
# MAGIC         current_timestamp as start_date,
# MAGIC         CAST('3000-01-01' AS TIMESTAMP) as end_date,
# MAGIC         'Y' as is_current
# MAGIC FROM datamodeling.default.scdtyp2_source

# COMMAND ----------

#Now let's create a view for the target
spark.sql("""SELECT *,
        current_timestamp as start_date,
        CAST('3000-01-01' AS TIMESTAMP) as end_date,
        'Y' as is_current
FROM datamodeling.default.scdtyp2_source""").createOrReplaceTempView("src")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **SCD TYPE 2 MEGE CONDISTIONS**
# MAGIC
# MAGIC **MERGE-1** : This command will check if we have any data in the target table that is updated in the source, and will mark it as expired. 

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype2_table AS trg
# MAGIC USING src 
# MAGIC ON src.prod_id = trg.prod_id
# MAGIC AND trg.is_current = 'Y'
# MAGIC
# MAGIC -- When We have New Data With Updates
# MAGIC WHEN MATCHED AND (
# MAGIC   src.prod_cat <> trg.prod_cat OR
# MAGIC   src.processDate <> trg.processDate OR
# MAGIC   src.prod_name <> trg.prod_name
# MAGIC ) THEN 
# MAGIC   UPDATE SET 
# MAGIC     trg.end_date = current_timestamp(),
# MAGIC     trg.is_current = 'N'

# COMMAND ----------

# MAGIC %md
# MAGIC **MERGE-2** : This command will bring all the non-expired commands bcz we have filter of "is_current = 'Y'". So, this will not bring the updated records as well bcz previous MERGE command marked it as expired. So all the new records [including updated] will be inserted in this MERGE.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO datamodeling.gold.scdtype2_table AS trg
# MAGIC USING src 
# MAGIC ON trg.prod_id = src.prod_id 
# MAGIC AND trg.is_current = 'Y' 
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT 
# MAGIC (
# MAGIC   prod_id,
# MAGIC   prod_name,
# MAGIC   prod_cat,
# MAGIC   processDate,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   is_current
# MAGIC ) VALUES (
# MAGIC   src.prod_id,
# MAGIC   src.prod_name,
# MAGIC   src.prod_cat,
# MAGIC   src.processDate,
# MAGIC   src.start_date,
# MAGIC   src.end_date,
# MAGIC   src.is_current
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.gold.scdtype2_table

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now let's update the data in the source and run the source and merge again to see 
# MAGIC update datamodeling.default.scdtyp2_source
# MAGIC set prod_cat = 'newcategory'
# MAGIC where prod_id = 3