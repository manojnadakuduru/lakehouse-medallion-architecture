# Databricks notebook source
# MAGIC %md
# MAGIC ### Dimension Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.silver.silver_table
# MAGIC --We start with creating dimensional table first and then at the end fact table

# COMMAND ----------

# MAGIC %sql
# MAGIC --First let's create a dimensional table on top of the customer related columns and also we need to take care of primary key
# MAGIC select *,row_number() over(order by customer_id) as DimCustomerKey from 
# MAGIC (
# MAGIC select
# MAGIC   distinct customer_id, -- to remove the duplicate customer names because every customer can have multiple orders. Here the focus is on customer table so only one entry for customer.
# MAGIC   --row_number() over(order by customer_id) as DimCustomerKey,
# MAGIC   customer_email,
# MAGIC   customer_name,
# MAGIC   customer_name_upper
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC --Now let's create a fact table on top of the order related column
# MAGIC --We will creata a surrogate key only after removing the duplicates not along with it. Hence we added the first line and commented the inside code

# COMMAND ----------

# MAGIC %md
# MAGIC ##### whenever we create a dimensional table we create surrogate key as well. which gives my primary key column with the serial numbers.if we want to join dimensional table with fact table we use surrogate key instead of primary key.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimenstion table for Customers

# COMMAND ----------

# MAGIC %sql
# MAGIC --creating the same above thing with CTE
# MAGIC with rem_dup as(
# MAGIC select
# MAGIC   distinct customer_id, 
# MAGIC   customer_email,
# MAGIC   customer_name,
# MAGIC   customer_name_upper
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC select *,
# MAGIC row_number() over(order by customer_id) as DimCustomerKey
# MAGIC from rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now let's create a table on top of it
# MAGIC create or replace table datamodeling.gold.DimCustomers
# MAGIC As
# MAGIC with rem_dup as(
# MAGIC select
# MAGIC   distinct customer_id, 
# MAGIC   customer_email,
# MAGIC   customer_name,
# MAGIC   customer_name_upper
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC select *,
# MAGIC row_number() over(order by customer_id) as DimCustomerKey
# MAGIC from rem_dup

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dimension table for Products**

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   distinct product_id,
# MAGIC   product_name,
# MAGIC   product_category
# MAGIC from
# MAGIC   datamodeling.silver.silver_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Now let's create a table on top of it
# MAGIC create or replace table datamodeling.gold.DimProducts
# MAGIC As
# MAGIC with rem_dup as(
# MAGIC select
# MAGIC   distinct product_id,
# MAGIC   product_name,
# MAGIC   product_category
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC select *,
# MAGIC row_number() over(order by Product_id) as DimProductKey
# MAGIC from rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.gold.dimproducts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension table for Payments

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table datamodeling.gold.DimPayments
# MAGIC As
# MAGIC with rem_dup as
# MAGIC (
# MAGIC select 
# MAGIC   distinct payment_type
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC select *, row_number() over (order by payment_type) as DimPaymentKey
# MAGIC from rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.gold.dimpayments

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dim table for Region

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table datamodeling.gold.DimRegions
# MAGIC As
# MAGIC with rem_dup as
# MAGIC (
# MAGIC select 
# MAGIC   distinct country
# MAGIC from
# MAGIC   datamodeling.silver.silver_table
# MAGIC )
# MAGIC select *, row_number() over (order by country) as DimRegionKey
# MAGIC from rem_dup

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.gold.dimregions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension for sales (because the entire theme needs to be captured)

# COMMAND ----------

spark.sql("select * from datamodeling.silver.silver_table").columns
# just to see all the columns

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table datamodeling.gold.DimSales
# MAGIC As
# MAGIC select 
# MAGIC  row_number() over (order by order_id) as DimSaleKey,
# MAGIC  order_id,
# MAGIC  order_date,
# MAGIC  customer_id,
# MAGIC  customer_name,
# MAGIC  customer_email,
# MAGIC  product_id,
# MAGIC  product_name,
# MAGIC  product_category,
# MAGIC  payment_type,
# MAGIC  country,
# MAGIC  last_updated,
# MAGIC  Customer_Name_Upper,
# MAGIC  processDate
# MAGIC from
# MAGIC   datamodeling.silver.silver_table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from datamodeling.gold.dimsales

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Now time for fact table creation using the numeric columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fact Table 
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE datamodeling.gold.FactSales
# MAGIC As
# MAGIC select 
# MAGIC   C.DimCustomerKey,
# MAGIC   P.DimProductKey,
# MAGIC   PY.DimPaymentKey,
# MAGIC   R.DimRegionKey,
# MAGIC   S.DimSaleKey,
# MAGIC   F.quantity,
# MAGIC   F.unit_price
# MAGIC from
# MAGIC   datamodeling.silver.silver_table F 
# MAGIC left join 
# MAGIC   datamodeling.gold.dimcustomers C
# MAGIC on
# MAGIC   F.customer_id = C.customer_id
# MAGIC left join 
# MAGIC   datamodeling.gold.dimproducts P
# MAGIC on
# MAGIC   F.product_id = P.product_id
# MAGIC left join 
# MAGIC   datamodeling.gold.dimpayments PY
# MAGIC on
# MAGIC   F.payment_type = PY.payment_type
# MAGIC left join 
# MAGIC   datamodeling.gold.dimregions R
# MAGIC on
# MAGIC   F.country = R.country
# MAGIC left join 
# MAGIC   datamodeling.gold.dimsales S
# MAGIC on F.order_id = S.order_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select *from datamodeling.gold.factsales