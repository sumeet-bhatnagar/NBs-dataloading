# Databricks notebook source
# MAGIC %sql
# MAGIC select 'hello world!!'

# COMMAND ----------

df_iris_table = spark.read.format('sqlserver') \
.option('host','dbserversumeetb.database.windows.net') \
.option('port','1433') \
.option('user','sumeet') \
.option('password','master@123') \
.option('database','db') \
.option('dbtable','iris_data') \
.load() 

# COMMAND ----------

df_iris_table.head()

# COMMAND ----------


