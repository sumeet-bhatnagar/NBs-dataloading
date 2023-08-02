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

account_name = 'sav1sumeetb'
container_name = 'sagen1container'
access_key = '<access_key>'
source_url = f'wasbs://{container_name}@{account_name}.blob.core.windows.net'
mount_point_url = '/mnt/gen1dataset'
extra_key = f'fs.azure.account.key.{account_name}.blob.core.windows.net'
extra_val = access_key
extra_configs_dict = {extra_key : extra_val}

print(extra_configs_dict)

# COMMAND ----------

dbutils.fs.mount(source=source_url,mount_point=mount_point_url,extra_configs=extra_configs_dict)

# COMMAND ----------

dbutils.fs.ls(mount_point_url)

# COMMAND ----------

# version 2 mounting

account_name_2 = 'sav2sumeetb'
container_name_2 = 'sagen2container'

source_url = f'abfss://{container_name_2}@{account_name_2}.dfs.core.windows.net/input'

spark.conf.set(f'fs.azure.account.auth.type.{account_name_2}.dfs.core.windows.net','SAS')
spark.conf.set(f'fs.azure.sas.token.provider.type.{account_name_2}.dfs.core.windows.net','org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider')
spark.conf.set(f'fs.azure.sas.fixed.token.{account_name_2}.dfs.core.windows.net','<sas_token>')

df = spark.read.csv(f'abfs://{container_name_2}@{account_name_2}.dfs.core.windows.net/input/iris.csv')


# COMMAND ----------

df.head(10)

# COMMAND ----------


